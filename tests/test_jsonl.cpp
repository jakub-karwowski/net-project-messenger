// test_jsonl.cpp - the CLI's output and input plumbing: JSON escaping and ids,
// the event formats, JsonlWriter (blocking, EPIPE, non-blocking try_write) and
// LineReader (line splitting, end of input, stop).
//
// Plain assertions, no framework.

#include "CliFormat.hpp"
#include "Json.hpp"
#include "JsonlWriter.hpp"
#include "LineReader.hpp"

#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <variant>
#include <vector>

namespace {

using namespace std::chrono_literals;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

std::string as_json(std::string_view s) {
    std::string out;
    json::append_string(out, s);
    return out;
}

void test_escaping() {
    check(as_json("plain") == R"("plain")", "plain text");
    check(as_json("a\"b\\c") == R"("a\"b\\c")", "quote and backslash");
    check(as_json("l1\nl2\r\t") == R"("l1\nl2\r\t")", "newline, CR, tab");
    check(as_json("\x1b[2J\x07") == R"("\u001b[2J\u0007")", "ESC and BEL become \\u escapes");
    check(as_json("x\x7f") == R"("x\u007f")", "DEL");
    check(as_json("\xc2\x9b" "31m") == R"("\u009b31m")", "a C1 control (CSI) in UTF-8");
    check(as_json("\xe2\x80\xa8") == "\"\\u2028\"", "U+2028");
    check(as_json("za\xc5\xbc\xc3\xb3\xc5\x82\xc4\x87 \xf0\x9f\x99\x82") == "\"za\xc5\xbc\xc3\xb3\xc5\x82\xc4\x87 \xf0\x9f\x99\x82\"",
          "ordinary UTF-8 (Polish letters, an emoji) is kept");
    check(as_json("a\xff" "b") == "\"a\\ufffdb\"", "an invalid byte becomes U+FFFD");
    check(as_json("\xc0\xaf") == "\"\\ufffd\\ufffd\"", "an overlong encoding is invalid");
    check(as_json("\xed\xa0\x80") == "\"\\ufffd\\ufffd\\ufffd\"", "a surrogate is invalid");
    check(as_json("\xe2\x82") == "\"\\ufffd\\ufffd\"", "a truncated sequence is invalid");
    check(as_json(std::string("a\0b", 3)) == R"("a\u0000b")", "NUL");
}

void test_ids() {
    check(json::hex_id(0x1a) == "000000000000001a", "ids are 16 hex digits");
    check(json::hex_id(0xffffffffffffffff) == "ffffffffffffffff", "the largest id");
    check(json::parse_hex_id("000000000000001a") == 0x1a && json::parse_hex_id("1A") == 0x1a, "parse, either case");
    check(!json::parse_hex_id("") && !json::parse_hex_id("0") && !json::parse_hex_id("1g")
              && !json::parse_hex_id("11112222333344445"),
          "rejected: empty, 0, not hex, too long");
}

void test_events() {
    check(cli::ready({"127.0.0.1", 40123}, 2) == R"({"ev":"ready","listen":"127.0.0.1:40123","pts":2})", "ready");
    check(cli::reply(msg::Reply{3, true, 0x1a, {}}) == R"({"ev":"reply","req":3,"ok":true,"conv":"000000000000001a"})",
          "reply ok with a conversation");
    check(cli::reply_error(4, "unknown conversation") == R"({"ev":"reply","req":4,"ok":false,"error":"unknown conversation"})",
          "reply error");
    check(cli::conv_state(msg::ConvState{0x1a, msg::ConvStatus::Failed, "socket_error: Connection refused"})
              == R"({"ev":"conv_state","conv":"000000000000001a","status":"failed","detail":"socket_error: Connection refused"})",
          "conv_state with detail");
    check(cli::conv_state(msg::ConvState{0x1a, msg::ConvStatus::Connected, {}})
              == R"({"ev":"conv_state","conv":"000000000000001a","status":"connected"})",
          "conv_state without detail");
    const msg::StoredMessage m{0, 0x77, "hi \"there\"", msg::WallClock::time_point{} + std::chrono::milliseconds{1'764'000'000'123},
                               msg::DeliveryState::Received};
    check(cli::message(msg::MsgAppended{0x1a, 0, m})
              == R"({"ev":"msg","conv":"000000000000001a","id":0,"from":"peer","body":"hi \"there\"","state":"received","at":"2025-11-24T16:00:00.123Z"})",
          "msg");
    check(cli::msg_status(msg::MsgStatus{0x1a, 2, msg::DeliveryState::Sent})
              == R"({"ev":"msg_status","conv":"000000000000001a","id":2,"state":"sent"})",
          "msg_status");
    msg::ConvSnapshot snap{0x1a, {m}, msg::ConvStatus::Connected};
    snap.msgs[0].sender = kNoId;
    check(cli::snapshot(snap)
              == R"({"ev":"snapshot","conv":"000000000000001a","status":"connected","msgs":[{"id":0,"from":"me","body":"hi \"there\"","state":"received","at":"2025-11-24T16:00:00.123Z"}]})",
          "snapshot");
    check(cli::convs(msg::ConvList{{msg::ConvSummary{0x1a, msg::ConvStatus::New, {"10.0.0.2", 7000}, 3}}})
              == R"({"ev":"convs","convs":[{"conv":"000000000000001a","status":"new","remote":"10.0.0.2:7000","unread":3}]})",
          "convs");
    check(cli::convs(msg::ConvList{}) == R"({"ev":"convs","convs":[]})", "convs, empty");
    check(cli::shutdown_forced(Escalation{ShutdownPhase::Emergency, "graceful_timeout", {"store", "router"}})
              == R"({"ev":"shutdown_forced","phase":"emergency","reason":"graceful_timeout","stuck":["store","router"]})",
          "shutdown_forced");
    check(cli::shutdown_done() == R"({"ev":"shutdown_done"})", "shutdown_done");
    check(cli::fatal("cannot listen") == R"({"ev":"fatal","error":"cannot listen"})", "fatal");
}

std::string read_all(int fd) {
    std::string out;
    char buf[4096];
    for (;;) {
        const ssize_t n = ::read(fd, buf, sizeof buf);
        if (n <= 0) { return out; }
        out.append(buf, static_cast<std::size_t>(n));
    }
}

void test_writer() {
    std::signal(SIGPIPE, SIG_IGN);   // as the Supervisor does in the app
    int fds[2];
    check(::pipe(fds) == 0, "pipe");
    {
        JsonlWriter w(fds[1]);
        check(w.write(R"({"a":1})") && w.write(R"({"b":2})"), "two lines written");
        ::close(fds[1]);
        check(read_all(fds[0]) == "{\"a\":1}\n{\"b\":2}\n", "each line ends with a newline");
        ::close(fds[0]);
    }

    check(::pipe(fds) == 0, "pipe");
    {
        JsonlWriter w(fds[1]);
        ::close(fds[0]);   // the reader goes away
        check(!w.write("{}"), "EPIPE: write returns false");
        check(w.closed() && !w.write("{}") && !w.try_write("{}"), "and the writer stays closed");
        ::close(fds[1]);
    }

    check(::pipe(fds) == 0, "pipe");
    {
        ::fcntl(fds[1], F_SETFL, O_NONBLOCK);   // only to fill the pipe from this thread
        const std::string chunk(4096, 'x');
        while (::write(fds[1], chunk.data(), chunk.size()) > 0) {}
        ::fcntl(fds[1], F_SETFL, 0);
        JsonlWriter w(fds[1]);
        const auto start = std::chrono::steady_clock::now();
        check(!w.try_write(R"({"ev":"shutdown_forced"})"), "try_write on a full pipe gives up");
        check(std::chrono::steady_clock::now() - start < 100ms, "without blocking");
        std::string big(5000, 'y');
        ::close(fds[0]);
        ::close(fds[1]);
        check(!JsonlWriter(-1).try_write(big), "try_write refuses a line longer than PIPE_BUF");
    }
}

std::vector<msg::FrontendMsg> collect(ThreadQueue<msg::FrontendMsg>& q, std::size_t until_closed_limit = 100) {
    std::vector<msg::FrontendMsg> out;
    const auto end = std::chrono::steady_clock::now() + 2s;
    while (std::chrono::steady_clock::now() < end && out.size() < until_closed_limit) {
        auto m = q.try_pop();
        if (!m) {
            std::this_thread::sleep_for(1ms);
            continue;
        }
        out.push_back(std::move(*m));
        if (std::holds_alternative<msg::InputClosed>(out.back())) { break; }
    }
    return out;
}

void test_line_reader() {
    int fds[2];
    check(::pipe(fds) == 0, "pipe");
    ThreadQueue<msg::FrontendMsg> q;
    LineReader reader(fds[0], q, Logger{});
    std::thread t([&] { reader.run(); });
    const std::string input = "first\r\n\nsecond line\nsplit ";
    std::ignore = ::write(fds[1], input.data(), input.size());
    std::this_thread::sleep_for(20ms);
    const std::string more = "across writes\nno newline at the end";
    std::ignore = ::write(fds[1], more.data(), more.size());
    ::close(fds[1]);
    t.join();
    auto got = collect(q);
    std::vector<std::pair<std::uint64_t, std::string>> lines;
    bool closed = false;
    for (auto& m : got) {
        if (auto* l = std::get_if<msg::InputLine>(&m)) { lines.emplace_back(l->line_no, l->text); }
        if (std::holds_alternative<msg::InputClosed>(m)) { closed = true; }
    }
    const std::vector<std::pair<std::uint64_t, std::string>> expected{
        {1, "first"}, {2, ""}, {3, "second line"}, {4, "split across writes"}, {5, "no newline at the end"}};
    check(lines == expected, "lines split at \\n, \\r dropped, blank lines numbered, a line split across reads joined");
    check(closed && std::holds_alternative<msg::InputClosed>(got.back()), "InputClosed after the last line");
    ::close(fds[0]);

    // stop() while the input stays open.
    check(::pipe(fds) == 0, "pipe");
    ThreadQueue<msg::FrontendMsg> q2;
    LineReader idle(fds[0], q2, Logger{});
    std::thread t2([&] { idle.run(); });
    std::this_thread::sleep_for(20ms);
    const auto start = std::chrono::steady_clock::now();
    idle.stop();
    t2.join();
    check(std::chrono::steady_clock::now() - start < 500ms, "stop() ends the reader while its input is open");
    ::close(fds[0]);
    ::close(fds[1]);
}

}  // namespace

int main() {
    std::cout << "test_jsonl\n";
    test_escaping();
    test_ids();
    test_events();
    test_writer();
    test_line_reader();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
