// test_cli_e2e.cpp - real `node --cli` processes, driven the way an agent
// drives them: commands written to stdin, JSON events read from stdout.
//
// Covers: ready with the bound port, dial, chat both ways (with text that must
// be escaped), /open with history, /list, /close and /reconnect keeping the
// conversations, errors with their line numbers, end of input → shutdown_done
// and exit 0, /quit, SIGTERM with --stay, a closed stdout, and a start-up
// failure.
//
// Plain assertions, no framework. Every wait is bounded; a child that hangs
// is killed.

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <optional>
#include <poll.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

#include "CliProcess.hpp"

namespace {

using namespace cli_process;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// ------------------------------------------------------------------ tests

void test_session() {
    Node a, b;
    const auto pa = ready_port(a);
    const auto pb = ready_port(b);
    check(pa && pb && *pa != 0 && *pb != 0 && a.lines().size() == 1, "ready is the first line and names the bound port");
    if (!pa || !pb) { return; }
    check(a.lines().front().find("\"pts\":1") != std::string::npos, "ready reports the IO workers");

    // Dial.
    const auto dial = a.send("/dial 127.0.0.1 " + std::to_string(*pb));
    const auto r = a.reply(dial);
    const std::string ca = r ? str_field(*r, "conv") : "";
    check(r && r->find("\"ok\":true") != std::string::npos && ca.size() == 16, "dial: reply with the new conversation");
    check(a.wait_event("conv_state", conv_is(ca, "connected")).has_value(), "A: connected");
    const auto b_up = b.wait_event("conv_state", [](const std::string& l) { return str_field(l, "status") == "connected"; });
    const std::string cb = b_up ? str_field(*b_up, "conv") : "";
    check(cb.size() == 16, "B: a new conversation, connected");

    // A → B.
    auto from = a.mark();
    const auto send = a.send("/send " + ca + " hello B");
    check(a.reply(send).has_value(), "send: reply");
    check(b.wait_event("msg", [&](const std::string& l) {
        return str_field(l, "conv") == cb && str_field(l, "body") == "hello B" && str_field(l, "from") == "peer";
    }).has_value(), "B receives the message");
    check(a.wait_event("msg_status", [&](const std::string& l) {
        return str_field(l, "conv") == ca && str_field(l, "state") == "sent";
    }, from).has_value(), "A sees it sent");

    // B opens the conversation: its history comes as a snapshot; then plain text goes there.
    const auto open = b.send("/open " + cb);
    check(b.reply(open).has_value(), "/open: reply");
    const auto snap = b.wait_event("snapshot", [&](const std::string& l) { return str_field(l, "conv") == cb; });
    check(snap && snap->find("hello B") != std::string::npos, "/open: a snapshot with the history");
    b.send("hi \"A\" \x1b[2J\\o/");
    const auto got = a.wait_event("msg", [&](const std::string& l) { return str_field(l, "from") == "peer"; });
    check(got && str_field(*got, "body") == "hi \"A\" \\u001b[2J\\o/", "A receives B's plain text (escaped)");
    check(got && got->find('\x1b') == std::string::npos, "no raw escape byte reaches A's output");

    // List.
    const auto list = a.send("/list");
    check(a.reply(list).has_value(), "/list: reply");
    const auto convs = a.wait_event("convs");
    check(convs && convs->find(ca) != std::string::npos && convs->find("\"status\":\"connected\"") != std::string::npos,
          "/list: the conversation, connected");

    // Close and reconnect keep the conversations.
    from = a.mark();
    auto b_from = b.mark();
    a.send("/close " + ca);
    check(a.wait_event("conv_state", conv_is(ca, "disconnected"), from).has_value(), "A: disconnected after /close");
    check(b.wait_event("conv_state", conv_is(cb, "disconnected"), b_from).has_value(), "B: disconnected");
    from = a.mark();
    b_from = b.mark();
    a.send("/reconnect " + ca);
    check(a.wait_event("conv_state", conv_is(ca, "connected"), from).has_value(), "A: connected again");
    check(b.wait_event("conv_state", conv_is(cb, "connected"), b_from).has_value(), "B: connected again, same conversation");

    // Errors carry their line number; blank lines count.
    const auto bad = a.send("/frobnicate");
    const auto bad_reply = a.reply(bad);
    check(bad_reply && bad_reply->find("\"ok\":false") != std::string::npos
              && str_field(*bad_reply, "error").starts_with("unknown command"),
          "an unknown command: reply ok=false with the error");
    a.send("");
    const auto help = a.send("/help");
    check(a.reply(help).has_value() && a.wait_event("notice").has_value(), "/help after a blank line: its line number");

    // End of input: A shuts down cleanly; B sees it go.
    b_from = b.mark();
    a.close_stdin();
    const auto code = a.wait_exit();
    check(code == 0, "end of input: exit 0 (got " + (code ? std::to_string(*code) : std::string("no exit")) + ", last line: "
                         + (a.lines().empty() ? std::string() : a.lines().back()) + ")");
    check(!a.lines().empty() && is_event(a.lines().back(), "shutdown_done"), "shutdown_done is A's last line");
    check(b.wait_event("conv_state", conv_is(cb, "disconnected"), b_from).has_value(), "B: disconnected");

    const auto quit = b.send("/quit");
    check(b.reply(quit).has_value(), "/quit: reply");
    check(b.wait_exit() == 0 && is_event(b.lines().back(), "shutdown_done"), "/quit: shutdown_done, exit 0");
    check(all_json_lines(a) && all_json_lines(b), "every output line is one JSON object");
}

void test_sigterm_with_stay() {
    Node c({"--stay"});
    check(ready_port(c).has_value(), "ready");
    c.close_stdin();
    std::this_thread::sleep_for(200ms);
    check(!c.wait_exit(100ms).has_value(), "--stay: still running after end of input");
    c.signal(SIGTERM);
    check(c.wait_exit() == 0 && is_event(c.lines().back(), "shutdown_done"), "SIGTERM: normal shutdown, exit 0");
}

void test_stdout_closed() {
    Node d;
    check(ready_port(d).has_value(), "ready");
    d.close_stdout();
    d.send("/list");   // its output hits a closed pipe
    check(d.wait_exit() == 0, "stdout closed: the node quits normally");
}

void test_listen_failure() {
    Node first;
    const auto port = ready_port(first);
    Node second({"--listen", "127.0.0.1:" + std::to_string(port.value_or(1))});
    const auto fatal = second.wait_event("fatal");
    check(fatal && str_field(*fatal, "error").starts_with("cannot listen"), "a port in use: fatal event");
    check(second.wait_exit() == 1, "and exit code 1");
}

}  // namespace

int main() {
    std::cout << "test_cli_e2e\n";
    test_session();
    test_sigterm_with_stay();
    test_stdout_closed();
    test_listen_failure();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
