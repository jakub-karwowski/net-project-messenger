// test_shutdown.cpp - quitting under load, in both frontends.
//
// What a shutdown must guarantee, checked on real node processes:
//   - with data still queued for a peer that does not read, the node waits the
//     grace period for it, then exits anyway: exit 0, within grace + the margin
//     before the watchdog would force it, and no forced shutdown;
//   - with a peer that keeps reading, everything the node reported "sent" is
//     delivered before it exits;
//   - quitting (SIGTERM) in the middle of traffic in both directions exits 0 in
//     time, and the other node carries on;
//   - the terminal UI exits the same way on SIGTERM and on Ctrl+C, with the
//     terminal reset;
//   - the log is complete: every line well-formed, the last one written after
//     every worker had stopped.
//
// The "stalled peer" is a raw socket that completes the handshake and then
// stops reading, so data piles up in the node's buffers.
//
// Plain assertions, no framework. Every wait is bounded.

#include "CliProcess.hpp"
#include "TcpSocket.hpp"
#include "TuiProcess.hpp"
#include "Wire.hpp"

#include <atomic>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace cli_process;
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

std::string temp_path(const char* name) {
    return std::string("/tmp/test_shutdown_") + name + "_" + std::to_string(::getpid()) + ".log";
}

long long ms(Clock::duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

// ------------------------------------------------------------------ a peer that can stall

class RawPeer {
public:
    RawPeer() = default;
    wire::Endpoint endpoint() const { return {"127.0.0.1", listener_.local_port().value_or(0)}; }

    // Accepts the node's dial and answers its hello; afterwards reads nothing
    // until drain() is called.
    bool accept_and_handshake() {
        auto conn = listener_.accept_client();
        if (!conn) { return false; }
        conn_.emplace(std::move(*conn));
        conn_->with_receive_timeout(100ms);
        std::array<std::byte, 4096> buf{};
        const auto until = Clock::now() + 3s;
        while (Clock::now() < until) {
            if (auto next = dec_.next(); auto* m = std::get_if<wire::Message>(&next)) {
                const auto* hello = std::get_if<wire::ConnectInfo>(m);
                if (hello == nullptr) { return false; }
                auto ack = wire::encode(wire::ConnectInfoAck{99, hello->peer_id});
                return ack && conn_->send(*ack).has_value();
            }
            const auto n = conn_->receive(buf);
            if (n) { dec_.feed(std::span<const std::byte>(buf).first(*n)); }
        }
        return false;
    }

    // Reads until the node closes the connection; returns the chat messages seen.
    std::size_t drain(std::chrono::milliseconds limit = 10s) {
        std::size_t chats = 0;
        std::array<std::byte, 65536> buf{};
        const auto until = Clock::now() + limit;
        while (Clock::now() < until) {
            for (;;) {
                auto next = dec_.next();
                auto* m = std::get_if<wire::Message>(&next);
                if (m == nullptr) {
                    if (std::holds_alternative<wire::NeedMore>(next)) { break; }
                    continue;
                }
                if (std::holds_alternative<wire::ChatMessage>(*m)) { ++chats; }
            }
            const auto n = conn_->receive(buf);
            if (n) { dec_.feed(std::span<const std::byte>(buf).first(*n)); continue; }
            if (n.error() != SocketError::WouldBlock && n.error() != SocketError::Interrupted) { break; }   // closed
        }
        return chats;
    }

private:
    TcpListener listener_{"127.0.0.1", 0, 4};
    std::optional<TcpConnection> conn_;
    wire::Decoder dec_;
};

// ------------------------------------------------------------------ log checks

struct LogCheck {
    std::size_t lines = 0;
    bool well_formed = true;
    std::string last;
    std::string text;
};

LogCheck read_log(const std::string& path) {
    LogCheck c;
    std::ifstream in(path);
    std::stringstream all;
    all << in.rdbuf();
    c.text = all.str();
    const std::regex line_re(R"(^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z +\d+\.\d{6} (DEBUG|INFO|METRIC|WARN|ERROR|FATAL) +\[[^\]]+\] [^:]+: .*$)");
    std::istringstream lines(c.text);
    for (std::string line; std::getline(lines, line);) {
        ++c.lines;
        if (!std::regex_match(line, line_re)) { c.well_formed = false; }
        c.last = line;
    }
    if (!c.text.empty() && c.text.back() != '\n') { c.well_formed = false; }
    return c;
}

// Seconds since start (the log's monotonic column) of the first line containing `marker`.
std::optional<double> log_time(const std::string& text, const std::string& marker) {
    const auto at = text.find(marker);
    if (at == std::string::npos) { return std::nullopt; }
    const auto line_start = text.rfind('\n', at);
    std::istringstream line(text.substr(line_start == std::string::npos ? 0 : line_start + 1));
    std::string wall;
    double mono = 0;
    if (!(line >> wall >> mono)) { return std::nullopt; }
    return mono;
}

struct ShutdownTimes {
    long long network_ms = -1;   // start of the shutdown → the Router's last IO worker stopped
    long long total_ms = -1;     // start of the shutdown → every worker joined (the log's last line)
};

// Checks the log and returns how long the shutdown took, by the node's own
// clock. The network part ends when the last IO worker has stopped; the rest
// is the frontend writing out what it still has, and the threads joining.
ShutdownTimes check_log(const std::string& path, const std::string& what) {
    const auto log = read_log(path);
    check(log.lines > 10 && log.well_formed, what + ": every log line is complete and well-formed (" + std::to_string(log.lines) + " lines)");
    check(log.last.find("all workers stopped; exit code 0") != std::string::npos,
          what + ": the log ends with the line written after every worker stopped, got: " + log.last);
    for (const char* thread : {"[store]", "[router]", "[pt-0]", "[balancer]", "[supervisor]", "[main]"}) {
        check(log.text.find(thread) != std::string::npos, what + ": the log has lines from " + thread);
    }
    // KEEP_LOGS=1 keeps the node logs in /tmp for a look at the timeline.
    if (std::getenv("KEEP_LOGS") == nullptr) { std::remove(path.c_str()); }
    const auto begin = log_time(log.text, "shutdown phase=graceful");
    const auto network = log_time(log.text, "all IO workers stopped");
    const auto end = log_time(log.text, "all workers stopped");
    check(begin && network && end, what + ": the log has the start and the end of the shutdown");
    if (!begin || !network || !end) { return {}; }
    return {static_cast<long long>((*network - *begin) * 1000.0), static_cast<long long>((*end - *begin) * 1000.0)};
}

// Sends `count` messages of `size` bytes to `conv` from its own thread (the
// node's stdout must be read meanwhile, or both sides block), then closes stdin.
std::thread flood(Node& n, const std::string& conv, int count, std::size_t size, std::atomic<Clock::time_point>* eof_at) {
    return std::thread([&n, conv, count, size, eof_at] {
        const std::string body(size, 'x');
        for (int i = 0; i < count; ++i) { n.send("/send " + conv + " " + std::to_string(i) + ":" + body); }
        if (eof_at != nullptr) {
            eof_at->store(Clock::now());
            n.close_stdin();
        }
    });
}

std::string connect_to(Node& n, const wire::Endpoint& ep) {
    const auto req = n.send("/dial " + ep.address + " " + std::to_string(ep.port));
    const auto r = n.reply(req);
    return r ? str_field(*r, "conv") : std::string{};
}

std::size_t count_status(const Node& n, const std::string& state) {
    std::size_t c = 0;
    for (const auto& l : n.lines()) {
        if (is_event(l, "msg_status") && str_field(l, "state") == state) { ++c; }
    }
    return c;
}

bool forced(const Node& n) {
    for (const auto& l : n.lines()) {
        if (is_event(l, "shutdown_forced")) { return true; }
    }
    return false;
}

// ------------------------------------------------------------------ tests

void test_stalled_peer_waits_grace_then_exits() {
    const auto log = temp_path("stalled");
    RawPeer peer;
    Node a({"--shutdown-grace", "500", "--log", log, "--log-level", "debug"});
    check(a.wait_event("ready").has_value(), "ready");
    std::thread accept([&] { check(peer.accept_and_handshake(), "the stalled peer completes the handshake"); });
    const std::string conv = connect_to(a, peer.endpoint());
    accept.join();
    check(a.wait_event("conv_state", [&](const std::string& l) { return str_field(l, "status") == "connected"; }).has_value(),
          "connected to the stalled peer");

    // 300 × 60 KB: far more than the socket and the pool buffer hold.
    std::atomic<Clock::time_point> eof_at{};
    auto writer = flood(a, conv, 300, 60'000, &eof_at);
    const auto code = a.wait_exit(10s);
    writer.join();
    const auto shutdown = check_log(log, "stalled peer");

    std::cout << "  stalled peer: exit " << code.value_or(-1) << ", network shutdown " << shutdown.network_ms << " ms, total "
              << shutdown.total_ms << " ms (grace 500); sent " << count_status(a, "sent") << ", refused "
              << count_status(a, "failed") << '\n';
    check(code == 0, "a node that cannot flush still exits normally: exit 0");
    check(!forced(a), "without a forced shutdown");
    check(is_event(a.lines().back(), "shutdown_done"), "shutdown_done is the last event");
    check(shutdown.network_ms >= 450, "it waited the grace period for the data (" + std::to_string(shutdown.network_ms) + " ms)");
    check(shutdown.network_ms < 700, "then gave up on it at once (" + std::to_string(shutdown.network_ms) + " ms)");
    check(count_status(a, "failed") > 0, "messages beyond the buffers were refused, and reported failed");
}

void test_reading_peer_gets_everything_sent() {
    const auto log = temp_path("flush");
    RawPeer peer;
    Node a({"--shutdown-grace", "5000", "--log", log, "--log-level", "debug"});
    check(a.wait_event("ready").has_value(), "ready");
    std::thread accept([&] { check(peer.accept_and_handshake(), "peer handshake"); });
    const std::string conv = connect_to(a, peer.endpoint());
    accept.join();
    check(a.wait_event("conv_state", [&](const std::string& l) { return str_field(l, "status") == "connected"; }).has_value(),
          "connected");

    std::atomic<Clock::time_point> eof_at{};
    auto writer = flood(a, conv, 300, 60'000, &eof_at);
    writer.join();
    // The peer starts reading only once the shutdown has begun (the node's log
    // says so), so the data is still queued when the shutdown starts. The
    // node's output is read meanwhile, or it would stall writing it.
    bool begun = false;
    const auto until = Clock::now() + 10s;
    while (!begun && Clock::now() < until) {
        std::ifstream in(log);
        std::stringstream text;
        text << in.rdbuf();
        begun = text.str().find("shutdown phase=graceful") != std::string::npos;
        std::ignore = a.wait_for([](const std::string&) { return false; }, a.mark(), 20ms);
    }
    check(begun, "the shutdown started with the data still queued");
    std::size_t delivered = 0;
    std::thread reader([&] { delivered = peer.drain(); });
    const auto code = a.wait_exit(10s);
    reader.join();
    const auto sent = count_status(a, "sent");
    const auto shutdown = check_log(log, "flush on quit");

    std::cout << "  reading peer: exit " << code.value_or(-1) << ", network shutdown " << shutdown.network_ms << " ms, total "
              << shutdown.total_ms << " ms (grace 5000); sent " << sent << ", delivered " << delivered << '\n';
    check(code == 0 && !forced(a), "exit 0, not forced");
    check(sent > 0 && delivered == sent, "every message reported sent was delivered before the node exited ("
                                             + std::to_string(delivered) + " of " + std::to_string(sent) + ")");
    check(shutdown.network_ms > 0 && shutdown.network_ms < 4000,
          "the flush took place during the shutdown, which ended once it was done, not at the deadline ("
              + std::to_string(shutdown.network_ms) + " ms)");
}

void test_sigterm_during_two_way_traffic() {
    const auto log = temp_path("sigterm");
    Node a({"--log", log, "--log-level", "debug"});
    Node b;
    const auto ready_b = b.wait_event("ready");
    check(a.wait_event("ready") && ready_b, "both ready");
    const std::string listen = ready_b ? str_field(*ready_b, "listen") : "127.0.0.1:0";
    const auto colon = listen.rfind(':');
    const std::string ca = connect_to(a, wire::Endpoint{listen.substr(0, colon),
                                                        static_cast<std::uint16_t>(std::stoi(listen.substr(colon + 1)))});
    check(a.wait_event("conv_state", [&](const std::string& l) { return str_field(l, "status") == "connected"; }).has_value(),
          "A connected");
    const auto up_b = b.wait_event("conv_state", [](const std::string& l) { return str_field(l, "status") == "connected"; });
    const std::string cb = up_b ? str_field(*up_b, "conv") : "";

    auto wa = flood(a, ca, 2000, 1000, nullptr);
    auto wb = flood(b, cb, 2000, 1000, nullptr);
    std::this_thread::sleep_for(150ms);   // in the middle of it
    const auto start = Clock::now();
    a.signal(SIGTERM);
    const auto code = a.wait_exit(10s);
    const auto took = ms(Clock::now() - start);
    wa.join();
    wb.join();

    const auto shutdown = check_log(log, "sigterm");
    std::cout << "  SIGTERM mid-traffic: exit " << code.value_or(-1) << " after " << took << " ms, network shutdown "
              << shutdown.network_ms << " ms\n";
    check(code == 0 && !forced(a), "SIGTERM mid-traffic: exit 0, not forced");
    check(took < 2000, "within grace + the watchdog's margin (" + std::to_string(took) + " ms)");
    check(b.wait_event("conv_state", [&](const std::string& l) {
        return str_field(l, "conv") == cb && str_field(l, "status") == "disconnected";
    }).has_value(), "B sees A go");
    const auto quit = b.send("/quit");
    check(b.reply(quit).has_value() && b.wait_exit() == 0, "B carries on, then quits normally");
}

void test_tui_quits_in_time() {
    for (const bool use_signal : {false, true}) {
        const std::string how = use_signal ? "SIGTERM" : "Ctrl+C";
        const auto log = temp_path(use_signal ? "tui_term" : "tui_ctrlc");
        Node peer;
        const auto ready = peer.wait_event("ready");
        const std::string listen = ready ? str_field(*ready, "listen") : "";
        tui_process::TuiProcess tui({"--log", log, "--log-level", "debug"});
        check(tui.wait_text("listening on"), how + ": the TUI is up");
        tui.type("/dial 127.0.0.1 " + listen.substr(listen.rfind(':') + 1) + "\r");
        check(tui.wait_text("connected"), how + ": connected");
        tui.type("a last message\r");
        check(peer.wait_event("msg", [](const std::string& l) { return str_field(l, "body") == "a last message"; }).has_value(),
              how + ": a message got through");

        const auto start = Clock::now();
        if (use_signal) { tui.signal(SIGTERM); } else { tui.type("\x03"); }
        const auto code = tui.wait_exit();
        const auto took = ms(Clock::now() - start);
        const auto shutdown = check_log(log, "tui " + how);
        std::cout << "  TUI " << how << ": exit " << code.value_or(-1) << " after " << took << " ms, network shutdown "
                  << shutdown.network_ms << " ms\n";
        check(code == 0, how + ": exit 0");
        check(took < 2000, how + ": within grace + the watchdog's margin (" + std::to_string(took) + " ms)");
        check(tui.output().rfind("\033[?25h") != std::string::npos, how + ": the cursor is shown again");
        const auto quit = peer.send("/quit");
        check(peer.reply(quit).has_value() && peer.wait_exit() == 0, how + ": the peer quits normally");
    }
}

}  // namespace

int main() {
    std::cout << "test_shutdown\n";
    test_stalled_peer_waits_grace_then_exits();
    test_reading_peer_gets_everything_sent();
    test_sigterm_during_two_way_traffic();
    test_tui_quits_in_time();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
