// test_loadgen.cpp - the load generator against a real `node --cli --echo`.
//
// The histogram is checked on its own; the generator runs in-process against
// a node process and must account for every message: what it sent in the
// measurement window either came back, was refused by its own buffer, or was
// abandoned on a connection churn dropped.
//
// Plain assertions, no framework.

#include "CliProcess.hpp"
#include "LoadGen.hpp"
#include "TcpSocket.hpp"

#include <cmath>
#include <algorithm>
#include <fstream>
#include <cstring>
#include <cstdio>
#include <iostream>
#include <string>

#include <unistd.h>

namespace {

using namespace cli_process;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

void test_histogram() {
    loadgen::Histogram h;
    for (int i = 1; i <= 1000; ++i) { h.record(static_cast<std::int64_t>(i) * 1'000'000); }   // 1..1000 ms
    const auto p50 = h.quantile(0.5) / 1e6;
    const auto p99 = h.quantile(0.99) / 1e6;
    check(h.count() == 1000 && h.max() == 1'000'000'000, "count and max");
    check(p50 >= 500 && p50 <= 500 * 1.04, "p50 within the 3% bucket resolution (" + std::to_string(p50) + ")");
    check(p99 >= 990 && p99 <= 990 * 1.04, "p99 within the 3% bucket resolution (" + std::to_string(p99) + ")");
    check(std::abs(h.mean() / 1e6 - 500.5) < 1e-6, "the mean is exact");
    loadgen::Histogram empty;
    check(empty.quantile(0.5) == 0 && empty.count() == 0, "an empty histogram");
}

std::optional<wire::Endpoint> start_target(Node& node) {
    const auto ready = node.wait_event("ready");
    if (!ready) { return std::nullopt; }
    const std::string listen = str_field(*ready, "listen");
    const auto colon = listen.rfind(':');
    return wire::Endpoint{listen.substr(0, colon), static_cast<std::uint16_t>(std::stoi(listen.substr(colon + 1)))};
}

loadgen::Config quick(const wire::Endpoint& target) {
    loadgen::Config c;
    c.target = target;
    c.peers = 8;
    c.rate = 200;
    c.duration = 1.0;
    c.warmup = 0.3;
    c.grace = 1.0;
    return c;
}

void test_steady_rate() {
    Node node({"--echo"});
    const auto target = start_target(node);
    check(target.has_value(), "the echo node is up");
    if (!target) { return; }
    const auto r = loadgen::run(quick(*target));
    check(r.active_ready == 8 && r.dial_failures == 0, "every peer completed the handshake");
    check(r.handshake.count() == 8, "one handshake time per peer");
    check(r.sent >= 1400 && r.sent <= 1700, "about rate x peers x duration sent (" + std::to_string(r.sent) + ")");
    check(r.echoed == r.sent && r.send_failed == 0 && r.protocol_errors == 0, "every message came back once");
    check(r.rtt.count() == r.echoed && r.rtt.quantile(0.5) > 0, "a round-trip time for each");
    std::uint64_t per_second = 0;
    for (const auto& s : r.seconds) { per_second += s.sent; }
    check(r.seconds.size() == 1 && per_second == r.sent, "the per-second series adds up");
    const auto json = loadgen::to_json(quick(*target), r);
    check(json.starts_with("{\"config\":") && json.find("\"lost\":0") != std::string::npos, "the JSON summary");
    node.close_stdin();
    check(node.wait_exit() == 0, "the node exits normally");
    check(node.lines().size() == 2, "an echo node prints nothing per message or per peer (" + std::to_string(node.lines().size())
                                        + " lines)");
}

void test_idle_and_slow_peers() {
    Node node({"--echo"});
    const auto target = start_target(node);
    if (!target) { return; }
    auto c = quick(*target);
    c.idle_peers = 50;
    c.slow_peers = 2;
    c.size = 1024;
    const auto r = loadgen::run(c);
    check(r.active_ready == 8 && r.idle_ready == 50 && r.slow_ready == 2, "active, idle and slow peers all connected");
    check(r.echoed == r.sent && r.sent > 0, "peers that stop reading do not cost the others a message");
    node.close_stdin();
    check(node.wait_exit() == 0, "the node exits normally");
}

void test_churn() {
    Node node({"--echo"});
    const auto target = start_target(node);
    if (!target) { return; }
    auto c = quick(*target);
    c.churn = 20;
    c.duration = 1.5;
    const auto r = loadgen::run(c);
    check(r.reconnects >= 20, "about churn x time reconnects (" + std::to_string(r.reconnects) + ")");
    check(r.reconnect.count() + 2 >= r.reconnects, "almost every reconnect completed its handshake");
    check(r.echoed + r.abandoned + r.send_failed == r.sent,
          "every message is accounted for: echoed, or abandoned on a dropped connection");
    check(r.disconnects == 0 && r.protocol_errors == 0, "the node never closed a connection itself");
    node.close_stdin();
    check(node.wait_exit() == 0, "the node exits normally");
}

void test_closed_loop() {
    Node node({"--echo"});
    const auto target = start_target(node);
    if (!target) { return; }
    auto c = quick(*target);
    c.rate = 0;
    c.window = 4;
    c.size = 4096;
    const auto r = loadgen::run(c);
    check(r.echoed == r.sent && r.sent > 1000, "closed loop: as fast as the echoes return (" + std::to_string(r.sent) + ")");
    node.close_stdin();
    check(node.wait_exit() == 0, "the node exits normally");
}

void test_no_target() {
    wire::Endpoint nobody;
    {
        TcpListener l("127.0.0.1", 0, 1);
        nobody = {"127.0.0.1", l.local_port().value_or(0)};
    }
    auto c = quick(nobody);
    c.ramp_timeout = 2;
    const auto r = loadgen::run(c);
    check(r.active_ready == 0 && r.dial_failures == 8 && r.sent == 0, "nothing listening: every dial fails, nothing sent");
}

// The deepest the node's Store and Router inboxes were, by its own
// queue_depth log lines.
std::size_t deepest_inbox(const std::string& log_path) {
    std::ifstream in(log_path);
    std::size_t deepest = 0;
    for (std::string line; std::getline(in, line);) {
        if (line.find("queue_depth") == std::string::npos) { continue; }
        for (const char* key : {" store_max=", " router_max="}) {
            const auto at = line.find(key);
            if (at != std::string::npos) { deepest = std::max<std::size_t>(deepest, std::stoull(line.substr(at + std::strlen(key)))); }
        }
    }
    return deepest;
}

struct Flood {
    std::size_t deepest = 0;
    loadgen::Result result;
    int exit_code = -1;
};

// Far more than the node can echo, for a couple of seconds.
Flood flood(const std::string& inbox_high) {
    const std::string log = "/tmp/test_loadgen_flood_" + inbox_high + "_" + std::to_string(::getpid()) + ".log";
    std::remove(log.c_str());
    Flood f;
    Node node({"--echo", "--inbox-high", inbox_high, "--log", log, "--log-level", "metric"});
    const auto target = start_target(node);
    if (!target) { return f; }
    auto c = quick(*target);
    c.peers = 32;
    c.rate = 20000;   // 640k msg/s offered
    c.duration = 2.0;
    c.warmup = 0.5;
    c.grace = 0.5;
    f.result = loadgen::run(c);
    node.close_stdin();
    f.exit_code = node.wait_exit().value_or(-1);
    f.deepest = deepest_inbox(log);
    std::remove(log.c_str());
    return f;
}

void test_backpressure_bounds_the_inboxes() {
    constexpr std::size_t kHigh = 2000;
    const Flood on = flood(std::to_string(kHigh));
    const Flood off = flood("0");
    // Over the high mark by at most what is already read when the IO worker
    // checks (one read, up to 64 KiB of frames) and, for the Router, what the
    // Store still holds when the IO worker stops.
    check(on.deepest > 0 && on.deepest <= 2 * kHigh + 2000,
          "with backpressure the inboxes stay near the watermark (deepest " + std::to_string(on.deepest) + ")");
    check(off.deepest > 5 * kHigh, "without it they grow far past it (deepest " + std::to_string(off.deepest) + ")");
    check(on.result.disconnects == 0 && on.result.protocol_errors == 0 && on.result.active_ready == 32,
          "the node slows its peers down; it never drops one");
    // Not checked: how many measured messages come back. The backlog now waits
    // in the kernel's socket buffers, several MB per connection on loopback, so
    // in a two-second flood every measured message is still queued behind the
    // warm-up when the run ends. The node's exit without backpressure is not
    // checked either: its backlog may not drain in the grace (a forced exit is
    // what backpressure fixes).
    check(on.exit_code == 0, "the node exits normally, its backlog drained in time");
}

}  // namespace

int main() {
    std::cout << "test_loadgen\n";
    test_histogram();
    test_steady_rate();
    test_idle_and_slow_peers();
    test_churn();
    test_closed_loop();
    test_no_target();
    test_backpressure_bounds_the_inboxes();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
