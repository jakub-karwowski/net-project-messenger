// test_metrics.cpp - Metrics counters, snapshot JSON, queue high-water marks, MetricsWorker.

#include "Metrics.hpp"
#include "ThreadWorkers.hpp"

#include <cctype>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
int g_failures = 0;
void check(bool ok, const std::string& what) {
    if (!ok) { ++g_failures; std::cout << "FAIL: " << what << '\n'; }
}
bool has(const std::string& s, const std::string& key) {
    return s.find("\"" + key + "\":") != std::string::npos;
}

// Minimal well-formedness: flat object, quoted keys, every value numeric.
bool flat_numeric_object(const std::string& s) {
    if (s.size() < 2 || s.front() != '{' || s.back() != '}') { return false; }
    std::size_t i = 1;
    while (i < s.size() - 1) {
        if (s[i] != '"') { return false; }
        const auto q = s.find('"', i + 1);
        if (q == std::string::npos || s[q + 1] != ':') { return false; }
        i = q + 2;
        const auto start = i;
        while (i < s.size() - 1 && s[i] != ',') { ++i; }
        if (i == start) { return false; }
        for (auto j = start; j < i; ++j) {
            if (!(std::isdigit(static_cast<unsigned char>(s[j])) || s[j] == '-')) { return false; }
        }
        if (i < s.size() - 1) { ++i; }
    }
    return true;
}
} // namespace

int main() {
    p2p::Metrics m;
    p2p::bump(&m, &p2p::Metrics::accepts, 3);
    p2p::live_inc(&m);
    p2p::bump(nullptr, &p2p::Metrics::accepts); // no-op
    const std::string json = m.snapshot_json({4, 5, 6});
    for (const char* k : {"t_ns", "accepts", "connections_live", "bytes_in", "frames_in", "frames_out",
                          "send_failures", "send_backpressure_drops", "reconnects", "evictions",
                          "echoed", "render_q_hwm", "store_q_hwm", "sender_q_hwm", "utime_us",
                          "stime_us", "nvcsw", "nivcsw", "maxrss_kb", "threads", "fds"}) {
        check(has(json, k), std::string{"snapshot has "} + k);
    }
    check(flat_numeric_object(json), "snapshot is a flat numeric JSON object: " + json);
    check(json.find("\"accepts\":3,") != std::string::npos, "accepts value");
    check(json.find("\"connections_live\":1,") != std::string::npos, "live gauge");
    check(json.find("\"store_q_hwm\":5,") != std::string::npos, "queue marks");
    check(json.find("\"threads\":0,") == std::string::npos, "threads read from /proc");

    {
        ThreadQueue<int> q;
        for (int i = 0; i < 7; ++i) { check(q.push(i), "push"); }
        for (int i = 0; i < 5; ++i) { (void)q.try_pop(); }
        check(q.size() == 2, "size");
        check(q.high_water() == 7, "high water 7");
        (void)q.push(1);
        check(q.high_water() == 7, "high water unchanged");
    }

    {
        ThreadQueue<Event> store_queue;
        p2p::PeerTable peers;
        p2p::Metrics sm;
        p2p::PeerSession session{store_queue, peers};
        session.set_metrics(&sm);
        std::vector<std::byte> bytes;
        for (const auto& msg : {wire::make_connect(1, 0, {wire::Endpoint{"127.0.0.1", 1}}),
                                wire::make_chat_message(0, 0, "hello"),
                                wire::make_chat_message(0, 0, "world")}) {
            auto frame = wire::encode(msg);
            check(frame.has_value(), "encode");
            bytes.insert(bytes.end(), frame->begin(), frame->end());
        }
        const std::size_t half = bytes.size() / 2; // partial frame across calls
        check(session.on_bytes({bytes.data(), half}), "on_bytes 1");
        check(session.on_bytes({bytes.data() + half, bytes.size() - half}), "on_bytes 2");
        check(sm.bytes_in.load() == bytes.size(), "bytes_in");
        check(sm.frames_in.load() == 3, "frames_in == 3, got " + std::to_string(sm.frames_in.load()));
    }

    {
        const std::string path = "/tmp/test_metrics_" + std::to_string(::getpid()) + ".jsonl";
        p2p::MetricsWorker worker{path, [&] { return m.snapshot_json(); },
                                  std::chrono::milliseconds{50}};
        std::thread t{[&] { worker(); }};
        std::this_thread::sleep_for(std::chrono::milliseconds{200});
        worker.stop();
        t.join();
        std::ifstream in{path};
        std::string line;
        int lines = 0;
        while (std::getline(in, line)) {
            ++lines;
            check(flat_numeric_object(line), "line is JSON: " + line);
        }
        check(lines >= 3, "MetricsWorker lines: " + std::to_string(lines));
        std::remove(path.c_str());
    }

    if (g_failures == 0) { std::cout << "test_metrics: all passed\n"; }
    return g_failures == 0 ? 0 : 1;
}
