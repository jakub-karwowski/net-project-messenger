#pragma once

// Process-wide counters for the performance experiment. Every counter is a
// relaxed atomic bumped with a single fetch_add (no locks, no allocation), so
// components take a nullable `Metrics*` and use the inline helpers below.
// snapshot_json() renders one flat JSON object.
//
// Wiring a new transport (e.g. MultiplexedIo) -- NetConfig::metrics (and the
// make_io_stack caller) provides the instance; it may be nullptr:
//   accept of a connection      -> bump(m, &Metrics::accepts); live_inc(m)
//   connection closed/dropped   -> live_dec(m)
//   idle-prune / stalled close  -> bump(m, &Metrics::evictions) (plus live_dec on close)
//   FrameChannel                 -> session.set_metrics(m); it then counts
//                                  bytes_in and frames_in by itself
// Outbound counters (frames_out, send_failures, send_backpressure_drops,
// reconnects) are counted by SenderWorker; echoed by StoreWorker.

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <utility>

namespace p2p {

// Queue high-water marks (ThreadQueue::high_water()).
struct QueueMarks {
    std::size_t render{0};
    std::size_t store{0};
    std::size_t sender{0};
};

struct Metrics {
    using QueueMarks = p2p::QueueMarks;
    std::atomic<uint64_t> accepts{0};
    std::atomic<int64_t> connections_live{0}; // gauge
    std::atomic<uint64_t> bytes_in{0};
    std::atomic<uint64_t> frames_in{0};
    std::atomic<uint64_t> frames_out{0};
    std::atomic<uint64_t> send_failures{0};
    std::atomic<uint64_t> send_backpressure_drops{0};
    std::atomic<uint64_t> reconnects{0};
    std::atomic<uint64_t> evictions{0};
    std::atomic<uint64_t> echoed{0};

    // One flat JSON object: t_ns (steady clock), the counters, queue marks and
    // process stats (rusage, threads, fds). Allocates; call from a stats thread.
    std::string snapshot_json(const QueueMarks& marks = {}) const;
};

inline void bump(Metrics* m, std::atomic<uint64_t> Metrics::*field, uint64_t n = 1) noexcept {
    if (m) { (m->*field).fetch_add(n, std::memory_order_relaxed); }
}
inline void live_inc(Metrics* m) noexcept {
    if (m) { m->connections_live.fetch_add(1, std::memory_order_relaxed); }
}
inline void live_dec(Metrics* m) noexcept {
    if (m) { m->connections_live.fetch_sub(1, std::memory_order_relaxed); }
}

// Writes one snapshot line (JSONL, flushed) to `path` at start, then every
// `interval`, and a final line when stopped. Run operator() on its own thread.
class MetricsWorker {
public:
    MetricsWorker(std::string path, std::function<std::string()> snapshot,
                  std::chrono::milliseconds interval = std::chrono::seconds{1})
        : path_(std::move(path)), snapshot_(std::move(snapshot)), interval_(interval) {}
    void operator()();
    void stop() noexcept; // idempotent, any thread
private:
    std::string path_;
    std::function<std::string()> snapshot_;
    std::chrono::milliseconds interval_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_{false};
};

} // namespace p2p
