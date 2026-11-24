#pragma once

// loadgen: a load generator that speaks the node's wire protocol directly.
//
// One process simulates many peers, all on one thread (an EpollConnectionPool),
// so it scales to thousands of peers without a thread each. Every simulated
// peer dials the target node, completes the real handshake (hello, Ack), and
// then sends chat messages at a fixed rate, or keeps a fixed number in flight
// (closed loop). The target runs `node --cli --echo`, so each message comes
// straight back; its body carries the send time, which gives the round-trip
// time.
//
// Kinds of peers
//   active  send and count (the measurement)
//   idle    complete the handshake, then stay silent (connection load only)
//   slow    send like active peers but never read: the echoes pile up at the
//           target, which must keep serving everybody else
// Churn drops a random active peer's connection every 1/churn seconds and
// reconnects it, re-presenting the name the node gave it, so the node takes
// the reconnect path.
//
// Phases: ramp (all handshakes), warmup (traffic, not counted), measurement,
// grace (no new sends; late echoes of measured messages still count).

#include "Wire.hpp"

#include <bit>
#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

namespace loadgen {

struct Config {
    wire::Endpoint target{"127.0.0.1", 0};
    int peers = 16;               // active peers
    double rate = 100.0;          // messages per second per active peer; 0: closed loop
    int window = 0;               // most messages in flight per peer; 0: no limit (rate > 0 only)
    int size = 64;                // body bytes (at least 16: send time and sequence number)
    double duration = 5.0;        // measurement, seconds
    double warmup = 1.0;          // traffic before the measurement, seconds
    double grace = 2.0;           // after the measurement: wait this long for late echoes
    double ramp_timeout = 10.0;   // most time for all handshakes
    int idle_peers = 0;
    int slow_peers = 0;
    int slow_receive_buffer = 4096;   // SO_RCVBUF of a slow peer, so it stalls soon
    double churn = 0.0;           // reconnects per second
    int dial_batch = 64;          // most dials in flight at once during the ramp
};

// Log-bucketed histogram of durations in ns: 32 sub-buckets per power of two
// (about 3% resolution), fixed memory.
class Histogram {
public:
    void record(std::int64_t ns);
    std::uint64_t count() const noexcept { return count_; }
    std::int64_t max() const noexcept { return max_; }
    double mean() const noexcept { return count_ ? sum_ / static_cast<double>(count_) : 0.0; }
    // Upper edge of the bucket that holds the q-quantile.
    std::int64_t quantile(double q) const noexcept;

private:
    static std::size_t index(std::uint64_t v) noexcept;
    static std::int64_t upper(std::size_t idx) noexcept;

    std::vector<std::uint64_t> buckets_ = std::vector<std::uint64_t>(64 * 32, 0);
    std::uint64_t count_ = 0;
    std::int64_t max_ = 0;
    double sum_ = 0.0;
};

struct SecondSample {
    std::uint64_t sent = 0;       // measured messages sent in this second
    std::uint64_t echoed = 0;     // echoes received in this second (of measured messages)
    Histogram rtt;
};

struct Result {
    int active_ready = 0, idle_ready = 0, slow_ready = 0;
    int dial_failures = 0;
    Histogram handshake;          // first handshakes: dial → Ack
    Histogram reconnect;          // churn: dial → Ack
    std::uint64_t reconnects = 0;
    // Messages sent during the measurement window, and what became of them.
    std::uint64_t sent = 0;
    std::uint64_t echoed = 0;
    std::uint64_t send_failed = 0;     // refused by our own buffer (the target reads too slowly)
    std::uint64_t abandoned = 0;       // in flight on a connection churn dropped: never counted as lost
    std::uint64_t behind = 0;          // sends skipped because the generator fell more than 1 s behind
    std::uint64_t window_blocked = 0;  // sends skipped because the window was full
    std::uint64_t disconnects = 0;     // connections the target closed
    std::uint64_t protocol_errors = 0;
    Histogram rtt;
    double measured_seconds = 0.0;
    // The measurement window on the monotonic clock (CLOCK_MONOTONIC, ns), so
    // a harness can line its own samples up with it.
    std::int64_t measure_begin_ns = 0;
    std::int64_t measure_end_ns = 0;
    std::vector<SecondSample> seconds;
};

Result run(const Config& config);

// One JSON object (one line): the configuration and the results, durations in
// milliseconds.
std::string to_json(const Config& config, const Result& result);

}  // namespace loadgen
