#pragma once

// The Balancer's model and decisions, as plain functions of plain data: no
// threads, no queues, time passed in. The BalancerWorker feeds it reports and
// ticks and sends what decide() returns.
//
// Model
//   per link   EWMA of received and sent bytes/s and frames/s, on which worker
//              it is, when it was last active and last moved
//   per worker the load: sum over its links of bytes/s / byte_cost, plus
//              link_cost per link; whether it is stalled (missed k_missed probes)
//
// Decisions, at every tick
//   policy     the next policy_len placements, weighted round robin with
//              weights max(eps, capacity - load) over usable workers; sent only
//              when the shares changed noticeably
//   scale out  mean load above hi_watermark * capacity: start a worker
//   scale in   mean load below lo_watermark * capacity for scale_in_ticks:
//              retire the least loaded worker
//   rebalance  max - min load above imbalance_threshold: move the busiest
//              eligible links from the busiest worker to the idlest, at most
//              max_moves_per_tick, never a link moved within move_cooldown,
//              never one that would only swap which worker is the busier
//   evict      over max_links (0: never): close the links idle the longest,
//              if idle for at least idle_evict_after
//
// A stalled worker is neither a source nor a target of anything: a thread that
// does not answer probes cannot release links either. Stalling is detected,
// not cured.
//
// Hold. While any worker reports that it paused reading (backpressure: the
// Store's or Router's inbox is congested), the bottleneck is downstream of the
// workers, and their rates show what they were allowed to read, not what their
// peers send. No scale out, no scale in and no moves then: more, fewer or
// rearranged workers cannot help, and the low rates would read as idle
// workers. The scale-in streak is frozen. Placement of new links and eviction
// go on: neither changes the workers.

#include "Ids.hpp"
#include "Messages.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <vector>

namespace balance {

using Clock = std::chrono::steady_clock;

struct Config {
    std::chrono::milliseconds period{500};
    double alpha = 0.3;                      // EWMA weight of the newest sample
    double byte_cost = 1'000'000.0;          // bytes/s that make one unit of load
    double link_cost = 0.002;                // load of an idle link
    double capacity = 1.0;                   // load one worker can carry
    double hi_watermark = 0.75;              // of capacity, mean over workers
    double lo_watermark = 0.25;
    int k_missed = 3;                        // probes unanswered before a worker counts as stalled
    std::size_t policy_len = 64;
    double policy_change = 0.1;              // largest share difference that is not worth a new policy
    std::size_t min_pts = 1;
    std::size_t max_pts = 4;
    int scale_in_ticks = 10;
    double imbalance_threshold = 0.2;        // load units
    std::chrono::milliseconds move_cooldown{10'000};
    std::size_t max_moves_per_tick = 2;
    std::size_t max_links = 0;               // 0: no eviction
    std::chrono::milliseconds idle_evict_after{60'000};
};

struct LinkLoad {
    LinkId link = kNoId;
    PeerId peer = kNoId;
    PtId pt = 0;
    bool placed = false;                     // pt is known (a live sample has been seen)
    double rx_bps = 0.0, tx_bps = 0.0, fps = 0.0;
    Clock::time_point first_seen{}, last_active{}, last_moved{};
    // since the last report folded in for this link
    std::uint64_t pending_rx = 0, pending_tx = 0, pending_frames = 0;
};

struct PtLoad {
    PtId pt = 0;
    Clock::time_point last_report{};
    bool reported_this_tick = true;          // a new worker is not late before its first probe
    int missed = 0;
    bool stalled = false;
    std::size_t cmd_backlog = 0;
    std::uint32_t paused_ms = 0;             // reads paused during the last tick (backpressure)
    std::uint32_t paused_pending_ms = 0;     // reported since, folded in by end_tick
};

struct State {
    std::map<PtId, PtLoad> pts;
    std::map<LinkId, LinkLoad> links;
    std::vector<PtId> policy;                // the last policy sent
    std::uint64_t policy_epoch = 0;
    int low_ticks = 0;                       // consecutive ticks below the low watermark
    bool spawn_pending = false;              // a SpawnPt not yet answered by TopologyChanged
    std::optional<PtId> retiring;            // a RetirePt not yet answered
};

struct Decisions {
    std::optional<msg::SetPolicy> policy;
    bool spawn = false;
    std::optional<PtId> retire;
    std::vector<msg::MoveLink> moves;
    std::vector<LinkId> evict;
    int low_ticks = 0;                       // the counter's next value
    bool hold = false;                       // a worker paused reading: no spawn, retire or moves
};

// ---- folding facts into the state

void on_topology(State& s, const msg::TopologyChanged& t, Clock::time_point now);
void on_link_gone(State& s, LinkId link);
// Accumulates the counters; the rates are updated once per tick (end_tick).
void on_report(State& s, const msg::PtReport& r);
// Turns the accumulated counters of the past `elapsed` into EWMA rates, and
// counts missed probes.
void end_tick(State& s, const Config& c, std::chrono::duration<double> elapsed, Clock::time_point now);

// ---- reading the state

double link_load(const LinkLoad& l, const Config& c);
// Load of every known worker (stalled ones included).
std::map<PtId, double> pt_loads(const State& s, const Config& c);
std::map<PtId, std::size_t> pt_link_counts(const State& s);
// Workers that may receive or give links: known, not stalled, not retiring.
std::vector<PtId> usable_pts(const State& s);

// ---- deciding

Decisions decide(const State& s, const Config& c, Clock::time_point now);
// Records what was decided (sent policy, pending spawn/retire, last_moved).
void commit(State& s, const Decisions& d, Clock::time_point now);

// The weighted round-robin sequence for `weights` (smooth WRR: deterministic,
// and each worker's entries are spread evenly).
std::vector<PtId> weighted_sequence(const std::map<PtId, double>& weights, std::size_t length);

}  // namespace balance
