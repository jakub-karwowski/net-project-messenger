#pragma once

// The Balancer thread: probes the IO workers every period, keeps the load
// model (BalancerPolicy) up to date from their reports, and sends the Router
// its decisions: placement policy, worker spawn/retire, link moves, evictions.
//
// Its ticks come from a timer on its own inbox (BalancerTick), so a test can
// push BalancerTick{} by hand instead of waiting. The Router validates every
// request; the Balancer only proposes.

#include "BalancerPolicy.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"

#include <cstdint>

class BalancerWorker {
public:
    // With `timer` false no ticks are scheduled (tests push BalancerTick themselves).
    BalancerWorker(balance::Config config, ThreadQueue<msg::BalancerMsg>& inbox, ThreadQueue<msg::RouterMsg>& router,
                   Logger log, bool timer = true);
    ~BalancerWorker();

    BalancerWorker(const BalancerWorker&) = delete;
    BalancerWorker& operator=(const BalancerWorker&) = delete;

    void on(msg::PtReport& m);
    void on(msg::TopologyChanged& m);
    void on(msg::LinkGone& m);
    void on(msg::StopBalancer& m);
    void on(msg::BalancerTick& m);

    bool done() const noexcept { return done_; }
    const balance::State& state() const noexcept { return state_; }

private:
    using Clock = balance::Clock;

    void send_router(msg::RouterMsg m);
    void log_metrics();

    balance::Config config_;
    ThreadQueue<msg::BalancerMsg>& inbox_;
    ThreadQueue<msg::RouterMsg>& router_;
    Logger log_;
    std::uint64_t timer_ = 0;   // 0: no timer
    balance::State state_;
    std::uint64_t probe_seq_ = 0;
    Clock::time_point last_tick_;
    bool done_ = false;
};
