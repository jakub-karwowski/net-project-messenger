#include "BalancerWorker.hpp"

#include <tuple>
#include <utility>

BalancerWorker::BalancerWorker(balance::Config config, ThreadQueue<msg::BalancerMsg>& inbox,
                               ThreadQueue<msg::RouterMsg>& router, Logger log, bool timer)
    : config_(config),
      inbox_(inbox),
      router_(router),
      log_(log.with_component("balancer")),
      last_tick_(Clock::now()) {
    if (timer) { timer_ = inbox_.every(config_.period, msg::BalancerTick{}); }
}

BalancerWorker::~BalancerWorker() {
    if (timer_ != 0) { inbox_.cancel(timer_); }
}

void BalancerWorker::on(msg::PtReport& m) {
    balance::on_report(state_, m);
}

void BalancerWorker::on(msg::TopologyChanged& m) {
    balance::on_topology(state_, m, Clock::now());
    log_.metric("topology={} pt={}", m.added ? "added" : "removed", m.pt);
}

void BalancerWorker::on(msg::LinkGone& m) {
    balance::on_link_gone(state_, m.link);
}

void BalancerWorker::on(msg::StopBalancer&) {
    if (timer_ != 0) {
        inbox_.cancel(timer_);
        timer_ = 0;
    }
    done_ = true;
}

void BalancerWorker::on(msg::BalancerTick&) {
    const auto now = Clock::now();
    // Fold in the reports answering the previous probe, then probe again: the
    // answers to this probe arrive before the next tick.
    balance::end_tick(state_, config_, now - last_tick_, now);
    last_tick_ = now;
    send_router(msg::ProbeAll{++probe_seq_});

    for (const auto& [pt, load] : state_.pts) {
        if (load.stalled && load.missed == config_.k_missed) {
            log_.warn("pt-{} stalled: {} probes unanswered; excluded from placement and moves", pt, load.missed);
        }
    }

    auto decisions = balance::decide(state_, config_, now);
    // Decisions are measurement data: Metric lines, key=value, logged before
    // commit() so a move still names the pt it leaves.
    if (decisions.spawn) { log_.metric("decision=spawn pts={}", state_.pts.size()); }
    if (decisions.retire) { log_.metric("decision=retire pt={}", *decisions.retire); }
    for (const auto& move : decisions.moves) {
        const auto it = state_.links.find(move.link);
        log_.metric("decision=move link={} from={} to={}", move.link, it == state_.links.end() ? 0 : it->second.pt, move.to);
    }
    for (const LinkId link : decisions.evict) { log_.metric("decision=evict link={}", link); }
    balance::commit(state_, decisions, now);

    if (decisions.policy) { send_router(std::move(*decisions.policy)); }
    if (decisions.spawn) { send_router(msg::SpawnPt{}); }
    if (decisions.retire) { send_router(msg::RetirePt{*decisions.retire}); }
    for (const auto& move : decisions.moves) { send_router(move); }
    for (const LinkId link : decisions.evict) { send_router(msg::EvictLink{link}); }
    log_metrics();
}

void BalancerWorker::log_metrics() {
    if (!log_.enabled(LogLevel::Metric)) { return; }
    const auto loads = balance::pt_loads(state_, config_);
    const auto counts = balance::pt_link_counts(state_);
    for (const auto& [pt, load] : state_.pts) {
        log_.metric("pt={} links={} load={:.4f} stalled={} backlog={}", pt, counts.at(pt), loads.at(pt), load.stalled,
                    load.cmd_backlog);
    }
}

void BalancerWorker::send_router(msg::RouterMsg m) {
    std::ignore = router_.push(std::move(m));   // false: the Router is shutting down
}
