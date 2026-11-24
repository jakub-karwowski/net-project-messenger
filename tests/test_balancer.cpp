// test_balancer.cpp - the Balancer's model and decisions (BalancerPolicy) as
// pure functions, plus the BalancerWorker driven by hand-pushed ticks.
//
// Rates are set directly where a test is about a decision, not about the EWMA.
//
// Plain assertions, no framework.

#include "BalancerPolicy.hpp"
#include "BalancerWorker.hpp"
#include "Worker.hpp"

#include <algorithm>
#include <chrono>
#include <iostream>
#include <map>
#include <string>
#include <vector>

namespace {

using namespace std::chrono_literals;
using balance::Clock;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

const Clock::time_point t0 = Clock::now();

balance::Config config() {
    balance::Config c;
    c.byte_cost = 1000.0;      // 1000 B/s = one unit of load
    c.link_cost = 0.0;         // keeps the arithmetic exact
    c.min_pts = 1;
    c.max_pts = 4;
    c.scale_in_ticks = 3;
    c.move_cooldown = 10s;
    return c;
}

balance::State with_pts(std::initializer_list<PtId> pts) {
    balance::State s;
    for (const PtId pt : pts) { balance::on_topology(s, msg::TopologyChanged{pt, true}, t0); }
    return s;
}

// A link on `pt` whose load is exactly `load` (with config()).
void put_link(balance::State& s, LinkId link, PtId pt, double load) {
    balance::LinkLoad l;
    l.link = link;
    l.peer = link * 100;
    l.pt = pt;
    l.placed = true;
    l.rx_bps = load * 1000.0;
    l.first_seen = l.last_active = t0;
    s.links[link] = l;
}

void test_weighted_sequence() {
    const auto seq = balance::weighted_sequence({{0, 3.0}, {1, 1.0}}, 8);
    const auto zeros = std::ranges::count(seq, PtId{0});
    check(seq.size() == 8 && zeros == 6, "weights 3:1 give 6 of 8 entries to the heavier worker");
    std::size_t run = 0, longest = 0;
    for (const PtId p : seq) { run = p == 0 ? run + 1 : 0; longest = std::max(longest, run); }
    check(longest <= 3, "entries are spread out, not bunched");
    check(seq == balance::weighted_sequence({{0, 3.0}, {1, 1.0}}, 8), "deterministic");
    check(balance::weighted_sequence({}, 8).empty(), "no workers: empty");
}

void test_ewma_and_loads() {
    auto c = config();
    auto s = with_pts({0});
    msg::PtReport r{0, 1, t0, 0, {msg::LinkSample{7, 70, 1000, 0, 10, 0, false}}};
    balance::on_report(s, r);
    balance::end_tick(s, c, 1s, t0 + 1s);
    check(std::abs(s.links.at(7).rx_bps - 300.0) < 1e-9, "first sample: alpha * rate (0.3 * 1000 B/s)");
    balance::on_report(s, r);
    balance::end_tick(s, c, 1s, t0 + 2s);
    check(std::abs(s.links.at(7).rx_bps - 510.0) < 1e-9, "second sample: 0.3 * 1000 + 0.7 * 300");
    check(std::abs(balance::pt_loads(s, c).at(0) - 0.51) < 1e-9, "worker load = link bytes/s / byte_cost");
    c.link_cost = 0.1;
    check(std::abs(balance::pt_loads(s, c).at(0) - 0.61) < 1e-9, "plus link_cost per link");
    balance::end_tick(s, c, 1s, t0 + 3s);
    check(s.links.at(7).rx_bps < 510.0, "no traffic in a tick: the rate decays");
}

void test_gone_samples_and_moves() {
    auto s = with_pts({0, 1});
    balance::on_report(s, msg::PtReport{0, 1, t0, 0, {msg::LinkSample{7, 70, 10, 0, 1, 0, false}}});
    check(s.links.at(7).pt == 0 && s.links.at(7).placed, "a live sample places the link");
    // The link moved: worker 1 reports it live, worker 0 sends its last counters.
    balance::on_report(s, msg::PtReport{1, 2, t0 + 1s, 0, {msg::LinkSample{7, 70, 5, 0, 1, 0, false}}});
    balance::on_report(s, msg::PtReport{0, 2, t0 + 1s, 0, {msg::LinkSample{7, 70, 5, 0, 1, 0, true}}});
    check(s.links.at(7).pt == 1, "a gone sample from the old worker does not move it back");
    check(s.links.at(7).last_moved == t0 + 1s, "the move is noticed and timed");
    check(s.links.at(7).pending_rx == 20, "the bytes of every sample count");
    balance::on_link_gone(s, 7);
    check(!s.links.contains(7), "LinkGone forgets the link");
}

void test_policy() {
    const auto c = config();
    auto s = with_pts({0, 1});
    auto d = balance::decide(s, c, t0);
    check(d.policy && d.policy->next_assignments.size() == c.policy_len && d.policy->epoch == 1,
          "first tick: a policy of policy_len entries, epoch 1");
    const auto zeros = d.policy ? std::ranges::count(d.policy->next_assignments, PtId{0}) : 0;
    check(zeros == 32, "idle workers share it equally");
    balance::commit(s, d, t0);
    check(!balance::decide(s, c, t0).policy, "nothing changed: no new policy");

    put_link(s, 1, 0, 0.8);   // worker 0 is busy: weights 0.2 and 1.0
    d = balance::decide(s, c, t0);
    const auto busy = d.policy ? std::ranges::count(d.policy->next_assignments, PtId{0}) : 0;
    check(d.policy && d.policy->epoch == 2 && busy >= 10 && busy <= 11, "a busy worker gets a smaller share (~1/6)");
}

void test_stall_detection() {
    const auto c = config();
    auto s = with_pts({0, 1});
    // The first tick after a worker appears is free: it has not been probed yet.
    for (int tick = 1; tick <= c.k_missed + 1; ++tick) {
        balance::on_report(s, msg::PtReport{0, static_cast<std::uint64_t>(tick), t0, 0, {}});
        balance::end_tick(s, c, 500ms, t0);
        check(s.pts.at(1).stalled == (tick == c.k_missed + 1),
              "worker 1 is stalled after k_missed unanswered probes, tick " + std::to_string(tick));
    }
    check(!s.pts.at(0).stalled, "a worker that answers is not stalled");
    put_link(s, 1, 1, 0.9);
    const auto d = balance::decide(s, c, t0);
    check(d.policy && std::ranges::count(d.policy->next_assignments, PtId{1}) == 0, "a stalled worker gets no new links");
    check(d.moves.empty(), "nothing is moved off or onto a stalled worker");
    balance::on_report(s, msg::PtReport{1, 9, t0, 0, {}});
    balance::end_tick(s, c, 500ms, t0);
    check(!s.pts.at(1).stalled, "it recovers once it answers again");
}

void test_scale_out_and_in() {
    const auto c = config();
    auto s = with_pts({0, 1});
    put_link(s, 1, 0, 0.9);
    put_link(s, 2, 1, 0.9);
    auto d = balance::decide(s, c, t0);
    check(d.spawn, "mean load above the high watermark: start a worker");
    balance::commit(s, d, t0);
    check(!balance::decide(s, c, t0).spawn, "not again until the new worker shows up");
    balance::on_topology(s, msg::TopologyChanged{2, true}, t0);
    balance::on_topology(s, msg::TopologyChanged{3, true}, t0);
    put_link(s, 3, 2, 0.9);
    put_link(s, 4, 3, 0.9);
    check(!balance::decide(s, c, t0).spawn, "never above max_pts");

    auto idle = with_pts({0, 1, 2});
    put_link(idle, 1, 0, 0.05);
    put_link(idle, 2, 1, 0.01);
    put_link(idle, 3, 2, 0.02);
    std::optional<PtId> retired;
    for (int tick = 1; tick <= c.scale_in_ticks; ++tick) {
        d = balance::decide(idle, c, t0);
        balance::commit(idle, d, t0);
        if (d.retire) { retired = d.retire; }
        check(d.retire.has_value() == (tick == c.scale_in_ticks), "retire only after scale_in_ticks quiet ticks, tick "
                                                                       + std::to_string(tick));
    }
    check(retired == PtId{1}, "the least loaded worker is retired");
    d = balance::decide(idle, c, t0);
    check(!d.retire && d.policy && std::ranges::count(d.policy->next_assignments, PtId{1}) == 0,
          "while it retires: no second retire, and it gets no new links");
    balance::on_topology(idle, msg::TopologyChanged{1, false}, t0);
    check(!idle.retiring && !idle.pts.contains(1), "TopologyChanged{removed} ends the retirement");

    auto one = with_pts({0});
    for (int tick = 0; tick < 10; ++tick) {
        d = balance::decide(one, c, t0);
        balance::commit(one, d, t0);
        check(!d.retire, "never below min_pts");
    }
}

void test_rebalance() {
    const auto c = config();
    auto s = with_pts({0, 1});
    put_link(s, 1, 0, 0.5);
    put_link(s, 2, 0, 0.3);
    put_link(s, 3, 0, 0.1);
    auto d = balance::decide(s, c, t0);
    check(d.moves.size() == 1 && d.moves[0].link == 1 && d.moves[0].to == 1,
          "the busiest link moves to the idle worker, and only as much as narrows the gap");
    balance::commit(s, d, t0);
    check(s.links.at(1).last_moved == t0 && s.links.at(1).pt == 1, "commit records the move");

    // The model now has 0.4 vs 0.5: balanced enough, nothing moves.
    check(balance::decide(s, c, t0 + 1s).moves.empty(), "within the threshold: no moves");

    // Hysteresis: a link moved recently stays put even if it is the best candidate.
    auto h = with_pts({0, 1});
    put_link(h, 1, 0, 0.5);
    put_link(h, 2, 0, 0.1);
    h.links.at(1).last_moved = t0;
    d = balance::decide(h, c, t0 + 5s);
    check(d.moves.size() == 1 && d.moves[0].link == 2, "a link within its move cooldown is skipped");
    d = balance::decide(h, c, t0 + 11s);
    check(!d.moves.empty() && d.moves[0].link == 1, "after the cooldown it may move again");

    // One hot link alone never ping-pongs: moving it would only swap the roles.
    auto lone = with_pts({0, 1});
    put_link(lone, 1, 0, 0.9);
    check(balance::decide(lone, c, t0).moves.empty(), "a single link bigger than the gap is not moved");

    // Never more than max_moves_per_tick.
    auto many = with_pts({0, 1});
    for (LinkId l = 1; l <= 10; ++l) { put_link(many, l, 0, 0.1); }
    check(balance::decide(many, c, t0).moves.size() == c.max_moves_per_tick, "at most max_moves_per_tick moves");
}

void test_eviction() {
    auto c = config();
    c.max_links = 1;
    c.idle_evict_after = 30s;
    auto s = with_pts({0});
    put_link(s, 1, 0, 0.0);
    put_link(s, 2, 0, 0.0);
    s.links.at(1).last_active = t0;
    s.links.at(2).last_active = t0 + 50s;
    const auto d = balance::decide(s, c, t0 + 60s);
    check(d.evict == std::vector<LinkId>{1}, "over max_links: the link idle for longer than idle_evict_after goes");
    c.max_links = 0;
    check(balance::decide(s, c, t0 + 60s).evict.empty(), "max_links 0: never");
}

void test_worker_with_manual_ticks() {
    ThreadQueue<msg::BalancerMsg> inbox;
    ThreadQueue<msg::RouterMsg> router;
    BalancerWorker b(config(), inbox, router, Logger{}, /*timer=*/false);
    std::ignore = inbox.push(msg::TopologyChanged{0, true});
    std::ignore = inbox.push(msg::TopologyChanged{1, true});
    std::ignore = inbox.push(msg::BalancerTick{});
    worker::drain(b, inbox);
    std::vector<msg::RouterMsg> out;
    while (auto m = router.try_pop()) { out.push_back(std::move(*m)); }
    check(!out.empty() && std::holds_alternative<msg::ProbeAll>(out[0]) && std::get<msg::ProbeAll>(out[0]).seq == 1,
          "a tick probes every worker (seq 1)");
    check(std::ranges::any_of(out, [](const auto& m) { return std::holds_alternative<msg::SetPolicy>(m); }),
          "and sends the first policy");
    std::ignore = inbox.push(msg::StopBalancer{});
    worker::drain(b, inbox);
    check(b.done(), "StopBalancer ends it");
}

}  // namespace

int main() {
    std::cout << "test_balancer\n";
    test_weighted_sequence();
    test_ewma_and_loads();
    test_gone_samples_and_moves();
    test_policy();
    test_stall_detection();
    test_scale_out_and_in();
    test_rebalance();
    test_eviction();
    test_worker_with_manual_ticks();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
