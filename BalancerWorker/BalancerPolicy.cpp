#include "BalancerPolicy.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <utility>

namespace balance {

namespace {

constexpr double kMinWeight = 1e-3;   // a full worker still gets the odd link rather than none

std::map<PtId, double> shares(const std::vector<PtId>& sequence) {
    std::map<PtId, double> out;
    for (const PtId pt : sequence) { out[pt] += 1.0; }
    for (auto& [pt, n] : out) { n /= static_cast<double>(sequence.size()); }
    return out;
}

bool shares_differ(const std::map<PtId, double>& a, const std::map<PtId, double>& b, double tolerance) {
    std::set<PtId> keys;
    for (const auto& [k, v] : a) { keys.insert(k); }
    for (const auto& [k, v] : b) { keys.insert(k); }
    return std::ranges::any_of(keys, [&](PtId k) {
        const double x = a.contains(k) ? a.at(k) : 0.0;
        const double y = b.contains(k) ? b.at(k) : 0.0;
        return std::abs(x - y) > tolerance || (x == 0.0) != (y == 0.0);
    });
}

}  // namespace

// ------------------------------------------------------------------ folding

void on_topology(State& s, const msg::TopologyChanged& t, Clock::time_point now) {
    if (t.added) {
        PtLoad pt;
        pt.pt = t.pt;
        pt.last_report = now;
        s.pts.insert_or_assign(t.pt, pt);
        s.spawn_pending = false;
        return;
    }
    s.pts.erase(t.pt);
    if (s.retiring == t.pt) { s.retiring.reset(); }
    std::erase_if(s.policy, [&](PtId p) { return p == t.pt; });
}

void on_link_gone(State& s, LinkId link) {
    s.links.erase(link);
}

void on_report(State& s, const msg::PtReport& r) {
    if (auto it = s.pts.find(r.pt); it != s.pts.end()) {
        it->second.reported_this_tick = true;
        it->second.last_report = r.at;
        it->second.cmd_backlog = r.cmd_backlog;
        it->second.paused_pending_ms += r.paused_ms;
    }
    for (const auto& sample : r.links) {
        auto [it, inserted] = s.links.try_emplace(sample.link);
        LinkLoad& l = it->second;
        if (inserted) {
            l.link = sample.link;
            l.first_seen = r.at;
            l.last_active = r.at;
        }
        l.peer = sample.peer;
        l.pending_rx += sample.rx_bytes;
        l.pending_tx += sample.tx_bytes;
        l.pending_frames += sample.rx_frames + sample.tx_frames;
        // A "gone" sample is the old worker's last word on a link that has left
        // it; only a live sample says where the link is now. A report built
        // before our last move of the link (it answered an earlier probe) is
        // stale on that point and cannot undo the move.
        if (!sample.gone) {
            if (!l.placed) {
                l.pt = r.pt;
                l.placed = true;
            } else if (l.pt != r.pt && r.at > l.last_moved) {
                l.pt = r.pt;
                l.last_moved = r.at;
            }
        }
        if (sample.rx_bytes + sample.tx_bytes > 0) { l.last_active = r.at; }
    }
}

void end_tick(State& s, const Config& c, std::chrono::duration<double> elapsed, Clock::time_point) {
    const double seconds = std::max(elapsed.count(), 1e-3);
    for (auto& [id, l] : s.links) {
        const double rx = static_cast<double>(l.pending_rx) / seconds;
        const double tx = static_cast<double>(l.pending_tx) / seconds;
        const double fps = static_cast<double>(l.pending_frames) / seconds;
        l.rx_bps = c.alpha * rx + (1.0 - c.alpha) * l.rx_bps;
        l.tx_bps = c.alpha * tx + (1.0 - c.alpha) * l.tx_bps;
        l.fps = c.alpha * fps + (1.0 - c.alpha) * l.fps;
        l.pending_rx = l.pending_tx = l.pending_frames = 0;
    }
    for (auto& [id, pt] : s.pts) {
        pt.missed = pt.reported_this_tick ? 0 : pt.missed + 1;
        pt.stalled = pt.missed >= c.k_missed;
        pt.reported_this_tick = false;
        pt.paused_ms = std::exchange(pt.paused_pending_ms, 0);
    }
}

// ------------------------------------------------------------------ reading

double link_load(const LinkLoad& l, const Config& c) {
    return (l.rx_bps + l.tx_bps) / c.byte_cost + c.link_cost;
}

std::map<PtId, double> pt_loads(const State& s, const Config& c) {
    std::map<PtId, double> out;
    for (const auto& [id, pt] : s.pts) { out[id] = 0.0; }
    for (const auto& [id, l] : s.links) {
        if (l.placed && out.contains(l.pt)) { out[l.pt] += link_load(l, c); }
    }
    return out;
}

std::map<PtId, std::size_t> pt_link_counts(const State& s) {
    std::map<PtId, std::size_t> out;
    for (const auto& [id, pt] : s.pts) { out[id] = 0; }
    for (const auto& [id, l] : s.links) {
        if (l.placed && out.contains(l.pt)) { ++out[l.pt]; }
    }
    return out;
}

std::vector<PtId> usable_pts(const State& s) {
    std::vector<PtId> out;
    for (const auto& [id, pt] : s.pts) {
        if (!pt.stalled && s.retiring != id) { out.push_back(id); }
    }
    return out;
}

std::vector<PtId> weighted_sequence(const std::map<PtId, double>& weights, std::size_t length) {
    std::vector<PtId> out;
    if (weights.empty()) { return out; }
    const double total = std::accumulate(weights.begin(), weights.end(), 0.0,
                                         [](double sum, const auto& w) { return sum + w.second; });
    std::map<PtId, double> current;
    for (const auto& [pt, w] : weights) { current[pt] = 0.0; }
    out.reserve(length);
    for (std::size_t i = 0; i < length; ++i) {
        for (auto& [pt, cur] : current) { cur += weights.at(pt); }
        const auto best = std::ranges::max_element(current, {}, [](const auto& e) { return e.second; });
        out.push_back(best->first);
        best->second -= total;
    }
    return out;
}

// ------------------------------------------------------------------ deciding

Decisions decide(const State& s, const Config& c, Clock::time_point now) {
    Decisions d;
    d.low_ticks = s.low_ticks;
    auto loads = pt_loads(s, c);
    const auto usable = usable_pts(s);
    if (usable.empty()) { return d; }

    // Placement policy.
    std::map<PtId, double> weights;
    for (const PtId pt : usable) { weights[pt] = std::max(kMinWeight, c.capacity - loads[pt]); }
    const double total_weight = std::accumulate(weights.begin(), weights.end(), 0.0,
                                                [](double sum, const auto& w) { return sum + w.second; });
    std::map<PtId, double> wanted;
    for (const auto& [pt, w] : weights) { wanted[pt] = w / total_weight; }
    if (s.policy.empty() || shares_differ(wanted, shares(s.policy), c.policy_change)) {
        d.policy = msg::SetPolicy{weighted_sequence(weights, c.policy_len), s.policy_epoch + 1};
    }

    // Backpressure: the workers' rates show what they were allowed to read.
    d.hold = std::ranges::any_of(s.pts, [](const auto& e) { return e.second.paused_ms > 0; });

    // Scaling, by the mean load of the usable workers.
    if (!d.hold) {
        const double mean = std::accumulate(usable.begin(), usable.end(), 0.0,
                                            [&](double sum, PtId pt) { return sum + loads[pt]; })
                          / static_cast<double>(usable.size());
        const bool can_spawn = s.pts.size() < c.max_pts && !s.spawn_pending;
        if (mean > c.hi_watermark * c.capacity && can_spawn) {
            d.spawn = true;
            d.low_ticks = 0;
        } else if (mean < c.lo_watermark * c.capacity && s.pts.size() > c.min_pts && usable.size() > 1 && !s.retiring) {
            d.low_ticks = s.low_ticks + 1;
            if (d.low_ticks >= c.scale_in_ticks) {
                d.retire = *std::ranges::min_element(usable, {}, [&](PtId pt) { return loads[pt]; });
                d.low_ticks = 0;
            }
        } else {
            d.low_ticks = 0;
        }
    }

    // Rebalance between usable workers (a worker being retired empties itself).
    std::vector<PtId> pool = usable;
    if (d.retire) { std::erase(pool, *d.retire); }
    if (!d.hold && pool.size() >= 2) {
        auto hot = *std::ranges::max_element(pool, {}, [&](PtId pt) { return loads[pt]; });
        auto cold = *std::ranges::min_element(pool, {}, [&](PtId pt) { return loads[pt]; });
        if (loads[hot] - loads[cold] > c.imbalance_threshold) {
            std::vector<const LinkLoad*> candidates;
            for (const auto& [id, l] : s.links) {
                const bool cooled = l.last_moved == Clock::time_point{} || now - l.last_moved >= c.move_cooldown;
                if (l.placed && l.pt == hot && cooled) { candidates.push_back(&l); }
            }
            std::ranges::sort(candidates, std::greater<>{}, [&](const LinkLoad* l) { return link_load(*l, c); });
            for (const LinkLoad* l : candidates) {
                if (d.moves.size() >= c.max_moves_per_tick) { break; }
                const double gap = loads[hot] - loads[cold];
                const double moved = link_load(*l, c);
                // Moving it must narrow the gap: a link worth more than the gap
                // would just make the other worker the busy one.
                if (moved >= gap) { continue; }
                d.moves.push_back(msg::MoveLink{l->link, cold});
                loads[hot] -= moved;
                loads[cold] += moved;
            }
        }
    }

    // Eviction of the longest idle links.
    if (c.max_links > 0 && s.links.size() > c.max_links) {
        std::vector<const LinkLoad*> idle;
        for (const auto& [id, l] : s.links) {
            if (now - l.last_active >= c.idle_evict_after) { idle.push_back(&l); }
        }
        std::ranges::sort(idle, {}, [](const LinkLoad* l) { return l->last_active; });
        const std::size_t over = s.links.size() - c.max_links;
        for (std::size_t i = 0; i < std::min(over, idle.size()); ++i) { d.evict.push_back(idle[i]->link); }
    }
    return d;
}

void commit(State& s, const Decisions& d, Clock::time_point now) {
    if (d.policy) {
        s.policy = d.policy->next_assignments;
        s.policy_epoch = d.policy->epoch;
    }
    if (d.spawn) { s.spawn_pending = true; }
    if (d.retire) { s.retiring = d.retire; }
    s.low_ticks = d.low_ticks;
    for (const auto& m : d.moves) {
        if (auto it = s.links.find(m.link); it != s.links.end()) {
            it->second.last_moved = now;
            it->second.pt = m.to;   // assumed until the next report says otherwise
        }
    }
}

}  // namespace balance
