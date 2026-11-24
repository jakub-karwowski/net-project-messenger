// test_balancer_e2e.cpp - the Balancer thread wired to a real Router.
//
// Node A starts with one IO worker (it may grow to two). Two peers stream
// messages to it at a steady rate, so its only worker carries two busy links.
// The Balancer must then start a second worker, move one link onto it while
// the traffic keeps flowing, and, once the traffic stops, retire a worker
// again. Every message must arrive exactly once and in order throughout.
//
// Plain assertions, no framework. Every wait is bounded.

#include "NodeHarness.hpp"

#include <iostream>
#include <map>
#include <string>
#include <thread>

namespace {

using namespace harness;
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

// Dials `to` from `from`; returns {from's conv, to's conv}.
std::pair<ConvId, ConvId> connect(Node& from, Node& to) {
    const ReqId req = from.request(msg::DialReq{0, to.endpoint()});
    const auto r = from.reply(req);
    const ConvId conv_from = r && r->ok ? r->conv : kNoId;
    const bool up = from.state(ConvStatus::Connected, conv_from).has_value();
    const auto conv_to = to.state(ConvStatus::Connected);
    check(conv_from != kNoId && up && conv_to, "connected");
    return {conv_from, conv_to.value_or(kNoId)};
}

void test_scale_out_move_and_scale_in() {
    RouterConfig rc;
    rc.min_pts = 1;
    rc.max_pts = 2;
    balance::Config bc;
    bc.period = 50ms;
    bc.byte_cost = 20'000.0;          // 20 KB/s per unit of load: the test traffic saturates a worker
    bc.min_pts = 1;
    bc.max_pts = 2;
    bc.scale_in_ticks = 10;           // half a second of quiet
    bc.move_cooldown = 2s;
    Node a(rc, bc), b, c;
    auto [cb, ca_b] = connect(b, a);
    auto [cc, ca_c] = connect(c, a);
    check(a.router->pt_count() == 1, "A starts with one IO worker");

    // ~1.5 s of traffic from both peers, 1 KB per message, one message per
    // millisecond each: about 1 MB/s into A's single worker.
    constexpr int kMessages = 1500;
    const std::string pad(1000, 'x');
    std::thread sender([&] {
        for (int i = 0; i < kMessages; ++i) {
            b.request(msg::SendText{0, cb, std::to_string(i) + ":" + pad});
            c.request(msg::SendText{0, cc, std::to_string(i) + ":" + pad});
            std::this_thread::sleep_for(1ms);
        }
    });

    // Read everything A receives, checking order per conversation.
    std::map<ConvId, int> next{{ca_b, 0}, {ca_c, 0}};
    bool in_order = true;
    std::size_t max_pts_seen = 1;
    const auto until = Clock::now() + 30s;
    while ((next[ca_b] < kMessages || next[ca_c] < kMessages) && Clock::now() < until) {
        max_pts_seen = std::max(max_pts_seen, a.router->pt_count());
        auto m = a.frontend.try_pop();
        if (!m) {
            std::this_thread::sleep_for(1ms);
            continue;
        }
        const auto* got = std::get_if<msg::MsgAppended>(&*m);
        if (got == nullptr || got->msg.state != msg::DeliveryState::Received) { continue; }
        const auto colon = got->msg.body.find(':');
        const int seq = std::stoi(got->msg.body.substr(0, colon));
        if (!next.contains(got->conv) || seq != next[got->conv]) { in_order = false; }
        next[got->conv] = seq + 1;
    }
    sender.join();

    check(next[ca_b] == kMessages && next[ca_c] == kMessages,
          "every message from both peers arrived (" + std::to_string(next[ca_b]) + ", " + std::to_string(next[ca_c]) + " of "
              + std::to_string(kMessages) + ")");
    check(in_order, "in order, none lost or repeated, while links moved");
    check(max_pts_seen == 2, "under load the Balancer started a second worker");
    check(a.router->moves_completed() >= 1,
          "and moved a link onto it (" + std::to_string(a.router->moves_completed()) + " moves)");

    // Traffic has stopped: the load decays and the Balancer retires a worker.
    check(eventually([&] { return a.router->pt_count() == 1; }, 10s), "once idle, it scales back in to one worker");
    // The links survived the retirement too.
    check(b.request(msg::SendText{0, cb, "after"}) && a.message(ca_b, "after").has_value(),
          "traffic still flows after the scale-in");
}

}  // namespace

int main() {
    std::cout << "test_balancer_e2e\n";
    test_scale_out_move_and_scale_in();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
