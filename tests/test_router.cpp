// test_router.cpp - two complete nodes in one process, over loopback.
//
// Each node runs its Store, Router (with its IO workers) and Listener on their
// own threads, exactly as the app will. The test is each node's frontend: it
// sends Store requests and waits for the events. No Balancer: the Router's own
// placement is used, and moves are requested by hand.
//
// Plain assertions, no framework. Every wait is bounded.

#include "ListenerWorker.hpp"
#include "RouterWorker.hpp"
#include "StoreWorker.hpp"
#include "Worker.hpp"

#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <variant>
#include <vector>

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using msg::ConvStatus;
using Sub = msg::Subscription;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// Waits for the first message of type T in `q` that satisfies `pred`; others
// are discarded.
template <class T, class V>
std::optional<T> wait_for(ThreadQueue<V>& q, const std::function<bool(const T&)>& pred,
                          std::chrono::milliseconds limit = 3s) {
    const auto until = Clock::now() + limit;
    while (Clock::now() < until) {
        auto m = q.try_pop();
        if (!m) {
            std::this_thread::sleep_for(1ms);
            continue;
        }
        if (auto* t = std::get_if<T>(&*m); t != nullptr && pred(*t)) { return std::move(*t); }
    }
    return std::nullopt;
}

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds limit = 3s) {
    const auto until = Clock::now() + limit;
    while (Clock::now() < until) {
        if (pred()) { return true; }
        std::this_thread::sleep_for(2ms);
    }
    return pred();
}

struct Node {
    ThreadQueue<msg::FrontendMsg> frontend;
    ThreadQueue<msg::StoreMsg> store_q;
    ThreadQueue<msg::RouterMsg> router_q;
    ThreadQueue<msg::BalancerMsg> balancer_q;   // nobody consumes it here; the test may read it
    TcpListener listener{"127.0.0.1", 0, 64};
    NullPersistence db;
    std::optional<StoreWorker> store;
    std::optional<RouterWorker> router;
    std::optional<ListenerWorker> accept;
    std::unique_ptr<WorkerThread> store_thread;
    std::unique_ptr<WorkerThread> router_thread;
    std::unique_ptr<WorkerThread> listener_thread;
    ReqId next_req = 1;

    explicit Node(RouterConfig rc = {}) {
        StoreConfig sc;
        sc.advertised = {endpoint()};
        sc.default_subscription = Sub{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify};
        store.emplace(sc, frontend, router_q, db, Logger{});
        router.emplace(rc, router_q, store_q, balancer_q, nullptr, Logger{}, [this] { listener.stop(); });
        accept.emplace(listener, router_q, Logger{});
        store_thread = std::make_unique<WorkerThread>("store", [this] { worker::run_inbox(*store, store_q); },
                                                      [this] { store_q.graceful_shutdown(); });
        router_thread = std::make_unique<WorkerThread>("router", [this] { router->run(); },
                                                       [this] { router_q.graceful_shutdown(); });
        listener_thread = std::make_unique<WorkerThread>("listener", [this] { accept->run(); },
                                                         [this] { listener.stop(); });
    }

    ~Node() {
        // Reverse order of start: intake, Router (stops its IO workers), Store.
        listener_thread.reset();
        router_thread.reset();
        store_thread.reset();
    }

    wire::Endpoint endpoint() const { return {"127.0.0.1", listener.local_port().value_or(0)}; }

    ReqId request(msg::StoreMsg m) {
        const ReqId req = next_req++;
        std::visit([req](auto& r) {
            if constexpr (requires { r.req; }) { r.req = req; }
        }, m);
        std::ignore = store_q.push(std::move(m));
        return req;
    }

    std::optional<msg::Reply> reply(ReqId req) {
        return wait_for<msg::Reply>(frontend, [req](const msg::Reply& r) { return r.req == req; });
    }

    // Waits until `conv` (any conv when 0) reaches `status`; returns the conv.
    std::optional<ConvId> state(ConvStatus status, ConvId conv = kNoId, std::chrono::milliseconds limit = 3s) {
        auto s = wait_for<msg::ConvState>(frontend, [&](const msg::ConvState& c) {
            return c.status == status && (conv == kNoId || c.conv == conv);
        }, limit);
        if (!s) { return std::nullopt; }
        return s->conv;
    }

    std::optional<msg::MsgAppended> message(ConvId conv, const std::string& body) {
        return wait_for<msg::MsgAppended>(frontend, [&](const msg::MsgAppended& m) {
            return m.conv == conv && m.msg.body == body && m.msg.state == msg::DeliveryState::Received;
        });
    }

    void drain_events() {
        while (frontend.try_pop()) {}
    }
};

// A connects to B; returns {A's conv, B's conv}.
std::pair<ConvId, ConvId> connect(Node& a, Node& b) {
    const ReqId req = a.request(msg::DialReq{0, b.endpoint()});
    const auto r = a.reply(req);
    const ConvId conv_a = r && r->ok ? r->conv : kNoId;
    const auto connected_a = a.state(ConvStatus::Connected, conv_a);
    const auto conv_b = b.state(ConvStatus::Connected);
    check(conv_a != kNoId && connected_a && conv_b, "A dials B: both sides connected");
    return {conv_a, conv_b.value_or(kNoId)};
}

bool chat(Node& from, ConvId from_conv, Node& to, ConvId to_conv, const std::string& body) {
    from.request(msg::SendText{0, from_conv, body});
    return to.message(to_conv, body).has_value();
}

// ------------------------------------------------------------------ tests

void test_dial_and_chat() {
    Node a, b;
    auto [ca, cb] = connect(a, b);
    check(chat(a, ca, b, cb, "hello B"), "A → B: the message arrives");
    check(chat(b, cb, a, ca, "hello A"), "B → A: the message arrives");
    a.request(msg::SendText{0, ca, "status please"});
    auto sent = wait_for<msg::MsgStatus>(a.frontend, [&](const msg::MsgStatus& s) {
        return s.conv == ca && s.state == msg::DeliveryState::Sent;
    });
    check(sent.has_value(), "the sender sees its message as sent");
    check(a.router->link_count() == 1 && b.router->link_count() == 1, "one link on each side");
}

void test_close_and_reconnect_keep_the_conversation() {
    Node a, b;
    auto [ca, cb] = connect(a, b);

    a.request(msg::CloseReq{0, ca});
    check(a.state(ConvStatus::Disconnected, ca).has_value(), "A closes: A disconnected");
    check(b.state(ConvStatus::Disconnected, cb).has_value(), "and B sees it");
    check(eventually([&] { return a.router->link_count() == 0 && b.router->link_count() == 0; }), "no links left");

    const ReqId req = a.request(msg::ReconnectReq{0, ca});
    check(a.reply(req).value_or(msg::Reply{}).ok, "A reconnects");
    check(a.state(ConvStatus::Connected, ca).has_value(), "A connected again, same conversation");
    check(b.state(ConvStatus::Connected, cb).has_value(), "B recognises A: same conversation on B");
    check(chat(a, ca, b, cb, "after reconnect"), "chat works after the reconnect");

    // Now B reconnects to A, using the endpoint A advertised.
    b.request(msg::CloseReq{0, cb});
    check(b.state(ConvStatus::Disconnected, cb) && a.state(ConvStatus::Disconnected, ca), "B closes");
    b.request(msg::ReconnectReq{0, cb});
    check(b.state(ConvStatus::Connected, cb) && a.state(ConvStatus::Connected, ca),
          "B reconnects to A's advertised endpoint; A recognises B");
    check(chat(b, cb, a, ca, "B dialled this time"), "chat works");
}

void test_simultaneous_reconnect_converges() {
    Node a, b;
    auto [ca, cb] = connect(a, b);
    int converged = 0;
    constexpr int kRounds = 10;
    for (int round = 0; round < kRounds; ++round) {
        a.request(msg::CloseReq{0, ca});
        const bool down = a.state(ConvStatus::Disconnected, ca).has_value() && b.state(ConvStatus::Disconnected, cb).has_value()
                       && eventually([&] { return a.router->link_count() == 0 && b.router->link_count() == 0; });
        if (!down) { break; }
        a.drain_events();
        b.drain_events();
        // Both dial at once.
        a.request(msg::ReconnectReq{0, ca});
        b.request(msg::ReconnectReq{0, cb});
        const bool up = a.state(ConvStatus::Connected, ca).has_value() && b.state(ConvStatus::Connected, cb).has_value();
        // Let the losing link close, then there must be exactly one on each side.
        const bool one = eventually([&] { return a.router->link_count() == 1 && b.router->link_count() == 1; });
        std::this_thread::sleep_for(50ms);
        const bool still_one = a.router->link_count() == 1 && b.router->link_count() == 1;
        const auto n = std::to_string(round);
        if (up && one && still_one && chat(a, ca, b, cb, "a" + n) && chat(b, cb, a, ca, "b" + n)) { ++converged; }
    }
    check(converged == kRounds, "simultaneous reconnect: one link on both sides and chat both ways, "
                                    + std::to_string(converged) + "/" + std::to_string(kRounds) + " rounds");
}

void test_dial_refused() {
    Node a;
    wire::Endpoint nobody;
    {
        TcpListener l("127.0.0.1", 0, 1);
        nobody = {"127.0.0.1", l.local_port().value_or(0)};
    }
    const ReqId req = a.request(msg::DialReq{0, nobody});
    const auto r = a.reply(req);
    auto failed = wait_for<msg::ConvState>(a.frontend, [&](const msg::ConvState& s) {
        return r && s.conv == r->conv && s.status == ConvStatus::Failed;
    });
    check(failed && failed->detail == "socket_error: Connection refused", "a refused dial: Failed with the reason");
    check(eventually([&] { return a.router->link_count() == 0; }), "and no link is left behind");
}

void test_move_keeps_the_stream() {
    RouterConfig rc;
    rc.min_pts = 2;
    Node a(rc), b;
    auto [ca, cb] = connect(a, b);
    check(a.router->pt_count() == 2, "A runs two IO workers");

    // Retire the worker holding the link (the first one: placement picks the
    // least loaded, lowest id) while messages flow in both directions.
    constexpr int kMessages = 50;
    for (int i = 0; i < kMessages / 2; ++i) { a.request(msg::SendText{0, ca, "m" + std::to_string(i)}); }
    std::ignore = a.router_q.push(msg::RetirePt{0});
    for (int i = kMessages / 2; i < kMessages; ++i) { a.request(msg::SendText{0, ca, "m" + std::to_string(i)}); }

    int in_order = 0;
    for (int i = 0; i < kMessages; ++i) {
        if (b.message(cb, "m" + std::to_string(i))) { ++in_order; }
    }
    check(in_order == kMessages, "every message sent around the move arrives, in order ("
                                     + std::to_string(in_order) + "/" + std::to_string(kMessages) + ")");
    auto retired = wait_for<msg::TopologyChanged>(a.balancer_q, [](const msg::TopologyChanged& t) {
        return t.pt == 0 && !t.added;
    });
    check(retired.has_value() && a.router->pt_count() == 1, "the retired worker stopped once its link had moved");
    check(chat(b, cb, a, ca, "inbound after the move"), "inbound traffic works on the new worker");
    check(chat(a, ca, b, cb, "outbound after the move"), "outbound traffic works on the new worker");
}

void test_quit() {
    // A's listening socket stays open until its Node is destroyed (in the app,
    // until the process exits), so B's connection is still accepted by the
    // kernel; nobody answers its hello. A short handshake timeout makes B give up quickly.
    RouterConfig rb;
    rb.io.handshake_timeout = 300ms;
    Node a, b(rb);
    auto [ca, cb] = connect(a, b);
    const ReqId req = a.request(msg::QuitReq{0});
    check(a.reply(req).value_or(msg::Reply{}).ok, "quit accepted");
    auto done = wait_for<msg::ShutdownDone>(a.frontend, [](const msg::ShutdownDone&) { return true; });
    check(done.has_value(), "A: ShutdownDone after the Router stopped");
    check(b.state(ConvStatus::Disconnected, cb).has_value(), "B sees A go away");
    // done() belongs to the Store's thread: wait for that thread to end (its loop
    // returns once done() is true), join it, then read.
    check(eventually([&] { return a.store_thread->finished(); }), "A's Store thread ends after ShutdownDone");
    a.store_thread->join();
    check(a.store->done(), "A's Store is done");
    const ReqId late = b.request(msg::ReconnectReq{0, cb});
    check(b.reply(late).value_or(msg::Reply{}).ok, "B tries to reconnect");
    auto failed = wait_for<msg::ConvState>(b.frontend, [&](const msg::ConvState& c) {
        return c.conv == cb && c.status == ConvStatus::Failed;
    });
    check(failed && failed->detail.starts_with("handshake_failed"), "A no longer answers: B's attempt fails");
}

}  // namespace

int main() {
    std::cout << "test_router\n";
    test_dial_and_chat();
    test_close_and_reconnect_keep_the_conversation();
    test_simultaneous_reconnect_converges();
    test_dial_refused();
    test_move_keeps_the_stream();
    test_quit();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
