#pragma once

// Test harness: complete nodes in one process, over loopback, and helpers to
// wait for their events. Used by test_router and test_balancer_e2e.

#include "BalancerWorker.hpp"
#include "ListenerWorker.hpp"
#include "RouterWorker.hpp"
#include "StoreWorker.hpp"
#include "Worker.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <variant>

namespace harness {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using msg::ConvStatus;
using Sub = msg::Subscription;

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

// One complete node: Store, Router (with its IO workers), Listener, and
// optionally a Balancer, each on its own thread. Without a Balancer nobody
// reads balancer_q, so a test may read it.
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
    std::optional<BalancerWorker> balancer;
    std::unique_ptr<WorkerThread> store_thread;
    std::unique_ptr<WorkerThread> router_thread;
    std::unique_ptr<WorkerThread> listener_thread;
    std::unique_ptr<WorkerThread> balancer_thread;
    ReqId next_req = 1;

    explicit Node(RouterConfig rc = {}, std::optional<balance::Config> bc = std::nullopt) {
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
        if (bc) {
            balancer.emplace(*bc, balancer_q, router_q, Logger{});
            balancer_thread = std::make_unique<WorkerThread>("balancer", [this] { worker::run_inbox(*balancer, balancer_q); },
                                                             [this] { balancer_q.graceful_shutdown(); });
        }
    }

    ~Node() {
        // Reverse order of start: Balancer, intake, Router (stops its IO workers), Store.
        balancer_thread.reset();
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


}  // namespace harness
