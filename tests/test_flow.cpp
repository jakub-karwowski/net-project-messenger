// test_flow.cpp - the pieces of inbound backpressure that need no network:
// QueueDepth (ThreadQueue::approx_size and the watermarks) and QueueMonitor.
//
// Plain assertions, no framework.

#include "QueueMonitor.hpp"
#include "ThreadQueue.hpp"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <variant>
#include <vector>

namespace {

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

struct Tick {};
struct Work { int value = 0; };
using Msg = std::variant<Work, Tick>;

bool depth_is(const ThreadQueue<Msg>& q, std::size_t n) {
    return q.approx_size() == n && q.size() == n;
}

void test_depth_follows_the_queue() {
    std::cout << "approx_size follows the queue\n";
    ThreadQueue<Msg> q;
    check(depth_is(q, 0), "empty");
    std::ignore = q.push(Work{1});
    std::ignore = q.emplace(Work{2});
    check(depth_is(q, 2), "push and emplace");
    std::ignore = q.try_pop();
    check(depth_is(q, 1), "pop");
    for (int i = 0; i < 300; ++i) { std::ignore = q.push(Work{i}); }
    for (int i = 0; i < 200; ++i) { std::ignore = q.try_pop(); }   // through a compaction
    check(depth_is(q, 101), "pops through a compaction");
    std::vector<Msg> batch;
    std::ignore = q.pop_all(batch, 50);
    check(depth_is(q, 51), "a partial pop_all");
    std::ignore = q.pop_all(batch);
    check(depth_is(q, 0), "a full pop_all");
}

void test_ticks_are_not_counted() {
    std::cout << "ticks are not counted\n";
    ThreadQueue<Msg> q;
    q.every(1ms, Tick{});
    std::ignore = q.push(Work{1});
    std::this_thread::sleep_for(5ms);
    const auto tick = q.try_pop();
    check(tick && std::holds_alternative<Tick>(*tick) && depth_is(q, 1), "a tick leaves the depth alone");
}

void test_depth_after_shutdown() {
    std::cout << "depth after shutdown\n";
    ThreadQueue<Msg> q;
    std::ignore = q.push(Work{1});
    q.graceful_shutdown();
    check(!q.push(Work{2}) && depth_is(q, 1), "a refused push does not count");
    std::ignore = q.try_pop();
    check(depth_is(q, 0), "draining after a graceful shutdown");
}

void test_depth_with_many_producers() {
    std::cout << "depth with many producers\n";
    ThreadQueue<Msg> q;
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p) {
        producers.emplace_back([&q] {
            for (int i = 0; i < 10000; ++i) { std::ignore = q.push(Work{i}); }
        });
    }
    std::size_t taken = 0;
    std::vector<Msg> batch;
    bool bounded = true;
    while (taken < 40000) {
        if (q.pop_all(batch, 64)) { taken += batch.size(); }
        bounded = bounded && q.approx_size() <= 40000 - taken;
    }
    for (auto& t : producers) { t.join(); }
    check(bounded, "never more than what is left to take");
    check(depth_is(q, 0), "zero once everything is taken");
}

void test_watermarks() {
    std::cout << "watermarks\n";
    bool threw = false;
    try { ThreadQueue<Msg> bad; bad.set_watermarks(10, 10); } catch (const std::invalid_argument&) { threw = true; }
    check(threw, "low must be below high");

    ThreadQueue<Msg> q;
    auto fill = [&q](int n) { for (int i = 0; i < n; ++i) { std::ignore = q.push(Work{i}); } };
    auto take = [&q](int n) { for (int i = 0; i < n; ++i) { std::ignore = q.try_pop(); } };
    fill(20);
    check(!q.congested(), "no watermarks: never congested");

    q.set_watermarks(10, 5);
    check(q.congested(), "set on a queue already past high: congested at once");
    take(10);
    check(q.congested(), "between the marks it stays congested");
    take(5);
    check(!q.congested(), "back at low: clear");
    fill(4);
    check(!q.congested(), "between the marks it stays clear");
    fill(1);
    check(q.congested(), "at high: congested");
    std::vector<Msg> batch;
    std::ignore = q.pop_all(batch, 4);
    check(q.congested(), "a pop_all that stays above low leaves it");
    std::ignore = q.pop_all(batch);
    check(!q.congested(), "a pop_all down to empty clears it");
    std::ignore = q.emplace(Work{1});
    q.set_watermarks(0, 0);
    fill(50);
    check(!q.congested(), "high 0 turns the watermarks off");
}

void test_watermarks_with_many_producers() {
    std::cout << "watermarks with many producers\n";
    ThreadQueue<Msg> q;
    q.set_watermarks(1000, 100);
    std::atomic<bool> go{false};
    std::vector<std::thread> producers;
    for (int p = 0; p < 4; ++p) {
        producers.emplace_back([&] {
            while (!go.load()) {}
            for (int i = 0; i < 5000; ++i) { std::ignore = q.push(Work{i}); }
        });
    }
    go = true;
    for (auto& t : producers) { t.join(); }
    check(q.congested() && q.approx_size() == 20000, "20000 queued: congested");
    std::vector<Msg> batch;
    while (q.approx_size() > 100) { std::ignore = q.pop_all(batch, 37); }
    check(!q.congested(), "drained to low: clear");
}

void test_monitor_lines() {
    std::cout << "QueueMonitor lines\n";
    ThreadQueue<Msg> store;
    ThreadQueue<Msg> router;
    QueueMonitor m(Logger{}, 1ms, 3);
    m.watch("store", store);
    m.watch("router", router);
    for (int i = 0; i < 7; ++i) { std::ignore = store.push(Work{i}); }
    check(m.sample().empty(), "no line after one sample of three");
    for (int i = 0; i < 5; ++i) { std::ignore = store.try_pop(); }
    std::ignore = router.push(Work{1});
    check(m.sample().empty(), "nor after two");
    const std::string line = m.sample();
    check(line == "queue_depth store=2 store_max=7 router=1 router_max=1", "the third gives a line: " + line);
    const std::string next = (m.sample(), m.sample(), m.sample());
    check(next == "queue_depth store=2 store_max=2 router=1 router_max=1", "the maximum starts again per line: " + next);
}

void test_monitor_thread() {
    std::cout << "QueueMonitor thread\n";
    ThreadQueue<Msg> q;
    QueueMonitor m(Logger{}, 1ms, 2);
    m.watch("q", q);
    std::thread t([&] { m.run(); });
    std::this_thread::sleep_for(20ms);
    const auto start = std::chrono::steady_clock::now();
    m.stop();
    t.join();
    check(std::chrono::steady_clock::now() - start < 500ms, "stop() ends run() promptly");
}

}  // namespace

int main() {
    test_depth_follows_the_queue();
    test_ticks_are_not_counted();
    test_depth_after_shutdown();
    test_depth_with_many_producers();
    test_watermarks();
    test_watermarks_with_many_producers();
    test_monitor_lines();
    test_monitor_thread();
    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
