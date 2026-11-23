// test_queue.cpp - ThreadQueue: FIFO and shutdown behaviour, and the timers
// (every()/cancel()) that let a consumer receive ticks as ordinary messages.
//
// Plain assertions, no framework. Every blocking pop() is guarded by a watchdog
// that force-shuts the queue, so a hang turns into a failed check.

#include "ThreadQueue.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <variant>

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

struct Tick { int timer = 0; };
struct Work { int value = 0; };
using Msg = std::variant<Work, Tick>;

bool is_tick(const std::optional<Msg>& m, int timer = -1) {
    return m && std::holds_alternative<Tick>(*m) && (timer < 0 || std::get<Tick>(*m).timer == timer);
}
bool is_work(const std::optional<Msg>& m, int value) {
    return m && std::holds_alternative<Work>(*m) && std::get<Work>(*m).value == value;
}

// Force-shuts `queue` after `limit` unless destroyed first.
template <class Q>
class Watchdog {
    public:
        Watchdog(Q& queue, std::chrono::milliseconds limit)
            : thread_([this, &queue, limit] {
                  std::unique_lock lock(mutex_);
                  if (!cv_.wait_for(lock, limit, [this] { return done_; })) {
                      fired_ = true;
                      queue.force_shutdown();
                  }
              }) {}
        ~Watchdog() {
            { std::lock_guard lock(mutex_); done_ = true; }
            cv_.notify_all();
            thread_.join();
            if (fired_) { check(false, "watchdog fired: pop() did not return in time"); }
        }
    private:
        std::mutex mutex_;
        std::condition_variable cv_;
        bool done_ = false;
        bool fired_ = false;
        std::thread thread_;
};

void run(const char* title, const std::function<void()>& test) {
    std::cout << title << '\n';
    test();
}

// --- existing behaviour ------------------------------------------------------

void test_fifo() {
    ThreadQueue<int> q;
    check(q.push(1) && q.push(2) && q.emplace(3), "push/emplace while open");
    check(q.size() == 3, "size counts queued values");
    check(q.pop() == 1 && q.pop() == 2 && q.try_pop() == 3, "values come out in order");
    check(!q.try_pop().has_value(), "try_pop on an empty queue returns at once");
}

void test_graceful_shutdown_drains() {
    ThreadQueue<int> q;
    std::ignore = q.push(1);
    std::ignore = q.push(2);
    q.graceful_shutdown();
    check(!q.push(3), "push is refused after shutdown");
    check(q.pop() == 1 && q.pop() == 2, "queued values are still delivered");
    check(!q.pop().has_value(), "then pop() reports the end");
}

void test_force_shutdown_discards() {
    ThreadQueue<int> q;
    std::ignore = q.push(1);
    q.force_shutdown();
    check(!q.pop().has_value(), "pop() returns nothing at once after a forced shutdown");
}

void test_shutdown_wakes_blocked_pop() {
    ThreadQueue<int> q;
    std::thread t([&] { std::this_thread::sleep_for(50ms); q.graceful_shutdown(); });
    Watchdog dog(q, 2s);
    check(!q.pop().has_value(), "a blocked pop() returns when the queue shuts down");
    t.join();
}

// --- timers --------------------------------------------------------------------

void test_tick_arrives_after_period() {
    ThreadQueue<Msg> q;
    q.every(60ms, Tick{1});
    Watchdog dog(q, 2s);
    const auto start = Clock::now();
    const auto m = q.pop();
    const auto elapsed = Clock::now() - start;
    check(is_tick(m, 1), "pop() returns the tick");
    check(elapsed >= 55ms && elapsed < 1s, "after about one period");
}

void test_ticks_go_first() {
    ThreadQueue<Msg> q;
    q.every(20ms, Tick{1});
    std::this_thread::sleep_for(50ms);   // the tick is due now
    std::ignore = q.push(Work{7});
    check(is_tick(q.try_pop(), 1), "a due tick comes before queued work");
    check(is_work(q.try_pop(), 7), "then the work");
}

void test_late_consumer_gets_one_tick() {
    ThreadQueue<Msg> q;
    q.every(10ms, Tick{1});
    std::this_thread::sleep_for(200ms);   // 20 periods late
    check(is_tick(q.try_pop(), 1), "one tick for the late consumer");
    check(!q.try_pop().has_value(), "and no burst of missed ones");
}

void test_ticks_keep_their_rate() {
    ThreadQueue<Msg> q;
    q.every(40ms, Tick{1});
    Watchdog dog(q, 3s);
    const auto start = Clock::now();
    int ticks = 0;
    while (ticks < 5 && is_tick(q.pop(), 1)) { ++ticks; }
    const auto elapsed = Clock::now() - start;
    check(ticks == 5, "five ticks");
    check(elapsed >= 190ms && elapsed < 600ms, "at the timer's rate");
}

void test_several_timers() {
    ThreadQueue<Msg> q;
    q.every(30ms, Tick{1});
    q.every(70ms, Tick{2});
    Watchdog dog(q, 3s);
    int ones = 0, twos = 0;
    const auto until = Clock::now() + 300ms;
    while (Clock::now() < until) {
        const auto m = q.pop();
        if (is_tick(m, 1)) { ++ones; } else if (is_tick(m, 2)) { ++twos; } else { break; }
    }
    check(ones >= 7 && ones <= 11, "the fast timer ticks about 10 times (" + std::to_string(ones) + ")");
    check(twos >= 3 && twos <= 5, "the slow timer ticks about 4 times (" + std::to_string(twos) + ")");
}

void test_cancel() {
    ThreadQueue<Msg> q;
    const auto id = q.every(20ms, Tick{1});
    q.cancel(id);
    q.cancel(id);       // twice: ignored
    q.cancel(12345);    // unknown: ignored
    std::thread t([&] { std::this_thread::sleep_for(100ms); std::ignore = q.push(Work{1}); });
    Watchdog dog(q, 2s);
    check(is_work(q.pop(), 1), "a cancelled timer no longer ticks");
    t.join();
}

void test_timer_added_while_waiting() {
    ThreadQueue<Msg> q;
    std::thread t([&] { std::this_thread::sleep_for(50ms); q.every(30ms, Tick{3}); });
    Watchdog dog(q, 2s);
    const auto start = Clock::now();
    const auto m = q.pop();   // blocked with no timer when every() is called
    const auto elapsed = Clock::now() - start;
    t.join();
    check(is_tick(m, 3), "a pop() that was waiting sees the new timer");
    check(elapsed >= 75ms && elapsed < 1s, "and returns its first tick on time");
}

void test_no_ticks_after_shutdown() {
    ThreadQueue<Msg> q;
    q.every(10ms, Tick{1});
    std::ignore = q.push(Work{1});
    std::this_thread::sleep_for(30ms);
    q.graceful_shutdown();
    Watchdog dog(q, 2s);
    check(is_work(q.pop(), 1), "queued work is still drained");
    check(!q.pop().has_value(), "but no tick is produced after shutdown");
}

// A tick lands in a move-only inbox: the timer keeps the Tick, never a T.
void test_move_only_inbox() {
    using MoveOnlyMsg = std::variant<std::unique_ptr<int>, Tick>;
    ThreadQueue<MoveOnlyMsg> q;
    q.every(10ms, Tick{5});
    std::ignore = q.push(std::make_unique<int>(9));
    std::this_thread::sleep_for(20ms);
    const auto first = q.try_pop();
    check(first && std::holds_alternative<Tick>(*first), "a tick in a move-only inbox");
    const auto second = q.try_pop();
    check(second && std::holds_alternative<std::unique_ptr<int>>(*second)
              && *std::get<std::unique_ptr<int>>(*second) == 9,
          "the move-only value after it");
}

// every() only accepts a tick the inbox can hold.
template <class Q, class TickT>
concept CanTick = requires(Q& q, TickT t) { q.every(std::chrono::milliseconds{1}, t); };
struct Unrelated {};
static_assert(CanTick<ThreadQueue<Msg>, Tick>);
static_assert(!CanTick<ThreadQueue<Msg>, Unrelated>);
static_assert(!CanTick<ThreadQueue<int>, Tick>);

}  // namespace

int main() {
    run("FIFO order", test_fifo);
    run("graceful shutdown drains", test_graceful_shutdown_drains);
    run("forced shutdown discards", test_force_shutdown_discards);
    run("shutdown wakes a blocked pop", test_shutdown_wakes_blocked_pop);
    run("timer: tick after one period", test_tick_arrives_after_period);
    run("timer: ticks go before queued work", test_ticks_go_first);
    run("timer: a late consumer gets one tick", test_late_consumer_gets_one_tick);
    run("timer: ticks keep their rate", test_ticks_keep_their_rate);
    run("timer: several timers", test_several_timers);
    run("timer: cancel", test_cancel);
    run("timer: added while pop() waits", test_timer_added_while_waiting);
    run("timer: no ticks after shutdown", test_no_ticks_after_shutdown);
    run("timer: move-only inbox", test_move_only_inbox);

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
