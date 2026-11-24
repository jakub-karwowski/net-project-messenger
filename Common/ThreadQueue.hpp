#pragma once

#include <algorithm>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <queue>
#include <type_traits>
#include <utility>
#include <vector>

// A FIFO handing values from any number of producer threads to a consumer.
//
// Timers (optional): the consumer can ask its own queue for a message every
// `period` with every(). pop() then returns that message, built on the spot, as
// soon as it is due, ahead of anything already queued. A tick is an ordinary T,
// so T must be able to hold it (typically: the tick type is one alternative of
// the consumer's std::variant inbox) and the consumer handles it like any other
// message. Because a tick is built when it is due rather than queued in advance,
// a consumer that falls behind gets one tick, never a backlog of them.
//
// Shutdown: after graceful_shutdown() pop() still returns what is queued, then
// nullopt; after force_shutdown() it returns nullopt at once. No ticks are
// produced once either has been called.
template <typename T>
class ThreadQueue {
    public:
        using Clock = std::chrono::steady_clock;
        using TimerId = std::uint64_t;

        ThreadQueue() = default;
        ThreadQueue(const ThreadQueue&) = delete;
        ThreadQueue& operator=(const ThreadQueue&) = delete;
        ~ThreadQueue() {
            graceful_shutdown();
        }

        [[nodiscard]] bool push(T value) {
            {
                std::scoped_lock lock(mutex_);
                if (state_ != QueueState::Open) { return false; }
                queue_.push(std::move(value));
            }
            condition_.notify_one();
            return true;
        }
        template <typename... Args>
        [[nodiscard]] bool emplace(Args&&... args) {
            {
                std::scoped_lock lock(mutex_);
                if (state_ != QueueState::Open) { return false; }
                queue_.emplace(std::forward<Args>(args)...);
            }
            condition_.notify_one();
            return true;
        }

        // Blocks until a timer is due, a value is queued, or the queue is shut down
        // (nullopt; after graceful_shutdown() only once the queue is empty). A due
        // tick is returned before queued values.
        [[nodiscard]] std::optional<T> pop() {
            std::unique_lock<std::mutex> lock(mutex_);
            for (;;) {
                if (auto value = take_locked(Clock::now())) { return value; }
                if (state_ != QueueState::Open) { return std::nullopt; }
                if (timers_.empty()) {
                    condition_.wait(lock);
                } else {
                    condition_.wait_until(lock, earliest_due_locked());
                }
                // Woken by a push, a new timer, a shutdown, a deadline, or
                // spuriously: the top of the loop sorts out which.
            }
        }

        // Non-blocking counterpart of pop(): a due tick or the front value, else
        // std::nullopt at once, without changing the queue's state. Lets the caller
        // drain a burst of work, do its post-processing, then call pop() again.
        [[nodiscard]] std::optional<T> try_pop() {
            std::scoped_lock lock(mutex_);
            return take_locked(Clock::now());
        }

        // Every `period` (> 0; shorter is raised to 1 ms), pop() returns T(tick).
        // The first tick is due one period from now. A tick that is late does not
        // shift the schedule (no drift); one that is late by a whole period or
        // more is not made up (no burst). Safe from any thread.
        template <class Tick>
            requires std::constructible_from<T, const Tick&> && std::copy_constructible<Tick>
        TimerId every(Clock::duration period, Tick tick) {
            period = std::max<Clock::duration>(period, std::chrono::milliseconds{1});
            TimerId id = 0;
            {
                std::scoped_lock lock(mutex_);
                id = next_timer_id_++;
                // The timer keeps the Tick, not a T: T may be move-only.
                timers_.push_back(Timer{Clock::now() + period, period,
                                        [tick = std::move(tick)] { return T(tick); }, id});
            }
            // A pop() waiting for a later deadline (or for no deadline) must look again.
            condition_.notify_all();
            return id;
        }

        // Stops a timer. A tick already returned by pop() stays returned; no new one
        // is made. Unknown ids are ignored. Safe from any thread.
        void cancel(TimerId id) noexcept {
            // erase_if shifts the later timers down by move-assignment. That never
            // copies a tick or allocates: std::function hands over its small inline
            // buffer or its heap pointer. Checked here so `noexcept` stays honest.
            static_assert(std::is_nothrow_move_assignable_v<Timer>);
            std::scoped_lock lock(mutex_);
            std::erase_if(timers_, [id](const Timer& timer) { return timer.id == id; });
        }

        // Current number of queued values, not counting ticks (takes the lock; for
        // metrics, not hot paths).
        [[nodiscard]] std::size_t size() const {
            std::scoped_lock lock(mutex_);
            return queue_.size();
        }
        void graceful_shutdown() noexcept { shutdown(ThreadQueue::QueueState::GracefulShutdown); }
        void force_shutdown() noexcept { shutdown( ThreadQueue::QueueState::ForceShutdown); }
    private:
        enum class QueueState {
            Open,
            GracefulShutdown,
            ForceShutdown,
        };
        struct Timer {
            Clock::time_point due;
            Clock::duration period;
            std::function<T()> make;
            TimerId id;
        };

        std::queue<T> queue_;
        std::vector<Timer> timers_;   // a few per queue: a linear scan beats a heap
        mutable std::mutex mutex_;
        std::condition_variable condition_;
        QueueState state_{QueueState::Open};
        TimerId next_timer_id_{1};

        // The next value pop()/try_pop() should return, if any: nothing after a
        // forced shutdown, a due tick while open, else the front of the queue.
        std::optional<T> take_locked(Clock::time_point now) {
            if (state_ == QueueState::ForceShutdown) { return std::nullopt; }
            if (state_ == QueueState::Open) {
                if (auto tick = take_due_tick_locked(now)) { return tick; }
            }
            if (queue_.empty()) { return std::nullopt; }
            T value = std::move(queue_.front());
            queue_.pop();
            return value;
        }

        // The most overdue timer's tick, rescheduled for its next period.
        std::optional<T> take_due_tick_locked(Clock::time_point now) {
            const auto due = std::min_element(timers_.begin(), timers_.end(),
                [](const Timer& a, const Timer& b) { return a.due < b.due; });
            if (due == timers_.end() || due->due > now) { return std::nullopt; }

            // From the old deadline, so lateness does not accumulate; but never
            // into the past, so a consumer that fell behind gets no burst.
            due->due += due->period;
            if (due->due <= now) { due->due = now + due->period; }
            return due->make();
        }

        Clock::time_point earliest_due_locked() const {
            return std::min_element(timers_.begin(), timers_.end(),
                [](const Timer& a, const Timer& b) { return a.due < b.due; })->due;
        }

        void shutdown(const QueueState state) noexcept {
            {
                std::scoped_lock lock(mutex_);
                if (state_ == QueueState::ForceShutdown) { return; }
                state_ = state;
            }
            condition_.notify_all();
        }
};
