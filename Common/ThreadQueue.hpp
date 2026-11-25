#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
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
// Batches: pop_all() hands the consumer everything queued in one go, under one
// lock, so a busy consumer pays for the lock and the wake-up once per batch
// rather than once per value.
//
// Shutdown: after graceful_shutdown() pop() still returns what is queued, then
// nullopt; after force_shutdown() it returns nullopt at once. No ticks are
// produced once either has been called. pop_all() follows the same rules.
// How full a queue is, readable from any thread without the queue's lock: for
// producers that throttle on a consumer's backlog, and for monitoring. The
// queue publishes its exact count after every change, under its lock; a reader
// sees a recent value.
//
// Watermarks (optional): with set_watermarks(high, low), congested() turns on
// when the queue reaches `high` and off only once it is back at `low`. The
// switch happens under the queue's lock, at the crossing itself, so every
// reader sees the same answer, and a queue hovering around one value does not
// flip it on every message.
class QueueDepth {
    public:
        [[nodiscard]] std::size_t approx_size() const noexcept { return depth_.load(std::memory_order_relaxed); }
        [[nodiscard]] bool congested() const noexcept { return congested_.load(std::memory_order_relaxed); }

    protected:
        QueueDepth() = default;
        ~QueueDepth() = default;
        QueueDepth(const QueueDepth&) = delete;
        QueueDepth& operator=(const QueueDepth&) = delete;

        // Both require the queue's lock.
        void publish_depth(std::size_t n) noexcept {
            depth_.store(n, std::memory_order_relaxed);
            if (high_ == 0) { return; }
            if (n >= high_) {
                congested_.store(true, std::memory_order_relaxed);
            } else if (n <= low_) {
                congested_.store(false, std::memory_order_relaxed);
            }
        }
        void set_depth_watermarks(std::size_t high, std::size_t low) noexcept {
            high_ = high;
            low_ = low;
            const std::size_t n = depth_.load(std::memory_order_relaxed);
            congested_.store(high_ != 0 && n >= high_, std::memory_order_relaxed);
        }

    private:
        // A cache line of their own: every push and pop writes them, and readers
        // on other cores should not pull the queue's other members along.
        alignas(64) std::atomic<std::size_t> depth_{0};
        std::atomic<bool> congested_{false};
        std::size_t high_ = 0;   // 0: no watermarks
        std::size_t low_ = 0;
};

template <typename T>
class ThreadQueue : public QueueDepth {
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
                queue_.push_back(std::move(value));
                publish_depth_locked();
            }
            condition_.notify_one();
            return true;
        }
        template <typename... Args>
        [[nodiscard]] bool emplace(Args&&... args) {
            {
                std::scoped_lock lock(mutex_);
                if (state_ != QueueState::Open) { return false; }
                queue_.emplace_back(std::forward<Args>(args)...);
                publish_depth_locked();
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

        // Blocks like pop(), then replaces `out` with the due ticks followed by up to
        // `max` (at least 1) queued values, in order. Returns false, with `out`
        // empty, when pop() would return nullopt.
        //
        // `out` is cleared before the lock is taken, so the values it held are
        // destroyed outside it. When the whole queue fits, `out` and the queue swap
        // storage: passing the same vector every time, the two keep trading the
        // same buffers and a steady consumer allocates nothing.
        [[nodiscard]] bool pop_all(std::vector<T>& out, std::size_t max = std::numeric_limits<size_t>::max()) {
            out.clear();
            max = std::max<std::size_t>(max, 1);
            std::unique_lock<std::mutex> lock(mutex_);
            for (;;) {
                if (state_ == QueueState::ForceShutdown) { return false; }
                if (state_ == QueueState::Open) {
                    // Each tick taken is rescheduled past now, so this ends.
                    while (auto tick = take_due_tick_locked(Clock::now())) { out.push_back(std::move(*tick)); }
                }
                const std::size_t queued = queue_.size() - head_;
                if (queued != 0) {
                    if (out.empty() && head_ == 0 && queued <= max) {
                        std::swap(out, queue_);
                    } else {
                        const auto first = queue_.begin() + static_cast<std::ptrdiff_t>(head_);
                        const auto count = static_cast<std::ptrdiff_t>(std::min(queued, max));
                        std::ranges::move(first, first + count, std::back_inserter(out));
                        head_ += static_cast<std::size_t>(count);
                        compact_locked();
                    }
                    publish_depth_locked();
                }
                if (!out.empty()) { return true; }
                if (state_ != QueueState::Open) { return false; }
                if (timers_.empty()) {
                    condition_.wait(lock);
                } else {
                    condition_.wait_until(lock, earliest_due_locked());
                }
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

        // Turns on congested() (see QueueDepth). Requires low < high; high 0
        // turns the watermarks off. Safe from any thread.
        void set_watermarks(std::size_t high, std::size_t low) {
            if (high != 0 && low >= high) {
                throw std::invalid_argument("ThreadQueue: the low watermark must be below the high one");
            }
            std::scoped_lock lock(mutex_);
            set_depth_watermarks(high, low);
        }

        // Current number of queued values, not counting ticks. Takes the lock;
        // approx_size() does not.
        [[nodiscard]] std::size_t size() const {
            std::scoped_lock lock(mutex_);
            return queue_.size() - head_;
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

        // Values from head_ on are queued; those before it were taken by pop() and
        // are erased by compact_locked().
        std::vector<T> queue_;
        std::size_t head_ = 0;
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
            if (head_ == queue_.size()) { return std::nullopt; }
            T value = std::move(queue_[head_++]);
            compact_locked();
            publish_depth_locked();
            return value;
        }

        void publish_depth_locked() noexcept { publish_depth(queue_.size() - head_); }

        // Keeps the taken prefix from growing without bound: dropped at once when
        // nothing is left after it, and moved out once it is most of the vector
        // (an amortised O(1) cost per value).
        void compact_locked() {
            if (head_ == queue_.size()) {
                queue_.clear();
                head_ = 0;
            } else if (head_ >= 64 && head_ * 2 >= queue_.size()) {
                queue_.erase(queue_.begin(), queue_.begin() + static_cast<std::ptrdiff_t>(head_));
                head_ = 0;
            }
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
