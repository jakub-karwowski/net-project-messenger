#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <utility>

template <typename T>
class ThreadQueue {
    public:
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
        [[nodiscard]] std::optional<T> pop() {
            std::unique_lock<std::mutex> lock(mutex_);
            condition_.wait(lock, [this] { return !queue_.empty() || (state_ != QueueState::Open);});
            if (state_ == QueueState::ForceShutdown) {
                return std::nullopt;
            }
            if (!queue_.empty()) {
                T value = std::move(queue_.front());
                queue_.pop();
                return value;
            }
            return std::nullopt;
        }
        void graceful_shutdown() noexcept { shutdown(ThreadQueue::QueueState::GracefulShutdown); }
        void force_shutdown() noexcept { shutdown( ThreadQueue::QueueState::ForceShutdown); }
    private:
        enum class QueueState { 
            Open,
            GracefulShutdown,
            ForceShutdown,
        };
        std::queue<T> queue_;
        std::mutex mutex_;
        std::condition_variable condition_;
        QueueState state_{QueueState::Open};

        void shutdown(const QueueState state) noexcept {
            {
                std::scoped_lock lock(mutex_);
                if (state_ == QueueState::ForceShutdown) { return; }
                state_ = state;
            }
            condition_.notify_all();
        }
};