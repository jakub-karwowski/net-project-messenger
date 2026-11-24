#include "Worker.hpp"

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <pthread.h>

WorkerThread::WorkerThread(std::string name, std::function<void()> body, std::function<void()> stop,
                           FatalHandler* fatal)
    : name_(std::move(name)),
      stop_(std::move(stop)),
      fatal_(fatal),
      thread_([this, b = std::move(body)]() mutable { main(std::move(b)); }) {}

WorkerThread::~WorkerThread() {
    request_stop();
    join();
}

void WorkerThread::request_stop() noexcept {
    if (stop_requested_.exchange(true, std::memory_order_acq_rel)) { return; }
    if (!stop_) { return; }
    try {
        stop_();
    } catch (...) {
        // A stop function that throws is a bug in that worker; the thread may
        // now never return. The Supervisor's deadlines still bound the exit.
    }
}

void WorkerThread::join() noexcept {
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) {
        thread_.join();
    }
}

void WorkerThread::main(std::function<void()> body) noexcept {
    // Linux limits thread names to 15 bytes plus the terminator.
    const std::string short_name = name_.substr(0, 15);
    pthread_setname_np(pthread_self(), short_name.c_str());

    const char* failure = nullptr;
    std::string what;
    try {
        body();
    } catch (const std::exception& e) {
        failure = "exception";
        try { what = e.what(); } catch (...) {}
    } catch (...) {
        failure = "unknown exception";
    }

    if (failure != nullptr) {
        const std::string_view text = what.empty() ? std::string_view(failure) : std::string_view(what);
        if (fatal_ != nullptr) {
            fatal_->fatal(name_, text);
        } else {
            std::fprintf(stderr, "fatal: thread %s: %.*s\n", name_.c_str(),
                         static_cast<int>(text.size()), text.data());
            std::abort();
        }
    }
    finished_.store(true, std::memory_order_release);
}
