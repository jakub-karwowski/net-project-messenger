#pragma once

// The inbox loop every worker runs, and the thread that runs it.
//
//   dispatch(w, msg)      std::visit onto w.on(X&); a missing handler for any
//                         alternative is a compile error that names it.
//   run_inbox(w, inbox, max_batch)
//                         take batches + dispatch until the inbox closes or w.done().
//   drain(w, inbox)       the same without blocking, for single-threaded tests.
//   WorkerThread          owns one std::thread: names it, catches whatever
//                         escapes the body and reports it as fatal, stops it
//                         with the worker's own `stop` function, joins it.

#include "ThreadQueue.hpp"

#include <atomic>
#include <concepts>
#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

// Where a WorkerThread reports a body that ended with an exception.
//
// Workers depend on each other through their queues, so a dead worker leaves
// the rest waiting for messages that never come, and the process cannot shut
// down through its usual message-driven path. A failed worker is therefore a
// process-wide failure. WorkerThread catches the exception (so it never reaches
// std::terminate without a word) and hands it here. The handler decides what
// the process does: the Supervisor logs it and starts an emergency shutdown
// that does not depend on the other workers.
//
// An interface rather than a std::function so Worker.hpp needs neither the
// Supervisor nor the Logger. WorkerThread holds a plain pointer; the handler
// must outlive the thread.
//
// fatal() runs on the failing thread, after its stack has unwound and before
// WorkerThread::finished() turns true, so the failure is recorded before anyone
// can see the thread as finished. It must not throw and must return quickly.
class FatalHandler {
public:
    virtual void fatal(std::string_view thread, std::string_view what) noexcept = 0;

protected:
    ~FatalHandler() = default;
};

namespace worker {

template <class W, class M>
concept Handles = requires(W& w, M& m) { w.on(m); };

// A separate function so the compiler's "required from here" note names `M`,
// the inbox alternative that has no handler.
template <class W, class M>
constexpr void require_handler() {
    static_assert(Handles<W, M>, "worker has no on(M&) handler for this inbox alternative (see M below)");
}

template <class W, class... Ts>
void dispatch(W& w, std::variant<Ts...>& msg) {
    if constexpr ((Handles<W, Ts> && ...)) {
        std::visit([&w](auto& m) { w.on(m); }, msg);
    } else {
        (require_handler<W, Ts>(), ...);
    }
}

template <class W>
concept ReportsDone = requires(const W& w) { { w.done() } -> std::convertible_to<bool>; };

// The thread body of every inbox-driven worker. Ends when the inbox closes
// (pop() returns nullopt) or, for workers that define done(), right after the
// message that made done() true.
//
// Messages are taken in batches of up to `max_batch` (at least 1): one lock and
// at most one wake-up per batch. A due tick is seen once per batch, so the cap
// bounds how late it can be. Whatever is left of a batch when done() turns
// true is dropped, like whatever is still queued.
template <class W, class T>
void run_inbox(W& w, ThreadQueue<T>& inbox, std::size_t max_batch) {
    std::vector<T> batch;
    while (inbox.pop_all(batch, max_batch)) {
        for (T& m : batch) {
            dispatch(w, m);
            if constexpr (ReportsDone<W>) {
                if (w.done()) { return; }
            }
        }
    }
}

// Handles everything queued right now (including due ticks) and returns how
// many messages it handled. Stops early when done() becomes true. For tests.
template <class W, class T>
std::size_t drain(W& w, ThreadQueue<T>& inbox) {
    std::size_t handled = 0;
    while (auto m = inbox.try_pop()) {
        dispatch(w, *m);
        ++handled;
        if constexpr (ReportsDone<W>) {
            if (w.done()) { break; }
        }
    }
    return handled;
}

}  // namespace worker

// One worker thread.
//
// - `body` runs on the new thread. Whatever it throws is caught and passed to
//   `fatal` (or, without one, printed to stderr before std::abort()).
// - `stop` wakes the body so it returns: closing its inbox, stopping its pool,
//   signalling its event. It is called at most once, from whichever thread calls
//   request_stop() first, and must be safe from any thread.
// - The destructor requests a stop and joins.
//
// Neither copyable nor movable: the Supervisor keeps a pointer to it.
class WorkerThread {
public:
    WorkerThread(std::string name, std::function<void()> body, std::function<void()> stop,
                 FatalHandler* fatal = nullptr);
    ~WorkerThread();

    WorkerThread(const WorkerThread&) = delete;
    WorkerThread& operator=(const WorkerThread&) = delete;
    WorkerThread(WorkerThread&&) = delete;
    WorkerThread& operator=(WorkerThread&&) = delete;

    void request_stop() noexcept;
    bool stop_requested() const noexcept { return stop_requested_.load(std::memory_order_acquire); }

    // Joins once; later calls return immediately. Call it from one owning
    // thread, never from the worker thread itself.
    void join() noexcept;

    // True once the body has returned (or thrown). Read by the Supervisor to name
    // the threads that are stuck.
    bool finished() const noexcept { return finished_.load(std::memory_order_acquire); }

    const std::string& name() const noexcept { return name_; }

private:
    void main(std::function<void()> body) noexcept;

    std::string name_;
    std::function<void()> stop_;
    FatalHandler* fatal_;
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> finished_{false};
    std::thread thread_;   // last: started once every other member exists
};
