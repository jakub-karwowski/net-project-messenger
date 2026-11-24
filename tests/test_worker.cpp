// test_worker.cpp - Worker.hpp: dispatch (with the compile-time handler check),
// run_inbox / drain / done(), queue ticks as messages, and WorkerThread:
// naming, stop called once, exceptions routed to the FatalHandler, and a stop
// function for every kind of blocker (inbox, pool, listener, event + poll).
//
// Plain assertions, no framework. Every blocking wait is bounded, so a hang
// turns into a failed check.

#include "Worker.hpp"

#include "TcpSocket.hpp"
#include "ThreadQueue.hpp"
#include "WakeupEvent.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <mutex>
#include <poll.h>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
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

struct Add { int value = 0; };
struct Finish {};
struct Tick {};
using Msg = std::variant<Add, Finish, Tick>;

struct Summer {
    int total = 0;
    int ticks = 0;
    bool finished = false;
    void on(Add& m) { total += m.value; }
    void on(Finish&) { finished = true; }
    void on(Tick&) { ++ticks; }
    bool done() const { return finished; }
};

// No done(): runs until the inbox closes.
struct Counter {
    int seen = 0;
    void on(Add&) { ++seen; }
    void on(Finish&) { ++seen; }
    void on(Tick&) { ++seen; }
};

struct MissingTick {
    void on(Add&) {}
    void on(Finish&) {}
};

static_assert(worker::Handles<Summer, Add> && worker::Handles<Summer, Tick>);
static_assert(!worker::Handles<MissingTick, Tick>, "a missing handler is detected");
static_assert(worker::ReportsDone<Summer> && !worker::ReportsDone<Counter>);

struct RecordingFatal final : FatalHandler {
    std::mutex mutex;
    std::vector<std::pair<std::string, std::string>> calls;
    void fatal(std::string_view thread, std::string_view what) noexcept override {
        std::scoped_lock lock(mutex);
        calls.emplace_back(std::string(thread), std::string(what));
    }
};

// Waits until `pred` holds or `limit` passes.
template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds limit = 2000ms) {
    const auto until = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < until) {
        if (pred()) { return true; }
        std::this_thread::sleep_for(1ms);
    }
    return pred();
}

void test_dispatch_and_drain() {
    Summer s;
    ThreadQueue<Msg> q;
    std::ignore = q.push(Add{2});
    std::ignore = q.push(Add{3});
    std::ignore = q.push(Finish{});
    std::ignore = q.push(Add{100});
    const auto handled = worker::drain(s, q);
    check(handled == 3, "drain stops right after the message that made done() true");
    check(s.total == 5 && s.finished, "drain dispatched to the right handlers");
    check(q.size() == 1, "messages after done() stay queued");

    Counter c;
    check(worker::drain(c, q) == 1 && c.seen == 1, "drain without done() takes everything queued");
    check(worker::drain(c, q) == 0, "drain on an empty queue returns at once");
}

void test_run_inbox() {
    // Ends on done().
    {
        Summer s;
        ThreadQueue<Msg> q;
        std::ignore = q.push(Add{1});
        std::ignore = q.push(Finish{});
        std::ignore = q.push(Add{50});
        worker::run_inbox(s, q);
        check(s.total == 1 && q.size() == 1, "run_inbox returns after done(), leaving the rest");
    }
    // Ends when the inbox closes; a graceful close still delivers what is queued.
    {
        Counter c;
        ThreadQueue<Msg> q;
        std::ignore = q.push(Add{});
        std::ignore = q.push(Add{});
        q.graceful_shutdown();
        worker::run_inbox(c, q);
        check(c.seen == 2, "run_inbox drains a gracefully closed inbox, then returns");
    }
    // Ticks from a queue timer arrive as ordinary messages.
    {
        Summer s;
        ThreadQueue<Msg> q;
        const auto id = q.every(5ms, Tick{});
        std::thread t([&] { worker::run_inbox(s, q); });
        std::this_thread::sleep_for(50ms);
        q.cancel(id);
        std::ignore = q.push(Finish{});
        t.join();
        check(s.ticks >= 3, "timer ticks are dispatched to on(Tick&) (" + std::to_string(s.ticks) + ")");
    }
}

void test_thread_name_and_finished() {
    std::promise<std::string> name;
    auto got = name.get_future();
    ThreadQueue<Msg> q;
    WorkerThread t("store-worker-long-name", [&] {
        std::array<char, 16> buf{};
        pthread_getname_np(pthread_self(), buf.data(), buf.size());
        name.set_value(buf.data());
        Counter c;
        worker::run_inbox(c, q);
    }, [&] { q.graceful_shutdown(); });
    check(got.wait_for(2s) == std::future_status::ready && got.get() == "store-worker-lo",
          "the thread is named after the worker (truncated to 15 bytes)");
    check(!t.finished(), "finished() is false while the body runs");
    t.request_stop();
    t.join();
    check(t.finished(), "finished() is true once the body returned");
    t.join();   // a second join is a no-op
}

void test_stop_called_once() {
    std::atomic<int> stops{0};
    ThreadQueue<Msg> q;
    {
        WorkerThread t("once", [&] { Counter c; worker::run_inbox(c, q); },
                       [&] { stops.fetch_add(1); q.graceful_shutdown(); });
        std::vector<std::thread> callers;
        for (int i = 0; i < 8; ++i) { callers.emplace_back([&] { t.request_stop(); }); }
        for (auto& c : callers) { c.join(); }
        check(t.stop_requested(), "stop_requested() reports the request");
    }   // the destructor requests again and joins
    check(stops == 1, "stop runs exactly once, whatever the number of callers");
}

void test_exception_goes_to_fatal() {
    RecordingFatal fatal;
    {
        WorkerThread t("boom", [] { throw std::runtime_error("handler bug"); }, {}, &fatal);
        check(eventually([&] { return t.finished(); }), "a thread whose body throws still finishes");
    }
    check(fatal.calls.size() == 1 && fatal.calls[0].first == "boom" && fatal.calls[0].second == "handler bug",
          "the exception reaches the FatalHandler with the thread name and what()");

    RecordingFatal fatal2;
    {
        WorkerThread t("boom2", [] { throw 42; }, {}, &fatal2);
    }
    check(fatal2.calls.size() == 1 && fatal2.calls[0].second == "unknown exception",
          "a non-std exception is reported as 'unknown exception'");
}

// One stop function per kind of blocker. Each body blocks in
// its call; request_stop() must make it return.
void test_stop_per_blocker() {
    {
        ThreadQueue<Msg> q;
        WorkerThread t("inbox", [&] { Counter c; worker::run_inbox(c, q); }, [&] { q.graceful_shutdown(); });
        std::this_thread::sleep_for(10ms);
        t.request_stop();
        check(eventually([&] { return t.finished(); }), "inbox worker: graceful_shutdown() ends pop()");
    }
    {
        EpollConnectionPool pool;
        std::atomic<bool> saw_stopped{false};
        WorkerThread t("pool", [&] {
            std::array<std::byte, 256> buf{};
            for (;;) {
                auto r = pool.receive(buf);
                if (!r && r.error().connection_id == 0 && r.error().error == SocketError::Stopped) {
                    saw_stopped = true;
                    return;
                }
            }
        }, [&] { pool.stop(); });
        std::this_thread::sleep_for(10ms);
        t.request_stop();
        check(eventually([&] { return t.finished(); }) && saw_stopped, "pool worker: pool.stop() ends receive()");
    }
    {
        TcpListener listener("127.0.0.1", 0, 4);
        std::atomic<bool> saw_stopped{false};
        WorkerThread t("listener", [&] {
            for (;;) {
                auto c = listener.accept_client();
                if (!c && c.error() == SocketError::Stopped) { saw_stopped = true; return; }
            }
        }, [&] { listener.stop(); });
        std::this_thread::sleep_for(10ms);
        t.request_stop();
        check(eventually([&] { return t.finished(); }) && saw_stopped,
              "listener worker: listener.stop() ends accept_client()");
    }
    {
        WakeupEvent event;
        std::atomic<bool> stopping{false};
        int pipefd[2];
        check(::pipe(pipefd) == 0, "pipe for the poll test");
        WorkerThread t("reader", [&] {
            while (!stopping) {
                pollfd fds[2] = {{pipefd[0], POLLIN, 0}, {event.fd(), POLLIN, 0}};
                ::poll(fds, 2, -1);
                if (fds[1].revents & POLLIN) { event.clear(); }
            }
        }, [&] { stopping = true; event.signal(); });
        std::this_thread::sleep_for(10ms);
        t.request_stop();
        check(eventually([&] { return t.finished(); }), "poll worker: a WakeupEvent ends poll() on an idle fd");
        ::close(pipefd[0]);
        ::close(pipefd[1]);
    }
}

}  // namespace

int main() {
    std::cout << "test_worker\n";
    test_dispatch_and_drain();
    test_run_inbox();
    test_thread_name_and_finished();
    test_stop_called_once();
    test_exception_goes_to_fatal();
    test_stop_per_blocker();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
