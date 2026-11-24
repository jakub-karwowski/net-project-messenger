// test_supervisor.cpp - the Supervisor: shutdown watchdog and signal handling.
//
// Each scenario runs in a fresh child process (this binary re-executed with
// `--scenario <name> <log path>`), because the paths under test change the signal
// mask, _exit() the process, or receive signals. The parent checks the exit
// code, how long the child took, and what it wrote to its log and stderr.
//
// Timings are shrunk: graceful 300 ms, emergency 200 ms.
//
// Plain assertions, no framework.

#include "Logger.hpp"
#include "Supervisor.hpp"
#include "ThreadQueue.hpp"
#include "Worker.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <map>
#include <stdexcept>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <variant>

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

constexpr auto kGraceful = 300ms;
constexpr auto kEmergency = 200ms;
constexpr auto kProgressMargin = 150ms;
constexpr auto kGracefulCap = 700ms;

// ------------------------------------------------------------------ child side

struct Job { int value = 0; };
struct Block {};   // the handler blocks until the worker's stop function runs
struct Wedge {};   // the handler blocks forever, ignoring any stop
using Inbox = std::variant<Job, Block, Wedge>;

// A worker whose handlers can be made to block, for the stuck-thread cases.
struct TestWorker {
    std::atomic<bool>& released;
    void on(Job&) {}
    void on(Block&) {
        while (!released.load()) { std::this_thread::sleep_for(1ms); }
    }
    void on(Wedge&) {
        for (;;) { std::this_thread::sleep_for(10ms); }
    }
};

// What every scenario shares: a log writer thread and one inbox worker, both
// registered with the Supervisor exactly as App will do it.
struct Child {
    LogCore core{LogLevel::Debug};
    LogWriter writer;
    Supervisor supervisor;
    ThreadQueue<Inbox> inbox;
    std::atomic<bool> released{false};
    TestWorker worker{released};
    std::unique_ptr<WorkerThread> log_thread;
    std::unique_ptr<WorkerThread> worker_thread;

    explicit Child(const char* log_path)
        : writer(core, open_log_file(log_path), true),
          supervisor(ShutdownTimings{kGraceful, kEmergency, kProgressMargin, kGracefulCap}, Logger(core, "main")) {
        supervisor.start();   // before any other thread
        log_thread = std::make_unique<WorkerThread>(
            "logger", [this] { writer.run(); }, [this] { core.queue().graceful_shutdown(); }, &supervisor);
        worker_thread = std::make_unique<WorkerThread>(
            "store", [this] { worker::run_inbox(worker, inbox); },
            [this] { released = true; inbox.graceful_shutdown(); }, &supervisor);
        supervisor.register_queue([this] { inbox.force_shutdown(); });
        supervisor.register_log_queue([this] { core.queue().graceful_shutdown(); });
        // The logger thread is not registered as a thread: it is expected to be
        // running (draining) during an emergency, so it would always be listed
        // as stuck. Its queue is registered with register_log_queue instead.
        supervisor.register_thread(*worker_thread);
    }

    // main's side of the shutdown: join the worker, then the logger, then finish.
    int finish() {
        worker_thread->join();
        core.queue().graceful_shutdown();
        log_thread->join();
        return supervisor.finish();
    }
};

void marker(const char* text) {
    std::ignore = ::write(STDERR_FILENO, text, std::strlen(text));
}

int scenario(const std::string& name, const char* log_path) {
    Child c(log_path);

    if (name == "clean") {
        std::ignore = c.inbox.push(Job{1});
        c.supervisor.begin_shutdown("quit");
        c.inbox.graceful_shutdown();   // the message-driven path finishing on time
        return c.finish();
    }
    if (name == "graceful_timeout") {
        // The worker is stuck in a handler that only its stop function releases.
        std::ignore = c.inbox.push(Block{});
        std::this_thread::sleep_for(20ms);
        c.supervisor.begin_shutdown("quit");
        return c.finish();
    }
    if (name == "hard_exit") {
        c.supervisor.set_terminal_restore([]() noexcept { marker("TERMINAL_RESTORED\n"); });
        std::ignore = c.inbox.push(Wedge{});
        std::this_thread::sleep_for(20ms);
        c.supervisor.begin_shutdown("quit");
        return c.finish();   // never returns: the hard exit ends the process
    }
    if (name == "fatal") {
        WorkerThread boom("boom", [] { throw std::runtime_error("handler bug"); }, {}, &c.supervisor);
        c.supervisor.register_thread(boom);
        boom.join();
        return c.finish();   // the emergency closed the inbox, so this returns
    }
    if (name == "sigterm") {
        c.supervisor.set_graceful_trigger([&c] {
            marker("TRIGGER\n");
            c.inbox.graceful_shutdown();   // stands in for QuitReq → … → ShutdownDone
        });
        ::kill(::getpid(), SIGTERM);
        return c.finish();
    }
    if (name == "second_sigterm") {
        std::ignore = c.inbox.push(Block{});
        c.supervisor.set_graceful_trigger([] { marker("TRIGGER\n"); });
        std::this_thread::sleep_for(20ms);
        ::kill(::getpid(), SIGTERM);
        std::this_thread::sleep_for(50ms);
        ::kill(::getpid(), SIGTERM);   // escalate long before the graceful deadline
        return c.finish();
    }
    if (name == "third_signal") {
        std::ignore = c.inbox.push(Wedge{});
        std::this_thread::sleep_for(20ms);
        for (int i = 0; i < 3; ++i) {
            ::kill(::getpid(), SIGINT);
            std::this_thread::sleep_for(30ms);
        }
        return c.finish();
    }
    if (name == "progress_extends") {
        // The worker is slow but keeps reporting progress, past the graceful
        // deadline (300 ms) though not past the cap (700 ms); then it finishes.
        c.supervisor.begin_shutdown("quit");
        const auto until = Clock::now() + kGraceful * 2;
        while (Clock::now() < until) {
            c.supervisor.progress();
            std::this_thread::sleep_for(50ms);
        }
        c.inbox.graceful_shutdown();
        return c.finish();
    }
    if (name == "progress_capped") {
        // Progress forever is still bounded by the cap.
        std::ignore = c.inbox.push(Block{});
        std::this_thread::sleep_for(20ms);
        c.supervisor.begin_shutdown("quit");
        std::atomic<bool> stop{false};
        std::thread ticker([&] {
            while (!stop) {
                c.supervisor.progress();
                std::this_thread::sleep_for(20ms);
            }
        });
        const int code = c.finish();
        stop = true;
        ticker.join();
        return code;
    }
    if (name == "main_stuck_in_write") {
        // An agent that stopped reading stdout: main blocks in write() forever.
        int fds[2];
        if (::pipe(fds) != 0) { return 99; }
        std::thread killer([] {
            std::this_thread::sleep_for(50ms);
            ::kill(::getpid(), SIGTERM);
        });
        killer.detach();
        std::string chunk(65536, 'x');
        for (;;) { std::ignore = ::write(fds[1], chunk.data(), chunk.size()); }
    }
    std::fprintf(stderr, "unknown scenario %s\n", name.c_str());
    return 98;
}

// ------------------------------------------------------------------ parent side

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

std::string read_file(const std::string& path) {
    std::string out;
    FILE* f = std::fopen(path.c_str(), "r");
    if (f == nullptr) { return out; }
    char buf[4096];
    size_t n;
    while ((n = std::fread(buf, 1, sizeof buf, f)) > 0) { out.append(buf, n); }
    std::fclose(f);
    return out;
}

struct Outcome {
    int exit_code = -1;      // -1: killed or did not exit
    std::chrono::milliseconds elapsed{};
    std::string log;
    std::string err;
};

Outcome run_child(const std::string& self, const std::string& name) {
    char log_name[] = "/tmp/test_supervisor_log_XXXXXX";
    char err_name[] = "/tmp/test_supervisor_err_XXXXXX";
    ::close(::mkstemp(log_name));
    const int err_fd = ::mkstemp(err_name);

    const auto start = Clock::now();
    const pid_t pid = ::fork();
    if (pid == 0) {
        ::dup2(err_fd, STDERR_FILENO);
        ::execl(self.c_str(), self.c_str(), "--scenario", name.c_str(), log_name, static_cast<char*>(nullptr));
        ::_exit(97);
    }
    ::close(err_fd);

    Outcome out;
    int status = 0;
    for (;;) {
        const pid_t r = ::waitpid(pid, &status, WNOHANG);
        if (r == pid) { break; }
        if (Clock::now() - start > 5s) {
            ::kill(pid, SIGKILL);
            ::waitpid(pid, &status, 0);
            std::cout << "  scenario " << name << " hung; killed\n";
            break;
        }
        std::this_thread::sleep_for(2ms);
    }
    out.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
    if (WIFEXITED(status)) { out.exit_code = WEXITSTATUS(status); }
    out.log = read_file(log_name);
    out.err = read_file(err_name);
    ::unlink(log_name);
    ::unlink(err_name);
    return out;
}

bool has(const std::string& text, const std::string& part) { return text.find(part) != std::string::npos; }

void report(const std::string& name, const Outcome& o) {
    std::cout << "  " << name << ": exit=" << o.exit_code << " elapsed=" << o.elapsed.count() << "ms\n";
}

void run_all(const std::string& self) {
    const auto slack = 400ms;   // process start-up, scheduling on a loaded machine

    {
        auto o = run_child(self, "clean");
        report("clean", o);
        check(o.exit_code == kExitClean, "clean: exit 0");
        check(o.elapsed < kGraceful, "clean: finishes before the graceful deadline");
        check(has(o.log, "phase=graceful reason=quit"), "clean: the graceful phase is logged");
        check(!has(o.log, "phase=emergency"), "clean: no escalation");
    }
    {
        auto o = run_child(self, "graceful_timeout");
        report("graceful_timeout", o);
        check(o.exit_code == kExitShutdownForced, "graceful timeout: exit 3");
        check(o.elapsed >= kGraceful && o.elapsed < kGraceful + kEmergency + slack,
              "graceful timeout: escalates at the graceful deadline, then finishes");
        check(has(o.log, "phase=emergency reason=graceful_timeout stuck=[store]"),
              "graceful timeout: the log names the stuck thread");
    }
    {
        auto o = run_child(self, "hard_exit");
        report("hard_exit", o);
        check(o.exit_code == kExitHard, "hard exit: exit 4");
        check(o.elapsed >= kGraceful + kEmergency && o.elapsed < kGraceful + kEmergency + slack,
              "hard exit: happens at graceful + emergency deadlines");
        check(has(o.err, "TERMINAL_RESTORED"), "hard exit: the terminal restore hook runs");
        check(has(o.err, "phase=hard_exit reason=emergency_timeout stuck=[store]"),
              "hard exit: stderr names the stuck thread");
        check(has(o.log, "phase=hard_exit reason=emergency_timeout stuck=[store]"),
              "hard exit: the last line reaches the log file through the raw fd");
    }
    {
        auto o = run_child(self, "fatal");
        report("fatal", o);
        check(o.exit_code == kExitWorkerFatal, "fatal: exit 2");
        check(o.elapsed < kGraceful, "fatal: skips the graceful phase");
        check(has(o.log, "FATAL") && has(o.log, "thread boom failed: handler bug"),
              "fatal: the log names the thread and the exception");
        check(has(o.log, "phase=emergency reason=fatal:boom"), "fatal: emergency is entered for the failure");
    }
    {
        auto o = run_child(self, "sigterm");
        report("sigterm", o);
        check(o.exit_code == kExitClean, "SIGTERM: a clean shutdown, exit 0");
        check(has(o.err, "TRIGGER"), "SIGTERM: the graceful trigger runs");
        check(has(o.log, "received SIGTERM") && has(o.log, "reason=SIGTERM"), "SIGTERM: logged with its reason");
    }
    {
        auto o = run_child(self, "second_sigterm");
        report("second_sigterm", o);
        check(o.exit_code == kExitShutdownForced, "second SIGTERM: exit 3");
        check(o.elapsed < kGraceful, "second SIGTERM: escalates before the graceful deadline");
        check(has(o.log, "phase=emergency reason=second_signal"), "second SIGTERM: logged");
    }
    {
        auto o = run_child(self, "third_signal");
        report("third_signal", o);
        check(o.exit_code == kExitHard, "third signal: hard exit");
        check(o.elapsed < kGraceful, "third signal: does not wait for any deadline");
        check(has(o.err, "reason=third_signal"), "third signal: reason on stderr");
    }
    {
        auto o = run_child(self, "progress_extends");
        report("progress_extends", o);
        check(o.exit_code == kExitClean, "progress: a slow shutdown that keeps moving is not forced, exit 0");
        check(o.elapsed >= kGraceful * 2 - 50ms && !has(o.log, "phase=emergency"),
              "progress: it ran well past the graceful deadline without escalating");
    }
    {
        auto o = run_child(self, "progress_capped");
        report("progress_capped", o);
        check(o.exit_code == kExitShutdownForced, "progress cap: endless progress is still forced, exit 3");
        check(o.elapsed >= kGracefulCap - 50ms && o.elapsed < kGracefulCap + kEmergency + slack,
              "progress cap: at the cap, not before");
    }
    {
        auto o = run_child(self, "main_stuck_in_write");
        report("main_stuck_in_write", o);
        check(o.exit_code == kExitHard, "main stuck in write(): the process still exits (hard exit)");
        check(o.elapsed < kGraceful + kEmergency + slack, "main stuck in write(): within the deadlines");
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc == 4 && std::string(argv[1]) == "--scenario") {
        return scenario(argv[2], argv[3]);
    }
    std::cout << "test_supervisor\n";
    run_all("/proc/self/exe");
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
