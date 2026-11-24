#pragma once

// Shutdown watchdog and signal handling.
//
// Once shutdown starts, the process exits within a bounded time:
//
//   Running ──quit / signal──► Graceful ──deadline──► Emergency ──deadline──► hard exit
//      └──worker exception──────────────────────────► Emergency
//   (a second signal skips from Graceful to Emergency, a third to the hard exit)
//
//   Graceful   the normal shutdown: workers pass the quit along their queues and
//              stop; main joins everything.
//   Emergency  every registered queue is force-closed (the log queue only
//              gracefully, so it drains), every registered thread is asked to
//              stop, main joins.
//   Hard exit  restore the terminal, write one line to the log fd and stderr,
//              _exit(4). No destructors, no locks.
//
// The Supervisor runs on its own thread, because the main thread is one of the
// things that can get stuck (a blocked stdout write, a join that never returns).

#include "Logger.hpp"
#include "WakeupEvent.hpp"
#include "Worker.hpp"

#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

struct ShutdownTimings {
    std::chrono::milliseconds graceful{2000};    // grace + 1 s by default
    std::chrono::milliseconds emergency{1000};

    static ShutdownTimings from_grace(std::chrono::milliseconds grace) {
        return {grace + std::chrono::seconds{1}, std::chrono::seconds{1}};
    }
};

enum class ShutdownPhase : std::uint8_t { Running, Graceful, Emergency, Done };
std::string_view to_string(ShutdownPhase phase) noexcept;

// Process exit codes.
inline constexpr int kExitClean = 0;
inline constexpr int kExitStartupFailed = 1;
inline constexpr int kExitWorkerFatal = 2;
inline constexpr int kExitShutdownForced = 3;
inline constexpr int kExitHard = 4;

// Passed to the escalation hook when shutdown is forced into Emergency.
struct Escalation {
    ShutdownPhase phase;
    std::string reason;
    std::vector<std::string> stuck;   // registered threads that had not finished
};

class Supervisor final : public FatalHandler {
public:
    explicit Supervisor(ShutdownTimings timings = {}, Logger log = {});
    ~Supervisor();   // calls finish() if main has not

    Supervisor(const Supervisor&) = delete;
    Supervisor& operator=(const Supervisor&) = delete;

    // Call on the main thread before any other thread exists: blocks SIGINT,
    // SIGTERM and SIGHUP for this thread and every thread started after it,
    // ignores SIGPIPE, and starts the supervisor thread, which receives those
    // signals through a signalfd.
    void start();

    // Registration. Everything registered must outlive finish().
    void register_queue(std::function<void()> force_shutdown);
    void register_log_queue(std::function<void()> graceful_shutdown);
    void register_thread(WorkerThread& thread);

    // Called (on the supervisor thread) when a signal starts the shutdown;
    // normally pushes QuitReq{0} to the Store. Not called by begin_shutdown().
    void set_graceful_trigger(std::function<void()> trigger);
    // TUI only: restores termios and the screen. Must be async-signal-safe.
    void set_terminal_restore(void (*restore)() noexcept);
    // Called on the supervisor thread when shutdown is forced (CLI:
    // `shutdown_forced` event). Must not block.
    void set_escalation_hook(std::function<void(const Escalation&)> hook);

    // The frontend started the shutdown itself (/quit, EOF, Ctrl+C): arms the
    // graceful deadline. Idempotent; ignored once shutdown is under way.
    void begin_shutdown(std::string_view reason);

    // A worker thread failed: skip Graceful and go straight to Emergency.
    void fatal(std::string_view thread, std::string_view what) noexcept override;

    // Main calls this after every join. Stops the supervisor thread and returns
    // the exit code: 0, kExitWorkerFatal or kExitShutdownForced.
    [[nodiscard]] int finish();

    ShutdownPhase phase() const noexcept;

private:
    void loop();
    void handle_signal(int signo);
    void handle_deadline();
    // Moves to Emergency unless already there or past.
    void enter_emergency(std::string reason, int exit_code);
    void run_emergency_actions();
    [[noreturn]] void hard_exit(std::string_view reason) noexcept;
    std::vector<std::string> stuck_threads() const;   // takes mutex_

    const ShutdownTimings timings_;
    Logger log_;

    mutable std::mutex mutex_;
    ShutdownPhase phase_ = ShutdownPhase::Running;
    std::chrono::steady_clock::time_point deadline_{};
    std::string reason_;
    int exit_code_ = kExitClean;
    bool emergency_pending_ = false;   // actions not yet run by the supervisor thread
    int signals_seen_ = 0;

    std::vector<std::function<void()>> queues_;
    std::vector<std::function<void()>> log_queues_;
    std::vector<WorkerThread*> threads_;
    std::function<void()> graceful_trigger_;
    std::function<void(const Escalation&)> escalation_hook_;
    void (*terminal_restore_)() noexcept = nullptr;

    WakeupEvent wakeup_;
    int signal_fd_ = -1;
    std::thread thread_;
};
