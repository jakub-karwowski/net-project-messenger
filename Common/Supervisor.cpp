#include "Supervisor.hpp"

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <poll.h>
#include <pthread.h>
#include <sys/signalfd.h>
#include <system_error>
#include <tuple>
#include <unistd.h>

namespace {

using Clock = std::chrono::steady_clock;

sigset_t handled_signals() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, SIGHUP);
    return set;
}

const char* signal_name(int signo) noexcept {
    switch (signo) {
        case SIGINT: return "SIGINT";
        case SIGTERM: return "SIGTERM";
        case SIGHUP: return "SIGHUP";
        default: return "signal";
    }
}

// write(2) until done or failed; used only on the hard-exit path.
void write_raw(int fd, const char* data, std::size_t size) noexcept {
    if (fd < 0) { return; }
    while (size > 0) {
        const ssize_t n = ::write(fd, data, size);
        if (n > 0) { data += n; size -= static_cast<std::size_t>(n); continue; }
        if (n < 0 && errno == EINTR) { continue; }
        return;
    }
}

}  // namespace

std::string_view to_string(ShutdownPhase phase) noexcept {
    switch (phase) {
        case ShutdownPhase::Running: return "running";
        case ShutdownPhase::Graceful: return "graceful";
        case ShutdownPhase::Emergency: return "emergency";
        case ShutdownPhase::Done: return "done";
    }
    return "?";
}

Supervisor::Supervisor(ShutdownTimings timings, Logger log)
    : timings_(timings), log_(log.with_component("supervisor")) {}

Supervisor::~Supervisor() {
    if (thread_.joinable()) { std::ignore = finish(); }
    if (signal_fd_ >= 0) { ::close(signal_fd_); }
}

void Supervisor::start() {
    const sigset_t set = handled_signals();
    if (const int err = pthread_sigmask(SIG_BLOCK, &set, nullptr); err != 0) {
        throw std::system_error(err, std::generic_category(), "pthread_sigmask");
    }
    std::signal(SIGPIPE, SIG_IGN);
    signal_fd_ = ::signalfd(-1, &set, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signal_fd_ < 0) {
        throw std::system_error(errno, std::generic_category(), "signalfd");
    }
    thread_ = std::thread([this] {
        pthread_setname_np(pthread_self(), "supervisor");
        loop();
    });
}

void Supervisor::register_queue(std::function<void()> force_shutdown) {
    std::scoped_lock lock(mutex_);
    queues_.push_back(std::move(force_shutdown));
}

void Supervisor::register_log_queue(std::function<void()> graceful_shutdown) {
    std::scoped_lock lock(mutex_);
    log_queues_.push_back(std::move(graceful_shutdown));
}

void Supervisor::register_thread(WorkerThread& thread) {
    std::scoped_lock lock(mutex_);
    threads_.push_back(&thread);
}

void Supervisor::set_graceful_trigger(std::function<void()> trigger) {
    std::scoped_lock lock(mutex_);
    graceful_trigger_ = std::move(trigger);
}

void Supervisor::set_terminal_restore(void (*restore)() noexcept) {
    std::scoped_lock lock(mutex_);
    terminal_restore_ = restore;
}

void Supervisor::set_escalation_hook(std::function<void(const Escalation&)> hook) {
    std::scoped_lock lock(mutex_);
    escalation_hook_ = std::move(hook);
}

ShutdownPhase Supervisor::phase() const noexcept {
    std::scoped_lock lock(mutex_);
    return phase_;
}

void Supervisor::begin_shutdown(std::string_view reason) {
    {
        std::scoped_lock lock(mutex_);
        if (phase_ != ShutdownPhase::Running) { return; }
        phase_ = ShutdownPhase::Graceful;
        deadline_ = Clock::now() + timings_.graceful;
        reason_ = reason;
    }
    log_.info("shutdown phase=graceful reason={} deadline_ms={}", reason, timings_.graceful.count());
    wakeup_.signal();
}

void Supervisor::fatal(std::string_view thread, std::string_view what) noexcept {
    log_.fatal("thread {} failed: {}", thread, what);
    try {
        std::string reason = "fatal:";
        reason += thread;
        enter_emergency(std::move(reason), kExitWorkerFatal);
    } catch (...) {
        // Out of memory while failing: nothing graceful is left to try.
        hard_exit("fatal");
    }
}

void Supervisor::enter_emergency(std::string reason, int exit_code) {
    {
        std::scoped_lock lock(mutex_);
        if (phase_ == ShutdownPhase::Emergency || phase_ == ShutdownPhase::Done) { return; }
        phase_ = ShutdownPhase::Emergency;
        deadline_ = Clock::now() + timings_.emergency;
        reason_ = std::move(reason);
        if (exit_code_ == kExitClean) { exit_code_ = exit_code; }
        emergency_pending_ = true;
    }
    wakeup_.signal();
}

std::vector<std::string> Supervisor::stuck_threads() const {
    std::scoped_lock lock(mutex_);
    std::vector<std::string> stuck;
    for (const WorkerThread* t : threads_) {
        if (!t->finished()) { stuck.push_back(t->name()); }
    }
    return stuck;
}

void Supervisor::run_emergency_actions() {
    Escalation esc{ShutdownPhase::Emergency, {}, stuck_threads()};
    std::vector<std::function<void()>> queues, log_queues;
    std::vector<WorkerThread*> threads;
    std::function<void(const Escalation&)> hook;
    {
        std::scoped_lock lock(mutex_);
        esc.reason = reason_;
        queues = queues_;
        log_queues = log_queues_;
        threads = threads_;
        hook = escalation_hook_;
    }

    std::string stuck_list;
    for (const auto& name : esc.stuck) {
        if (!stuck_list.empty()) { stuck_list += ','; }
        stuck_list += name;
    }
    log_.warn("shutdown phase=emergency reason={} stuck=[{}] deadline_ms={}",
              esc.reason, stuck_list, timings_.emergency.count());

    if (hook) {
        try { hook(esc); } catch (...) {}
    }
    for (auto& force : queues) { force(); }
    for (auto& graceful : log_queues) { graceful(); }
    for (WorkerThread* t : threads) { t->request_stop(); }
}

void Supervisor::handle_signal(int signo) {
    int seen = 0;
    ShutdownPhase phase{};
    std::function<void()> trigger;
    {
        std::scoped_lock lock(mutex_);
        seen = ++signals_seen_;
        phase = phase_;
        trigger = graceful_trigger_;
    }
    log_.info("received {} (#{})", signal_name(signo), seen);

    switch (phase) {
        case ShutdownPhase::Running:
            begin_shutdown(signal_name(signo));
            if (trigger) {
                try { trigger(); } catch (...) {}
            }
            break;
        case ShutdownPhase::Graceful:
            enter_emergency("second_signal", kExitShutdownForced);
            break;
        case ShutdownPhase::Emergency:
            hard_exit("third_signal");
        case ShutdownPhase::Done:
            break;
    }
}

void Supervisor::handle_deadline() {
    ShutdownPhase phase{};
    {
        std::scoped_lock lock(mutex_);
        phase = phase_;
    }
    if (phase == ShutdownPhase::Graceful) {
        enter_emergency("graceful_timeout", kExitShutdownForced);
    } else if (phase == ShutdownPhase::Emergency) {
        hard_exit("emergency_timeout");
    }
}

void Supervisor::loop() {
    for (;;) {
        bool run_actions = false;
        int timeout_ms = -1;
        {
            std::scoped_lock lock(mutex_);
            if (phase_ == ShutdownPhase::Done) { return; }
            run_actions = std::exchange(emergency_pending_, false);
            if (phase_ == ShutdownPhase::Graceful || phase_ == ShutdownPhase::Emergency) {
                const auto left = std::chrono::ceil<std::chrono::milliseconds>(deadline_ - Clock::now());
                timeout_ms = static_cast<int>(std::max<std::chrono::milliseconds::rep>(0, left.count()));
            }
        }
        if (run_actions) {
            run_emergency_actions();
            continue;   // recompute: the actions may have taken a while
        }

        pollfd fds[2] = {{signal_fd_, POLLIN, 0}, {wakeup_.fd(), POLLIN, 0}};
        const int ready = ::poll(fds, 2, timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) { continue; }
            log_.error("poll failed: {}", std::strerror(errno));
            continue;
        }
        if (ready == 0) {
            handle_deadline();
            continue;
        }
        if (fds[1].revents & POLLIN) { wakeup_.clear(); }
        if (fds[0].revents & POLLIN) {
            signalfd_siginfo info{};
            while (::read(signal_fd_, &info, sizeof info) == static_cast<ssize_t>(sizeof info)) {
                handle_signal(static_cast<int>(info.ssi_signo));
            }
        }
    }
}

void Supervisor::hard_exit(std::string_view reason) noexcept {
    // Only async-signal-safe calls from here: another thread may hold any lock,
    // so nothing here may allocate or lock. The registered names are read
    // without the mutex; registration finished long before a hard exit.
    if (terminal_restore_ != nullptr) { terminal_restore_(); }

    char line[512];
    int n = std::snprintf(line, sizeof line,
                          "FATAL  [supervisor] supervisor: shutdown phase=hard_exit reason=%.*s stuck=[",
                          static_cast<int>(reason.size()), reason.data());
    bool first = true;
    for (const WorkerThread* t : threads_) {
        if (t->finished() || n <= 0 || static_cast<std::size_t>(n) >= sizeof line - 40) { continue; }
        n += std::snprintf(line + n, sizeof line - static_cast<std::size_t>(n), "%s%s",
                           first ? "" : ",", t->name().c_str());
        first = false;
    }
    if (n > 0 && static_cast<std::size_t>(n) < sizeof line - 3) {
        line[n++] = ']';
        line[n++] = '\n';
        const int log_fd = log_.core() != nullptr ? log_.core()->raw_fd() : -1;
        write_raw(log_fd, line, static_cast<std::size_t>(n));
        if (log_fd != STDERR_FILENO) { write_raw(STDERR_FILENO, line, static_cast<std::size_t>(n)); }
    }
    ::_exit(kExitHard);
}

int Supervisor::finish() {
    {
        std::scoped_lock lock(mutex_);
        phase_ = ShutdownPhase::Done;
    }
    wakeup_.signal();
    if (thread_.joinable() && thread_.get_id() != std::this_thread::get_id()) { thread_.join(); }
    std::scoped_lock lock(mutex_);
    return exit_code_;
}
