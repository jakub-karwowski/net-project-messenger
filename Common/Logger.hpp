#pragma once

// Logging through one queue, written by a single Logger thread.
//
//   LogCore    the queue plus the shared level filter; one per process.
//   Logger     a cheap handle {core, thread, component} that every thread copies.
//              The level check runs before formatting, so a disabled level costs
//              one atomic load. Logging never throws and never blocks.
//   LogWriter  the Logger thread's body: pops records, writes one line each.
//
// stdout belongs to the frontend, so the sink is a file descriptor (normally a
// log file). Text is escaped on the way out, so every record stays one line
// and nothing in it can drive a terminal that displays the log.

#include "ThreadQueue.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <format>
#include <optional>
#include <string>
#include <string_view>
#include <utility>

enum class LogLevel : std::uint8_t { Debug, Info, Metric, Warn, Error, Fatal, Off };

std::string_view to_string(LogLevel level) noexcept;
std::optional<LogLevel> parse_log_level(std::string_view text) noexcept;

// A name of at most 15 bytes stored inline, so a record never points at memory
// its producer may have freed by the time the Logger writes it.
struct ShortName {
    std::array<char, 16> bytes{};

    ShortName() = default;
    ShortName(std::string_view text) noexcept {   // NOLINT: implicit on purpose
        // Copies at most size() - 1 bytes; the rest stays zero, so the name is
        // always terminated.
        text.copy(bytes.data(), bytes.size() - 1);
    }
    std::string_view view() const noexcept { return std::string_view(bytes.data()); }
    friend bool operator==(const ShortName& a, const ShortName& b) noexcept { return a.view() == b.view(); }
};

struct LogRecord {
    std::chrono::steady_clock::time_point ts{};
    std::chrono::system_clock::time_point wall{};
    ShortName thread;
    LogLevel level = LogLevel::Info;
    ShortName component;
    std::string text;
};

class LogCore {
public:
    explicit LogCore(LogLevel level = LogLevel::Info) noexcept : level_(level) {}

    ThreadQueue<LogRecord>& queue() noexcept { return queue_; }
    LogLevel level() const noexcept { return level_.load(std::memory_order_relaxed); }
    void set_level(LogLevel level) noexcept { level_.store(level, std::memory_order_relaxed); }
    std::chrono::steady_clock::time_point epoch() const noexcept { return epoch_; }

    // The descriptor the active LogWriter writes to, or -1. The Supervisor's
    // hard exit writes its last line here directly, bypassing the queue.
    int raw_fd() const noexcept { return raw_fd_.load(std::memory_order_acquire); }
    void set_raw_fd(int fd) noexcept { raw_fd_.store(fd, std::memory_order_release); }

private:
    ThreadQueue<LogRecord> queue_;
    std::atomic<LogLevel> level_;
    std::atomic<int> raw_fd_{-1};
    const std::chrono::steady_clock::time_point epoch_ = std::chrono::steady_clock::now();
};

class Logger {
public:
    Logger() = default;   // a disabled logger: every call is a no-op
    Logger(LogCore& core, std::string_view thread, std::string_view component = {}) noexcept
        : core_(&core), thread_(thread), component_(component.empty() ? thread : component) {}

    // The same sink and level for another thread (e.g. a worker this thread starts).
    Logger for_thread(std::string_view thread) const noexcept {
        Logger copy = *this;
        copy.thread_ = ShortName(thread);
        copy.component_ = ShortName(thread);
        return copy;
    }

    Logger with_component(std::string_view component) const noexcept {
        Logger copy = *this;
        copy.component_ = ShortName(component);
        return copy;
    }

    bool enabled(LogLevel level) const noexcept {
        return core_ != nullptr && level != LogLevel::Off && level >= core_->level();
    }

    template <class... Args>
    void log(LogLevel level, std::format_string<Args...> fmt, Args&&... args) const noexcept {
        if (!enabled(level)) { return; }
        try {
            emit(level, std::format(fmt, std::forward<Args>(args)...));
        } catch (...) {
            // Logging must never take a worker down (formatting can only throw
            // bad_alloc here); the record is dropped.
        }
    }

    template <class... Args> void debug(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Debug, f, std::forward<Args>(a)...); }
    template <class... Args> void info(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Info, f, std::forward<Args>(a)...); }
    template <class... Args> void metric(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Metric, f, std::forward<Args>(a)...); }
    template <class... Args> void warn(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Warn, f, std::forward<Args>(a)...); }
    template <class... Args> void error(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Error, f, std::forward<Args>(a)...); }
    template <class... Args> void fatal(std::format_string<Args...> f, Args&&... a) const noexcept { log(LogLevel::Fatal, f, std::forward<Args>(a)...); }

    LogCore* core() const noexcept { return core_; }

private:
    void emit(LogLevel level, std::string text) const noexcept;

    LogCore* core_ = nullptr;
    ShortName thread_;
    ShortName component_;
};

// One line per record:
//   2026-11-24T10:00:00.123Z     12.345678 INFO   [store] router: text
// The second column is seconds since `epoch` (monotonic), for ordering and
// latency maths. Control characters (C0, DEL, and the C1 range in UTF-8) are
// escaped as \n, \t, \r or \xNN.
std::string format_log_line(const LogRecord& record, std::chrono::steady_clock::time_point epoch);
void append_log_escaped(std::string& out, std::string_view text);

// The Logger thread's body. Writes everything queued until the queue is shut
// down; a graceful shutdown drains what is left first.
class LogWriter {
public:
    // Writes to `fd`. With `owns_fd` the descriptor is closed by the destructor.
    LogWriter(LogCore& core, int fd, bool owns_fd = false) noexcept;
    ~LogWriter();
    LogWriter(const LogWriter&) = delete;
    LogWriter& operator=(const LogWriter&) = delete;

    void run();

private:
    void write_all(const std::string& bytes) noexcept;

    LogCore& core_;
    int fd_;
    bool owns_fd_;
};

// Opens (creates, appends to) a log file. Returns the descriptor or -1.
int open_log_file(const std::string& path) noexcept;
