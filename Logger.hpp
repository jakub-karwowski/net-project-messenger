#pragma once

#include "ThreadQueue.hpp"
#include <cstdint>
#include <string>
#include <utility>

// Diagnostics channel, separate from the user-facing EventError (which lands in
// the chat UI). Records are pushed onto one shared queue and consumed by the
// single LoggerWorker, so log lines never interleave and never touch the terminal
// the render thread is drawing on.
enum class LogLevel : uint8_t { Info, Warn, Error };

struct LogRecord {
    LogLevel level{LogLevel::Info};
    std::string message;
};

// A cheap, copyable producer handle onto the shared log queue. A default Logger
// has no queue and silently drops every record, so workers can be built without
// a logger.
class Logger {
public:
    Logger() = default;
    explicit Logger(ThreadQueue<LogRecord>* logger_queue) noexcept : queue_(logger_queue) {}

    void info(std::string message) const { log(LogLevel::Info, std::move(message)); }
    void warn(std::string message) const { log(LogLevel::Warn, std::move(message)); }
    void error(std::string message) const { log(LogLevel::Error, std::move(message)); }
    void log(LogLevel level, std::string message) const {
        if (queue_ != nullptr) { (void)queue_->emplace(LogRecord{level, std::move(message)}); }
    }

private:
    ThreadQueue<LogRecord>* queue_{nullptr};
};
