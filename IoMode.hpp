#pragma once

#include <optional>
#include <string_view>

// Runtime I/O variant selected with --io=. See Transport.hpp for what each builds.
enum class IoMode { Threads, PoolSender, Poll, Epoll };

inline std::optional<IoMode> parse_io_mode(std::string_view text) {
    if (text == "threads")     { return IoMode::Threads; }
    if (text == "pool-sender") { return IoMode::PoolSender; }
    if (text == "poll")        { return IoMode::Poll; }
    if (text == "epoll")       { return IoMode::Epoll; }
    return std::nullopt;
}

inline std::string_view to_string(IoMode mode) {
    switch (mode) {
        case IoMode::Threads:    return "threads";
        case IoMode::PoolSender: return "pool-sender";
        case IoMode::Poll:       return "poll";
        case IoMode::Epoll:      return "epoll";
    }
    return "threads";
}
