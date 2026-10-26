#pragma once

#include <optional>
#include <string_view>

// Runtime I/O variant selected with --io=. See Transport.hpp for what each builds.
enum class IoMode { Threads, PoolSender, Poll, Epoll,
                         PollDuplex, EpollDuplex, PollSplit, EpollSplit, PeerThreads };

inline std::optional<IoMode> parse_io_mode(std::string_view text) {
    if (text == "threads")     { return IoMode::Threads; }
    if (text == "pool-sender") { return IoMode::PoolSender; }
    if (text == "poll")        { return IoMode::Poll; }
    if (text == "epoll")       { return IoMode::Epoll; }
    if (text == "poll-duplex")  { return IoMode::PollDuplex; }
    if (text == "epoll-duplex") { return IoMode::EpollDuplex; }
    if (text == "poll-split")   { return IoMode::PollSplit; }
    if (text == "epoll-split")  { return IoMode::EpollSplit; }
    if (text == "peer-threads") { return IoMode::PeerThreads; }
    return std::nullopt;
}

inline std::string_view to_string(IoMode mode) {
    switch (mode) {
        case IoMode::Threads:    return "threads";
        case IoMode::PoolSender: return "pool-sender";
        case IoMode::Poll:       return "poll";
        case IoMode::Epoll:      return "epoll";
        case IoMode::PollDuplex:  return "poll-duplex";
        case IoMode::EpollDuplex: return "epoll-duplex";
        case IoMode::PollSplit:   return "poll-split";
        case IoMode::EpollSplit:  return "epoll-split";
        case IoMode::PeerThreads: return "peer-threads";
    }
    return "threads";
}
