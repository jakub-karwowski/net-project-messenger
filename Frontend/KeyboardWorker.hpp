#pragma once

// The TUI's input thread: reads the terminal byte by byte and sends each key to
// the frontend. It waits at most `poll_timeout` at a time, so stop() takes
// effect within that.

#include "Logger.hpp"
#include "Messages.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"

#include <atomic>
#include <chrono>

class KeyboardWorker {
public:
    KeyboardWorker(const TerminalUI& terminal, ThreadQueue<msg::FrontendMsg>& out, Logger log,
                   std::chrono::milliseconds poll_timeout = std::chrono::milliseconds{100}) noexcept;

    void run();
    void stop() noexcept { stopping_.store(true, std::memory_order_release); }   // any thread

private:
    // After ESC: the rest of an escape sequence, if it arrives in time.
    void read_sequence();
    void emit(msg::Key key);

    const TerminalUI& terminal_;
    ThreadQueue<msg::FrontendMsg>& out_;
    Logger log_;
    int poll_ms_;
    std::atomic<bool> stopping_{false};
};
