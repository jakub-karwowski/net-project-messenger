#pragma once

// The CLI's input thread: reads lines from a descriptor (stdin) and hands each
// to the frontend as InputLine{line_no, text}, then InputClosed at end of file.
//
// Lines end at '\n'; one '\r' before it is dropped. A last line without '\n'
// still counts. Line numbers start at 1 and count blank lines too, since the
// CLI uses them as request ids.
//
// It waits in poll() on the descriptor and a WakeupEvent, so stop() ends it
// even while the input stays open.

#include "Logger.hpp"
#include "Messages.hpp"
#include "ThreadQueue.hpp"
#include "WakeupEvent.hpp"

#include <atomic>
#include <cstdint>
#include <string>

class LineReader {
public:
    LineReader(int fd, ThreadQueue<msg::FrontendMsg>& out, Logger log);   // throws if the wake-up event cannot be created

    void run();
    void stop() noexcept;   // any thread

private:
    void emit_line(std::string line);

    int fd_;
    ThreadQueue<msg::FrontendMsg>& out_;
    Logger log_;
    WakeupEvent wakeup_;
    std::atomic<bool> stopping_{false};
    std::uint64_t line_no_ = 0;
};
