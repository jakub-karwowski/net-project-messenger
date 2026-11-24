#pragma once

// Writes one line at a time to a descriptor (the CLI's stdout).
//
// write() blocks until the whole line is out. A reader that has gone away
// (EPIPE; SIGPIPE is ignored by the Supervisor) or any other write error closes
// the writer for good: every later write() returns false at once, and the
// frontend treats that as a request to quit.
//
// try_write() never blocks, for the Supervisor's escalation notice while the
// main thread may itself be stuck in write(): it gives up when another write is
// in progress or the descriptor has no room for the whole line.

#include <atomic>
#include <mutex>
#include <string_view>

class JsonlWriter {
public:
    explicit JsonlWriter(int fd) noexcept : fd_(fd) {}

    bool write(std::string_view line) noexcept;
    bool try_write(std::string_view line) noexcept;
    bool closed() const noexcept { return closed_.load(std::memory_order_acquire); }

private:
    bool write_all_locked(std::string_view bytes) noexcept;

    int fd_;
    std::mutex mutex_;
    std::atomic<bool> closed_{false};
};
