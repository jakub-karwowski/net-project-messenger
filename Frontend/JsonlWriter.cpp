#include "JsonlWriter.hpp"

#include <cerrno>
#include <climits>
#include <poll.h>
#include <string>
#include <unistd.h>

bool JsonlWriter::write(std::string_view line) noexcept {
    std::scoped_lock lock(mutex_);
    if (closed()) { return false; }
    try {
        std::string bytes;
        bytes.reserve(line.size() + 1);
        bytes.append(line);
        bytes += '\n';
        return write_all_locked(bytes);
    } catch (...) {   // out of memory for the copy
        return false;
    }
}

bool JsonlWriter::try_write(std::string_view line) noexcept {
    std::unique_lock lock(mutex_, std::try_to_lock);
    if (!lock.owns_lock() || closed()) { return false; }
    // A write of up to PIPE_BUF bytes to a pipe is atomic, and POLLOUT means at
    // least that much room: then write() cannot block.
    if (line.size() + 1 > PIPE_BUF) { return false; }
    pollfd p{fd_, POLLOUT, 0};
    if (::poll(&p, 1, 0) != 1 || (p.revents & POLLOUT) == 0) { return false; }
    char buf[PIPE_BUF];
    line.copy(buf, line.size());
    buf[line.size()] = '\n';
    return ::write(fd_, buf, line.size() + 1) == static_cast<ssize_t>(line.size() + 1);
}

bool JsonlWriter::write_all_locked(std::string_view bytes) noexcept {
    while (!bytes.empty()) {
        const ssize_t n = ::write(fd_, bytes.data(), bytes.size());
        if (n > 0) {
            bytes.remove_prefix(static_cast<std::size_t>(n));
            continue;
        }
        if (n < 0 && errno == EINTR) { continue; }
        closed_.store(true, std::memory_order_release);
        return false;
    }
    return true;
}
