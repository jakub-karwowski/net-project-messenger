#include "LineReader.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <poll.h>
#include <tuple>
#include <unistd.h>
#include <utility>

LineReader::LineReader(int fd, ThreadQueue<msg::FrontendMsg>& out, Logger log)
    : fd_(fd), out_(out), log_(log.with_component("input")) {}

void LineReader::stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    wakeup_.signal();
}

void LineReader::emit_line(std::string line) {
    if (!line.empty() && line.back() == '\r') { line.pop_back(); }
    std::ignore = out_.push(msg::InputLine{++line_no_, std::move(line)});   // false: the frontend has stopped
}

void LineReader::run() {
    std::string pending;
    std::array<char, 64 * 1024> buf{};
    while (!stopping_.load(std::memory_order_acquire)) {
        pollfd fds[2] = {{fd_, POLLIN, 0}, {wakeup_.fd(), POLLIN, 0}};
        if (::poll(fds, 2, -1) < 0) {
            if (errno == EINTR) { continue; }
            log_.error("poll on input failed: {}", std::strerror(errno));
            break;
        }
        if ((fds[1].revents & POLLIN) != 0) { wakeup_.clear(); }
        if (stopping_.load(std::memory_order_acquire)) { return; }
        if ((fds[0].revents & (POLLIN | POLLHUP | POLLERR)) == 0) { continue; }

        const ssize_t n = ::read(fd_, buf.data(), buf.size());
        if (n < 0) {
            if (errno == EINTR || errno == EAGAIN) { continue; }
            log_.error("read from input failed: {}", std::strerror(errno));
            break;
        }
        if (n == 0) { break; }   // end of input

        std::string_view chunk(buf.data(), static_cast<std::size_t>(n));
        for (auto nl = chunk.find('\n'); nl != std::string_view::npos; nl = chunk.find('\n')) {
            pending.append(chunk.substr(0, nl));
            emit_line(std::exchange(pending, {}));
            chunk.remove_prefix(nl + 1);
        }
        pending.append(chunk);
    }
    if (!pending.empty()) { emit_line(std::move(pending)); }
    std::ignore = out_.push(msg::InputClosed{});
}
