#include "WakeupEvent.hpp"

#include <cerrno>
#include <cstdint>
#include <sys/eventfd.h>
#include <system_error>
#include <tuple>
#include <unistd.h>
#include <utility>

WakeupEvent::WakeupEvent() : fd_(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)) {
    if (fd_ < 0) { throw std::system_error(errno, std::system_category(), "eventfd"); }
}

WakeupEvent::~WakeupEvent() {
    if (fd_ >= 0) { ::close(fd_); }
}

WakeupEvent::WakeupEvent(WakeupEvent&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

WakeupEvent& WakeupEvent::operator=(WakeupEvent&& other) noexcept {
    if (this == &other) { return *this; }
    if (fd_ >= 0) { ::close(fd_); }
    fd_ = std::exchange(other.fd_, -1);
    return *this;
}

void WakeupEvent::signal() const noexcept {
    const uint64_t one = 1;
    // EAGAIN means the counter is already non-zero: a wake-up is pending anyway.
    std::ignore = ::write(fd_, &one, sizeof(one));
}

void WakeupEvent::clear() const noexcept {
    uint64_t counter = 0;
    // EAGAIN means there was nothing to clear.
    std::ignore = ::read(fd_, &counter, sizeof(counter));
}

int WakeupEvent::fd() const noexcept {
    return fd_;
}
