#include "TxPoller.hpp"

#include <cerrno>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

// ---- PollBackend ----------------------------------------------------------

void PollBackend::arm(int id, int fd) { set_.upsert(id, fd, POLLOUT); }
void PollBackend::disarm(int id, int) noexcept { set_.erase(id); }
void PollBackend::wake() const noexcept { set_.wake(); }

void PollBackend::wait(std::vector<int>& ready) {
    const int n = set_.wait(-1);
    set_.drain_wake();
    if (n <= 0) { return; }
    set_.for_each_ready([&](int id, short) {
        ready.push_back(id);
        return false;
    });
}

// ---- EpollBackend ---------------------------------------------------------

namespace {
constexpr std::uint64_t kWakeToken = 0;
}

EpollBackend::EpollBackend() {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) { throw SocketException(SocketError::Unknown); }
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        ::close(epoll_fd_);
        throw SocketException(SocketError::Unknown);
    }
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.u64 = kWakeToken;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &ev) < 0) {
        ::close(wake_fd_);
        ::close(epoll_fd_);
        throw SocketException(SocketError::Unknown);
    }
}

EpollBackend::~EpollBackend() {
    if (wake_fd_ >= 0) { ::close(wake_fd_); }
    if (epoll_fd_ >= 0) { ::close(epoll_fd_); }
}

void EpollBackend::arm(int id, int fd) {
    epoll_event ev{};
    ev.events = EPOLLOUT;
    ev.data.u64 = static_cast<std::uint64_t>(id) + 1; // 0 is the wake token
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev);
}

void EpollBackend::disarm(int, int fd) noexcept {
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

void EpollBackend::wake() const noexcept {
    const std::uint64_t one = 1;
    [[maybe_unused]] const ssize_t w = ::write(wake_fd_, &one, sizeof(one));
}

void EpollBackend::wait(std::vector<int>& ready) {
    epoll_event events[64];
    const int n = ::epoll_wait(epoll_fd_, events, 64, -1);
    for (int i = 0; i < n; ++i) {
        if (events[i].data.u64 == kWakeToken) {
            std::uint64_t v = 0;
            while (::read(wake_fd_, &v, sizeof(v)) > 0) {}
            continue;
        }
        ready.push_back(static_cast<int>(events[i].data.u64 - 1));
    }
}

// ---- TxPoller -------------------------------------------------------------

template <class Backend>
TxPoller<Backend>::TxPoller(std::size_t max_tx_buffer_bytes)
    : max_tx_buffer_bytes_(max_tx_buffer_bytes) {}

template <class Backend>
TxPoller<Backend>::~TxPoller() {
    close();
    std::lock_guard lock(mutex_);
    for (auto it = entries_.begin(); it != entries_.end();) {
        disarm_locked(it->first, it->second);
        it = entries_.erase(it);
    }
}

template <class Backend>
void TxPoller<Backend>::arm_locked(int id, Entry& entry) noexcept {
    if (entry.armed) { return; }
    try {
        backend_.arm(id, entry.connection->native_handle());
        entry.armed = true;
    } catch (...) {}
    backend_.wake();
}

template <class Backend>
void TxPoller<Backend>::disarm_locked(int id, Entry& entry) noexcept {
    if (!entry.armed) { return; }
    backend_.disarm(id, entry.connection->native_handle());
    entry.armed = false;
}

template <class Backend>
void TxPoller<Backend>::drop_locked(Map::iterator it, bool shutdown_socket) noexcept {
    disarm_locked(it->first, it->second);              // out of the set first ...
    if (shutdown_socket) {
        (void)it->second.connection->shutdown(TcpConnection::Shutdown::Both);
    }
    entries_.erase(it);                                // ... then release the ref
    backend_.wake();
}

template <class Backend>
bool TxPoller<Backend>::add(int id, std::shared_ptr<TcpConnection> connection) {
    if (id < 0 || !connection || connection->native_handle() < 0) { return false; }
    // Only touch the options when needed: the rx pool may already be reading.
    if (!connection->options().non_blocking) { connection->with_non_blocking(true); }
    if (!connection->apply_options_now()) { return false; }

    std::lock_guard lock(mutex_);
    if (closed_.load(std::memory_order_relaxed)) { return false; }
    return entries_.try_emplace(id, Entry{std::move(connection), {}, 0, false}).second;
}

template <class Backend>
bool TxPoller<Backend>::flush_locked(Entry& entry) noexcept {
    while (entry.tx_offset < entry.tx_buffer.size()) {
        const std::span<const std::byte> pending{entry.tx_buffer.data() + entry.tx_offset,
                                                 entry.tx_buffer.size() - entry.tx_offset};
        const auto sent = entry.connection->send_once(pending, MSG_DONTWAIT);
        if (!sent) {
            if (sent.error() == SocketError::Interrupted) { continue; }
            if (sent.error() == SocketError::WouldBlock) { return true; }
            return false;
        }
        if (*sent == 0) { return false; }
        entry.tx_offset += *sent;
    }
    entry.tx_buffer.clear();
    entry.tx_offset = 0;
    return true;
}

template <class Backend>
std::expected<void, SocketError> TxPoller<Backend>::send(
    int id, std::span<const std::byte> source) noexcept {
    std::lock_guard lock(mutex_);
    if (closed_.load(std::memory_order_relaxed)) {
        return std::unexpected(SocketError::ConnectionClosed);
    }
    const auto it = entries_.find(id);
    if (it == entries_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    if (source.empty()) { return {}; }

    Entry& entry = it->second;
    const std::size_t pending = entry.tx_buffer.size() - entry.tx_offset;
    if (source.size_bytes() > max_tx_buffer_bytes_
        || (pending != 0 && source.size_bytes() > max_tx_buffer_bytes_ - pending)) {
        return std::unexpected(SocketError::WouldBlock);
    }

    std::size_t consumed = 0;
    if (pending == 0) {
        entry.tx_buffer.clear();
        entry.tx_offset = 0;
        while (consumed < source.size_bytes()) {
            const auto sent = entry.connection->send_once(source.subspan(consumed), MSG_DONTWAIT);
            if (sent && *sent > 0) { consumed += *sent; continue; }
            if (!sent && sent.error() == SocketError::Interrupted) { continue; }
            if (!sent && sent.error() == SocketError::WouldBlock) { break; }
            const SocketError error = sent ? SocketError::ConnectionClosed : sent.error();
            drop_locked(it, true);
            return std::unexpected(error);
        }
        if (consumed == source.size_bytes()) { return {}; }
    }

    queued_sends_.fetch_add(1, std::memory_order_relaxed);
    try {
        if (entry.tx_offset > 0 && entry.tx_offset >= entry.tx_buffer.size() / 2) {
            entry.tx_buffer.erase(entry.tx_buffer.begin(),
                                  entry.tx_buffer.begin() + static_cast<std::ptrdiff_t>(entry.tx_offset));
            entry.tx_offset = 0;
        }
        entry.tx_buffer.insert(entry.tx_buffer.end(),
                               source.begin() + static_cast<std::ptrdiff_t>(consumed), source.end());
    } catch (...) {
        return std::unexpected(SocketError::Unknown);
    }
    arm_locked(it->first, entry);
    return {};
}

template <class Backend>
bool TxPoller<Backend>::remove(int id) noexcept {
    std::lock_guard lock(mutex_);
    const auto it = entries_.find(id);
    if (it == entries_.end()) { return false; }
    drop_locked(it, false);
    return true;
}

template <class Backend>
void TxPoller<Backend>::close() noexcept {
    closed_.store(true, std::memory_order_relaxed);
    backend_.wake();
}

template <class Backend>
void TxPoller<Backend>::run() {
    std::vector<int> ready;
    for (;;) {
        {
            std::lock_guard lock(mutex_);
            if (closed_.load(std::memory_order_relaxed)) { return; }
            backend_.prepare();
        }
        ready.clear();
        backend_.wait(ready);

        std::lock_guard lock(mutex_);
        if (closed_.load(std::memory_order_relaxed)) { return; }
        for (const int id : ready) {
            const auto it = entries_.find(id);
            if (it == entries_.end()) { continue; } // removed meanwhile
            Entry& entry = it->second;
            flushes_.fetch_add(1, std::memory_order_relaxed);
            if (!flush_locked(entry)) {
                drop_locked(it, true);
            } else if (entry.tx_buffer.empty()) {
                disarm_locked(id, entry);
            }
        }
    }
}

template class TxPoller<PollBackend>;
template class TxPoller<EpollBackend>;
