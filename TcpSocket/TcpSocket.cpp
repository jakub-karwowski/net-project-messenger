#include "TcpSocket.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <chrono>
#include <expected>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace {
    SocketError get_last_socket_error() noexcept {
        switch (errno) {
            case EADDRINUSE:   return SocketError::AddressInUse;
            case ECONNREFUSED: return SocketError::ConnectionRefused;
            case ECONNRESET:   return SocketError::ConnectionReset;
            case EPIPE:        return SocketError::BrokenPipe;
            case EINTR:        return SocketError::Interrupted;
            case EAGAIN:       
#if EAGAIN != EWOULDBLOCK
            case EWOULDBLOCK:
#endif
                return SocketError::WouldBlock;
            default:           return SocketError::Unknown;
        }
    }

    constexpr const char* to_string(SocketError err) noexcept {
        switch (err) {
            case SocketError::AddressInUse:      return "Address already in use";
            case SocketError::ConnectionRefused: return "Connection refused";
            case SocketError::ConnectionReset:   return "Connection reset by peer";
            case SocketError::BrokenPipe:        return "Broken pipe";
            case SocketError::WouldBlock:        return "Operation would block";
            case SocketError::Interrupted:       return "Operation interrupted";
            case SocketError::InvalidAddress:    return "Invalid address";
            case SocketError::ConnectionClosed:  return "Connection closed by peer";
            case SocketError::ConnectionNotFound: return "Connection not found in pool";
            default:                             return "Unknown socket error";
        }
    }

    // A retryable failure means the same call may succeed if attempted again.
    constexpr bool is_retryable(SocketError err) noexcept {
        return err == SocketError::WouldBlock || err == SocketError::Interrupted;
    }

    constexpr timeval to_timeval(std::chrono::milliseconds timeout) noexcept {
        return timeval{
            .tv_sec  = static_cast<time_t>(timeout.count() / 1000),
            .tv_usec = static_cast<suseconds_t>((timeout.count() % 1000) * 1000),
        };
    }
}

SocketException::SocketException(SocketError code) : std::runtime_error(to_string(code)), code_(code) {}
SocketError SocketException::code() const noexcept { return code_; }

TcpListener::TcpListener(std::string_view local_address, unsigned port, int backlog) {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) { throw SocketException(get_last_socket_error()); }

    int opt = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        close(listen_fd_);
        throw SocketException(get_last_socket_error());
    }

    // Value-initialized, so the padding (sin_zero) is zeroed too. A partial
    // aggregate init would leave it out and trigger -Wmissing-field-initializers.
    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, std::string(local_address).c_str(), &addr.sin_addr) != 1) {
        close(listen_fd_);
        throw std::invalid_argument("Invalid IPv4 address format");
    }

    if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(listen_fd_);
        throw SocketException(get_last_socket_error());
    }
    if (listen(listen_fd_, backlog) < 0) {
        close(listen_fd_);
        throw SocketException(get_last_socket_error());; 
    }
}

TcpListener::TcpListener(TcpListener&& other) noexcept : listen_fd_(std::exchange(other.listen_fd_, -1 )) {}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept {
    if (this == &other) { return *this; }
    if (listen_fd_ >= 0) { close(listen_fd_); }
    this->listen_fd_ = std::exchange(other.listen_fd_, -1);
    return *this;
}

TcpListener::~TcpListener() {
    if (listen_fd_ >= 0) { close(listen_fd_); };
}

std::expected<TcpConnection, SocketError> TcpListener::accept_client() noexcept {
    struct sockaddr_in client_addr{};
    socklen_t client_len{sizeof(client_addr)};
    int client_fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) { return std::unexpected(get_last_socket_error()); }
    return TcpConnection(client_fd);
}

TcpConnection::TcpConnection(int connected_fd) noexcept : fd_(connected_fd) {}

TcpConnection::TcpConnection(TcpConnection&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      options_(other.options_),
      options_applied_(other.options_applied_) {}

TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this == &other) { return *this; }
    if (fd_ >= 0) { close(fd_); }
    fd_ = std::exchange(other.fd_, -1);
    options_ = other.options_;
    options_applied_ = other.options_applied_;
    return *this;
}

TcpConnection::~TcpConnection() {
    if (fd_ >= 0) { close(fd_); }
}

std::expected<bool, SocketError> TcpConnection::shutdown(Shutdown how) noexcept {
    if (fd_ < 0) { return false; }  // moved-from: nothing to wake

    const int mode = (how == Shutdown::Read)  ? SHUT_RD
                   : (how == Shutdown::Write) ? SHUT_WR
                                              : SHUT_RDWR;
    if (::shutdown(fd_, mode) < 0) {
        // The peer is already gone or this direction was already shut down;
        // either way there is nothing left to wake.
        if (errno == ENOTCONN || errno == EINVAL) { return false; }
        return std::unexpected(get_last_socket_error());
    }
    return true;
}

void TcpConnection::shutdown_quietly() noexcept {
    [[maybe_unused]] const auto result = shutdown();
}

TcpConnection& TcpConnection::with_retries(unsigned attempts, std::chrono::milliseconds delay) noexcept {
    options_.max_retries = attempts;
    options_.retry_delay = delay;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_send_timeout(std::chrono::milliseconds timeout) noexcept {
    options_.send_timeout = timeout;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_receive_timeout(std::chrono::milliseconds timeout) noexcept {
    options_.receive_timeout = timeout;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_non_blocking(bool enable) noexcept {
    options_.non_blocking = enable;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_tcp_nodelay(bool enable) noexcept {
    options_.tcp_nodelay = enable;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_keep_alive(bool enable) noexcept {
    options_.keep_alive = enable;
    options_applied_ = false;
    return *this;
}

TcpConnection& TcpConnection::with_options(Options options) noexcept {
    options_ = options;
    options_applied_ = false;
    return *this;
}

const TcpConnection::Options& TcpConnection::options() const noexcept {
    return options_;
}

int TcpConnection::native_handle() const noexcept {
    return fd_;
}

std::expected<void, SocketError> TcpConnection::apply_options() noexcept {
    if (options_applied_ || fd_ < 0) { return {}; }

    // A zero timeval disables the timeout (i.e. blocks indefinitely).
    const timeval send_tv = to_timeval(options_.send_timeout);
    if (setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &send_tv, sizeof(send_tv)) < 0) {
        return std::unexpected(get_last_socket_error());
    }

    const timeval recv_tv = to_timeval(options_.receive_timeout);
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &recv_tv, sizeof(recv_tv)) < 0) {
        return std::unexpected(get_last_socket_error());
    }

    const int nodelay = options_.tcp_nodelay ? 1 : 0;
    if (setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay)) < 0) {
        return std::unexpected(get_last_socket_error());
    }

    const int keepalive = options_.keep_alive ? 1 : 0;
    if (setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, &keepalive, sizeof(keepalive)) < 0) {
        return std::unexpected(get_last_socket_error());
    }

    const int flags = fcntl(fd_, F_GETFL, 0);
    if (flags < 0) { return std::unexpected(get_last_socket_error()); }
    const int updated_flags = options_.non_blocking ? (flags | O_NONBLOCK) : (flags & ~O_NONBLOCK);
    if (fcntl(fd_, F_SETFL, updated_flags) < 0) {
        return std::unexpected(get_last_socket_error());
    }

    options_applied_ = true;
    return {};
}

std::expected<TcpConnection, SocketError> TcpConnection::connect(std::string_view server_address, unsigned port) noexcept {
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) { 
        return std::unexpected(get_last_socket_error()); 
    }

    // Value-initialized for the same reason as in TcpListener: sin_zero must be zero.
    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, std::string(server_address).c_str(), &server_addr.sin_addr) != 1) {
        close(client_fd);
        return std::unexpected(SocketError::Unknown); 
    }

    if (::connect(client_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(client_fd);
        return std::unexpected(get_last_socket_error());
    }
    return TcpConnection(client_fd);
}

std::expected<size_t, SocketError> TcpConnection::send(std::span<const std::byte> source) noexcept {
    if (auto applied = apply_options(); !applied) {
        return std::unexpected(applied.error());
    }

    for (unsigned attempt = 0; ; ++attempt) {
        // MSG_NOSIGNAL: a peer that has gone away makes send() fail with EPIPE
        // instead of raising SIGPIPE, which would otherwise kill the process.
        const ssize_t ret = ::send(fd_, source.data(), source.size_bytes(), MSG_NOSIGNAL);
        if (ret >= 0) { return static_cast<size_t>(ret); }

        const SocketError err = get_last_socket_error();
        if (attempt >= options_.max_retries || !is_retryable(err)) {
            return std::unexpected(err);
        }
        if (options_.retry_delay.count() > 0) {
            std::this_thread::sleep_for(options_.retry_delay);
        }
    }
}

std::expected<size_t, SocketError> TcpConnection::receive(std::span<std::byte> destination) noexcept {
    if (destination.empty()) { return 0; }
    if (auto applied = apply_options(); !applied) {
        return std::unexpected(applied.error());
    }

    for (unsigned attempt = 0; ; ++attempt) {
        const ssize_t ret = ::recv(fd_, destination.data(), destination.size_bytes(), 0);
        if (ret > 0) { return static_cast<size_t>(ret); }
        if (ret == 0) { return std::unexpected(SocketError::ConnectionClosed); }

        const SocketError err = get_last_socket_error();
        if (attempt >= options_.max_retries || !is_retryable(err)) {
            return std::unexpected(err);
        }
        if (options_.retry_delay.count() > 0) {
            std::this_thread::sleep_for(options_.retry_delay);
        }
    }
}

std::expected<size_t, SocketError> TcpConnection::send_once(std::span<const std::byte> source,
                                                            int flags) noexcept {
    if (fd_ < 0) { return std::unexpected(SocketError::ConnectionClosed); }
    if (auto applied = apply_options(); !applied) {
        return std::unexpected(applied.error());
    }

    const ssize_t ret = ::send(fd_, source.data(), source.size_bytes(), flags | MSG_NOSIGNAL);
    if (ret >= 0) { return static_cast<size_t>(ret); }
    return std::unexpected(get_last_socket_error());
}

std::expected<size_t, SocketError> TcpConnection::receive_once(std::span<std::byte> destination,
                                                               int flags) noexcept {
    if (fd_ < 0) { return std::unexpected(SocketError::ConnectionClosed); }
    if (auto applied = apply_options(); !applied) {
        return std::unexpected(applied.error());
    }

    // 0 bytes means the peer performed an orderly shutdown.
    const ssize_t ret = ::recv(fd_, destination.data(), destination.size_bytes(), flags);
    if (ret >= 0) { return static_cast<size_t>(ret); }
    return std::unexpected(get_last_socket_error());
}

PollSet::PollSet() {
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        throw SocketException(get_last_socket_error());
    }
    fds_.push_back(pollfd{.fd = wake_fd_, .events = POLLIN, .revents = 0});
}

PollSet::~PollSet() {
    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
}

void PollSet::upsert(int id, int fd, short events) {
    const auto [it, inserted] =
        desired_.try_emplace(id, pollfd{.fd = fd, .events = events, .revents = 0});
    if (inserted) {
        dirty_ = true;
        return;
    }
    if (it->second.fd == fd && it->second.events == events) { return; }

    it->second = pollfd{.fd = fd, .events = events, .revents = 0};
    dirty_ = true;
}

void PollSet::erase(int id) noexcept {
    if (desired_.erase(id) != 0) { dirty_ = true; }
}

void PollSet::prepare() {
    if (!dirty_) { return; }
    dirty_ = false;

    // resize() keeps the capacity, so rebuilding is allocation-free in steady state.
    fds_.resize(1); // slot 0 stays the wake descriptor
    ids_.clear();
    fds_.reserve(desired_.size() + 1);
    ids_.reserve(desired_.size());

    for (const auto& [id, pfd] : desired_) {
        fds_.push_back(pfd);
        ids_.push_back(id);
    }

    if (!ids_.empty()) {
        cursor_ %= ids_.size();
    } else {
        cursor_ = 0;
    }
}

int PollSet::wait(int timeout_ms) noexcept {
    return ::poll(fds_.data(), fds_.size(), timeout_ms);
}

void PollSet::wake() const noexcept {
    if (wake_fd_ < 0) { return; }
    const std::uint64_t one = 1;
    // EAGAIN here only means a wake-up is already pending, which is fine.
    [[maybe_unused]] const ssize_t written = ::write(wake_fd_, &one, sizeof(one));
}

void PollSet::drain_wake() const noexcept {
    if (wake_fd_ < 0) { return; }
    std::uint64_t value = 0;
    while (::read(wake_fd_, &value, sizeof(value)) > 0) {
    }
}

TcpConnectionPool::TcpConnectionPool(std::size_t max_connections,
                                     std::chrono::milliseconds idle_timeout,
                                     std::size_t max_tx_buffer_bytes)
    : max_connections_(max_connections == 0 ? 1 : max_connections),
      idle_timeout_ms_(idle_timeout.count() > 0
                           ? static_cast<std::int64_t>(idle_timeout.count())
                           : 0),
      max_tx_buffer_bytes_(max_tx_buffer_bytes) {
    // Sweep a few times per idle window: small enough to notice expiry promptly,
    // bounded so a large timeout still prunes and a tiny one does not spin.
    constexpr std::int64_t kMinSweepMs = 10;
    constexpr std::int64_t kMaxSweepMs = 250;
    sweep_interval_ms_ = idle_timeout_ms_ <= 0
                             ? kMaxSweepMs
                             : std::clamp<std::int64_t>(idle_timeout_ms_ / 4, kMinSweepMs, kMaxSweepMs);
    next_sweep_ms_ = now_ms() + sweep_interval_ms_;
}

TcpConnectionPool::~TcpConnectionPool() {
    close_pool();

    // Move the sockets out under the lock and let them close outside of it so
    // no thread is stuck on mutex_ while descriptors are being torn down.
    std::unordered_map<int, Entry> drained;
    {
        std::lock_guard lock(mutex_);
        drained.swap(connections_);
    }
    drained.clear();
}

std::int64_t TcpConnectionPool::now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int TcpConnectionPool::add_connection(TcpConnection connection) noexcept {
    if (connection.native_handle() < 0) { return -1; } // moved-from connection

    // The pool never blocks on a single socket: poll() decides readiness, the raw
    // hooks pass MSG_DONTWAIT, and this arms O_NONBLOCK on the first hook call too.
    connection.with_non_blocking(true);

    std::lock_guard lock(mutex_);
    if (closed_.load(std::memory_order_relaxed)) { return -1; }

    const std::int64_t now = now_ms();
    const int id = next_id_++;
    const int fd = connection.native_handle();

    connections_.try_emplace(id, Entry{std::move(connection), now, {}, 0});
    poll_set_.upsert(id, fd, POLLIN);

    // Enforce the cap after inserting so the newcomer participates; the LRU
    // tie-break below prefers older ids, so a same-millisecond burst keeps it.
    prune_locked(now);

    poll_set_.wake();
    return id;
}

bool TcpConnectionPool::remove_connection(int connection_id) noexcept {
    std::lock_guard lock(mutex_);
    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return false; }

    drop_locked(it);
    poll_set_.wake();
    return true;
}

void TcpConnectionPool::close_pool() noexcept {
    closed_.store(true, std::memory_order_relaxed);
    poll_set_.wake();
}

TcpConnectionPool::Iterator TcpConnectionPool::drop_locked(Iterator it) noexcept {
    it->second.connection.shutdown_quietly();
    poll_set_.erase(it->first);
    return connections_.erase(it);
}

void TcpConnectionPool::prune_locked(std::int64_t now) noexcept {
    if (idle_timeout_ms_ > 0) {
        for (auto it = connections_.begin(); it != connections_.end();) {
            if (now - it->second.last_active_ms >= idle_timeout_ms_) {
                pending_closed_.push_back(it->first);
                it = drop_locked(it);
            } else {
                ++it;
            }
        }
    }

    // Least recently active first: the freshest additions have the newest
    // stamps and therefore survive a burst that overflows the pool. Ties go to
    // the lower id, so the connection just added is never the one evicted.
    while (connections_.size() > max_connections_) {
        auto oldest = connections_.begin();
        for (auto it = connections_.begin(); it != connections_.end(); ++it) {
            const bool older = it->second.last_active_ms < oldest->second.last_active_ms;
            const bool same_age_lower_id =
                it->second.last_active_ms == oldest->second.last_active_ms
                && it->first < oldest->first;
            if (older || same_age_lower_id) { oldest = it; }
        }
        pending_closed_.push_back(oldest->first);
        drop_locked(oldest);
    }
}

bool TcpConnectionPool::flush_tx_locked(Entry& entry) noexcept {
    while (entry.tx_offset < entry.tx_buffer.size()) {
        const std::span<const std::byte> pending{entry.tx_buffer.data() + entry.tx_offset,
                                                 entry.tx_buffer.size() - entry.tx_offset};
        const auto sent = entry.connection.send_once(pending, MSG_DONTWAIT);
        if (!sent) {
            if (sent.error() == SocketError::Interrupted) { continue; }
            if (sent.error() == SocketError::WouldBlock) {
                return true; // still back-pressured: wait for the next POLLOUT
            }
            return false; // fatal write error
        }
        if (*sent == 0) { return false; } // zero-length write on a broken stream

        entry.tx_offset += *sent;
        entry.last_active_ms = now_ms();
    }

    entry.tx_buffer.clear();
    entry.tx_offset = 0;
    return true;
}

std::expected<ReceiveResult, SocketError> TcpConnectionPool::receive(
    std::span<std::byte> destination) noexcept {
    if (destination.empty()) {
        // A zero-length recv() would be indistinguishable from a clean EOF.
        return std::unexpected(SocketError::Unknown);
    }

    for (;;) {
        {
            std::lock_guard lock(mutex_);
            if (closed_.load(std::memory_order_relaxed)) {
                return std::unexpected(SocketError::ConnectionClosed);
            }

            const std::int64_t current = now_ms();
            if (current >= next_sweep_ms_) {
                prune_locked(current);
                next_sweep_ms_ = current + sweep_interval_ms_;
            }

            // Drops made outside this read path (prune, fatal write in send()) are
            // reported here, one per call, before blocking again.
            if (!pending_closed_.empty()) {
                const int id = pending_closed_.front();
                pending_closed_.pop_front();
                return ReceiveResult{.connection_id = id, .bytes_read = 0};
            }

            // Rebuild the kernel array from the desired state. Safe without a copy:
            // PollSet's array is owned by this thread, and no wait() is in flight
            // while mutex_ is held.
            poll_set_.prepare();
        }

        const int timeout_ms = static_cast<int>(
            std::clamp<std::int64_t>(next_sweep_ms_ - now_ms(), 0, sweep_interval_ms_));

        // No lock is held while blocked: producers only touch desired_ state and
        // poke the wake descriptor, never the array being polled.
        const int ready = poll_set_.wait(timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) { continue; }
            return std::unexpected(get_last_socket_error());
        }

        std::lock_guard lock(mutex_);
        poll_set_.drain_wake();

        if (closed_.load(std::memory_order_relaxed)) {
            return std::unexpected(SocketError::ConnectionClosed);
        }
        if (ready == 0) {
            continue; // poll timeout: the next iteration performs the sweep
        }

        std::optional<ReceiveResult> output;

        poll_set_.for_each_ready([&](int conn_id, short revents) -> bool {
            const auto it = connections_.find(conn_id);
            if (it == connections_.end()) { return false; } // vanished while waiting

            Entry& entry = it->second;

            if ((revents & POLLOUT) != 0 && entry.tx_buffer.size() > entry.tx_offset) {
                if (!flush_tx_locked(entry)) {
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    return true;
                }
                // Buffer state changed: re-arm or clear POLLOUT on the next prepare().
                poll_set_.upsert(conn_id, entry.connection.native_handle(), entry.desired_events());
            }

            if ((revents & POLLIN) != 0) {
                const auto received = entry.connection.receive_once(destination, MSG_DONTWAIT);
                if (received) {
                    if (*received > 0) {
                        entry.last_active_ms = now_ms();
                        output = ReceiveResult{.connection_id = conn_id, .bytes_read = *received};
                        return true;
                    }
                    // Clean EOF: the peer shut down its write side.
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    return true;
                }
                if (received.error() != SocketError::WouldBlock
                    && received.error() != SocketError::Interrupted) {
                    // Fatal read error: report it as a disconnect.
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    return true;
                }
            }

            if ((revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
                drop_locked(it);
                output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                return true;
            }

            return false; // keep scanning the remaining ready sockets
        });

        if (output) { return *output; }
    }
}

std::expected<void, SocketError> TcpConnectionPool::send(
    int connection_id,
    std::span<const std::byte> source) noexcept {
    std::lock_guard lock(mutex_);

    if (closed_.load(std::memory_order_relaxed)) {
        return std::unexpected(SocketError::ConnectionClosed);
    }

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) {
        return std::unexpected(SocketError::ConnectionNotFound);
    }
    if (source.empty()) { return {}; }

    Entry& entry = it->second;
    const std::size_t pending = entry.tx_buffer.size() - entry.tx_offset;

    // Reject up front when the buffer cannot be accepted atomically: either it
    // is larger than the cap, or it would overflow the already pending bytes.
    if (source.size_bytes() > max_tx_buffer_bytes_) {
        return std::unexpected(SocketError::WouldBlock);
    }
    if (pending != 0 && source.size_bytes() > max_tx_buffer_bytes_ - pending) {
        return std::unexpected(SocketError::WouldBlock);
    }

    std::size_t consumed = 0;
    if (pending == 0) {
        entry.tx_buffer.clear();
        entry.tx_offset = 0;

        while (consumed < source.size_bytes()) {
            const auto sent = entry.connection.send_once(source.subspan(consumed), MSG_DONTWAIT);
            if (sent && *sent > 0) {
                consumed += *sent;
                continue;
            }
            if (!sent && sent.error() == SocketError::Interrupted) { continue; }
            if (!sent && sent.error() == SocketError::WouldBlock) { break; }

            // Fatal write error (or a zero-length write on a non-empty stream).
            const SocketError error = sent ? SocketError::ConnectionClosed : sent.error();
            pending_closed_.push_back(connection_id);
            drop_locked(it);
            poll_set_.wake();
            return std::unexpected(error);
        }

        entry.last_active_ms = now_ms();
        if (consumed == source.size_bytes()) { return {}; }
    }

    // Slow path: keep the unsent remainder and let receive() finish it on POLLOUT.
    // Only compact once the dead prefix dominates, so appends stay amortized O(1).
    if (entry.tx_offset > 0 && entry.tx_offset >= entry.tx_buffer.size() / 2) {
        entry.tx_buffer.erase(
            entry.tx_buffer.begin(),
            entry.tx_buffer.begin() + static_cast<std::ptrdiff_t>(entry.tx_offset));
        entry.tx_offset = 0;
    }
    entry.tx_buffer.insert(entry.tx_buffer.end(),
                           source.begin() + static_cast<std::ptrdiff_t>(consumed),
                           source.end());
    entry.last_active_ms = now_ms();

    // Ask receive() to arm POLLOUT for this connection on its next pass.
    poll_set_.upsert(connection_id, entry.connection.native_handle(), entry.desired_events());
    poll_set_.wake();
    return {};
}

EpollConnectionPool::EpollConnectionPool(std::size_t max_connections,
                                         std::chrono::milliseconds idle_timeout,
                                         std::size_t max_tx_buffer_bytes)
    : max_connections_(max_connections == 0 ? 1 : max_connections),
      idle_timeout_ms_(idle_timeout.count() > 0
                           ? static_cast<std::int64_t>(idle_timeout.count())
                           : 0),
      max_tx_buffer_bytes_(max_tx_buffer_bytes) {
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw SocketException(get_last_socket_error());
    }

    // The wake-up descriptor lives in the same epoll set, so a blocked
    // epoll_wait() returns as soon as a producer nudges it.
    wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd_ < 0) {
        ::close(epoll_fd_);
        throw SocketException(get_last_socket_error());
    }

    epoll_event wake_event{};
    wake_event.events = EPOLLIN;
    wake_event.data.u64 = kWakeToken;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_fd_, &wake_event) < 0) {
        ::close(wake_fd_);
        ::close(epoll_fd_);
        throw SocketException(get_last_socket_error());
    }

    // +2 covers the wake descriptor plus a transiently over-capacity pool.
    events_.resize(max_connections_ + 2);

    constexpr std::int64_t kMinSweepMs = 10;
    constexpr std::int64_t kMaxSweepMs = 250;
    sweep_interval_ms_ = idle_timeout_ms_ <= 0
                             ? kMaxSweepMs
                             : std::clamp<std::int64_t>(idle_timeout_ms_ / 4, kMinSweepMs, kMaxSweepMs);
    next_sweep_ms_ = now_ms() + sweep_interval_ms_;
}

EpollConnectionPool::~EpollConnectionPool() {
    close_pool();

    // Close the sockets outside the lock (closing also drops them from the epoll set).
    ConnectionMap drained;
    {
        std::lock_guard lock(mutex_);
        drained.swap(connections_);
    }
    drained.clear();

    if (wake_fd_ >= 0) {
        ::close(wake_fd_);
        wake_fd_ = -1;
    }
    if (epoll_fd_ >= 0) {
        ::close(epoll_fd_);
        epoll_fd_ = -1;
    }
}

std::int64_t EpollConnectionPool::now_ms() noexcept {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

std::uint32_t EpollConnectionPool::desired_events(const Entry& entry) noexcept {
    return entry.tx_buffer.size() > entry.tx_offset
               ? static_cast<std::uint32_t>(EPOLLIN | EPOLLOUT)
               : static_cast<std::uint32_t>(EPOLLIN);
}

void EpollConnectionPool::wake() const noexcept {
    if (wake_fd_ < 0) { return; }
    const std::uint64_t one = 1;
    // EAGAIN here only means a wake-up is already pending, which is fine.
    [[maybe_unused]] const ssize_t written = ::write(wake_fd_, &one, sizeof(one));
}

void EpollConnectionPool::drain_wake() const noexcept {
    if (wake_fd_ < 0) { return; }
    std::uint64_t value = 0;
    while (::read(wake_fd_, &value, sizeof(value)) > 0) {
    }
}

void EpollConnectionPool::arm_locked(int id, Entry& entry, std::uint32_t events) noexcept {
    if (entry.events == events) { return; }

    epoll_event event{};
    event.events = events;
    event.data.u64 = static_cast<std::uint64_t>(id);
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, entry.connection.native_handle(), &event) == 0) {
        entry.events = events;
    }
}

int EpollConnectionPool::add_connection(TcpConnection connection) noexcept {
    const int fd = connection.native_handle();
    if (fd < 0) { return -1; } // moved-from connection

    connection.with_non_blocking(true);

    std::lock_guard lock(mutex_);
    if (closed_.load(std::memory_order_relaxed)) { return -1; }

    const std::int64_t now = now_ms();
    const int id = next_id_++;

    const auto [it, inserted] = connections_.try_emplace(
        id, Entry{std::move(connection), now, {}, 0, EPOLLIN});
    (void)inserted;

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.u64 = static_cast<std::uint64_t>(id);
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
        // The descriptor cannot be multiplexed; do not keep a cached entry for it.
        connections_.erase(it); // closes the socket
        return -1;
    }

    // Enforce the cap after inserting so the newcomer participates; the LRU
    // tie-break below prefers older ids, so a same-millisecond burst keeps it.
    prune_locked(now);

    wake();
    return id;
}

bool EpollConnectionPool::remove_connection(int connection_id) noexcept {
    std::lock_guard lock(mutex_);
    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return false; }

    drop_locked(it);
    wake();
    return true;
}

void EpollConnectionPool::close_pool() noexcept {
    closed_.store(true, std::memory_order_relaxed);
    wake();
}

EpollConnectionPool::Iterator EpollConnectionPool::drop_locked(Iterator it) noexcept {
    // DEL before the descriptor is shut down/closed; epoll_ctl is safe while
    // another thread may be blocked in epoll_wait().
    (void)::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->second.connection.native_handle(), nullptr);
    it->second.connection.shutdown_quietly();
    return connections_.erase(it);
}

void EpollConnectionPool::prune_locked(std::int64_t now) noexcept {
    if (idle_timeout_ms_ > 0) {
        for (auto it = connections_.begin(); it != connections_.end();) {
            if (now - it->second.last_active_ms >= idle_timeout_ms_) {
                pending_closed_.push_back(it->first);
                it = drop_locked(it);
            } else {
                ++it;
            }
        }
    }

    // Least recently active first: the freshest additions have the newest
    // stamps. Ties go to the lower id, so the connection just added is never
    // the one evicted.
    while (connections_.size() > max_connections_) {
        auto oldest = connections_.begin();
        for (auto it = connections_.begin(); it != connections_.end(); ++it) {
            const bool older = it->second.last_active_ms < oldest->second.last_active_ms;
            const bool same_age_lower_id =
                it->second.last_active_ms == oldest->second.last_active_ms
                && it->first < oldest->first;
            if (older || same_age_lower_id) { oldest = it; }
        }
        pending_closed_.push_back(oldest->first);
        drop_locked(oldest);
    }
}

bool EpollConnectionPool::flush_tx_locked(Entry& entry) noexcept {
    while (entry.tx_offset < entry.tx_buffer.size()) {
        const std::span<const std::byte> pending{entry.tx_buffer.data() + entry.tx_offset,
                                                 entry.tx_buffer.size() - entry.tx_offset};
        const auto sent = entry.connection.send_once(pending, MSG_DONTWAIT);
        if (!sent) {
            if (sent.error() == SocketError::Interrupted) { continue; }
            if (sent.error() == SocketError::WouldBlock) {
                return true; // still back-pressured: wait for the next EPOLLOUT
            }
            return false; // fatal write error
        }
        if (*sent == 0) { return false; } // zero-length write on a broken stream

        entry.tx_offset += *sent;
        entry.last_active_ms = now_ms();
    }

    entry.tx_buffer.clear();
    entry.tx_offset = 0;
    return true;
}

std::expected<ReceiveResult, SocketError> EpollConnectionPool::receive(
    std::span<std::byte> destination) noexcept {
    if (destination.empty()) {
        // A zero-length recv() would be indistinguishable from a clean EOF.
        return std::unexpected(SocketError::Unknown);
    }

    for (;;) {
        {
            std::lock_guard lock(mutex_);
            if (closed_.load(std::memory_order_relaxed)) {
                return std::unexpected(SocketError::ConnectionClosed);
            }

            const std::int64_t current = now_ms();
            if (current >= next_sweep_ms_) {
                prune_locked(current);
                next_sweep_ms_ = current + sweep_interval_ms_;
            }

            // Drops made outside this read path (prune, fatal write in send()) are
            // reported here, one per call, before blocking again.
            if (!pending_closed_.empty()) {
                const int id = pending_closed_.front();
                pending_closed_.pop_front();
                return ReceiveResult{.connection_id = id, .bytes_read = 0};
            }
        }

        const int timeout_ms = static_cast<int>(
            std::clamp<std::int64_t>(next_sweep_ms_ - now_ms(), 0, sweep_interval_ms_));

        // No lock is held while blocked. Producers may call epoll_ctl() (arm EPOLLOUT,
        // delete a connection) concurrently; the kernel serialises that against this
        // wait, and events_ is touched by this thread only.
        const int ready = ::epoll_wait(epoll_fd_, events_.data(),
                                       static_cast<int>(events_.size()), timeout_ms);
        if (ready < 0) {
            if (errno == EINTR) { continue; }
            return std::unexpected(get_last_socket_error());
        }

        std::lock_guard lock(mutex_);
        drain_wake();

        if (closed_.load(std::memory_order_relaxed)) {
            return std::unexpected(SocketError::ConnectionClosed);
        }
        if (ready == 0) {
            continue; // timeout: the next iteration performs the sweep
        }

        const std::size_t count = static_cast<std::size_t>(ready);
        std::optional<ReceiveResult> output;
        std::size_t serviced = 0;

        // Rotate the scan so one perpetually-ready socket cannot starve the rest.
        for (std::size_t step = 0; step < count; ++step) {
            const std::size_t idx = (cursor_ + step) % count;
            const epoll_event& event = events_[idx];

            if (event.data.u64 == kWakeToken) { continue; }

            const int conn_id = static_cast<int>(event.data.u64);
            const auto it = connections_.find(conn_id);
            if (it == connections_.end()) { continue; } // removed while waiting

            Entry& entry = it->second;

            if ((event.events & EPOLLOUT) != 0 && entry.tx_buffer.size() > entry.tx_offset) {
                if (!flush_tx_locked(entry)) {
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    serviced = idx;
                    break;
                }
                // Buffer state changed: disarm EPOLLOUT once it is fully drained.
                arm_locked(conn_id, entry, desired_events(entry));
            }

            if ((event.events & EPOLLIN) != 0) {
                const auto received = entry.connection.receive_once(destination, MSG_DONTWAIT);
                if (received) {
                    if (*received > 0) {
                        entry.last_active_ms = now_ms();
                        output = ReceiveResult{.connection_id = conn_id, .bytes_read = *received};
                        serviced = idx;
                        break;
                    }
                    // Clean EOF: the peer shut down its write side.
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    serviced = idx;
                    break;
                }
                if (received.error() != SocketError::WouldBlock
                    && received.error() != SocketError::Interrupted) {
                    // Fatal read error: report it as a disconnect.
                    drop_locked(it);
                    output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                    serviced = idx;
                    break;
                }
            }

            if ((event.events & (EPOLLERR | EPOLLHUP)) != 0) {
                drop_locked(it);
                output = ReceiveResult{.connection_id = conn_id, .bytes_read = 0};
                serviced = idx;
                break;
            }
        }

        if (output) {
            cursor_ = (serviced + 1) % count;
            return *output;
        }
    }
}

std::expected<void, SocketError> EpollConnectionPool::send(
    int connection_id,
    std::span<const std::byte> source) noexcept {
    std::lock_guard lock(mutex_);

    if (closed_.load(std::memory_order_relaxed)) {
        return std::unexpected(SocketError::ConnectionClosed);
    }

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) {
        return std::unexpected(SocketError::ConnectionNotFound);
    }
    if (source.empty()) { return {}; }

    Entry& entry = it->second;
    const std::size_t pending = entry.tx_buffer.size() - entry.tx_offset;

    // Reject up front when the buffer cannot be accepted atomically: either it
    // is larger than the cap, or it would overflow the already pending bytes.
    if (source.size_bytes() > max_tx_buffer_bytes_) {
        return std::unexpected(SocketError::WouldBlock);
    }
    if (pending != 0 && source.size_bytes() > max_tx_buffer_bytes_ - pending) {
        return std::unexpected(SocketError::WouldBlock);
    }

    std::size_t consumed = 0;
    if (pending == 0) {
        entry.tx_buffer.clear();
        entry.tx_offset = 0;

        while (consumed < source.size_bytes()) {
            const auto sent = entry.connection.send_once(source.subspan(consumed), MSG_DONTWAIT);
            if (sent && *sent > 0) {
                consumed += *sent;
                continue;
            }
            if (!sent && sent.error() == SocketError::Interrupted) { continue; }
            if (!sent && sent.error() == SocketError::WouldBlock) { break; }

            // Fatal write error (or a zero-length write on a non-empty stream).
            const SocketError error = sent ? SocketError::ConnectionClosed : sent.error();
            pending_closed_.push_back(connection_id);
            drop_locked(it);
            wake();
            return std::unexpected(error);
        }

        entry.last_active_ms = now_ms();
        if (consumed == source.size_bytes()) { return {}; }
    }

    // Slow path: keep the unsent remainder and let receive() finish it on EPOLLOUT.
    // Only compact once the dead prefix dominates, so appends stay amortized O(1).
    if (entry.tx_offset > 0 && entry.tx_offset >= entry.tx_buffer.size() / 2) {
        entry.tx_buffer.erase(
            entry.tx_buffer.begin(),
            entry.tx_buffer.begin() + static_cast<std::ptrdiff_t>(entry.tx_offset));
        entry.tx_offset = 0;
    }
    entry.tx_buffer.insert(entry.tx_buffer.end(),
                           source.begin() + static_cast<std::ptrdiff_t>(consumed),
                           source.end());
    entry.last_active_ms = now_ms();

    // Arm EPOLLOUT; epoll_ctl is safe while receive() may be blocked in epoll_wait().
    arm_locked(connection_id, entry, desired_events(entry));
    wake();
    return {};
}