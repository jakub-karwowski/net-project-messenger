#include "TcpSocket.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdint>
#include <expected>
#include <fcntl.h>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <optional>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <system_error>
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
            case EHOSTUNREACH:
            case ENETUNREACH:  return SocketError::HostUnreachable;
            case EMFILE:
            case ENFILE:       return SocketError::TooManyOpenFiles;
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
            case SocketError::BufferFull:        return "Transmit buffer full";
            case SocketError::ReadTimedOut:      return "Nothing received within the read timeout";
            case SocketError::WriteTimedOut:     return "Transmit buffer not flushed within the write timeout";
            case SocketError::ConnectTimedOut:   return "Connection not established within the connect timeout";
            case SocketError::HostUnreachable:   return "Host or network unreachable";
            case SocketError::NotConnected:      return "Connection is still being established";
            case SocketError::TooManyOpenFiles:  return "Too many open files";
            case SocketError::Stopped:           return "Stopped";
            default:                             return "Unknown socket error";
        }
    }

    // A retryable failure means the same call may succeed if attempted again.
    constexpr bool is_retryable(SocketError err) noexcept {
        return err == SocketError::WouldBlock || err == SocketError::Interrupted;
    }

    // IPv4 destination for connect(). nullopt for a malformed address, and for port
    // 0 or > 65535: Linux does not refuse a connect() to port 0, the SYN just goes
    // unanswered and the call blocks for the whole SYN-retry budget (~130 s with
    // tcp_syn_retries=6), and htons() would silently truncate a larger port.
    std::optional<sockaddr_in> parse_destination(std::string_view address, unsigned port) noexcept {
        if (port == 0 || port > 65535) { return std::nullopt; }
        char text[INET_ADDRSTRLEN] = {};   // inet_pton needs a terminated string
        if (address.size() >= sizeof(text)) { return std::nullopt; }
        std::copy(address.begin(), address.end(), text);

        // Value-initialized, so the padding (sin_zero) is zeroed too.
        sockaddr_in destination{};
        destination.sin_family = AF_INET;
        destination.sin_port = htons(static_cast<uint16_t>(port));
        if (::inet_pton(AF_INET, text, &destination.sin_addr) != 1) { return std::nullopt; }
        return destination;
    }

    // Formats an IPv4 socket address. Every caller passes an AF_INET address, for
    // which inet_ntop() cannot fail with a buffer of INET_ADDRSTRLEN.
    TcpEndpoint to_endpoint(const sockaddr_in& address) noexcept {
        char text[INET_ADDRSTRLEN] = {};
        std::ignore = ::inet_ntop(AF_INET, &address.sin_addr, text, sizeof(text));
        try {
            // At most 15 characters: fits std::string's inline buffer, so this
            // does not allocate in practice.
            return TcpEndpoint{text, ntohs(address.sin_port)};
        } catch (...) {
            return TcpEndpoint{};
        }
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

// ---------------------------------------------------------------------------
// WakeupEvent
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// TcpListener
// ---------------------------------------------------------------------------

TcpListener::TcpListener(std::string_view local_address, unsigned port, int backlog) {
    // Non-blocking: accept_client() waits in poll(), and a client that gives up
    // between poll() and accept() must not leave accept() blocked.
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
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

TcpListener::TcpListener(TcpListener&& other) noexcept
    : listen_fd_(std::exchange(other.listen_fd_, -1)),
      stop_event_(std::move(other.stop_event_)),
      stopping_(other.stopping_.load()) {}

TcpListener& TcpListener::operator=(TcpListener&& other) noexcept {
    if (this == &other) { return *this; }
    if (listen_fd_ >= 0) { close(listen_fd_); }
    listen_fd_ = std::exchange(other.listen_fd_, -1);
    stop_event_ = std::move(other.stop_event_);
    stopping_.store(other.stopping_.load());
    return *this;
}

TcpListener::~TcpListener() {
    if (listen_fd_ >= 0) { close(listen_fd_); };
}

std::expected<TcpConnection, SocketError> TcpListener::accept_client() noexcept {
    for (;;) {
        if (stopping_.load(std::memory_order_acquire)) { return std::unexpected(SocketError::Stopped); }
        if (auto waited = wait_for_client(); !waited) { return std::unexpected(waited.error()); }
        // Checked again after the wait: stop() wins over a client that is queued.
        if (stopping_.load(std::memory_order_acquire)) { return std::unexpected(SocketError::Stopped); }

        auto accepted = try_accept();
        if (accepted || !is_retryable(accepted.error())) { return accepted; }
        // WouldBlock / Interrupted: the client went away, or the wake-up was not
        // for us. Wait again.
    }
}

void TcpListener::stop() noexcept {
    // The flag is the request; the event only wakes a thread parked in poll().
    // It is never cleared, so every later wait returns at once as well.
    stopping_.store(true, std::memory_order_release);
    stop_event_.signal();
}

std::expected<uint16_t, SocketError> TcpListener::local_port() const noexcept {
    sockaddr_in address{};
    socklen_t length = sizeof(address);
    if (::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&address), &length) < 0) {
        return std::unexpected(get_last_socket_error());
    }
    return ntohs(address.sin_port);
}

std::expected<void, SocketError> TcpListener::wait_for_client() const noexcept {
    std::array<pollfd, 2> watched{{
        {.fd = listen_fd_,         .events = POLLIN, .revents = 0},
        {.fd = stop_event_.fd(),   .events = POLLIN, .revents = 0},
    }};
    for (;;) {
        if (::poll(watched.data(), watched.size(), -1) >= 0) { return {}; }
        if (errno != EINTR) { return std::unexpected(get_last_socket_error()); }
    }
}

std::expected<TcpConnection, SocketError> TcpListener::try_accept() const noexcept {
    sockaddr_in client{};
    socklen_t length = sizeof(client);
    // The accepted socket starts blocking; its own Options decide later.
    const int client_fd = ::accept4(listen_fd_, reinterpret_cast<sockaddr*>(&client), &length, SOCK_CLOEXEC);
    if (client_fd >= 0) { return TcpConnection(client_fd, to_endpoint(client)); }

    switch (errno) {
        // The queued client is gone again (reset, aborted), or Linux reported a
        // network error that belongs to it rather than to the listener: accept(2)
        // says to treat these like EAGAIN.
        case ECONNABORTED: case EPROTO: case ENETDOWN: case ENOPROTOOPT: case EHOSTDOWN:
        case ENONET: case EHOSTUNREACH: case EOPNOTSUPP: case ENETUNREACH:
            return std::unexpected(SocketError::WouldBlock);
        default:
            return std::unexpected(get_last_socket_error());
    }
}

// ---------------------------------------------------------------------------
// TcpConnection
// ---------------------------------------------------------------------------

TcpConnection::TcpConnection(int connected_fd, TcpEndpoint remote) noexcept
    : fd_(connected_fd), remote_(std::move(remote)) {}

// The moved-from side is left with fd -1 and an empty TcpEndpoint{} (a plain
// std::move of the string would leave it "valid but unspecified").
TcpConnection::TcpConnection(TcpConnection&& other) noexcept
    : fd_(std::exchange(other.fd_, -1)),
      options_(other.options_),
      options_applied_(other.options_applied_),
      remote_(std::exchange(other.remote_, TcpEndpoint{})) {}

TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this == &other) { return *this; }
    if (fd_ >= 0) { close(fd_); }
    fd_ = std::exchange(other.fd_, -1);
    options_ = other.options_;
    options_applied_ = other.options_applied_;
    remote_ = std::exchange(other.remote_, TcpEndpoint{});
    return *this;
}

const TcpEndpoint& TcpConnection::remote_endpoint() const noexcept {
    return remote_;
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

std::expected<void, SocketError> TcpConnection::apply_options_now() noexcept {
    return apply_options();
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
    const auto destination = parse_destination(server_address, port);
    if (!destination) { return std::unexpected(SocketError::InvalidAddress); }

    const int client_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (client_fd < 0) { return std::unexpected(get_last_socket_error()); }
    // Closes the descriptor on every early return; the peer is where we dial.
    TcpConnection connection(client_fd, to_endpoint(*destination));

    if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&*destination), sizeof(*destination)) < 0) {
        return std::unexpected(get_last_socket_error());
    }
    return connection;
}

std::expected<TcpConnection, SocketError> TcpConnection::connect_start(std::string_view server_address,
                                                                       unsigned port, Options options) noexcept {
    const auto destination = parse_destination(server_address, port);
    if (!destination) { return std::unexpected(SocketError::InvalidAddress); }

    const int client_fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (client_fd < 0) { return std::unexpected(get_last_socket_error()); }
    // Closes the descriptor on every early return. The peer is where we dial:
    // getpeername() could not tell while the handshake is still in flight.
    TcpConnection connection(client_fd, to_endpoint(*destination));

    // Non-blocking is not optional: a blocking connect() would stall the caller.
    options.non_blocking = true;
    connection.with_options(options);
    if (auto applied = connection.apply_options(); !applied) { return std::unexpected(applied.error()); }

    // EINPROGRESS is the normal outcome: the handshake is in flight. 0 means it
    // completed already (possible on loopback); connect_status() confirms that
    // once the descriptor is reported writable, which it is at once.
    if (::connect(client_fd, reinterpret_cast<const sockaddr*>(&*destination), sizeof(*destination)) < 0
        && errno != EINPROGRESS) {
        return std::unexpected(get_last_socket_error());
    }
    return connection;
}

std::expected<bool, SocketError> TcpConnection::connect_status() const noexcept {
    if (fd_ < 0) { return std::unexpected(SocketError::ConnectionClosed); }

    // SO_ERROR holds the outcome of a non-blocking connect() that failed.
    int so_error = 0;
    socklen_t length = sizeof(so_error);
    if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_error, &length) < 0) { so_error = errno; }
    if (so_error != 0) {
        if (so_error == ETIMEDOUT) { return std::unexpected(SocketError::ConnectTimedOut); }
        errno = so_error;
        return std::unexpected(get_last_socket_error());
    }

    // No error recorded: either connected, or the readiness was spurious and the
    // handshake is still going. Only a connected socket has a peer address.
    sockaddr_in peer{};
    socklen_t peer_length = sizeof(peer);
    if (::getpeername(fd_, reinterpret_cast<sockaddr*>(&peer), &peer_length) == 0) { return true; }
    if (errno == ENOTCONN) { return false; }
    return std::unexpected(get_last_socket_error());
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

std::expected<size_t, SocketError> TcpConnection::send_no_block(std::span<const std::byte> source) noexcept {
    size_t sent = 0;
    while (sent < source.size()) {
        const auto written = send_once(source.subspan(sent), MSG_DONTWAIT);
        if (written) { sent += *written; continue; }
        if (written.error() == SocketError::Interrupted) { continue; }
        if (written.error() == SocketError::WouldBlock)  { break; }
        // A dead socket: the bytes already written no longer matter.
        return std::unexpected(written.error());
    }
    return sent;
}

// ---------------------------------------------------------------------------
// EpollConnectionPool
// ---------------------------------------------------------------------------

EpollConnectionPool::EpollConnectionPool()
    : EpollConnectionPool(Timeouts{.read = std::chrono::milliseconds{0}, .write = std::chrono::seconds{5}}) {}

EpollConnectionPool::EpollConnectionPool(Timeouts default_timeouts, size_t max_tx_buffer)
    : default_timeouts_(default_timeouts), max_tx_buffer_(max_tx_buffer) {
    // wakeup_ is a member, created before this body runs; it closes itself if we throw.
    epoll_fd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        throw std::system_error(errno, std::system_category(), "epoll_create1");
    }

    epoll_event wake_event{};
    wake_event.events = EPOLLIN;
    wake_event.data.u64 = wakeToken_;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wakeup_.fd(), &wake_event) < 0) {
        const int err = errno;
        ::close(epoll_fd_);
        throw std::system_error(err, std::system_category(), "epoll_ctl(eventfd)");
    }
}

EpollConnectionPool::~EpollConnectionPool() {
    // Entries close their sockets as connections_ is destroyed; closing a
    // descriptor removes it from the epoll set on its own.
    ::close(epoll_fd_);
}

EpollConnectionPool::Entry::Entry(TcpConnection&& connection, Timeouts timeouts, Clock::time_point now) noexcept
    : connection(std::move(connection)), timeouts(timeouts), last_rx(now) {}

std::span<const std::byte> EpollConnectionPool::Entry::pending_tx() const noexcept {
    return std::span<const std::byte>(tx_).subspan(tx_head_);
}

bool EpollConnectionPool::Entry::append_tx(std::span<const std::byte> bytes) noexcept {
    try {
        // Reclaim the consumed prefix before growing, once it is at least half the vector.
        if (tx_head_ > 0 && tx_head_ >= tx_.size() / 2) {
            tx_.erase(tx_.begin(), tx_.begin() + static_cast<std::ptrdiff_t>(tx_head_));
            tx_head_ = 0;
        }
        tx_.insert(tx_.end(), bytes.begin(), bytes.end());   // strong guarantee: unchanged on throw
        return true;
    } catch (...) {
        return false;
    }
}

void EpollConnectionPool::Entry::consume_tx(size_t count) noexcept {
    tx_head_ += count;
    if (tx_head_ >= tx_.size()) {
        tx_.clear();
        tx_head_ = 0;
    }
}

std::vector<std::byte> EpollConnectionPool::Entry::take_tx() noexcept {
    // Erasing a prefix of trivially copyable bytes never allocates, so never throws.
    tx_.erase(tx_.begin(), tx_.begin() + static_cast<std::ptrdiff_t>(tx_head_));
    tx_head_ = 0;
    return std::exchange(tx_, {});
}

bool EpollConnectionPool::TimerNode::operator>(const TimerNode& other) const noexcept {
    return deadline > other.deadline;
}

bool EpollConnectionPool::push_timer_locked(const TimerNode& node, bool wake_if_earliest) noexcept {
    const bool earliest = timers_.empty() || node.deadline < timers_.top().deadline;
    try {
        timers_.push(node);
    } catch (...) {
        return false;
    }
    if (wake_if_earliest && earliest) { wakeup_.signal(); }
    return true;
}

void EpollConnectionPool::stop() noexcept {
    stopping_.store(true, std::memory_order_release);
    wakeup_.signal();
}

void EpollConnectionPool::interrupt() noexcept {
    // The flag, not the wake-up event, carries the request: handle_event_locked() drains
    // the wake-up event for every kind of wake-up, so only the flag survives until the
    // top of receive()'s loop sees it.
    interrupt_pending_.store(true, std::memory_order_release);
    wakeup_.signal();
}

std::expected<uint64_t, EpollError> EpollConnectionPool::connection_add(TcpConnection connection) noexcept {
    return connection_add(std::move(connection), default_timeouts_);
}

std::expected<uint64_t, EpollError> EpollConnectionPool::connection_add(TcpConnection connection,
                                                                        Timeouts timeouts,
                                                                        std::span<const std::byte> initial_tx) noexcept {
    if (connection.native_handle() < 0) { return std::unexpected(EpollError::InvalidConnection); }
    // Applied once, up front, so the pool's send_once()/receive_once() calls
    // never write the options state.
    if (!connection.apply_options_now()) { return std::unexpected(EpollError::OptionsFailed); }

    std::lock_guard<std::mutex> lock(connections_mutex_);
    const uint64_t id = connection_next_id_;
    const int fd = connection.native_handle();

    epoll_event event{};
    // Carried-over bytes are flushed by receive() like any buffered send().
    event.events = initial_tx.empty() ? EPOLLIN : (EPOLLIN | EPOLLOUT);
    event.data.u64 = id;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
        return std::unexpected(EpollError::RegisterFailed);
    }

    const auto now = Clock::now();
    EntryIt it;
    try {
        it = connections_.try_emplace(id, std::move(connection), timeouts, now).first;
    } catch (...) {   // map node allocation failed; the connection was not moved from
        std::ignore = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        return std::unexpected(EpollError::RegisterFailed);
    }
    ++connection_next_id_;

    // From here an out-of-memory failure drops the connection: it has been moved
    // into the entry, and a connection that can never time out is not kept.
    if (!initial_tx.empty()) {
        Entry& entry = it->second;
        if (!entry.append_tx(initial_tx)) {
            drop_locked(it);
            return std::unexpected(EpollError::RegisterFailed);
        }
        ++entry.tx_generation;
        if (timeouts.write.count() > 0
            && !push_timer_locked({now + timeouts.write, id, TimerNode::Kind::Write, entry.tx_generation}, true)) {
            drop_locked(it);
            return std::unexpected(EpollError::RegisterFailed);
        }
    }

    // An epoll_wait() in progress already watches the new descriptor; only a
    // timer can make receive() need to wake up sooner.
    if (!arm_read_timer_locked(it, true)) {
        drop_locked(it);
        return std::unexpected(EpollError::RegisterFailed);
    }
    return id;
}

std::expected<uint64_t, EpollError> EpollConnectionPool::connection_dial(std::string_view address, unsigned port,
                                                                         TcpConnection::Options options,
                                                                         Timeouts timeouts,
                                                                         std::chrono::milliseconds connect_timeout) noexcept {
    auto started = TcpConnection::connect_start(address, port, options);
    if (!started && started.error() == SocketError::InvalidAddress) {
        return std::unexpected(EpollError::InvalidAddress);
    }

    std::lock_guard<std::mutex> lock(connections_mutex_);
    const uint64_t id = connection_next_id_++;

    // The attempt could not even start (no descriptors, no route, ...): there is
    // no connection to keep, but the failure is reported like any other dial's.
    if (!started) {
        try {
            notices_.push_back(std::unexpected(ReceiveError{id, started.error()}));
        } catch (...) {
            return std::unexpected(EpollError::RegisterFailed);
        }
        wakeup_.signal();   // receive() may be parked in epoll_wait()
        return id;
    }

    const int fd = started->native_handle();
    const auto now = Clock::now();
    EntryIt it;
    try {
        it = connections_.try_emplace(id, std::move(*started), timeouts, now).first;
    } catch (...) {
        return std::unexpected(EpollError::RegisterFailed);
    }
    it->second.status = Entry::Status::Connecting;

    // Writability (or an error/hang-up) is how a non-blocking connect() completes.
    epoll_event event{};
    event.events = EPOLLOUT;
    event.data.u64 = id;
    if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) < 0) {
        connections_.erase(it);
        return std::unexpected(EpollError::RegisterFailed);
    }
    if (connect_timeout.count() > 0
        && !push_timer_locked({now + connect_timeout, id, TimerNode::Kind::Connect, 0}, true)) {
        drop_locked(it);
        return std::unexpected(EpollError::RegisterFailed);
    }
    return id;
}

std::expected<EpollConnectionPool::ReleasedConnection, SocketError>
EpollConnectionPool::connection_release(uint64_t connection_id) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    Entry& entry = it->second;
    switch (entry.status) {
        case Entry::Status::Open:       break;
        case Entry::Status::Closing:    return std::unexpected(SocketError::ConnectionClosed);
        case Entry::Status::Connecting: return std::unexpected(SocketError::NotConnected);
    }

    // Unlike drop_locked(): no shutdown(), so the socket stays usable. Its
    // timers go stale once the entry is gone (ids are never reused), and a stale
    // id left in ready_ is skipped by read_ready_locked().
    std::ignore = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, entry.connection.native_handle(), nullptr);
    ReleasedConnection released{std::move(entry.connection), entry.take_tx(), entry.stats};
    connections_.erase(it);
    forget_notices_locked(connection_id);
    return released;
}

std::expected<void, SocketError> EpollConnectionPool::set_timeouts(uint64_t connection_id,
                                                                   Timeouts timeouts) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    Entry& entry = it->second;
    entry.timeouts = timeouts;
    ++entry.rx_generation;   // retires the read timer armed for the old timeout

    // A dial arms its read timer when it connects; a closing connection is no
    // longer read, so it needs none.
    if (entry.status != Entry::Status::Open) { return {}; }
    entry.last_rx = Clock::now();
    if (!arm_read_timer_locked(it, true)) {
        fail_locked(it, SocketError::Unknown);
        return std::unexpected(SocketError::Unknown);
    }
    return {};
}

std::expected<EpollConnectionPool::ConnectionStats, SocketError>
EpollConnectionPool::take_stats(uint64_t connection_id) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    return std::exchange(it->second.stats, {});
}

void EpollConnectionPool::connection_remove(uint64_t connection_id) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return; }
    forget_notices_locked(connection_id);   // e.g. a Connected not yet reported

    // A dial in progress has nothing to flush: it is closed at once.
    if (it->second.status != Entry::Status::Connecting && !it->second.pending_tx().empty()) {
        // Keep flushing; the flush timer armed by send() still bounds how long.
        // EPOLLIN is dropped: nobody reads it any more, and level-triggered
        // EPOLLIN on unread data would spin the loop.
        it->second.status = Entry::Status::Closing;
        if (set_interest_locked(it->second, connection_id, EPOLLOUT)) { return; }
    }
    drop_locked(it);
}

void EpollConnectionPool::connection_drop(uint64_t connection_id) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it != connections_.end()) {
        forget_notices_locked(connection_id);
        drop_locked(it);
    }
}

std::expected<void, SocketError> EpollConnectionPool::send(uint64_t connection_id,
                                                           std::span<const std::byte> tx_buffer) noexcept {
    std::lock_guard<std::mutex> lock(connections_mutex_);

    const auto it = connections_.find(connection_id);
    if (it == connections_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    Entry& entry = it->second;
    if (entry.status == Entry::Status::Connecting) { return std::unexpected(SocketError::NotConnected); }
    if (entry.status != Entry::Status::Open) { return std::unexpected(SocketError::ConnectionClosed); }
    if (tx_buffer.empty()) { return {}; }

    // Checked before the fast path: in the worst case nothing reaches the
    // kernel, so the whole message must fit. That keeps send() all-or-nothing.
    const size_t queued = entry.pending_tx().size();
    if (tx_buffer.size() > max_tx_buffer_ - queued) { return std::unexpected(SocketError::BufferFull); }

    if (queued != 0) {
        // EPOLLOUT is armed and the write timer has been running since the buffer
        // became non-empty; the bytes only go behind the queued ones. The buffer
        // is unchanged on failure, so the connection stays usable.
        if (!entry.append_tx(tx_buffer)) { return std::unexpected(SocketError::BufferFull); }
        return {};
    }

    // Empty buffer: try the socket directly first.
    const auto written = entry.connection.send_no_block(tx_buffer);
    if (!written) {
        const SocketError err = written.error();
        fail_locked(it, err);
        return std::unexpected(err);
    }
    entry.stats.bytes_sent += *written;
    if (*written == tx_buffer.size()) { return {}; }

    // The kernel took only part of it. From here a failure drops the connection:
    // some bytes are already on the wire, so refusing the rest would corrupt the stream.
    if (!entry.append_tx(tx_buffer.subspan(*written))
        || !set_interest_locked(entry, connection_id, EPOLLIN | EPOLLOUT)) {
        fail_locked(it, SocketError::Unknown);
        return std::unexpected(SocketError::Unknown);
    }
    ++entry.tx_generation;
    if (entry.timeouts.write.count() > 0
        && !push_timer_locked({Clock::now() + entry.timeouts.write, connection_id, TimerNode::Kind::Write,
                               entry.tx_generation}, true)) {
        fail_locked(it, SocketError::Unknown);
        return std::unexpected(SocketError::Unknown);
    }
    return {};
}

EpollConnectionPool::ReceiveOutcome EpollConnectionPool::receive(std::span<std::byte> rx_buffer) noexcept {
    // A 0-byte recv() would be indistinguishable from the peer closing.
    if (rx_buffer.empty()) { return std::unexpected(ReceiveError{0, SocketError::Unknown}); }

    std::array<epoll_event, 64> events;
    for (;;) {
        int timeout = -1;
        {
            std::lock_guard<std::mutex> lock(connections_mutex_);
            if (stopping_.load(std::memory_order_acquire)) {
                return std::unexpected(ReceiveError{0, SocketError::Stopped});
            }
            // Before any connection's news: the caller asked to be woken, and a
            // busy pool must not keep it waiting behind a stream of data.
            if (interrupt_pending_.exchange(false, std::memory_order_acq_rel)) {
                return std::unexpected(ReceiveError{0, SocketError::Interrupted});
            }

            const auto now = Clock::now();
            expire_timers_locked(now);

            // Outcomes the pool already decided on come first: they need no I/O.
            if (!notices_.empty()) {
                ReceiveOutcome notice = std::move(notices_.front());
                notices_.pop_front();
                return notice;
            }
            while (!ready_.empty()) {
                const uint64_t id = ready_.front();
                ready_.pop_front();
                if (auto outcome = read_ready_locked(id, rx_buffer)) { return *outcome; }
            }

            timeout = epoll_timeout_locked(now);
        }

        // Never hold the lock while waiting: senders must be able to get in.
        const int count = ::epoll_wait(epoll_fd_, events.data(), static_cast<int>(events.size()), timeout);
        if (count < 0) {
            if (errno == EINTR) { continue; }
            return std::unexpected(ReceiveError{0, get_last_socket_error()});
        }

        std::lock_guard<std::mutex> lock(connections_mutex_);
        for (const epoll_event& event : std::span(events).first(static_cast<size_t>(count))) {
            handle_event_locked(event.data.u64, event.events);
        }
    }
}

void EpollConnectionPool::handle_event_locked(uint64_t token, uint32_t events) noexcept {
    if (token == wakeToken_) {
        wakeup_.clear();
        return;
    }

    const auto it = connections_.find(token);
    if (it == connections_.end()) { return; }   // removed while we were waiting

    if (it->second.status == Entry::Status::Closing) {
        // Only the flush matters now. A hang-up or error means it will never finish.
        if (events & (EPOLLERR | EPOLLHUP)) { drop_locked(it); return; }
        if (events & EPOLLOUT) { std::ignore = flush_locked(it); }
        return;
    }

    if (it->second.status == Entry::Status::Connecting) {
        // Only EPOLLOUT is watched; EPOLLERR/EPOLLHUP are always reported. Any of
        // them means connect() has finished, one way or the other.
        finish_connect_locked(it);
        return;
    }

    if ((events & EPOLLOUT) && !flush_locked(it)) { return; }

    // recv() reports what EPOLLHUP/EPOLLERR mean (0 bytes or the error), so
    // they go through the ready queue like data.
    if (events & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
        try {
            ready_.push_back(token);
        } catch (...) {
            // Out of memory: skip it. Level-triggered epoll reports it again.
        }
    }
}

bool EpollConnectionPool::flush_locked(EntryIt it) noexcept {
    Entry& entry = it->second;
    const auto written = entry.connection.send_no_block(entry.pending_tx());
    if (!written) {
        fail_locked(it, written.error());
        return false;
    }

    entry.stats.bytes_sent += *written;
    entry.consume_tx(*written);
    if (!entry.pending_tx().empty()) { return true; }

    // Drained before the deadline: the generation bump makes the write timer stale.
    ++entry.tx_generation;
    if (entry.status == Entry::Status::Closing) {   // graceful close done
        drop_locked(it);
        return false;
    }
    if (!set_interest_locked(entry, it->first, EPOLLIN)) {
        fail_locked(it, SocketError::Unknown);
        return false;
    }
    return true;
}

void EpollConnectionPool::expire_timers_locked(Clock::time_point now) noexcept {
    while (!timers_.empty() && timers_.top().deadline <= now) {
        const TimerNode node = timers_.top();
        timers_.pop();

        const auto it = connections_.find(node.entry_id);
        if (it == connections_.end()) { continue; }   // already gone
        Entry& entry = it->second;

        if (node.kind == TimerNode::Kind::Write) {
            if (entry.tx_generation != node.generation) { continue; }   // drained since: stale
            // Still not flushed: drop it even though data is queued.
            fail_locked(it, SocketError::WriteTimedOut);
            continue;
        }

        if (node.kind == TimerNode::Kind::Connect) {
            if (entry.status != Entry::Status::Connecting) { continue; }   // completed since: stale
            fail_locked(it, SocketError::ConnectTimedOut);
            continue;
        }

        // Read timer. A closing connection is no longer read, so it no longer
        // needs one (the write timer still bounds the flush). set_timeouts()
        // retires a timer by bumping the generation.
        if (entry.status != Entry::Status::Open || entry.rx_generation != node.generation) { continue; }
        const auto due = entry.last_rx + entry.timeouts.read;
        if (due <= now) {
            fail_locked(it, SocketError::ReadTimedOut);
        } else if (!push_timer_locked({due, node.entry_id, TimerNode::Kind::Read, entry.rx_generation}, false)) {
            // Data arrived since this node was pushed, so it is due again later. No
            // wake: this is receive()'s thread, it recomputes its timeout next.
            fail_locked(it, SocketError::Unknown);
        }
    }
}

int EpollConnectionPool::epoll_timeout_locked(Clock::time_point now) const noexcept {
    if (timers_.empty()) { return -1; }
    // Rounded up: rounding down would wake just before the deadline and spin.
    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(timers_.top().deadline - now);
    return static_cast<int>(std::clamp<std::chrono::milliseconds::rep>(remaining.count(), 0, INT_MAX));
}

std::optional<EpollConnectionPool::ReceiveOutcome>
EpollConnectionPool::read_ready_locked(uint64_t id, std::span<std::byte> rx_buffer) noexcept {
    const auto it = connections_.find(id);
    if (it == connections_.end() || it->second.status != Entry::Status::Open) { return std::nullopt; }

    // MSG_DONTWAIT: readiness can be spurious, and a blocking fd would stall the loop.
    const auto read = it->second.connection.receive_once(rx_buffer, MSG_DONTWAIT);

    if (read && *read > 0) {   // data
        it->second.last_rx = Clock::now();
        it->second.stats.bytes_received += *read;
        return ReceiveResult{id, *read};
    }
    if (read) {                // 0 bytes: orderly shutdown, reported like TcpConnection::receive()
        drop_locked(it);
        return std::unexpected(ReceiveError{id, SocketError::ConnectionClosed});
    }
    if (is_retryable(read.error())) {   // spurious: level-triggered epoll reports it again
        return std::nullopt;
    }
    const SocketError err = read.error();   // dead socket
    drop_locked(it);
    return std::unexpected(ReceiveError{id, err});
}

bool EpollConnectionPool::set_interest_locked(const Entry& entry, uint64_t id, uint32_t events) noexcept {
    epoll_event event{};
    event.events = events;
    event.data.u64 = id;
    return ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, entry.connection.native_handle(), &event) == 0;
}

void EpollConnectionPool::drop_locked(EntryIt it) noexcept {
    std::ignore = ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, it->second.connection.native_handle(), nullptr);
    it->second.connection.shutdown_quietly();
    connections_.erase(it);   // ~TcpConnection closes the descriptor; queued timers go stale
}

void EpollConnectionPool::fail_locked(EntryIt it, SocketError error) noexcept {
    // A connection the user is already removing gets no further results.
    if (it->second.status != Entry::Status::Closing) {
        try {
            notices_.push_back(std::unexpected(ReceiveError{it->first, error}));
            wakeup_.signal();   // send() runs on other threads: receive() may be parked in epoll_wait()
        } catch (...) {
            // Out of memory: the connection is still dropped, only its report is lost.
        }
    }
    drop_locked(it);
}

void EpollConnectionPool::finish_connect_locked(EntryIt it) noexcept {
    Entry& entry = it->second;

    const auto status = entry.connection.connect_status();
    if (!status) {   // the dial failed: report why and forget the connection
        fail_locked(it, status.error());
        return;
    }
    if (!*status) { return; }   // spurious readiness: epoll reports EPOLLOUT again once it connects

    // Connected: from now on it is an ordinary connection. Its connect timer goes
    // stale with the status change; the read timeout starts counting now.
    entry.status = Entry::Status::Open;
    entry.last_rx = Clock::now();
    if (!set_interest_locked(entry, it->first, EPOLLIN) || !arm_read_timer_locked(it, false)) {
        fail_locked(it, SocketError::Unknown);
        return;
    }
    try {
        notices_.push_back(ReceiveResult{it->first, 0, ReceiveResult::Kind::Connected});
    } catch (...) {
        // Without the report the caller never learns the dial worked: give it up.
        fail_locked(it, SocketError::Unknown);
    }
    // No wake-up: this runs on receive()'s thread, which checks notices_ next.
}

bool EpollConnectionPool::arm_read_timer_locked(EntryIt it, bool wake_if_earliest) noexcept {
    const Entry& entry = it->second;
    if (entry.timeouts.read.count() <= 0) { return true; }
    return push_timer_locked({entry.last_rx + entry.timeouts.read, it->first, TimerNode::Kind::Read,
                              entry.rx_generation}, wake_if_earliest);
}

void EpollConnectionPool::forget_notices_locked(uint64_t connection_id) noexcept {
    std::erase_if(notices_, [connection_id](const ReceiveOutcome& notice) {
        return (notice ? notice->connection_id : notice.error().connection_id) == connection_id;
    });
}
