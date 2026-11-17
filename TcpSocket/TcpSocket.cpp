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
    // Port 0 is not a destination: Linux does not refuse such a connect(), the SYN
    // just goes unanswered and the call blocks for the whole SYN-retry budget
    // (~130 s with tcp_syn_retries=6). Fail fast instead (and reject what htons()
    // would silently truncate).
    if (port == 0 || port > 65535) { return std::unexpected(SocketError::InvalidAddress); }
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
