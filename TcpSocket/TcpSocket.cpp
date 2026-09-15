#include "TcpSocket.hpp"

#include <arpa/inet.h>
#include <asm-generic/socket.h>
#include <expected>
#include <netinet/in.h>
#include <span>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

namespace {
    SocketError get_last_socket_error() noexcept {
        switch (errno) {
            case EADDRINUSE:   return SocketError::AddressInUse;
            case ECONNREFUSED: return SocketError::ConnectionRefused;
            case ECONNRESET:   return SocketError::ConnectionReset;
            case EPIPE:        return SocketError::BrokenPipe;
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
            default:                             return "Unknown socket error";
        }
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

    struct sockaddr_in addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {}
    };

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

TcpConnection::TcpConnection(TcpConnection&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

TcpConnection& TcpConnection::operator=(TcpConnection&& other) noexcept {
    if (this == &other) { return *this; }
    if (fd_ >= 0) { close(fd_); }
    this->fd_ = std::exchange(other.fd_, -1);
    return *this;
}

TcpConnection::~TcpConnection() {
    if (fd_ >= 0) { close(fd_); }
}

std::expected<TcpConnection, SocketError> TcpConnection::connect(std::string_view server_address, unsigned port) noexcept {
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) { 
        return std::unexpected(get_last_socket_error()); 
    }

    struct sockaddr_in server_addr = {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {}
    };

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
    ssize_t ret = ::send(fd_, source.data(), source.size_bytes(), 0);
    if (ret < 0) { return std::unexpected(get_last_socket_error()); }
    return static_cast<size_t>(ret);
}

std::expected<size_t, SocketError> TcpConnection::receive(std::span<std::byte> destination) noexcept {
    if (destination.empty()) { return 0; }
    ssize_t ret = ::recv(fd_, destination.data(), destination.size_bytes(), 0);
    if (ret < 0) { return std::unexpected(get_last_socket_error()); }
    if (ret == 0) { return std::unexpected(SocketError::ConnectionClosed); }
    return static_cast<size_t>(ret);
}