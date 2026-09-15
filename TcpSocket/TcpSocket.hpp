#pragma once

#include <cerrno>
#include <cstddef>
#include <expected>
#include <span>
#include <stdexcept>
#include <string_view>

enum class SocketError {
    AddressInUse,
    ConnectionRefused,
    ConnectionReset,
    WouldBlock,
    BrokenPipe,
    InvalidAddress,
    ConnectionClosed,
    Unknown
};

class SocketException : public std::runtime_error {
    public:
        explicit SocketException(SocketError code);
    
        SocketError code() const noexcept;
    
    private:
        SocketError code_;
};

class TcpConnection {
    public:
        explicit TcpConnection(int connected_fd) noexcept; 
        ~TcpConnection();
    
        TcpConnection(const TcpConnection&) = delete;
        TcpConnection& operator=(const TcpConnection&) = delete;
        
        TcpConnection(TcpConnection&& other) noexcept;
        TcpConnection& operator=(TcpConnection&& other) noexcept;
    
        std::expected<size_t, SocketError> send(std::span<const std::byte> source) noexcept;
        std::expected<size_t, SocketError> receive(std::span<std::byte> destination) noexcept;
    
        static std::expected<TcpConnection, SocketError> connect(std::string_view server_address, unsigned port) noexcept;

    private:
        int fd_; 
    };

class TcpListener {
    public:
        TcpListener(std::string_view local_address, unsigned port, int backlog);
        ~TcpListener();

        TcpListener(const TcpListener&) = delete;
        TcpListener& operator=(const TcpListener&) = delete;
        
        TcpListener(TcpListener&& other) noexcept;
        TcpListener& operator=(TcpListener&& other) noexcept;
    
        std::expected<TcpConnection, SocketError> accept_client() noexcept;
    private:
        int listen_fd_;
    };
