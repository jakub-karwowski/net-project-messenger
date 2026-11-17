#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string_view>
#include <sys/epoll.h>
#include <unordered_map>
#include <vector>


enum class SocketError {
    AddressInUse,
    ConnectionRefused,
    ConnectionReset,
    WouldBlock,
    BrokenPipe,
    Interrupted,
    InvalidAddress,
    ConnectionClosed,
    ConnectionNotFound,
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

        // Tunable behaviour of the underlying file descriptor. All values are
        // applied lazily to the socket before the next send()/receive() call.
        struct Options {
            unsigned max_retries = 0;                       // extra attempts after a retryable failure
            std::chrono::milliseconds retry_delay{0};       // pause between attempts
            std::chrono::milliseconds send_timeout{0};      // 0 disables SO_SNDTIMEO
            std::chrono::milliseconds receive_timeout{0};   // 0 disables SO_RCVTIMEO
            bool non_blocking = false;                      // O_NONBLOCK
            bool tcp_nodelay = false;                       // TCP_NODELAY
            bool keep_alive = false;                        // SO_KEEPALIVE
        };

        // Builder-style configuration: each call mutates the stored options and
        // returns *this so calls can be chained. Options are applied to the fd
        // on the next I/O operation; failures surface as a SocketError there.
        TcpConnection& with_retries(unsigned attempts,
                                    std::chrono::milliseconds delay = std::chrono::milliseconds{0}) noexcept;
        TcpConnection& with_send_timeout(std::chrono::milliseconds timeout) noexcept;
        TcpConnection& with_receive_timeout(std::chrono::milliseconds timeout) noexcept;
        TcpConnection& with_non_blocking(bool enable = true) noexcept;
        TcpConnection& with_tcp_nodelay(bool enable = true) noexcept;
        TcpConnection& with_keep_alive(bool enable = true) noexcept;
        TcpConnection& with_options(Options options) noexcept;

        const Options& options() const noexcept;

        // Applies the stored options to the fd right now instead of lazily on the
        // first I/O call. Once it has succeeded, send_once()/receive_once() only READ
        // the "applied" flag, so two threads may use one connection concurrently (one
        // reader, one writer). The with_* setters must NOT be called once a connection
        // is shared between threads (they mutate unsynchronised state).
        std::expected<void, SocketError> apply_options_now() noexcept;

        // Direction(s) to shut down at the socket level (see shutdown()).
        enum class Shutdown { Read, Write, Both };

        // Wakes any thread blocked in send()/receive() by shutting down the
        // underlying socket direction. Unlike the destructor this does NOT close
        // the descriptor, so it is safe to call from another thread while a
        // worker is parked in I/O. Returns true when a shutdown was actually
        // issued, or false when there was nothing to shut down (moved-from
        // socket or an already-closed peer/direction).
        std::expected<bool, SocketError> shutdown(Shutdown how = Shutdown::Both) noexcept;
    
        std::expected<size_t, SocketError> send(std::span<const std::byte> source) noexcept;
        std::expected<size_t, SocketError> receive(std::span<std::byte> destination) noexcept;
    
        // Blocking connect. Port 0 (or > 65535) fails at once with InvalidAddress.
        static std::expected<TcpConnection, SocketError> connect(std::string_view server_address, unsigned port) noexcept;

    protected:
        // Raw descriptor backing this connection, or -1 when moved from. Intended
        // for readiness polling/configuration (::poll(), fcntl()); all reads and
        // writes go through send()/receive() or the raw hooks below.
        int native_handle() const noexcept;

        // Orderly teardown before the descriptor is closed: sends FIN and wakes any
        // thread parked on the socket. The result of shutdown() is deliberately
        // discarded (a peer that already vanished is not an error).
        void shutdown_quietly() noexcept;

        // Single-syscall raw I/O for multiplexers (and subclasses). The caller owns
        // readiness (e.g. ::poll() on native_handle()) and supplies the flags; unlike
        // send()/receive() these never retry, never turn a 0-byte read into an error,
        // and report WouldBlock/Interrupted verbatim. MSG_NOSIGNAL is always added.
        std::expected<size_t, SocketError> send_once(std::span<const std::byte> source, int flags) noexcept;
        std::expected<size_t, SocketError> receive_once(std::span<std::byte> destination, int flags) noexcept;

        friend class EpollConnectionPool;

    private:
        std::expected<void, SocketError> apply_options() noexcept;

        int fd_;
        Options options_{};
        bool options_applied_ = false;
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

class EpollConnectionPool;


