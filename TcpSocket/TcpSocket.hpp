#pragma once

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <mutex>
#include <poll.h>
#include <span>
#include <stdexcept>
#include <string_view>
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

        friend class TcpConnectionPool;

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

// Result of a multiplexed receive operation.
struct ReceiveResult {
    int connection_id;      // ID of the connection that produced the event
    std::size_t bytes_read; // Number of bytes written to destination (0 indicates clean EOF / disconnect)
};

// Single-waiter poll set. Producers publish the desired state with upsert()/erase()
// under their own lock; wait() rebuilds the kernel array from that snapshot right
// before blocking, so the array ::poll() reads is never touched by another thread.
class PollSet {
public:
    PollSet();
    ~PollSet();

    PollSet(const PollSet&) = delete;
    PollSet& operator=(const PollSet&) = delete;

    // Safe at any time: only the desired-state table changes here.
    void upsert(int id, int fd, short events);
    void erase(int id) noexcept;

    // Breaks a blocked wait().
    void wake() const noexcept;

    // Call under the caller's lock immediately before wait().
    void prepare();

    // Call WITHOUT the caller's lock; blocks until a descriptor is ready.
    int wait(int timeout_ms) noexcept;

    // Call under the caller's lock after wait() returns.
    void drain_wake() const noexcept;

    // Visits ready (id, revents) pairs in round-robin order. The visitor returns
    // true to stop scanning, false to keep going.
    template <typename Visitor>
    void for_each_ready(Visitor&& visit) {
        const std::size_t n = ids_.size();
        for (std::size_t step = 0; step < n; ++step) {
            const std::size_t i = (cursor_ + step) % n;
            const short revents = fds_[i + 1].revents;
            if (revents == 0) { continue; }
            if (visit(ids_[i], revents)) {
                cursor_ = (i + 1) % n; // service the next socket first next time
                return;
            }
        }
    }

private:
    int wake_fd_{-1};
    bool dirty_{false};
    std::size_t cursor_{0};

    std::unordered_map<int, pollfd> desired_; // Producer-facing state (caller's lock)
    std::vector<pollfd> fds_;                 // Kernel array owned by prepare()/wait()
    std::vector<int> ids_;                    // fds_[i + 1] belongs to ids_[i]
};

// Thread-safe, full-duplex I/O multiplexer for managing a bounded pool of TCP connections.
//
// Uses ::poll() inside receive() to drive both incoming reads (POLLIN) and any pending
// asynchronous writes (POLLOUT) without requiring a separate background writer thread.
//
// receive() must be driven by a single thread; add_connection(), send(),
// remove_connection() and close_pool() may be called concurrently from other threads.
class TcpConnectionPool {
public:
    // Initializes the connection pool and allocates the internal wake-up descriptor.
    //
    // max_connections:     maximum live connections before the oldest idle ones are pruned.
    // idle_timeout:        duration of inactivity after which a connection is dropped.
    // max_tx_buffer_bytes: per-connection outbound buffer cap to protect against slow peers.
    explicit TcpConnectionPool(
        std::size_t max_connections = 128,
        std::chrono::milliseconds idle_timeout = std::chrono::seconds(60),
        std::size_t max_tx_buffer_bytes = 1024 * 1024);

    ~TcpConnectionPool();

    TcpConnectionPool(const TcpConnectionPool&) = delete;
    TcpConnectionPool& operator=(const TcpConnectionPool&) = delete;
    TcpConnectionPool(TcpConnectionPool&&) = delete;
    TcpConnectionPool& operator=(TcpConnectionPool&&) = delete;

    // Registers a new connection in the pool, sets it to non-blocking mode,
    // and wakes any thread blocked in receive().
    //
    // Returns the unique connection_id assigned to this socket, or -1 when the
    // connection is not usable (moved-from) or the pool has already been closed.
    int add_connection(TcpConnection connection) noexcept;

    // Explicitly removes and closes a connection by its ID.
    bool remove_connection(int connection_id) noexcept;

    // Signals the pool to shut down and unblocks any thread waiting in receive().
    void close_pool() noexcept;

    // Drives the ::poll() event loop: flushes pending outbound tx_buffers on POLLOUT,
    // sweeps idle/over-capacity connections, and blocks until at least one connection
    // has incoming bytes (POLLIN) or disconnects.
    //
    // destination: caller-provided buffer to receive raw bytes from a ready socket;
    //              must not be empty.
    // Returns a ReceiveResult with the connection_id and bytes read (0 on disconnect),
    // or a SocketError if the pool is closed or ::poll() fails fatally. A connection
    // with a fatal read/write error is dropped and reported as a disconnect
    // (bytes_read == 0).
    std::expected<ReceiveResult, SocketError> receive(
        std::span<std::byte> destination) noexcept;

    // Enqueues a complete buffer for non-blocking transmission to the specified connection.
    //
    // Fast path: if the connection's tx_buffer is currently empty, attempts an immediate
    // non-blocking ::send().
    // Slow path: any unsent remainder is appended to the connection's tx_buffer, the
    // poll set is marked dirty, and receive() arms POLLOUT and finishes flushing it.
    //
    // Returns void on success, or SocketError::ConnectionNotFound if the ID is unknown,
    // SocketError::ConnectionClosed if the pool is closed, the fatal write error if the
    // connection broke (in which case it is dropped from the pool), or
    // SocketError::WouldBlock when accepting the buffer would exceed
    // max_tx_buffer_bytes (backpressure limit, nothing is queued).
    std::expected<void, SocketError> send(
        int connection_id,
        std::span<const std::byte> source) noexcept;

private:
    struct Entry {
        TcpConnection connection;
        std::int64_t last_active_ms{};       // Monotonic timestamp of last activity
        std::vector<std::byte> tx_buffer;    // Pending outbound bytes awaiting POLLOUT
        std::size_t tx_offset{0};            // Read cursor into tx_buffer to avoid O(N) front erases

        short desired_events() const noexcept {
            return tx_buffer.size() > tx_offset
                ? static_cast<short>(POLLIN | POLLOUT)
                : static_cast<short>(POLLIN);
        }
    };

    using ConnectionMap = std::unordered_map<int, Entry>;
    using Iterator = ConnectionMap::iterator;

    static std::int64_t now_ms() noexcept;

    // Attempts to flush entry.tx_buffer via non-blocking ::send() (caller must hold mutex_).
    // Returns false if the socket encountered a fatal write error and should be dropped.
    bool flush_tx_locked(Entry& entry) noexcept;

    // Sweeps expired connections and enforces max_connections_ (caller must hold mutex_).
    void prune_locked(std::int64_t now) noexcept;

    // shutdown + erase from connections_ and the poll set; returns the next iterator.
    Iterator drop_locked(Iterator it) noexcept;

    std::mutex mutex_;
    int next_id_{1};
    std::size_t max_connections_;
    std::int64_t idle_timeout_ms_;
    std::size_t max_tx_buffer_bytes_;
    std::atomic<bool> closed_{false};

    std::int64_t sweep_interval_ms_{0};   // How often receive() sweeps idle/LRU entries
    std::int64_t next_sweep_ms_{0};       // When the next sweep is due

    PollSet poll_set_;
    ConnectionMap connections_;
};


