#pragma once

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <expected>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
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
    BufferFull,
    ReadTimedOut,
    WriteTimedOut,
    Stopped,
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

        // Writes as much of source as the kernel accepts right now and never blocks
        // (MSG_DONTWAIT, whatever O_NONBLOCK says). Returns the byte count written,
        // which may be anything from 0 to source.size(); WouldBlock is not an error.
        std::expected<size_t, SocketError> send_no_block(std::span<const std::byte> source) noexcept;
    
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

enum class EpollError {
    InvalidConnection,   // moved-from TcpConnection (no descriptor)
    OptionsFailed,       // apply_options_now() failed
    RegisterFailed,      // epoll_ctl(EPOLL_CTL_ADD) failed
};

// Event loop over many connections. One thread drives it by calling receive();
// any thread may call send()/connection_add()/connection_remove()/connection_drop()/stop().
//
// Outgoing data: send() writes straight to the socket when nothing is queued
// (fast path) and buffers the unsent rest, arming EPOLLOUT. receive() flushes
// the buffers as the sockets become writable.
//
// Timeouts (per connection, see Timeouts) drop a connection that received
// nothing for too long (ReadTimedOut) or whose TX buffer was not flushed in
// time (WriteTimedOut). Buffers are flushed and timers fire ONLY while a
// thread is inside receive().
class EpollConnectionPool {
    public:
        // bytes_read bytes from connection_id were written to rx_buffer.
        struct ReceiveResult { uint64_t connection_id; size_t bytes_read; };

        // Why receive() returned without data. For a nonzero connection_id the
        // connection has already been removed from the pool:
        //   ConnectionClosed  peer closed it (orderly shutdown)
        //   ReadTimedOut      nothing was received from it for Timeouts::read
        //   WriteTimedOut     its TX buffer was not flushed within Timeouts::write
        //   anything else     the socket error that killed it
        // connection_id == 0 is about the pool itself: Stopped after stop(), or the
        // error from epoll_wait() / an empty rx_buffer (Unknown).
        struct ReceiveError { uint64_t connection_id; SocketError error; };
        using ReceiveOutcome = std::expected<ReceiveResult, ReceiveError>;

        // Per-connection timeouts; 0 disables one. They are the pool's own timers,
        // unrelated to TcpConnection::Options::send/receive_timeout (SO_SNDTIMEO /
        // SO_RCVTIMEO only limit blocking calls, and the pool never blocks).
        struct Timeouts {
            // Drop the connection when nothing has been received for this long.
            std::chrono::milliseconds read{0};
            // Drop the connection when its TX buffer is still not empty this long
            // after it became non-empty.
            std::chrono::milliseconds write{0};
        };

        // Defaults: no read timeout, 5 s write timeout, 1 MiB TX buffer per connection.
        // Both throw std::system_error when the epoll/eventfd descriptors cannot be set up.
        EpollConnectionPool();
        explicit EpollConnectionPool(Timeouts default_timeouts, size_t max_tx_buffer = 1024 * 1024);
        ~EpollConnectionPool();
        EpollConnectionPool(const EpollConnectionPool&) = delete;
        EpollConnectionPool(EpollConnectionPool&&) = delete;
        EpollConnectionPool& operator=(const EpollConnectionPool&) = delete;
        EpollConnectionPool& operator=(EpollConnectionPool&&) = delete;

        // Blocks until there is something to report. Meanwhile flushes queued
        // outgoing data and drops connections whose timers expired. Connections are
        // served in turn. rx_buffer must not be empty. Single consumer: call it from
        // one thread only.
        ReceiveOutcome receive(std::span<std::byte> rx_buffer) noexcept;

        // Queues the whole of tx_buffer or nothing: fails with BufferFull when it does
        // not fit in the connection's free buffer space, ConnectionNotFound for an
        // unknown id, ConnectionClosed when the connection is being removed. A
        // socket error, or running out of memory for the buffer or its timer, drops
        // the connection (also reported by receive()).
        std::expected<void, SocketError> send(uint64_t connection_id, std::span<const std::byte> tx_buffer) noexcept;

        // Takes ownership of the connection. Its options are applied once here;
        // from now on all I/O on it goes through the pool. The read timeout starts
        // counting at once. The first overload uses the pool's default timeouts.
        std::expected<uint64_t, EpollError> connection_add(TcpConnection connection) noexcept;
        std::expected<uint64_t, EpollError> connection_add(TcpConnection connection, Timeouts timeouts) noexcept;

        // Graceful removal: send() refuses new data at once, queued data is still
        // flushed (bounded by the write timeout), then the connection is closed.
        // No-op for an unknown id; receive() reports nothing more for this id.
        void connection_remove(uint64_t connection_id) noexcept;

        // Immediate removal: queued data is discarded and the connection closed now.
        // No-op for an unknown id; receive() reports nothing more for this id.
        void connection_drop(uint64_t connection_id) noexcept;

        // Makes the current and every later receive() fail with Stopped.
        void stop() noexcept;

    private:
        using Clock = std::chrono::steady_clock;

        struct Entry {
            TcpConnection connection;
            enum class Status { Open, Closing } status = Status::Open;
            Timeouts timeouts;
            Clock::time_point last_rx;   // last received data (or when added)
            // Bumped whenever the write timer is armed or the buffer drains; a
            // write TimerNode is live only while its generation matches.
            uint64_t tx_generation = 0;

            Entry(TcpConnection&& connection, Timeouts timeouts, Clock::time_point now) noexcept;

            std::span<const std::byte> pending_tx() const noexcept;
            [[nodiscard]] bool append_tx(std::span<const std::byte> bytes) noexcept;   // false: out of memory
            void consume_tx(size_t count) noexcept;

        private:
            std::vector<std::byte> tx_;   // queued bytes are tx_[tx_head_, size)
            size_t tx_head_ = 0;
        };
        using EntryIt = std::map<uint64_t, Entry>::iterator;

        // Write timers: one per buffer fill, invalidated by tx_generation.
        // Read timers: at most one per connection. Receiving only moves last_rx;
        // when the node pops early it is pushed again for last_rx + read.
        struct TimerNode {
            enum class Kind : uint8_t { Read, Write };
            Clock::time_point deadline;
            uint64_t entry_id;
            Kind kind;
            uint64_t generation;   // Write only

            bool operator>(const TimerNode& other) const noexcept;
        };

        std::map<uint64_t, Entry> connections_;
        std::priority_queue<TimerNode, std::vector<TimerNode>, std::greater<>> timers_;
        std::deque<uint64_t> ready_;            // readable ids reported by the last epoll_wait()
        std::deque<ReceiveError> notices_;      // removals the pool made itself, reported first
        std::mutex connections_mutex_;
        std::atomic<bool> stopping_{false};
        int epoll_fd_ = -1;
        int wakeup_event_fd_ = -1;
        uint64_t connection_next_id_{1};
        Timeouts default_timeouts_;
        size_t max_tx_buffer_;

        static constexpr uint64_t wakeToken_ = 0;   // connection ids start at 1

        void wake() const noexcept;

        // All *_locked helpers require connections_mutex_ to be held.

        // Pushes a timer; false when out of memory (the caller drops the connection,
        // which can then never time out). wake_if_earliest: wake receive() when this
        // is now the earliest deadline, needed from any thread but receive()'s own,
        // since it may be parked in epoll_wait() with a later timeout.
        [[nodiscard]] bool push_timer_locked(const TimerNode& node, bool wake_if_earliest) noexcept;

        // token/events: one epoll_event's data.u64 and event mask.
        void handle_event_locked(uint64_t token, uint32_t events) noexcept;
        // Writes what the kernel takes; false when that removed the connection.
        [[nodiscard]] bool flush_locked(EntryIt it) noexcept;
        void expire_timers_locked(Clock::time_point now) noexcept;
        int epoll_timeout_locked(Clock::time_point now) const noexcept;
        // Reads one ready connection; nullopt when there was nothing to report
        // (spurious readiness, or the connection is gone or closing).
        std::optional<ReceiveOutcome> read_ready_locked(uint64_t id, std::span<std::byte> rx_buffer) noexcept;
        bool set_interest_locked(const Entry& entry, uint64_t id, uint32_t events) noexcept;
        void drop_locked(EntryIt it) noexcept;
        // Queues an error notice (none when Closing) and drops the connection.
        void fail_locked(EntryIt it, SocketError error) noexcept;
};
