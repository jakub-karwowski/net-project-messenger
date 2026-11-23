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
#include <string>
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
    ConnectTimedOut,     // a dial did not complete within its connect timeout
    HostUnreachable,     // EHOSTUNREACH / ENETUNREACH
    NotConnected,        // the connection is still being dialled
    TooManyOpenFiles,    // EMFILE / ENFILE: no descriptor for a new socket
    Stopped,
    Unknown
};

// An IPv4 address and port in plain C++ types (no <netinet/in.h> needed).
struct TcpEndpoint {
    std::string address;   // dotted quad, e.g. "127.0.0.1"
    uint16_t port = 0;
    friend bool operator==(const TcpEndpoint&, const TcpEndpoint&) = default;
};

// A wake-up signal (an eventfd). Any thread raises it with signal(); the thread
// that waits watches fd() in poll()/epoll next to its sockets, and calls clear()
// once it has woken. Signals raised before clear() collapse into one wake-up.
// It carries no meaning of its own: keep the reason for waking in a flag next to it.
class WakeupEvent {
    public:
        WakeupEvent();   // throws std::system_error when the eventfd cannot be created
        ~WakeupEvent();
        WakeupEvent(const WakeupEvent&) = delete;
        WakeupEvent& operator=(const WakeupEvent&) = delete;
        WakeupEvent(WakeupEvent&& other) noexcept;
        WakeupEvent& operator=(WakeupEvent&& other) noexcept;

        void signal() const noexcept;
        void clear() const noexcept;
        int fd() const noexcept;

    private:
        int fd_;
};

class SocketException : public std::runtime_error {
    public:
        explicit SocketException(SocketError code);
    
        SocketError code() const noexcept;
    
    private:
        SocketError code_;
};

// A TCP socket. Connections come from TcpListener::accept_client(),
// TcpConnection::connect() or TcpConnection::connect_start(), which always know
// the peer's address (see remote_endpoint()).
class TcpConnection {
    public:
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
    
        // Blocking connect. A malformed IPv4 address, or port 0 (or > 65535), fails at
        // once with InvalidAddress.
        static std::expected<TcpConnection, SocketError> connect(std::string_view server_address, unsigned port) noexcept;

        // Non-blocking connect, step 1: starts the TCP handshake and returns at once.
        // `options` are applied before connecting, except that the socket is always
        // non-blocking. Then wait until the descriptor is writable (POLLOUT/EPOLLOUT)
        // and ask connect_status() how it went.
        //
        // Fails with InvalidAddress for a malformed address or port 0 / > 65535, and
        // otherwise with whatever stopped the attempt from starting: socket(), the
        // options, or a connect() the kernel refused immediately (e.g. no route).
        static std::expected<TcpConnection, SocketError> connect_start(std::string_view server_address, unsigned port,
                                                                       Options options) noexcept;

        // Non-blocking connect, step 2, for a connection from connect_start() whose
        // descriptor was reported writable (or in error): true when the handshake
        // has completed, false when it is still in flight (the readiness was
        // spurious: wait again), or why it failed (ConnectionRefused,
        // HostUnreachable, ConnectTimedOut when the kernel gave up on its SYN
        // retries, ...).
        std::expected<bool, SocketError> connect_status() const noexcept;

        // The peer's address, recorded when the connection was created: what
        // accept() returned, or the destination of connect()/connect_start(). It
        // stays readable after the peer has gone away, and travels with the object
        // (into a pool and out of it again). A moved-from connection holds an empty
        // TcpEndpoint{}, as it holds no descriptor.
        const TcpEndpoint& remote_endpoint() const noexcept;

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
        friend class TcpListener;

    private:
        // Takes ownership of connected_fd, a socket connected (or connecting) to `remote`.
        TcpConnection(int connected_fd, TcpEndpoint remote) noexcept;

        std::expected<void, SocketError> apply_options() noexcept;

        int fd_;
        Options options_{};
        bool options_applied_ = false;
        TcpEndpoint remote_;
    };

class TcpListener {
    public:
        // Port 0 binds an ephemeral port; local_port() says which.
        TcpListener(std::string_view local_address, unsigned port, int backlog);
        ~TcpListener();

        TcpListener(const TcpListener&) = delete;
        TcpListener& operator=(const TcpListener&) = delete;

        // Not while another thread is inside accept_client().
        TcpListener(TcpListener&& other) noexcept;
        TcpListener& operator=(TcpListener&& other) noexcept;

        // Blocks until a client connects or stop() is called. Fails with:
        //   Stopped           stop() was called (now or earlier; it is permanent)
        //   TooManyOpenFiles  no descriptor for the new socket: back off and retry,
        //                     the client stays queued
        //   anything else     the error accept() reported
        // A client that gives up between being queued and being accepted is skipped.
        std::expected<TcpConnection, SocketError> accept_client() noexcept;

        // Makes the current and every later accept_client() return Stopped. Safe
        // from any thread, and more than once. The socket stays open (clients still
        // queued are refused when the listener is destroyed).
        void stop() noexcept;

        // The port the socket is bound to.
        std::expected<uint16_t, SocketError> local_port() const noexcept;

    private:
        // Blocks until a client is queued or stop() raised stop_event_.
        std::expected<void, SocketError> wait_for_client() const noexcept;
        // One non-blocking accept; WouldBlock when no client is ready any more.
        std::expected<TcpConnection, SocketError> try_accept() const noexcept;

        int listen_fd_;
        WakeupEvent stop_event_;
        std::atomic<bool> stopping_{false};
    };

enum class EpollError {
    InvalidConnection,   // moved-from TcpConnection (no descriptor)
    OptionsFailed,       // apply_options_now() failed
    RegisterFailed,      // epoll_ctl(EPOLL_CTL_ADD) failed, or out of memory
    InvalidAddress,      // connection_dial(): not an IPv4 address, or port 0 / > 65535
};

// Event loop over many connections. One thread drives it by calling receive();
// any thread may call the other members.
//
// Outgoing data: send() writes straight to the socket when nothing is queued
// (fast path) and buffers the unsent rest, arming EPOLLOUT. receive() flushes
// the buffers as the sockets become writable.
//
// Connections enter the pool either already connected (connection_add) or as a
// non-blocking dial (connection_dial) that receive() later reports as Connected
// or failed.
//
// Timeouts (per connection, see Timeouts) drop a connection that received
// nothing for too long (ReadTimedOut) or whose TX buffer was not flushed in
// time (WriteTimedOut); a dial has its own connect timeout (ConnectTimedOut).
// Buffers are flushed and timers fire ONLY while a thread is inside receive().
//
// A connection can leave the pool alive (connection_release) together with its
// unsent bytes, and join another pool with them (connection_add's initial_tx):
// this is how a connection moves between worker threads without losing data.
class EpollConnectionPool {
    public:
        // Kind::Data      bytes_read bytes from connection_id were written to rx_buffer.
        // Kind::Connected a connection_dial() completed; bytes_read is 0. The
        //                 connection now accepts send().
        struct ReceiveResult {
            enum class Kind : uint8_t { Data, Connected };
            uint64_t connection_id;
            size_t bytes_read;
            Kind kind = Kind::Data;
        };

        // Why receive() returned without data. For a nonzero connection_id the
        // connection has already been removed from the pool:
        //   ConnectionClosed  peer closed it (orderly shutdown)
        //   ReadTimedOut      nothing was received from it for Timeouts::read
        //   WriteTimedOut     its TX buffer was not flushed within Timeouts::write
        //   ConnectTimedOut   a dial did not complete within its connect timeout
        //   anything else     the socket error that killed it (for a dial, why
        //                     connect() failed: ConnectionRefused, HostUnreachable, ...)
        // connection_id == 0 is about the pool itself:
        //   Interrupted       interrupt() was called (reported once per batch of calls)
        //   Stopped           stop() was called
        //   Unknown           epoll_wait() failed, or rx_buffer was empty
        struct ReceiveError { uint64_t connection_id; SocketError error; };
        using ReceiveOutcome = std::expected<ReceiveResult, ReceiveError>;

        // Bytes moved on one connection since the previous take_stats() (or since
        // it joined this pool). Sent counts what the kernel accepted, not what was
        // queued.
        struct ConnectionStats {
            uint64_t bytes_received = 0;
            uint64_t bytes_sent = 0;
        };

        // A live connection taken out of the pool by connection_release().
        struct ReleasedConnection {
            TcpConnection connection;
            std::vector<std::byte> unsent_tx;   // queued bytes the kernel has not taken yet
            ConnectionStats stats;              // not yet collected by take_stats()
        };

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
        //
        // initial_tx is queued ahead of any later send() and flushed by receive(),
        // under the write timeout, exactly as if send() had buffered it. It is not
        // limited by max_tx_buffer: it carries the unsent bytes of a connection
        // released from another pool, and refusing them would corrupt the stream.
        std::expected<uint64_t, EpollError> connection_add(TcpConnection connection) noexcept;
        std::expected<uint64_t, EpollError> connection_add(TcpConnection connection, Timeouts timeouts,
                                                           std::span<const std::byte> initial_tx = {}) noexcept;

        // Starts a non-blocking connect (TcpConnection::connect_start) and returns
        // the new id at once. receive() later reports either
        // ReceiveResult{id, 0, Kind::Connected} or a ReceiveError{id, ...}
        // (ConnectionRefused, HostUnreachable, ConnectTimedOut, ...). Only a
        // malformed address fails here (InvalidAddress); every other failure, even
        // one that happens before the handshake starts, is reported through
        // receive(), so the caller has one code path. Until Connected, send() fails
        // with NotConnected.
        //
        // `options` are applied to the new socket, except that it is always
        // non-blocking. The read timeout starts counting when the connection
        // completes. A connect_timeout of 0 leaves the dial to the kernel's own
        // SYN-retry limit (minutes).
        std::expected<uint64_t, EpollError> connection_dial(std::string_view address, unsigned port,
                                                            TcpConnection::Options options,
                                                            Timeouts timeouts,
                                                            std::chrono::milliseconds connect_timeout) noexcept;

        // Takes a connected connection out of the pool WITHOUT closing it: no FIN
        // is sent, and bytes the peer already sent stay in the kernel's receive
        // buffer for the next owner. Its queued TX bytes and uncollected stats come
        // with it. Fails with ConnectionNotFound for an unknown id, ConnectionClosed
        // while it is being removed, NotConnected while it is still being dialled.
        // receive() reports nothing more for this id.
        //
        // Call it from the receive() thread (between receive() calls) when the
        // caller keeps per-connection state of its own, e.g. a frame decoder, that
        // must move together with the connection.
        std::expected<ReleasedConnection, SocketError> connection_release(uint64_t connection_id) noexcept;

        // Replaces a connection's timeouts. The read timeout restarts counting now
        // (0 disables it). A new write timeout applies from the next time the TX
        // buffer goes from empty to non-empty; a flush already being timed keeps
        // its deadline. Fails with ConnectionNotFound for an unknown id.
        std::expected<void, SocketError> set_timeouts(uint64_t connection_id, Timeouts timeouts) noexcept;

        // Returns the bytes moved since the previous call for this connection and
        // resets the counters. Fails with ConnectionNotFound for an unknown id.
        std::expected<ConnectionStats, SocketError> take_stats(uint64_t connection_id) noexcept;

        // Graceful removal: send() refuses new data at once, queued data is still
        // flushed (bounded by the write timeout), then the connection is closed.
        // No-op for an unknown id; receive() reports nothing more for this id.
        void connection_remove(uint64_t connection_id) noexcept;

        // Immediate removal: queued data is discarded and the connection closed now.
        // No-op for an unknown id; receive() reports nothing more for this id.
        void connection_drop(uint64_t connection_id) noexcept;

        // Makes the current receive(), or the next one if none is running, return
        // ReceiveError{0, Interrupted} once. Calls made before receive() gets to
        // report collapse into one report. Use it to make the receive() thread look
        // at work handed to it some other way (e.g. a command queue).
        void interrupt() noexcept;

        // Makes the current and every later receive() fail with Stopped.
        void stop() noexcept;

    private:
        using Clock = std::chrono::steady_clock;

        struct Entry {
            TcpConnection connection;
            // Connecting: a connection_dial() in progress (EPOLLOUT only, no reads).
            // Open: normal. Closing: connection_remove() is flushing it.
            enum class Status { Connecting, Open, Closing } status = Status::Open;
            Timeouts timeouts;
            Clock::time_point last_rx;   // last received data (or when added / connected)
            // Bumped whenever the write timer is armed or the buffer drains; a
            // write TimerNode is live only while its generation matches.
            uint64_t tx_generation = 0;
            // Bumped by set_timeouts(); a read TimerNode is live only while its
            // generation matches, so replacing the read timeout retires the old timer.
            uint64_t rx_generation = 0;
            ConnectionStats stats;

            Entry(TcpConnection&& connection, Timeouts timeouts, Clock::time_point now) noexcept;

            std::span<const std::byte> pending_tx() const noexcept;
            [[nodiscard]] bool append_tx(std::span<const std::byte> bytes) noexcept;   // false: out of memory
            void consume_tx(size_t count) noexcept;
            std::vector<std::byte> take_tx() noexcept;   // moves the queued bytes out, leaving none

        private:
            std::vector<std::byte> tx_;   // queued bytes are tx_[tx_head_, size)
            size_t tx_head_ = 0;
        };
        using EntryIt = std::map<uint64_t, Entry>::iterator;

        // Write timers: one per buffer fill, invalidated by tx_generation.
        // Read timers: one live per connection, invalidated by rx_generation.
        // Receiving only moves last_rx; when the node pops early it is pushed
        // again for last_rx + read.
        // Connect timers: one per dial; stale once the entry is no longer Connecting.
        struct TimerNode {
            enum class Kind : uint8_t { Read, Write, Connect };
            Clock::time_point deadline;
            uint64_t entry_id;
            Kind kind;
            uint64_t generation;   // Write: tx_generation, Read: rx_generation, Connect: unused

            bool operator>(const TimerNode& other) const noexcept;
        };

        std::map<uint64_t, Entry> connections_;
        std::priority_queue<TimerNode, std::vector<TimerNode>, std::greater<>> timers_;
        std::deque<uint64_t> ready_;            // readable ids reported by the last epoll_wait()
        // Outcomes the pool produced without a read (removals it made itself,
        // completed dials), reported before any data.
        std::deque<ReceiveOutcome> notices_;
        std::mutex connections_mutex_;
        std::atomic<bool> stopping_{false};
        std::atomic<bool> interrupt_pending_{false};
        // Wakes receive() out of epoll_wait(). Why it woke is told by stopping_,
        // interrupt_pending_, notices_ or the timers, never by the event itself.
        WakeupEvent wakeup_;
        int epoll_fd_ = -1;
        uint64_t connection_next_id_{1};
        Timeouts default_timeouts_;
        size_t max_tx_buffer_;

        static constexpr uint64_t wakeToken_ = 0;   // connection ids start at 1

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
        // A dial's socket became writable or failed: report Connected or the error.
        void finish_connect_locked(EntryIt it) noexcept;
        // Arms the read timer for a connection whose read timeout is nonzero,
        // counting from last_rx; false when out of memory.
        [[nodiscard]] bool arm_read_timer_locked(EntryIt it, bool wake_if_earliest) noexcept;
        // Discards queued notices for an id the caller has just taken away, so
        // receive() keeps its promise of reporting nothing more for it.
        void forget_notices_locked(uint64_t connection_id) noexcept;
};
