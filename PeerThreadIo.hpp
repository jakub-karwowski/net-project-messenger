#pragma once

// --io=peer-threads: ONE owner thread per peer, ONE duplex TCP connection per peer.
//
// Each link is a one-connection TcpConnectionPool (the poll() pool) driven by its
// owner thread. The pool already provides everything a duplex link needs: poll()
// on the socket plus its own wake descriptor, a thread-safe send() with a capped tx
// buffer (WouldBlock when full), flushing on POLLOUT, and a single {id, 0} report
// when the connection ends. So the owner thread only loops on pool.receive(),
// feeds the FrameChannel, and is the one and only caller of on_closed().
//
//   accept loop (run) --attach--> Link{pool(1), FrameChannel} <--receive()-- owner thread
//   SenderWorker      --open/send/close--> Link.pool.send() / close_pool()
//
// The TcpConnection is shared (shared_ptr) between the pool and the link's
// ReaderActivity only because the reused WatchdogWorker shuts a socket down through
// ReaderActivity::connection; the pool then reports the end and the owner exits.
// The pool's receive() blocks until data or close, so the heartbeat cannot advance
// while a peer is idle: the watchdog's stall detection is disabled for this
// transport (it guards blocking receives, and pool.receive() is always woken by
// close_pool() or a shutdown). Cap eviction still works: it ranks by last_read_ns.
//
// Shutdown: stop() wakes accept() and stops the watchdog; the watchdog's drain
// shuts every socket down and joins every owner thread (in its own thread). The
// destructor joins the watchdog thread. stop() itself never joins anything.

#include "FrameChannel.hpp"
#include "Logger.hpp"
#include "OutboundTransport.hpp"
#include "TcpSocket.hpp"
#include "Transport.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace p2p {

class PeerThreadIo final : public InboundTransport, public OutboundTransport {
public:
    static constexpr std::size_t kReadChunk = 4096;
    // Liveness comes from EOF/errors and the watchdog, not from idleness.
    static constexpr std::chrono::milliseconds kIdleTimeout{std::chrono::hours{24 * 365}};

    PeerThreadIo(const NetConfig& net, ThreadQueue<Event>& store_queue, PeerTable& peers,
                 Logger logger, std::size_t max_tx_buffer_bytes = 1024 * 1024);
    ~PeerThreadIo() override;

    // InboundTransport. run() blocks in accept until stop(). The watchdog thread is
    // started by the constructor, so outbound links work even if run() never listens.
    void run() override;
    void stop() noexcept override;

    // OutboundTransport. Thread-safe. WouldBlock = tx buffer full (nothing queued).
    std::expected<Handle, SocketError> open(const std::string& ip, uint16_t port,
                                            std::span<const std::byte> hello) override;
    std::expected<void, SocketError> send(Handle handle, std::span<const std::byte> data) override;
    void close(Handle handle) noexcept override;
    bool backpressure_is_drop() const noexcept override { return true; }
    bool duplex() const noexcept override { return true; }

private:
    struct Link {
        Link(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger,
             std::shared_ptr<TcpConnection> connection, int handle, Metrics* metrics,
             std::size_t max_tx_buffer_bytes);
        Link(const Link&) = delete;
        Link& operator=(const Link&) = delete;

        int handle;
        std::shared_ptr<ReaderActivity> activity; // shares the connection with the pool
        TcpConnectionPool pool;                   // exactly one connection: this peer's
        int pool_id{-1};
        FrameChannel channel;
        // The pool's counters already added to Metrics. Updated by the owner (after
        // receive(), where flushes happen) and by the sender (after send(), where
        // queueing happens), so both counters reach Metrics without waiting for the
        // other side. exchange() keeps the running total exact once the link quiesces.
        std::atomic<std::uint64_t> published_queued{0};
        std::atomic<std::uint64_t> published_flushes{0};
    };

    // Registers the link, starts its owner thread and hands it to the watchdog.
    // Null on failure (nothing left behind; the connection is closed).
    std::shared_ptr<Link> attach(TcpConnection&& connection, bool accepted);
    std::shared_ptr<Link> find(int handle);
    void owner_loop(const std::shared_ptr<Link>& link);
    // Adds the pool's write counters since the last call to the shared Metrics.
    void publish_tx_stats(Link& link) noexcept;
    void join_watchdog();

    NetConfig net_;
    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;
    std::size_t max_tx_buffer_bytes_;

    WatchdogWorker watchdog_;
    std::mutex registry_mutex_;
    std::unordered_map<int, std::shared_ptr<Link>> registry_;
    std::atomic<int> next_handle_{1};
    std::atomic<bool> stopping_{false};
    std::mutex join_mutex_;
    std::thread watchdog_thread_; // last member: started once the rest exists
};

} // namespace p2p
