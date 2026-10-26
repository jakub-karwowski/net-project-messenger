#pragma once

// --io=peer-threads: ONE owner thread per peer, ONE duplex TCP connection per peer.
//
// The owner thread is the only thread that reads the socket, flushes queued writes,
// closes the link and calls FrameChannel::on_closed(). It runs poll() on
// {socket, eventfd}: POLLIN always, POLLOUT only while bytes are queued, and the
// eventfd is how other threads (sender, close()) wake it. A 200 ms poll timeout
// keeps the ReaderActivity heartbeat advancing, so the (unchanged) WatchdogWorker
// can enforce --max-peers and detect stalls exactly as it does for `threads`.
//
//   accept loop (run) --attach--> Link --owner thread: poll{sock, eventfd}--> FrameChannel
//   SenderWorker      --open/send/close--> Link (immediate write under tx_mutex,
//                                            else queue + wake the owner)
//
// Writes from the sender thread and from the owner thread are serialized by the
// link's tx_mutex, so frames never interleave. A closed link is detected
// synchronously through Link::closed (under tx_mutex): send() then reports
// ConnectionNotFound.
//
// Shutdown: stop() wakes accept() and stops the watchdog; the watchdog's drain
// shuts every socket down and joins every owner thread (in its own thread). The
// destructor joins the watchdog thread. stop() itself never joins anything.

#include "FrameChannel.hpp"
#include "Logger.hpp"
#include "OutboundTransport.hpp"
#include "Transport.hpp"

#include <atomic>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace p2p {

class PeerThreadIo final : public InboundTransport, public OutboundTransport {
public:
    static constexpr std::size_t kReadChunk = 4096;
    static constexpr int kPollTimeoutMs = 200; // = ReaderWorker::kDefaultTickPeriod

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
             std::shared_ptr<TcpConnection> connection, int handle, Metrics* metrics);
        ~Link();
        Link(const Link&) = delete;
        Link& operator=(const Link&) = delete;

        int handle;
        std::shared_ptr<TcpConnection> conn;
        std::shared_ptr<ReaderActivity> activity; // same conn; heartbeat for the watchdog
        FrameChannel channel;
        int wake_fd{-1};                          // eventfd
        std::mutex tx_mutex;                      // guards tx_buffer, tx_offset, closed writes
        std::vector<std::byte> tx_buffer;
        std::size_t tx_offset{0};
        std::atomic<bool> closed{false};
    };

    // Registers the link, starts its owner thread and hands it to the watchdog.
    // Null on failure (nothing left behind; the connection is closed).
    std::shared_ptr<Link> attach(TcpConnection&& connection, bool accepted);
    std::shared_ptr<Link> find(int handle);
    void owner_loop(const std::shared_ptr<Link>& link);
    // Marks closed, shuts the socket down and wakes the owner (any thread).
    void shut_link(Link& link) noexcept;
    static void wake(Link& link) noexcept;
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
