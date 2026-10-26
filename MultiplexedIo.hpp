#pragma once

// poll / epoll variants: ONE connection pool, ONE I/O thread driving it, and the
// calling thread (the "listener thread") running a blocking accept loop. Three shapes:
//
//   Dual (poll, epoll): two TCP connections per peer pair. The accepted one is the
//     rx channel (a FrameChannel), the dialed one is the tx channel (no channel; bytes
//     arriving on it are ignored). Pool capacity 2 * max_peers.
//
//   Duplex (poll-duplex, epoll-duplex): ONE connection per peer pair carries both
//     directions. Accepted AND dialed connections get a FrameChannel registered as both
//     the peer's rx and tx slot; the Connect/ConnectAck handshake runs on that socket.
//     Pool capacity max_peers (+ slack).
//
//   Split (poll-split, epoll-split): Duplex, but writes go through a TxPoller with its
//     own thread. The same shared_ptr<TcpConnection> sits in the rx pool (reads only)
//     and in the TxPoller (writes only).
//
//   accept loop (run) --add_connection--> Pool <--receive()-- I/O thread
//   SenderWorker      --open/send/close-> Pool (or TxPoller)     |
//                                          inbound bytes -> FrameChannel::on_bytes
//
// Registry scheme (no lost bytes): `registry_` maps pool id -> {channel}.
// Whoever registers a connection holds `registry_mutex_` across
// pool.add_connection() (and tx_.add() in Split) AND the registry insert; the I/O
// thread takes the same mutex when it looks an id up. So receive() can never hand out
// bytes for an id whose entry is not there yet. In Dual mode an outbound id has a null
// channel; in Duplex/Split every entry has one. Channels are driven by the I/O thread
// only, except that close() (sender thread) calls on_closed() in Duplex/Split mode;
// FrameChannel serializes that with its handshake bind and marks itself closing,
// so a bind racing the close attaches nothing (see FrameChannel.hpp).

#include "Logger.hpp"
#include "OutboundTransport.hpp"
#include "FrameChannel.hpp"
#include "Transport.hpp"
#include "TxPoller.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <type_traits>
#include <unordered_map>

namespace p2p {

enum class ConnectionModel { Dual, Duplex };

// Tx policy: writes go through the (single) pool itself.
struct SamePoolTx {
    explicit SamePoolTx(std::size_t) {}
};

template <class Pool, class Tx = SamePoolTx>
class MultiplexedIo final : public InboundTransport, public OutboundTransport {
public:
    static constexpr std::chrono::milliseconds kIdleTimeout{std::chrono::hours{1}};
    static constexpr std::size_t kReadChunk = 4096;

    MultiplexedIo(const NetConfig& net, ThreadQueue<Event>& store_queue, PeerTable& peers,
                  Logger logger, std::size_t max_tx_buffer_bytes = 1024 * 1024,
                  ConnectionModel model = ConnectionModel::Dual);
    ~MultiplexedIo() override;

    // InboundTransport. run() blocks in accept until stop(). The I/O thread is
    // started by the constructor, so outbound connections work even if run() never
    // gets to listen (bind failure) or is never called.
    void run() override;
    void stop() noexcept override;

    // OutboundTransport. Thread-safe. send() passes the pool's WouldBlock through
    // (backpressure; nothing queued).
    std::expected<Handle, SocketError> open(const std::string& ip, uint16_t port,
                                            std::span<const std::byte> hello) override;
    std::expected<void, SocketError> send(Handle handle, std::span<const std::byte> data) override;
    void close(Handle handle) noexcept override;
    // A full tx buffer means a stalled peer: the sender drops the message.
    bool backpressure_is_drop() const noexcept override { return true; }
    bool duplex() const noexcept override { return model_ == ConnectionModel::Duplex; }

private:
    struct Registered {
        std::shared_ptr<FrameChannel> session; // null for an outbound connection
    };

    static constexpr bool kSplit = !std::is_same_v<Tx, SamePoolTx>;

    void io_loop();
    // Mirrors the pool's (and, in Split, the TxPoller's) write counters into Metrics.
    void publish_tx_stats() noexcept;
    // Adds the connection to the pool (and the TxPoller in Split); caller holds
    // registry_mutex_. Returns the pool id, or -1.
    int attach_locked(std::shared_ptr<TcpConnection> connection);
    std::shared_ptr<FrameChannel> make_duplex_channel(int id, bool count_accept);

    NetConfig net_;
    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;

    ConnectionModel model_;
    Pool pool_;
    Tx tx_;
    std::mutex registry_mutex_;
    std::unordered_map<int, Registered> registry_;
    std::atomic<bool> stopping_{false};
    std::size_t outbound_bytes_warned_{0}; // I/O thread only
    std::thread tx_thread_;                // Split only
    std::thread io_thread_;                // last member: started once the rest exists
};

} // namespace p2p
