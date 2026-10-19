#pragma once

// poll / epoll variants: ONE connection pool shared by the inbound and the
// outbound side, ONE I/O thread driving it, and the calling thread (the
// "listener thread") running a blocking accept loop.
//
//   accept loop (run) --add_connection--> Pool <--receive()-- I/O thread
//   SenderWorker      --open/send/close-> Pool                  |
//                                          inbound bytes -> FrameChannel::on_bytes
//
// Registry scheme (no lost bytes): `registry_` maps pool id -> {session}.
// Whoever registers a connection holds `registry_mutex_` across
// pool.add_connection() AND the registry insert; the I/O thread takes the same
// mutex when it looks an id up. So receive() can never hand out bytes for an id
// whose entry is not there yet, and an outbound id never gets a session (its
// entry has a null one). Sessions are driven by the I/O thread only.

#include "Logger.hpp"
#include "OutboundTransport.hpp"
#include "FrameChannel.hpp"
#include "Transport.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace p2p {

template <class Pool>
class MultiplexedIo final : public InboundTransport, public OutboundTransport {
public:
    static constexpr std::chrono::milliseconds kIdleTimeout{std::chrono::hours{1}};
    static constexpr std::size_t kReadChunk = 4096;

    MultiplexedIo(const NetConfig& net, ThreadQueue<Event>& store_queue, PeerTable& peers,
                  Logger logger, std::size_t max_tx_buffer_bytes = 1024 * 1024);
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

private:
    struct Registered {
        std::shared_ptr<FrameChannel> session; // null for an outbound connection
    };

    void io_loop();

    NetConfig net_;
    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;

    Pool pool_;
    std::mutex registry_mutex_;
    std::unordered_map<int, Registered> registry_;
    std::atomic<bool> stopping_{false};
    std::size_t outbound_bytes_warned_{0}; // I/O thread only
    std::thread io_thread_;                // last member: started once the rest exists
};

} // namespace p2p
