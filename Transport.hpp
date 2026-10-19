#pragma once

// Transport layer behind the workers: how inbound connections are accepted and
// read, and how outbound ones are written. make_io_stack() picks the variant.

#include "IoMode.hpp"
#include "OutboundTransport.hpp"
#include "ThreadWorkers.hpp"

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

// Accepts and reads inbound connections, feeding PeerSessions.
class InboundTransport {
public:
    virtual ~InboundTransport() = default;
    virtual void run() = 0;            // blocks until stop(); call from its own thread
    virtual void stop() noexcept = 0;  // idempotent, callable from any thread
};

struct NetConfig {
    std::string address{"127.0.0.1"};  // where we listen
    unsigned port{9000};
    std::size_t max_peers{p2p::ListenerWorker::kDefaultMaxReaders};
    int backlog{p2p::ListenerWorker::kDefaultBacklog};
    std::chrono::milliseconds send_timeout{p2p::SenderWorker::kDefaultSendTimeout};
};

namespace p2p {

// Today's inbound side: ListenerWorker (blocking accept) + ReaderWorker thread
// per connection + WatchdogWorker (cap = max_peers, liveness).
class ThreadedInbound final : public InboundTransport {
public:
    ThreadedInbound(const NetConfig& net, ThreadQueue<Event>& store_queue,
                    PeerTable& peers, Logger logger);
    void run() override { listener_(); }
    void stop() noexcept override { listener_.stop(); }
private:
    ListenerWorker listener_;
};

// Owns the inbound and outbound halves. Both pointers are always set.
struct IoStack {
    std::unique_ptr<InboundTransport> inbound;
    std::unique_ptr<OutboundTransport> outbound;
};

// Throws std::runtime_error for a mode that is not implemented yet.
IoStack make_io_stack(IoMode mode, const NetConfig& net, ThreadQueue<Event>& store_queue,
                      PeerTable& peers, Logger logger);

} // namespace p2p
