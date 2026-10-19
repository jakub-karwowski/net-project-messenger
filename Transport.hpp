#pragma once

// Transport layer behind the workers: how inbound connections are accepted and
// read, and how outbound ones are written. make_io_stack() picks the variant.

#include "IoMode.hpp"
#include "Metrics.hpp"
#include "OutboundTransport.hpp"
#include "ThreadWorkers.hpp"

#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <memory>
#include <string>

// Accepts and reads inbound connections, feeding PeerSessions.
class InboundTransport {
public:
    virtual ~InboundTransport() = default;
    virtual void run() = 0;            // blocks until stop(); call from its own thread
    virtual void stop() noexcept = 0;  // idempotent, callable from any thread

    // Readiness hook: invoked once, from the thread running run(), after the listen
    // socket is bound ("ip:port", no error) or when binding failed ("", reason).
    // Set before run(). Subclasses call notify_ready().
    void set_ready_callback(std::function<void(std::string, std::optional<std::string>)> cb) {
        ready_ = std::move(cb);
    }
protected:
    void notify_ready(std::string listen, std::optional<std::string> error = std::nullopt) {
        if (ready_) { ready_(std::move(listen), std::move(error)); }
    }
private:
    std::function<void(std::string, std::optional<std::string>)> ready_;
};

struct NetConfig {
    std::string address{"127.0.0.1"};  // where we listen
    unsigned port{9000};
    std::size_t max_peers{p2p::ListenerWorker::kDefaultMaxReaders};
    int backlog{p2p::ListenerWorker::kDefaultBacklog};
    std::chrono::milliseconds send_timeout{p2p::SenderWorker::kDefaultSendTimeout};
    // Optional counters (nullptr: off). A transport counts accepts / connections_live /
    // evictions itself and hands this to its PeerSessions (see Metrics.hpp).
    p2p::Metrics* metrics{nullptr};
};

namespace p2p {

// Today's inbound side: ListenerWorker (blocking accept) + ReaderWorker thread
// per connection + WatchdogWorker (cap = max_peers, liveness).
class ThreadedInbound final : public InboundTransport {
public:
    ThreadedInbound(const NetConfig& net, ThreadQueue<Event>& store_queue,
                    PeerTable& peers, Logger logger);
    void run() override {
        listener_.set_ready_callback([this](std::string listen, std::optional<std::string> error) {
            notify_ready(std::move(listen), std::move(error));
        });
        listener_();
    }
    void stop() noexcept override { listener_.stop(); }
private:
    ListenerWorker listener_;
};

// Owns the inbound and outbound halves. Both pointers are always set. They are
// shared_ptrs so one object can implement both halves (poll/epoll: MultiplexedIo
// is an InboundTransport and an OutboundTransport; both pointers then share its
// ownership). Separate halves just hold distinct objects.
struct IoStack {
    std::shared_ptr<InboundTransport> inbound;
    std::shared_ptr<OutboundTransport> outbound;
};

// Throws std::runtime_error for a mode that is not implemented yet.
IoStack make_io_stack(IoMode mode, const NetConfig& net, ThreadQueue<Event>& store_queue,
                      PeerTable& peers, Logger logger);

} // namespace p2p
