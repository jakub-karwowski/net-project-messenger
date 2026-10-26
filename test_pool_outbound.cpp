// test_pool_outbound.cpp - harness for p2p::PoolOutbound (--io=pool-sender).
//
// Plain assertions (no framework). Covers:
//   * open() sends the hello, send() delivers data to the peer
//   * a peer closing its end is detected: the next sends fail with ConnectionNotFound
//   * a peer that never reads: send() ends in WouldBlock (tx cap) and the
//     transport asks to drop rather than retry
//   * SenderWorker over PoolOutbound delivers messages, and drops + reports
//     (EventError, send_backpressure_drops) instead of declaring the peer unreachable
//   * end to end: two make_io_stack(PoolSender) nodes exchange a message

#include "PoolOutbound.hpp"
#include "Transport.hpp"
#include "Wire.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

std::vector<std::byte> bytes_of(const std::string& text) {
    std::vector<std::byte> out(text.size());
    for (std::size_t i = 0; i < text.size(); ++i) { out[i] = static_cast<std::byte>(text[i]); }
    return out;
}

// Reads exactly `count` bytes from `connection` (receive timeout must be set).
std::string read_exactly(TcpConnection& connection, std::size_t count, std::chrono::milliseconds timeout) {
    std::string out;
    std::array<std::byte, 4096> scratch{};
    const auto deadline = Clock::now() + timeout;
    while (out.size() < count && Clock::now() < deadline) {
        auto got = connection.receive(std::span<std::byte>{scratch.data(),
                                      std::min(scratch.size(), count - out.size())});
        if (!got) {
            if (got.error() == SocketError::WouldBlock) { continue; }
            break;
        }
        for (std::size_t i = 0; i < *got; ++i) { out.push_back(static_cast<char>(scratch[i])); }
    }
    return out;
}

// Pops events until `match` accepts one or the timeout passes.
template <class Pred>
std::optional<Event> wait_event(ThreadQueue<Event>& queue, Pred match, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (Clock::now() < deadline) {
        if (auto event = queue.try_pop()) {
            if (match(*event)) { return event; }
        } else {
            std::this_thread::sleep_for(5ms);
        }
    }
    return std::nullopt;
}

using Outbound = p2p::PoolOutbound<TcpConnectionPool>;

// ---------------------------------------------------------------------------

void test_open_send_and_peer_close() {
    constexpr unsigned kPort = 9961;
    TcpListener listener("127.0.0.1", kPort, 1);
    Outbound outbound;

    const auto hello = bytes_of("hello");
    auto handle = outbound.open("127.0.0.1", kPort, hello);
    check(handle.has_value(), "close: open succeeds");
    if (!handle) { return; }
    auto server = listener.accept_client();
    check(server.has_value(), "close: peer accepted");
    if (!server) { return; }
    server->with_receive_timeout(100ms);

    check(outbound.send(*handle, bytes_of("-data")).has_value(), "close: send succeeds");
    check(read_exactly(*server, 10, 2s) == "hello-data", "close: hello then data arrive in order");

    { auto closing = std::move(*server); } // peer goes away
    bool not_found = false;
    const auto deadline = Clock::now() + 3s;
    while (Clock::now() < deadline && !not_found) {
        auto sent = outbound.send(*handle, bytes_of("x"));
        not_found = !sent && sent.error() == SocketError::ConnectionNotFound;
        if (!not_found) { std::this_thread::sleep_for(10ms); }
    }
    check(not_found, "close: peer close surfaces as ConnectionNotFound");
    outbound.close(*handle); // idempotent on a dead handle
    // Claim a port no other test uses and release it, so the dial is reliably refused.
    constexpr unsigned kClosedPort = 9962;
    { TcpListener probe("127.0.0.1", kClosedPort, 1); }
    check(!outbound.open("127.0.0.1", kClosedPort, hello).has_value(), "close: dial to a closed port fails");
}

void test_backpressure_on_non_reading_peer() {
    constexpr unsigned kPort = 9963;
    TcpListener listener("127.0.0.1", kPort, 1);
    p2p::PoolOutboundOptions options;
    options.max_tx_buffer_bytes = 64 * 1024;
    Outbound outbound{options};
    check(outbound.backpressure_is_drop(), "backpressure: transport asks for drop semantics");

    auto handle = outbound.open("127.0.0.1", kPort, bytes_of("hi"));
    check(handle.has_value(), "backpressure: open succeeds");
    if (!handle) { return; }
    auto server = listener.accept_client(); // accepted but never read
    check(server.has_value(), "backpressure: peer accepted");

    const std::vector<std::byte> chunk(32 * 1024, std::byte{'a'});
    bool would_block = false;
    for (int i = 0; i < 4000 && !would_block; ++i) {
        auto sent = outbound.send(*handle, chunk);
        if (!sent) {
            check(sent.error() == SocketError::WouldBlock, "backpressure: failure is WouldBlock");
            would_block = true;
        }
    }
    check(would_block, "backpressure: a full tx buffer reports WouldBlock");
}

void test_sender_delivers_over_pool() {
    constexpr unsigned kPort = 9964;
    constexpr uint64_t kPeerId = 7;
    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue, sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});
    Outbound outbound;
    p2p::SenderWorker sender{store_queue, sender_queue, peers, outbound};
    std::thread worker{std::ref(sender)};

    (void)sender_queue.emplace(EventMessage{kPeerId, kPeerId, "one"});
    (void)sender_queue.emplace(EventMessage{kPeerId, kPeerId, "two"});
    auto server = listener.accept_client();
    check(server.has_value(), "sender: peer dialled");
    if (server) {
        server->with_receive_timeout(100ms);
        wire::Decoder decoder;
        std::vector<std::string> bodies;
        std::array<std::byte, 4096> scratch{};
        const auto deadline = Clock::now() + 3s;
        while (bodies.size() < 2 && Clock::now() < deadline) {
            auto item = decoder.next();
            if (!item) { break; }
            if (*item) {
                if (const auto* chat = wire::as_message(**item)) { bodies.push_back(chat->body); }
                continue;
            }
            auto got = server->receive(scratch);
            if (got) { decoder.feed(std::span<const std::byte>{scratch.data(), *got}); }
        }
        check(bodies == std::vector<std::string>{"one", "two"}, "sender: both messages delivered in order");
    }
    check(sender.send_backpressure_drops() == 0, "sender: no drops while the peer reads");
    sender_queue.graceful_shutdown();
    worker.join();
}

void test_sender_drops_on_backpressure() {
    constexpr unsigned kPort = 9965;
    constexpr uint64_t kPeerId = 9;
    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue, sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});
    p2p::PoolOutboundOptions options;
    options.max_tx_buffer_bytes = 64 * 1024;
    Outbound outbound{options};
    p2p::SenderWorker sender{store_queue, sender_queue, peers, outbound, {}, 3, 200ms};
    std::thread worker{std::ref(sender)};

    const std::string big(256 * 1024, 'z');
    constexpr int kMessages = 200;
    for (int i = 0; i < kMessages; ++i) {
        (void)sender_queue.emplace(EventMessage{kPeerId, kPeerId, big});
    }
    auto server = listener.accept_client(); // never read
    const auto start = Clock::now();
    sender_queue.graceful_shutdown();
    worker.join();
    // No sleep-and-retry on backpressure: 200 messages with a 200 ms retry delay
    // would take far longer than this if drops were retried.
    check(Clock::now() - start < 5s, "drop: backpressure does not sleep or retry");
    check(sender.send_backpressure_drops() > 0, "drop: drops are counted");

    bool saw_error = false, saw_unreachable = false;
    while (auto event = store_queue.try_pop()) {
        if (const auto* error = std::get_if<EventError>(&*event)) {
            saw_error = saw_error || error->message.starts_with("backpressure: message to peer#9");
        }
        if (std::holds_alternative<EventPeerDisconnected>(*event)) { saw_unreachable = true; }
    }
    check(saw_error, "drop: EventError reported to the store");
    check(!saw_unreachable, "drop: a slow peer is not declared unreachable");
}

struct Node {
    ThreadQueue<Event> store_queue, sender_queue;
    p2p::PeerTable peers;
    p2p::IoStack stack;
    std::unique_ptr<p2p::SenderWorker> sender;
    std::thread inbound, sender_thread;

    explicit Node(unsigned port)
        : stack([&] {
              NetConfig net;
              net.port = port;
              return p2p::make_io_stack(IoMode::PoolSender, net, store_queue, peers, {});
          }()) {
        sender = std::make_unique<p2p::SenderWorker>(store_queue, sender_queue, peers,
                                                     *stack.outbound);
        inbound = std::thread{[this] { stack.inbound->run(); }};
        sender_thread = std::thread{std::ref(*sender)};
    }
    ~Node() {
        stack.inbound->stop();
        sender_queue.graceful_shutdown();
        inbound.join();
        sender_thread.join();
    }
};

void test_two_nodes_exchange_message() {
    constexpr unsigned kPortA = 9976, kPortB = 9977;
    Node a{kPortA};
    Node b{kPortB};
    a.peers.insert(1, {"127.0.0.1", kPortB});
    (void)a.sender_queue.emplace(EventMessage{1, 1, "ping over pool"});
    auto got = wait_event(b.store_queue,
                          [](const Event& e) { return std::holds_alternative<EventMessage>(e); }, 3s);
    check(got.has_value(), "e2e: message reached the other node");
    if (got) { check(std::get<EventMessage>(*got).payload == "ping over pool", "e2e: payload intact"); }
}

} // namespace

int main() {
    test_open_send_and_peer_close();
    test_backpressure_on_non_reading_peer();
    test_sender_delivers_over_pool();
    test_sender_drops_on_backpressure();
    test_two_nodes_exchange_message();
    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
