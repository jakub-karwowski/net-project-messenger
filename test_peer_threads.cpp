// test_peer_threads.cpp - harness for PeerThreadIo (--io=peer-threads).
//
// Plain assertions (no framework), in the style of test_multiplexed.cpp:
//   1. two in-process nodes: exactly one TCP connection, both directions flow
//   2. order under queueing: 8 MB in 64 KiB writes to a slow reader arrive in order
//   3. backpressure: a peer that never reads makes send() return WouldBlock, and no
//      partial write is ever queued
//   4. close(handle): one disconnect event, both slots released, send -> ConnectionNotFound
//   5. cap eviction: max_peers=2 with 3 dialers evicts exactly one
//   6. shutdown guard: stop() and destruction with 64 live peers are quick

#include "PeerThreadIo.hpp"
#include "ThreadWorkers.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace {

using namespace std::chrono_literals;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
}

unsigned random_port() {
    static std::mt19937 rng{std::random_device{}()};
    return 12000 + rng() % 15000; // below the ephemeral range
}

bool send_all(TcpConnection& conn, const std::vector<std::byte>& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        auto result = conn.send(std::span<const std::byte>{data.data() + sent, data.size() - sent});
        if (!result || *result == 0) { return false; }
        sent += *result;
    }
    return true;
}

std::optional<TcpConnection> dial(unsigned port, int attempts = 200) {
    for (int i = 0; i < attempts; ++i) {
        auto conn = TcpConnection::connect("127.0.0.1", port);
        if (conn) { return std::move(*conn); }
        std::this_thread::sleep_for(10ms);
    }
    return std::nullopt;
}

std::vector<std::byte> hello_bytes(uint64_t name, unsigned port) {
    return *wire::encode(wire::make_connect(
        name, 0, std::vector<wire::Endpoint>{{"127.0.0.1", static_cast<uint16_t>(port)}}));
}

std::optional<Event> pop_for(ThreadQueue<Event>& queue, std::chrono::milliseconds timeout = 3s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (auto event = queue.try_pop()) { return event; }
        std::this_thread::sleep_for(2ms);
    }
    return std::nullopt;
}

template <class T>
std::optional<T> pop_as(ThreadQueue<Event>& queue, std::chrono::milliseconds timeout = 3s) {
    auto event = pop_for(queue, timeout);
    if (!event) { return std::nullopt; }
    if (auto* typed = std::get_if<T>(&*event)) { return std::move(*typed); }
    return std::nullopt;
}

template <class Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout = 5s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) { return true; }
        std::this_thread::sleep_for(5ms);
    }
    return pred();
}

// A bare PeerThreadIo plus the thread running its accept loop.
struct Node {
    static NetConfig make_net(unsigned port, std::size_t max_peers, p2p::Metrics* metrics) {
        NetConfig net;
        net.port = port;
        net.max_peers = max_peers;
        net.metrics = metrics;
        return net;
    }
    p2p::Metrics metrics;
    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    unsigned port;
    std::unique_ptr<p2p::PeerThreadIo> io;
    std::thread listener;

    explicit Node(std::size_t max_peers = 128)
        : port(random_port()),
          io(std::make_unique<p2p::PeerThreadIo>(make_net(port, max_peers, &metrics), store_queue,
                                                 peers, Logger{})) {
        listener = std::thread{[this] { io->run(); }};
    }
    ~Node() {
        io->stop();
        if (listener.joinable()) { listener.join(); }
    }
};

// Reads exactly `n` bytes (or fewer on EOF/timeout).
std::vector<std::byte> read_exact(TcpConnection& conn, std::size_t n) {
    std::vector<std::byte> got;
    std::byte scratch[8192];
    while (got.size() < n) {
        auto r = conn.receive(std::span<std::byte>{scratch, std::min(sizeof scratch, n - got.size())});
        if (!r || *r == 0) { break; }
        got.insert(got.end(), scratch, scratch + *r);
    }
    return got;
}

// --- 1. two nodes ----------------------------------------------------------------

struct FullNode {
    static NetConfig make_net(unsigned port, p2p::Metrics* metrics) {
        NetConfig net;
        net.port = port;
        net.metrics = metrics;
        return net;
    }
    p2p::Metrics metrics;
    ThreadQueue<Event> render_queue, store_queue, sender_queue;
    p2p::PeerTable peers;
    unsigned port;
    p2p::IoStack io;
    StoreWorker store;
    p2p::SenderWorker sender;
    std::thread store_t, sender_t, listener_t;

    FullNode(unsigned port_)
        : port(port_),
          io(p2p::make_io_stack(IoMode::PeerThreads, make_net(port_, &metrics), store_queue, peers, Logger{})),
          store(render_queue, store_queue, sender_queue),
          sender(store_queue, sender_queue, peers, *io.outbound,
                 LocalEndpoints{{wire::Endpoint{"127.0.0.1", static_cast<uint16_t>(port_)}}}) {
        store_t = std::thread{[this] { store(); }};
        sender_t = std::thread{[this] { sender(); }};
        listener_t = std::thread{[this] { io.inbound->run(); }};
    }
    ~FullNode() {
        io.inbound->stop();
        store_queue.graceful_shutdown();
        sender_queue.graceful_shutdown();
        render_queue.graceful_shutdown();
        store_t.join();
        sender_t.join();
        listener_t.join();
    }
};

void test_two_nodes() {
    const std::string tag = "peer-threads e2e:";
    const unsigned port_a = random_port();
    unsigned port_b = random_port();
    if (port_b == port_a) { ++port_b; }
    FullNode a{port_a};
    FullNode b{port_b};
    std::this_thread::sleep_for(100ms);

    check(a.store_queue.emplace(EventDialPeer{"127.0.0.1", static_cast<uint16_t>(port_b), 0}),
          tag + " dial queued");

    uint64_t conv_a = 0;
    for (int i = 0; i < 100 && conv_a == 0; ++i) {
        auto event = pop_for(a.render_queue, 3s);
        if (!event) { break; }
        if (auto* dialed = std::get_if<EventPeerDialed>(&*event)) { conv_a = dialed->conversation_id; }
    }
    check(conv_a != 0, tag + " A got a dialed conversation");

    uint64_t conv_b = 0;
    for (int i = 0; i < 100 && conv_b == 0; ++i) {
        auto event = pop_for(b.render_queue, 3s);
        if (!event) { break; }
        if (auto* connected = std::get_if<EventPeerDialed>(&*event)) { conv_b = connected->conversation_id; }
        else if (auto* notice = std::get_if<EventConversationNotification>(&*event)) { conv_b = notice->conversation_id; }
    }
    if (conv_b == 0) {
        (void)b.store_queue.emplace(EventListConversations{});
        for (int i = 0; i < 100 && conv_b == 0; ++i) {
            auto event = pop_for(b.render_queue, 3s);
            if (!event) { break; }
            if (auto* list = std::get_if<EventConversationList>(&*event);
                list && !list->conversations.empty()) {
                conv_b = list->conversations.front().conversation_id;
            }
        }
    }
    check(conv_b != 0, tag + " B has a conversation for A");
    if (conv_a == 0 || conv_b == 0) { return; }

    check(b.store_queue.emplace(EventSubscribe{conv_b, SubscriptionFlag::Message, 0}), tag + " B subscribed");
    check(a.store_queue.emplace(EventSubscribe{conv_a, SubscriptionFlag::Message, 0}), tag + " A subscribed");

    auto wait_text = [&](FullNode& node, const std::string& text) {
        for (int i = 0; i < 200; ++i) {
            auto event = pop_for(node.render_queue, 3s);
            if (!event) { return false; }
            if (auto* delta = std::get_if<EventConversationDelta>(&*event)) {
                for (const auto& m : delta->messages) { if (m.payload == text) { return true; } }
            } else if (auto* snap = std::get_if<EventConversationSnapshot>(&*event)) {
                if (snap->store) {
                    for (const auto& m : snap->store->store) { if (m.payload == text) { return true; } }
                }
            }
        }
        return false;
    };

    check(a.store_queue.emplace(EventComposeMessage{conv_a, "ping"}), tag + " A composes");
    check(wait_text(b, "ping"), tag + " B received ping");
    check(b.store_queue.emplace(EventComposeMessage{conv_b, "pong"}), tag + " B composes");
    check(wait_text(a, "pong"), tag + " A received pong");

    std::this_thread::sleep_for(200ms); // a dial-back would have happened by now
    const auto accepts = a.metrics.accepts.load() + b.metrics.accepts.load();
    check(accepts == 1, tag + " exactly one TCP connection in total (accepts="
                            + std::to_string(accepts) + ")");
    check(a.metrics.connections_live.load() == 1 && b.metrics.connections_live.load() == 1,
          tag + " both sides count that one connection live");
}

// --- 2. order under queueing -------------------------------------------------------

void test_order() {
    const std::string tag = "peer-threads order:";
    const unsigned port = random_port();
    TcpListener server("127.0.0.1", port, 4);
    Node node;
    const auto hello = hello_bytes(9, node.port);
    auto handle = node.io->open("127.0.0.1", port, hello);
    check(handle.has_value(), tag + " open");
    if (!handle) { return; }
    auto peer = server.accept_client();
    check(peer.has_value(), tag + " accepted");
    if (!peer) { return; }
    peer->with_receive_timeout(5s);

    constexpr std::size_t kChunk = 64 * 1024, kChunks = 128; // 8 MiB
    auto expected_byte = [](std::size_t chunk, std::size_t i) {
        return static_cast<std::byte>((chunk * 31 + i) % 251);
    };

    std::atomic<bool> sent_ok{true};
    std::thread writer{[&] {
        std::vector<std::byte> buf(kChunk);
        for (std::size_t c = 0; c < kChunks; ++c) {
            for (std::size_t i = 0; i < kChunk; ++i) { buf[i] = expected_byte(c, i); }
            for (;;) {
                auto r = node.io->send(*handle, buf);
                if (r) { break; }
                if (r.error() != SocketError::WouldBlock) { sent_ok = false; return; }
                std::this_thread::sleep_for(1ms); // backpressure: the sender retries
            }
        }
    }};

    check(read_exact(*peer, hello.size()) == hello, tag + " hello first");
    bool in_order = true;
    std::size_t total = 0;
    std::vector<std::byte> got;
    for (std::size_t c = 0; c < kChunks && in_order; ++c) {
        got = read_exact(*peer, kChunk);
        if (got.size() != kChunk) { in_order = false; break; }
        for (std::size_t i = 0; i < kChunk; ++i) {
            if (got[i] != expected_byte(c, i)) { in_order = false; break; }
        }
        total += got.size();
        if (c % 8 == 0) { std::this_thread::sleep_for(5ms); } // a slow reader forces queueing
    }
    writer.join();
    check(sent_ok.load(), tag + " writer saw no error");
    check(in_order && total == kChunk * kChunks, tag + " 8 MiB received in order (" + std::to_string(total) + ")");
    check(node.metrics.tx_queued_sends.load() > 0, tag + " the slow reader did force queueing");
}

// --- 3. backpressure ---------------------------------------------------------------

void test_backpressure() {
    const std::string tag = "peer-threads backpressure:";
    const unsigned port = random_port();
    TcpListener server("127.0.0.1", port, 4);
    Node node;
    const auto hello = hello_bytes(9, node.port);
    auto handle = node.io->open("127.0.0.1", port, hello);
    check(handle.has_value(), tag + " open");
    if (!handle) { return; }
    auto peer = server.accept_client();
    if (!peer) { check(false, tag + " accepted"); return; }

    std::vector<std::byte> chunk(64 * 1024, std::byte{0x5A});
    std::size_t accepted_bytes = hello.size();
    bool would_block = false;
    for (int i = 0; i < 4096 && !would_block; ++i) {
        auto r = node.io->send(*handle, chunk);
        if (r) { accepted_bytes += chunk.size(); }
        else {
            check(r.error() == SocketError::WouldBlock, tag + " the only failure is WouldBlock");
            would_block = true;
        }
    }
    check(would_block, tag + " send eventually returns WouldBlock");
    std::vector<std::byte> huge(2 * 1024 * 1024, std::byte{1});
    auto r = node.io->send(*handle, huge);
    check(!r && r.error() == SocketError::WouldBlock, tag + " a frame over the cap is refused whole");

    // Nothing partial was queued: draining delivers exactly the accepted bytes.
    peer->with_receive_timeout(2s);
    const auto got = read_exact(*peer, accepted_bytes);
    check(got.size() == accepted_bytes, tag + " peer receives exactly the accepted bytes ("
                                            + std::to_string(got.size()) + " of "
                                            + std::to_string(accepted_bytes) + ")");
    peer->with_receive_timeout(300ms);
    std::byte scratch[16];
    auto extra = peer->receive(std::span<std::byte>{scratch});
    check(!extra, tag + " no bytes beyond the accepted ones");
}

// --- 4. close(handle) --------------------------------------------------------------

void test_close() {
    const std::string tag = "peer-threads close:";
    const unsigned port = random_port();
    TcpListener server("127.0.0.1", port, 4);
    Node node;
    constexpr uint64_t kPeer = 555;
    node.peers.insert(kPeer, p2p::PeerTable::Address{"127.0.0.1", static_cast<uint16_t>(port)});

    auto handle = node.io->open("127.0.0.1", port, hello_bytes(9, node.port));
    check(handle.has_value(), tag + " open");
    if (!handle) { return; }
    constexpr uint64_t kSenderId = 999999999;
    (void)node.peers.attach_tx(kPeer, p2p::ChannelRef{kSenderId, *handle});

    auto remote = server.accept_client();
    if (!remote) { check(false, tag + " accepted"); return; }
    remote->with_receive_timeout(2s);
    (void)read_exact(*remote, hello_bytes(9, node.port).size());
    check(send_all(*remote, *wire::encode(wire::make_connect_ack(9, kPeer))), tag + " ack sent back");
    std::optional<p2p::ChannelRef> tx;
    for (int i = 0; i < 200; ++i) {
        tx = node.peers.tx_of(kPeer);
        if (tx && tx->connection_id != kSenderId) { break; }
        std::this_thread::sleep_for(5ms);
    }
    check(tx && tx->handle == *handle && tx->connection_id != kSenderId,
          tag + " ack re-attached tx under the channel's id");

    node.io->close(*handle);
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone && gone->peer_id == kPeer, tag + " one EventPeerDisconnected");
    check(!pop_for(node.store_queue, 300ms).has_value(), tag + " no second disconnect");
    check(!node.peers.tx_of(kPeer).has_value(), tag + " tx slot released");
    check(tx && !node.peers.release(kPeer, tx->connection_id), tag + " rx slot released too");
    auto sent = node.io->send(*handle, hello_bytes(1, 1));
    check(!sent && sent.error() == SocketError::ConnectionNotFound,
          tag + " send afterwards -> ConnectionNotFound");
    std::byte scratch[16];
    auto eof = remote->receive(std::span<std::byte>{scratch});
    check(!eof || *eof == 0, tag + " socket closed");
    check(wait_until([&] { return node.metrics.connections_live.load() == 0; }),
          tag + " connections_live back to 0");
}

// --- 5. cap eviction ---------------------------------------------------------------

void test_eviction() {
    const std::string tag = "peer-threads eviction:";
    Node node{2};
    std::vector<TcpConnection> clients;
    for (int i = 0; i < 3; ++i) {
        auto client = dial(node.port);
        if (!client) { check(false, tag + " connect"); return; }
        check(send_all(*client, hello_bytes(100 + i, 1)), tag + " hello");
        clients.push_back(std::move(*client));
        std::this_thread::sleep_for(50ms); // distinct last-read order
    }
    check(wait_until([&] { return node.metrics.evictions.load() == 1; }),
          tag + " exactly one eviction");
    // Events arrive in either order (the third connect races the eviction's close).
    int connected = 0, disconnected = 0;
    while (auto event = pop_for(node.store_queue, 500ms)) {
        if (std::holds_alternative<EventPeerConnected>(*event)) { ++connected; }
        else if (std::holds_alternative<EventPeerDisconnected>(*event)) { ++disconnected; }
    }
    check(connected == 3, tag + " three connects seen (" + std::to_string(connected) + ")");
    check(disconnected == 1, tag + " evicted peer reported disconnected exactly once ("
                                 + std::to_string(disconnected) + ")");
    check(wait_until([&] { return node.metrics.connections_live.load() == 2; }),
          tag + " two links left live");
    int not_found = 0, ok = 0;
    for (int handle = 1; handle <= 3; ++handle) {
        auto r = node.io->send(handle, hello_bytes(1, 1));
        if (!r && r.error() == SocketError::ConnectionNotFound) { ++not_found; }
        else if (r) { ++ok; }
    }
    check(not_found == 1 && ok == 2, tag + " send to the evicted handle -> ConnectionNotFound");
    check(node.metrics.evictions.load() == 1, tag + " evictions still 1");
}

// --- 6. shutdown guard ------------------------------------------------------------

void test_shutdown() {
    const std::string tag = "peer-threads shutdown:";
    constexpr int kPeers = 64;
    {
        auto node = std::make_unique<Node>(128);
        std::vector<TcpConnection> clients;
        for (int i = 0; i < kPeers; ++i) {
            auto client = dial(node->port);
            if (!client) { check(false, tag + " connect"); return; }
            check(send_all(*client, hello_bytes(1000 + i, 1)), tag + " hello");
            clients.push_back(std::move(*client));
        }
        int connected = 0;
        for (int i = 0; i < kPeers; ++i) {
            if (pop_as<EventPeerConnected>(node->store_queue)) { ++connected; }
        }
        check(connected == kPeers, tag + " all 64 peers live (" + std::to_string(connected) + ")");

        const auto t0 = std::chrono::steady_clock::now();
        node->io->stop();
        node->listener.join();
        const auto stop_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - t0).count();
        check(stop_ms < 1000, tag + " stop() + run() return in under 1 s (" + std::to_string(stop_ms) + " ms)");

        const auto t1 = std::chrono::steady_clock::now();
        const auto* m = &node->metrics;
        check(wait_until([&] { return m->connections_live.load() == 0; }, 1s),
              tag + " every owner thread exited (live=" + std::to_string(m->connections_live.load()) + ")");
        node.reset(); // destructor joins the watchdog (and so every owner thread)
        const auto destroy_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::steady_clock::now() - t1).count();
        check(destroy_ms < 1000, tag + " destruction under 1 s (" + std::to_string(destroy_ms) + " ms)");
        // Clients see EOF.
        clients.front().with_receive_timeout(1s);
        std::byte scratch[16];
        auto eof = clients.front().receive(std::span<std::byte>{scratch});
        check(!eof || *eof == 0, tag + " peers see their socket closed");
    }
    // A node with no peers also shuts down at once. (It first gets time to listen: a
    // stop() racing the bind can make its wake-up dial wait one SYN retransmit, 1 s;
    // that race exists in every transport and is not what this checks.)
    {
        Node idle;
        std::this_thread::sleep_for(100ms);
        const auto t0 = std::chrono::steady_clock::now();
        idle.io->stop();
        idle.listener.join();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - t0).count();
        check(ms < 1000, tag + " idle node stops in under 1 s (" + std::to_string(ms) + " ms)");
    }
}

} // namespace

int main() {
    test_two_nodes();
    test_order();
    test_backpressure();
    test_close();
    test_eviction();
    test_shutdown();
    std::cout << g_checks << " checks, " << g_failures << " failures\n";
    return g_failures == 0 ? 0 : 1;
}
