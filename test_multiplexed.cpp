// test_multiplexed.cpp - harness for MultiplexedIo (--io=poll / --io=epoll).
//
// Plain assertions (no framework). Every scenario is templated over both pools:
//   * inbound ConnectInfo + chat -> EventPeerConnected / EventMessage, peer close
//     -> EventPeerDisconnected
//   * a malformed frame closes the connection
//   * outbound open/send delivers bytes; the peer closing makes the next send
//     fail with ConnectionNotFound
//   * max_peers LRU pruning is reported as a disconnect
//   * 200 concurrent inbound connections are served by the single I/O thread
//   * a two-node exchange (Store + Sender + make_io_stack) in both modes

#include "MultiplexedIo.hpp"
#include "ThreadWorkers.hpp"
#include "TcpSocket.hpp"
#include "TestPorts.hpp"
#include "Wire.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <fstream>
#include <functional>
#include <future>
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

unsigned random_port() { return test_ports::random_port(); }

bool send_all(TcpConnection& conn, const std::vector<std::byte>& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        auto result = conn.send(std::span<const std::byte>{data.data() + sent, data.size() - sent});
        if (!result || *result == 0) { return false; }
        sent += *result;
    }
    return true;
}

std::optional<TcpConnection> dial(unsigned port, int attempts = 1000) {
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

std::vector<std::byte> chat_bytes(uint64_t name, const std::string& body) {
    return *wire::encode(wire::make_chat_message(name, 1, body));
}

// Positive waits use a generous deadline (they return as soon as the event is
// there); only "nothing else arrives" checks pass a short one.
constexpr auto kEventWait = 10s;

std::optional<Event> pop_for(ThreadQueue<Event>& queue, std::chrono::milliseconds timeout = kEventWait) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (auto event = queue.try_pop()) { return event; }
        std::this_thread::sleep_for(2ms);
    }
    return std::nullopt;
}

template <class T>
std::optional<T> pop_as(ThreadQueue<Event>& queue, std::chrono::milliseconds timeout = kEventWait) {
    auto event = pop_for(queue, timeout);
    if (!event) { return std::nullopt; }
    if (auto* typed = std::get_if<T>(&*event)) { return std::move(*typed); }
    return std::nullopt;
}

int thread_count() {
    std::ifstream status{"/proc/self/status"};
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) { return std::stoi(line.substr(8)); }
    }
    return -1;
}

// A MultiplexedIo plus the thread running its accept loop.
template <class Pool, class Tx = p2p::SamePoolTx,
          p2p::ConnectionModel Model = p2p::ConnectionModel::Dual>
struct Node {
    static NetConfig make_net(unsigned port, std::size_t max_peers) {
        NetConfig net;
        net.port = port;
        net.max_peers = max_peers;
        return net;
    }
    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    unsigned port{0};
    std::unique_ptr<p2p::MultiplexedIo<Pool, Tx>> io;
    std::thread listener;

    // Listening before the constructor returns. A random port another process
    // holds (ctest -j8) makes the bind fail: that node is discarded and the port
    // redrawn, instead of the test dialling someone else's listener.
    explicit Node(std::size_t max_peers = 16) {
        for (int attempt = 0; attempt < 20; ++attempt) {
            port = random_port();
            io = std::make_unique<p2p::MultiplexedIo<Pool, Tx>>(
                make_net(port, max_peers), store_queue, peers, Logger{}, 1024 * 1024, Model);
            if (test_ports::start_listening(*io, listener) || attempt == 19) { return; }
            io->stop();
            listener.join();
            io.reset();
            while (store_queue.try_pop()) {} // the failed bind's EventError
        }
    }
    ~Node() {
        io->stop();
        listener.join();
    }
};

// ---------------------------------------------------------------------------

template <class Pool>
void test_inbound(const std::string& tag) {
    Node<Pool> node;
    auto client = dial(node.port);
    check(client.has_value(), tag + " inbound: connected");
    if (!client) { return; }

    check(send_all(*client, hello_bytes(77, 1234)), tag + " inbound: hello sent");
    check(send_all(*client, chat_bytes(77, "hello")), tag + " inbound: chat sent");

    auto connected = pop_as<EventPeerConnected>(node.store_queue);
    check(connected.has_value(), tag + " inbound: EventPeerConnected");
    const uint64_t local_id = connected ? connected->peer_id : 0;
    check(connected && connected->assigned_peer_id == 77, tag + " inbound: peer's name for us");

    auto message = pop_as<EventMessage>(node.store_queue);
    check(message.has_value() && message->payload == "hello" && message->peer_id == local_id,
          tag + " inbound: EventMessage routed by peer id");

    client.reset();
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone.has_value() && gone->peer_id == local_id, tag + " inbound: EventPeerDisconnected");
}

template <class Pool>
void test_malformed(const std::string& tag) {
    Node<Pool> node;
    auto client = dial(node.port);
    check(client.has_value(), tag + " malformed: connected");
    if (!client) { return; }

    check(send_all(*client, hello_bytes(5, 1)), tag + " malformed: hello sent");
    auto connected = pop_as<EventPeerConnected>(node.store_queue);
    check(connected.has_value(), tag + " malformed: connected");

    check(send_all(*client, std::vector<std::byte>(64, std::byte{0xFF})),
          tag + " malformed: garbage sent");
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone.has_value(), tag + " malformed: disconnect reported");

    // The server side closed: reading eventually reports EOF/error.
    client->with_receive_timeout(kEventWait);
    std::byte scratch[16];
    auto got = client->receive(std::span<std::byte>{scratch});
    check(!got || *got == 0, tag + " malformed: socket closed by the node");
}

template <class Pool>
void test_outbound(const std::string& tag) {
    unsigned port = 0;
    TcpListener server = test_ports::listen_on_random_port(port, 4); // redraws if taken
    Node<Pool> node;

    const std::vector<std::byte> hello = hello_bytes(9, 4321);
    auto handle = node.io->open("127.0.0.1", port, hello);
    check(handle.has_value(), tag + " outbound: open");
    if (!handle) { return; }

    auto peer = server.accept_client();
    check(peer.has_value(), tag + " outbound: server accepted");
    if (!peer) { return; }
    peer->with_receive_timeout(kEventWait);

    auto read_exact = [&](std::size_t n) {
        std::vector<std::byte> got;
        std::byte scratch[4096];
        while (got.size() < n) {
            auto r = peer->receive(std::span<std::byte>{scratch});
            if (!r || *r == 0) { break; }
            got.insert(got.end(), scratch, scratch + *r);
        }
        return got;
    };

    check(read_exact(hello.size()) == hello, tag + " outbound: hello delivered first");

    const auto frame = chat_bytes(9, "payload");
    check(node.io->send(*handle, frame).has_value(), tag + " outbound: send ok");
    check(read_exact(frame.size()) == frame, tag + " outbound: frame delivered");

    // Bytes arriving on an outbound connection are ignored, not parsed.
    check(send_all(*peer, chat_bytes(1, "ignored")), tag + " outbound: stray bytes sent");
    std::this_thread::sleep_for(50ms);
    check(node.io->send(*handle, frame).has_value(), tag + " outbound: still usable after stray bytes");
    check(!node.store_queue.try_pop().has_value(), tag + " outbound: stray bytes produce no event");

    // Peer goes away: the I/O thread sees EOF, the next sends fail for good.
    { auto dead = std::move(*peer); } // closes the server side
    std::optional<SocketError> last;
    for (int i = 0; i < 1000; ++i) { // up to 10 s, until the I/O thread saw the EOF
        auto sent = node.io->send(*handle, frame);
        if (!sent) { last = sent.error(); if (*last == SocketError::ConnectionNotFound) { break; } }
        std::this_thread::sleep_for(10ms);
    }
    check(last == SocketError::ConnectionNotFound, tag + " outbound: dead handle -> ConnectionNotFound");
    node.io->close(*handle); // idempotent on a dead handle

    const unsigned closed = test_ports::closed_port(); // bound, then released: refused
    auto again = node.io->open("127.0.0.1", closed, hello); // nobody listens there
    check(!again.has_value(), tag + " outbound: open to a closed port fails");
}

template <class Pool>
void test_prune(const std::string& tag) {
    // max_peers = 2 -> the shared pool holds 4 connections.
    Node<Pool> node{2};
    std::vector<TcpConnection> clients;
    for (uint64_t i = 0; i < 5; ++i) {
        auto client = dial(node.port);
        check(client.has_value(), tag + " prune: connected");
        if (!client) { return; }
        check(send_all(*client, hello_bytes(100 + i, 1)), tag + " prune: hello sent");
        clients.push_back(std::move(*client));
        std::this_thread::sleep_for(20ms); // distinct activity stamps for the LRU order
    }
    // The newcomer that exceeds the cap evicts the LRU peer: 5 connects, 1 disconnect.
    int connects = 0, disconnects = 0;
    const auto deadline = std::chrono::steady_clock::now() + kEventWait;
    while (std::chrono::steady_clock::now() < deadline && (connects < 5 || disconnects < 1)) {
        auto event = pop_for(node.store_queue, 200ms);
        if (!event) { continue; }
        if (std::holds_alternative<EventPeerConnected>(*event)) { ++connects; }
        if (std::holds_alternative<EventPeerDisconnected>(*event)) { ++disconnects; }
    }
    check(connects == 5, tag + " prune: all five connects seen");
    check(disconnects == 1, tag + " prune: the evicted peer is reported as disconnected once");
}

template <class Pool>
void test_many(const std::string& tag) {
    constexpr int kClients = 200;
    const int before = thread_count();
    Node<Pool> node{256};
    std::vector<TcpConnection> clients;
    for (int i = 0; i < kClients; ++i) {
        auto client = dial(node.port);
        if (!client) { check(false, tag + " many: connect"); return; }
        check(send_all(*client, hello_bytes(1000 + i, 1)), tag + " many: hello");
        check(send_all(*client, chat_bytes(1000 + i, "m" + std::to_string(i))), tag + " many: chat");
        clients.push_back(std::move(*client));
    }
    int connects = 0, messages = 0, other = 0;
    const auto deadline = std::chrono::steady_clock::now() + 2 * kEventWait;
    while ((connects < kClients || messages < kClients) &&
           std::chrono::steady_clock::now() < deadline) {
        auto event = pop_for(node.store_queue, 1s);
        if (!event) { continue; }
        if (std::holds_alternative<EventPeerConnected>(*event)) { ++connects; }
        else if (std::holds_alternative<EventMessage>(*event)) { ++messages; }
        else { ++other; }
    }
    check(connects == kClients, tag + " many: all connects seen (" + std::to_string(connects) + ")");
    check(messages == kClients, tag + " many: all messages seen (" + std::to_string(messages) + ")");
    check(other == 0, tag + " many: no unexpected events");

    // Accept loop (test's thread) + ONE I/O thread, regardless of connection count.
    const int during = thread_count();
    check(before > 0 && during - before <= 2,
          tag + " many: only 2 threads for 200 connections (" + std::to_string(during - before) + ")");
}

// --- Two nodes: Store + Sender + make_io_stack -------------------------------

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
    bool listening{false}; // the listen socket is bound (see make_full_node)

    FullNode(IoMode mode, unsigned port_)
        : port(port_),
          io(p2p::make_io_stack(mode, make_net(port_, &metrics), store_queue, peers, Logger{})),
          store(render_queue, store_queue, sender_queue),
          sender(store_queue, sender_queue, peers, *io.outbound,
                 LocalEndpoints{{wire::Endpoint{"127.0.0.1", static_cast<uint16_t>(port_)}}}) {
        store_t = std::thread{[this] { store(); }};
        sender_t = std::thread{[this] { sender(); }};
        listening = test_ports::start_listening(*io.inbound, listener_t);
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

// A FullNode that is listening: a random port another process holds is redrawn
// (the node's own bind would fail and the peer would dial a stranger).
std::unique_ptr<FullNode> make_full_node(IoMode mode) {
    for (int attempt = 0;; ++attempt) {
        auto node = std::make_unique<FullNode>(mode, random_port());
        if (node->listening || attempt == 19) { return node; }
    }
}

void test_two_nodes(IoMode mode) {
    const std::string tag = std::string{to_string(mode)} + " e2e:";
    auto node_a = make_full_node(mode);
    auto node_b = make_full_node(mode);
    FullNode& a = *node_a;
    FullNode& b = *node_b;
    const unsigned port_b = b.port;
    check(a.listening && b.listening, tag + " both listening");

    check(a.store_queue.emplace(EventDialPeer{"127.0.0.1", static_cast<uint16_t>(port_b), 0}),
          tag + " dial queued");

    // A learns the conversation from the dial report; B learns it from the connect.
    uint64_t conv_a = 0;
    for (int i = 0; i < 100 && conv_a == 0; ++i) {
        auto event = pop_for(a.render_queue);
        if (!event) { break; }
        if (auto* dialed = std::get_if<EventPeerDialed>(&*event)) { conv_a = dialed->conversation_id; }
    }
    check(conv_a != 0, tag + " A got a dialed conversation");

    uint64_t conv_b = 0;
    for (int i = 0; i < 100 && conv_b == 0; ++i) {
        auto event = pop_for(b.render_queue, 3s); // a probe: the list request below is the fallback
        if (!event) { break; }
        if (auto* connected = std::get_if<EventPeerDialed>(&*event)) { conv_b = connected->conversation_id; }
        else if (auto* notice = std::get_if<EventConversationNotification>(&*event)) { conv_b = notice->conversation_id; }
    }
    // Fall back to asking the store what it holds.
    if (conv_b == 0) {
        b.store_queue.emplace(EventListConversations{});
        for (int i = 0; i < 100 && conv_b == 0; ++i) {
            auto event = pop_for(b.render_queue);
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
            auto event = pop_for(node.render_queue);
            if (!event) { return false; }
            auto matches = [&](const std::vector<ConversationStore::Message>& items) {
                for (const auto& m : items) { if (m.payload == text) { return true; } }
                return false;
            };
            if (auto* delta = std::get_if<EventConversationDelta>(&*event)) {
                if (matches(delta->messages)) { return true; }
            } else if (auto* snap = std::get_if<EventConversationSnapshot>(&*event)) {
                if (snap->store) {
                    for (const auto& m : snap->store->store) { if (m.payload == text) { return true; } }
                }
            }
        }
        return false;
    };

    // Deliver in both directions: each message travels on the sender's own
    // outbound connection and is read by the other node's I/O thread.
    check(a.store_queue.emplace(EventComposeMessage{conv_a, "ping"}), tag + " A composes");
    check(wait_text(b, "ping"), tag + " B received ping");
    check(b.store_queue.emplace(EventComposeMessage{conv_b, "pong"}), tag + " B composes");
    check(wait_text(a, "pong"), tag + " A received pong");

    const bool single = mode == IoMode::PollDuplex || mode == IoMode::EpollDuplex
                     || mode == IoMode::PollSplit || mode == IoMode::EpollSplit;
    std::this_thread::sleep_for(200ms); // a dial-back would have happened by now
    const auto accepts = a.metrics.accepts.load() + b.metrics.accepts.load();
    if (single) {
        // The pong reached A on its own dialed socket, which needs the ConnectAck
        // (peer identity) to have been processed there: the handshake completed.
        check(accepts == 1, tag + " exactly one TCP connection in total (accepts="
                                + std::to_string(accepts) + ")");
        check(a.metrics.connections_live.load() == 1 && b.metrics.connections_live.load() == 1,
              tag + " both sides count that one connection live");
    } else {
        // The dial-back is asynchronous: wait for it (it may lag the pong under load).
        auto total = [&] { return a.metrics.accepts.load() + b.metrics.accepts.load(); };
        for (int i = 0; i < 1000 && total() < 2; ++i) { std::this_thread::sleep_for(10ms); }
        check(total() == 2, tag + " two TCP connections in total (accepts="
                                + std::to_string(total()) + ")");
    }
}

// --- Duplex / split scenarios on a bare MultiplexedIo --------------------------

std::vector<std::byte> hello_adopt_bytes(uint64_t name, uint64_t returning, unsigned port) {
    return *wire::encode(wire::make_connect(
        name, returning, std::vector<wire::Endpoint>{{"127.0.0.1", static_cast<uint16_t>(port)}}));
}

template <class Pool, class Tx>
void test_duplex_inbound(const std::string& tag) {
    Node<Pool, Tx, p2p::ConnectionModel::Duplex> node;
    check(node.io->duplex(), tag + " duplex(): true");
    auto client = dial(node.port);
    check(client.has_value(), tag + " duplex inbound: connected");
    if (!client) { return; }
    check(send_all(*client, hello_bytes(77, 1234)), tag + " duplex inbound: hello");
    auto connected = pop_as<EventPeerConnected>(node.store_queue);
    check(connected.has_value(), tag + " duplex inbound: EventPeerConnected");
    if (!connected) { return; }
    const uint64_t peer = connected->peer_id;
    const auto tx = node.peers.tx_of(peer);
    check(tx.has_value() && tx->handle != 0 && connected->superseded == 0,
          tag + " duplex inbound: the accepted socket fills the tx slot");

    // The sender answers on the tx handle: the client reads the ConnectAck back.
    if (tx) {
        const auto ack = *wire::encode(wire::make_connect_ack(peer, 77));
        check(node.io->send(tx->handle, ack).has_value(), tag + " duplex inbound: ack sent");
        client->with_receive_timeout(kEventWait);
        std::vector<std::byte> got;
        std::byte scratch[256];
        while (got.size() < ack.size()) {
            auto r = client->receive(std::span<std::byte>{scratch});
            if (!r || *r == 0) { break; }
            got.insert(got.end(), scratch, scratch + *r);
        }
        check(got == ack, tag + " duplex inbound: ack delivered on the same socket");
    }
    check(send_all(*client, chat_bytes(77, "hi")), tag + " duplex inbound: chat");
    auto message = pop_as<EventMessage>(node.store_queue);
    check(message && message->payload == "hi", tag + " duplex inbound: message flows in");

    client.reset();
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone && gone->peer_id == peer, tag + " duplex inbound: peer close -> EventPeerDisconnected");
    check(!pop_for(node.store_queue, 300ms).has_value(),
          tag + " duplex inbound: exactly one disconnect");
    check(!node.peers.tx_of(peer).has_value(), tag + " duplex inbound: tx slot released too");
}

template <class Pool, class Tx>
void test_duplex_adopt(const std::string& tag) {
    Node<Pool, Tx, p2p::ConnectionModel::Duplex> node;
    auto first = dial(node.port);
    if (!first) { check(false, tag + " adopt: connect"); return; }
    check(send_all(*first, hello_bytes(40, 1)), tag + " adopt: first hello");
    auto c1 = pop_as<EventPeerConnected>(node.store_queue);
    check(c1.has_value(), tag + " adopt: first connected");
    if (!c1) { return; }
    const uint64_t peer = c1->peer_id;
    const int old_handle = node.peers.tx_of(peer) ? node.peers.tx_of(peer)->handle : 0;

    auto second = dial(node.port);
    if (!second) { check(false, tag + " adopt: connect 2"); return; }
    check(send_all(*second, hello_adopt_bytes(40, peer, 1)), tag + " adopt: returning hello");
    auto c2 = pop_as<EventPeerConnected>(node.store_queue);
    check(c2 && c2->peer_id == peer && c2->adopted, tag + " adopt: same peer adopted");
    check(c2 && c2->superseded == old_handle && old_handle != 0,
          tag + " adopt: old channel handed over as superseded");
    if (c2 && c2->superseded != 0) { node.io->close(c2->superseded); } // what the sender does

    first->with_receive_timeout(kEventWait);
    std::byte scratch[16];
    auto got = first->receive(std::span<std::byte>{scratch});
    check(!got || *got == 0, tag + " adopt: the old socket was closed");
    check(!pop_for(node.store_queue, 300ms).has_value(),
          tag + " adopt: superseded close is silent, peer stays connected");
    auto entry = node.peers.lookup(peer);
    check(entry && entry->status == p2p::PeerTable::Status::Connected,
          tag + " adopt: peer still connected");
    const auto tx = node.peers.tx_of(peer);
    check(tx && tx->handle != old_handle, tag + " adopt: tx slot holds the new channel");

    second.reset();
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone && gone->peer_id == peer, tag + " adopt: new channel close reports once");
}

template <class Pool, class Tx>
void test_duplex_dialed(const std::string& tag) {
    unsigned port = 0;
    TcpListener server = test_ports::listen_on_random_port(port, 4); // redraws if taken
    Node<Pool, Tx, p2p::ConnectionModel::Duplex> node;
    constexpr uint64_t kPeer = 555;
    node.peers.insert(kPeer, p2p::PeerTable::Address{"127.0.0.1", static_cast<uint16_t>(port)});

    const auto hello = hello_bytes(9, node.port);
    auto handle = node.io->open("127.0.0.1", port, hello);
    check(handle.has_value(), tag + " dialed: open");
    if (!handle) { return; }
    // What SenderWorker::open does: attach tx under its own minted id.
    constexpr uint64_t kSenderId = 999999999;
    (void)node.peers.attach_tx(kPeer, p2p::ChannelRef{kSenderId, *handle});

    auto remote = server.accept_client();
    check(remote.has_value(), tag + " dialed: accepted");
    if (!remote) { return; }
    remote->with_receive_timeout(kEventWait);
    std::vector<std::byte> got;
    std::byte scratch[4096];
    while (got.size() < hello.size()) {
        auto r = remote->receive(std::span<std::byte>{scratch});
        if (!r || *r == 0) { break; }
        got.insert(got.end(), scratch, scratch + *r);
    }
    check(got == hello, tag + " dialed: hello delivered");

    // The remote answers on the same socket.
    check(send_all(*remote, *wire::encode(wire::make_connect_ack(9, kPeer))),
          tag + " dialed: ack sent back");
    std::optional<p2p::ChannelRef> tx;
    for (int i = 0; i < 2000; ++i) { // up to 10 s, until the ack was processed
        tx = node.peers.tx_of(kPeer);
        if (tx && tx->connection_id != kSenderId) { break; }
        std::this_thread::sleep_for(5ms);
    }
    check(tx && tx->handle == *handle && tx->connection_id != kSenderId,
          tag + " dialed: ack re-attaches tx under the channel's id");
    // The dialed socket also carries traffic from the remote: a chat reaches the store.
    check(send_all(*remote, chat_bytes(1, "from remote")), tag + " dialed: chat sent");
    auto message = pop_as<EventMessage>(node.store_queue);
    check(message && message->peer_id == kPeer && message->payload == "from remote",
          tag + " dialed: rx works on the dialed socket");

    // The sender drops the handle: both slots go and the peer is reported gone once.
    node.io->close(*handle);
    auto gone = pop_as<EventPeerDisconnected>(node.store_queue);
    check(gone && gone->peer_id == kPeer, tag + " dialed: close reports the disconnect");
    check(!node.peers.tx_of(kPeer).has_value(), tag + " dialed: tx slot released");
    check(tx && !node.peers.release(kPeer, tx->connection_id),
          tag + " dialed: rx slot released too");
    auto entry = node.peers.lookup(kPeer);
    check(entry && entry->status == p2p::PeerTable::Status::Disconnected,
          tag + " dialed: status Disconnected");
    check(!pop_for(node.store_queue, 300ms).has_value(), tag + " dialed: no second disconnect");
    auto eof = remote->receive(std::span<std::byte>{scratch});
    check(!eof || *eof == 0, tag + " dialed: socket closed");
}

template <class Pool, class Tx>
void run_duplex_suite(const std::string& tag) {
    test_duplex_inbound<Pool, Tx>(tag);
    test_duplex_adopt<Pool, Tx>(tag);
    test_duplex_dialed<Pool, Tx>(tag);
}

} // namespace

int main() {
    test_inbound<TcpConnectionPool>("poll");
    test_inbound<EpollConnectionPool>("epoll");
    test_malformed<TcpConnectionPool>("poll");
    test_malformed<EpollConnectionPool>("epoll");
    test_outbound<TcpConnectionPool>("poll");
    test_outbound<EpollConnectionPool>("epoll");
    test_prune<TcpConnectionPool>("poll");
    test_prune<EpollConnectionPool>("epoll");
    test_many<TcpConnectionPool>("poll");
    test_many<EpollConnectionPool>("epoll");
    test_two_nodes(IoMode::Poll);
    test_two_nodes(IoMode::Epoll);
    run_duplex_suite<TcpConnectionPool, p2p::SamePoolTx>("poll-duplex");
    run_duplex_suite<EpollConnectionPool, p2p::SamePoolTx>("epoll-duplex");
    run_duplex_suite<TcpConnectionPool, PollTxPoller>("poll-split");
    run_duplex_suite<EpollConnectionPool, EpollTxPoller>("epoll-split");
    test_two_nodes(IoMode::PollDuplex);
    test_two_nodes(IoMode::EpollDuplex);
    test_two_nodes(IoMode::PollSplit);
    test_two_nodes(IoMode::EpollSplit);

    std::cout << g_checks << " checks, " << g_failures << " failures\n";
    return g_failures == 0 ? 0 : 1;
}
