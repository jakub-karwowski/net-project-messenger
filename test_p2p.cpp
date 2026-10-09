// test_p2p.cpp - harness for the p2p listener / reader / watchdog trio.
//
// Plain assertions (no framework): prints every failed check and exits non-zero
// if anything failed. Covers:
//   * PeerTable insert / lookup / erase semantics
//   * the watchdog unsticking a reader whose heartbeat stops advancing
//   * an end-to-end loopback: raw client -> ListenerWorker -> connect event,
//     then the message event, followed by the disconnect event when the client
//     goes away
//   * a peer that re-presents the id it was assigned keeping that id.

#include "ThreadWorkers.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

// Reader-thread cap for tests that do not exercise pruning: comfortably above the
// handful of clients they open.
constexpr std::size_t kUncappedReaders = 16;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "FAIL: " << what << '\n';
    }
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

// Spins until the listener is accepting, then returns a connected client.
std::optional<TcpConnection> dial(unsigned port, int attempts = 200) {
    for (int i = 0; i < attempts; ++i) {
        auto conn = TcpConnection::connect("127.0.0.1", port);
        if (conn) { return std::move(*conn); }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    return std::nullopt;
}

// ---------------------------------------------------------------------------

void test_peer_table() {
    p2p::PeerTable peers;
    check(peers.lookup(1) == std::nullopt, "PeerTable starts empty");

    check(peers.insert(1, {"127.0.0.1", 5000}), "PeerTable insert succeeds");
    check(!peers.insert(1, {"127.0.0.1", 6000}), "PeerTable rejects duplicate id");

    auto entry = peers.lookup(1);
    check(entry.has_value(), "PeerTable lookup finds the peer");
    check(entry && entry->address.ip == "127.0.0.1" && entry->address.port == 6000,
          "PeerTable lookup returns the refreshed address");
    check(entry && entry->status == p2p::PeerTable::Status::Connected,
          "PeerTable insert leaves the peer connected");

    check(peers.mark_disconnected(1), "PeerTable marks a known peer disconnected");
    auto gone = peers.lookup(1);
    check(gone.has_value(), "PeerTable keeps the peer after disconnect");
    check(gone && gone->status == p2p::PeerTable::Status::Disconnected,
          "PeerTable reports the disconnected status");
    check(!peers.mark_disconnected(99), "PeerTable cannot mark an unknown peer");

    check(!peers.insert(1, {"127.0.0.1", 7000}), "PeerTable re-insert still reports duplicate");
    auto back = peers.lookup(1);
    check(back && back->status == p2p::PeerTable::Status::Connected,
          "PeerTable reactivates a returning peer");

    check(peers.erase(1), "PeerTable erase removes the peer");
    check(peers.lookup(1) == std::nullopt, "PeerTable lookup misses after erase");
    check(!peers.erase(1), "PeerTable erase of an unknown id is false");
}

// The peer's inbound connection is owned by exactly one reader: when a newer
// connection adopts the same peer id (a reconnect), the older reader is superseded
// and must not report the still-live peer as gone.
void test_peer_table_connection_owner() {
    p2p::PeerTable peers;
    peers.insert(1, {"127.0.0.1", 5000});

    check(peers.set_connection(1, 100), "owner: first reader takes ownership");
    check(peers.set_connection(1, 200), "owner: a newer reader takes over");
    check(!peers.release_connection(1, 100),
          "owner: the superseded reader cannot release");
    auto live = peers.lookup(1);
    check(live && live->status == p2p::PeerTable::Status::Connected,
          "owner: a superseded exit leaves the peer connected");

    check(peers.release_connection(1, 200), "owner: the current reader releases");
    auto gone = peers.lookup(1);
    check(gone && gone->status == p2p::PeerTable::Status::Disconnected,
          "owner: releasing marks the peer disconnected");
    check(!peers.release_connection(1, 200), "owner: releasing twice is false");
    check(!peers.set_connection(0, 5), "owner: a zero peer id is rejected");
}

// A reader whose tick never advances must be unstuck by the watchdog.
void test_watchdog_stops_stalled_reader() {
    constexpr unsigned kPort = 9994;

    TcpListener listener("127.0.0.1", kPort, 1);
    auto client = dial(kPort);
    check(client.has_value(), "watchdog test: client connected");
    if (!client) { return; }

    auto server = listener.accept_client();
    check(server.has_value(), "watchdog test: listener accepted");
    if (!server) { return; }

    auto connection = std::make_shared<TcpConnection>(std::move(*server));
    connection->with_receive_timeout(std::chrono::milliseconds{5000});

    auto frozen = std::make_shared<p2p::ReaderActivity>(); // tick stays 0: never advances
    p2p::WatchdogWorker watchdog;
    watchdog.register_reader(frozen, connection);

    std::thread watchdog_thread{std::ref(watchdog)};

    std::array<std::byte, 64> scratch{};
    const auto start = std::chrono::steady_clock::now();
    auto received = connection->receive(std::span<std::byte>{scratch.data(), scratch.size()});
    const auto elapsed = std::chrono::steady_clock::now() - start;

    check(!received, "watchdog test: blocked receive returns after shutdown");
    check(elapsed < std::chrono::seconds{3},
          "watchdog test: unstuck well before the 5s recv timeout");

    watchdog.stop();
    watchdog_thread.join();
}

// End-to-end: a raw peer talks to a ListenerWorker and its events surface.
void test_listener_roundtrip() {
    constexpr unsigned kPort = 9993;
    constexpr uint64_t kPeerId = 42;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kUncappedReaders};

    std::thread listener_thread{std::ref(listener)};

    auto client = dial(kPort);
    check(client.has_value(), "roundtrip: client connected to listener");
    if (!client) {
        listener.stop();
        listener_thread.join();
        return;
    }

    auto hello = wire::encode(wire::make_connect(
        kPeerId, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    check(hello.has_value(), "roundtrip: hello encodes");
    if (hello) { check(send_all(*client, *hello), "roundtrip: hello sent"); }

    auto chat = wire::encode(wire::make_chat_message(kPeerId, 1, "hello"));
    check(chat.has_value(), "roundtrip: chat encodes");
    if (chat) { check(send_all(*client, *chat), "roundtrip: chat sent"); }

    // The reader names the peer itself and reports it before any message.
    uint64_t local_id = 0;
    std::optional<Event> connect_event = store_queue.pop();
    check(connect_event.has_value(), "roundtrip: a connect event was delivered");
    if (connect_event) {
        auto* connected = std::get_if<EventPeerConnected>(&*connect_event);
        check(connected != nullptr, "roundtrip: event is an EventPeerConnected");
        if (connected) {
            local_id = connected->peer_id;
            check(local_id != 0, "roundtrip: reader assigned the peer a local name");
            check(connected->assigned_peer_id == kPeerId,
                  "roundtrip: the name the peer gave us is echoed");
            check(connected->endpoints.size() == 1 &&
                      connected->endpoints.front() == wire::Endpoint{"127.0.0.1", kPort},
                  "roundtrip: connect advertises the peer's endpoint");
        }
    }

    std::optional<Event> message_event = store_queue.pop();
    check(message_event.has_value(), "roundtrip: an event was delivered");
    if (message_event) {
        auto* message = std::get_if<EventMessage>(&*message_event);
        check(message != nullptr, "roundtrip: event is an EventMessage");
        if (message) {
            check(message->peer_id == local_id, "roundtrip: message uses the local name");
            check(message->conversation_id == 0,
                  "roundtrip: reader leaves the local conversation unresolved");
            check(message->payload == "hello", "roundtrip: payload matches");
        }
    }

    auto address = peers.lookup(local_id);
    check(address.has_value() && address->address.port == kPort,
          "roundtrip: peer address registered from ConnectInfo");

    // Drop the client: the reader sees EOF and announces the disconnect.
    (void)client->shutdown(TcpConnection::Shutdown::Both);
    client.reset();

    std::optional<Event> disconnect_event = store_queue.pop();
    check(disconnect_event.has_value(), "roundtrip: a disconnect event was delivered");
    if (disconnect_event) {
        auto* disconnected = std::get_if<EventPeerDisconnected>(&*disconnect_event);
        check(disconnected != nullptr, "roundtrip: event is an EventPeerDisconnected");
        if (disconnected) {
            check(disconnected->conversation_id == 0,
                  "roundtrip: disconnect leaves the local conversation unresolved");
            check(disconnected->peer_id == local_id,
                  "roundtrip: disconnect uses the local name");
        }
    }
    auto after = peers.lookup(local_id);
    check(after.has_value() && after->status == p2p::PeerTable::Status::Disconnected,
          "roundtrip: peer marked disconnected (not erased)");

    listener.stop();
    listener_thread.join();
}

// A peer that re-presents the id we assigned it keeps that id (so a peer whose
// address changed resumes its old conversation); one that does not is new.
void test_listener_reuses_assigned_id() {
    constexpr unsigned kPort = 9994;
    constexpr uint64_t kFirstName = 42;
    constexpr uint64_t kReturnName = 43;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kUncappedReaders};

    std::thread listener_thread{std::ref(listener)};

    // First contact: the peer names us and we mint a local name for it.
    uint64_t local_id = 0;
    auto client = dial(kPort);
    check(client.has_value(), "return: first client connected");
    if (!client) {
        listener.stop();
        listener_thread.join();
        return;
    }

    auto hello = wire::encode(wire::make_connect(
        kFirstName, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    check(hello.has_value(), "return: hello encodes");
    if (hello) { check(send_all(*client, *hello), "return: hello sent"); }

    auto connect_event = store_queue.pop();
    check(connect_event.has_value(), "return: a connect event was delivered");
    if (connect_event) {
        const auto* connected = std::get_if<EventPeerConnected>(&*connect_event);
        check(connected != nullptr, "return: event is an EventPeerConnected");
        if (connected) {
            local_id = connected->peer_id;
            check(local_id != 0, "return: reader assigned the peer a local name");
        }
    }

    // Drop the client and wait for the reader to announce it, so the peer table is
    // settled before the same peer comes back.
    (void)client->shutdown(TcpConnection::Shutdown::Both);
    client.reset();
    auto disconnect_event = store_queue.pop();
    check(disconnect_event.has_value(), "return: a disconnect event was delivered");

    // Second contact: the peer re-presents the id we gave it, under a new name.
    auto returning = dial(kPort);
    check(returning.has_value(), "return: second client connected");
    if (returning) {
        auto again = wire::encode(wire::make_connect(
            kReturnName, local_id, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
        check(again.has_value(), "return: returning hello encodes");
        if (again) { check(send_all(*returning, *again), "return: returning hello sent"); }

        auto second_event = store_queue.pop();
        const auto* connected =
            second_event ? std::get_if<EventPeerConnected>(&*second_event) : nullptr;
        check(connected != nullptr, "return: the returning peer is reported");
        if (connected) {
            check(connected->peer_id == local_id,
                  "return: the id we assigned is adopted, not re-minted");
            check(connected->assigned_peer_id == kReturnName,
                  "return: the peer's new name for us is carried through");
        }

        const auto entry = peers.lookup(local_id);
        check(entry.has_value(), "return: the adopted id is in the peer table");
        check(entry && entry->assigned_id == kReturnName,
              "return: the peer's name for us is remembered for the next handshake");
        check(entry && entry->status == p2p::PeerTable::Status::Connected,
              "return: the returning peer is marked connected again");

        (void)returning->shutdown(TcpConnection::Shutdown::Both);
        returning.reset();
        (void)store_queue.pop(); // the disconnect event
    }

    listener.stop();
    listener_thread.join();
}

// Drains the queue until an EventPeerConnected for `peer_id` appears (ignoring
// every other event), or the timeout elapses.
bool wait_for_peer(ThreadQueue<Event>& queue, uint64_t peer_id,
                   std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        while (auto event = queue.try_pop()) {
            const auto* connected = std::get_if<EventPeerConnected>(&*event);
            if (connected != nullptr && connected->peer_id == peer_id) { return true; }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
}

// Returns the local id of the first peer that connects, or nullopt on timeout.
std::optional<uint64_t> wait_first_peer(ThreadQueue<Event>& queue,
                                        std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    do {
        while (auto event = queue.try_pop()) {
            if (const auto* connected = std::get_if<EventPeerConnected>(&*event)) {
                return connected->peer_id;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{5});
    } while (std::chrono::steady_clock::now() < deadline);
    return std::nullopt;
}

// The watchdog prunes the least-recently-read connection first.
void test_watchdog_prunes_oldest_reader() {
    constexpr unsigned kPort = 9983;

    TcpListener acceptor("127.0.0.1", kPort, 2);
    auto old_client = dial(kPort);
    auto new_client = dial(kPort);
    check(old_client.has_value() && new_client.has_value(), "prune: clients connected");
    if (!old_client || !new_client) { return; }

    auto old_server = acceptor.accept_client();
    auto new_server = acceptor.accept_client();
    check(old_server.has_value() && new_server.has_value(), "prune: server accepted");
    if (!old_server || !new_server) { return; }

    auto old_conn = std::make_shared<TcpConnection>(std::move(*old_server));
    auto new_conn = std::make_shared<TcpConnection>(std::move(*new_server));
    old_conn->with_receive_timeout(std::chrono::milliseconds{5000});
    new_conn->with_receive_timeout(std::chrono::milliseconds{5000});

    // Both readers are alive (their ticks advance), but the "old" one read much earlier.
    auto old_activity = std::make_shared<p2p::ReaderActivity>();
    auto new_activity = std::make_shared<p2p::ReaderActivity>();
    old_activity->last_read_ns.store(100, std::memory_order_relaxed);
    new_activity->last_read_ns.store(200, std::memory_order_relaxed);

    std::atomic<std::size_t> prune_requests{1};
    p2p::WatchdogWorker watchdog;
    watchdog.configure(prune_requests);
    watchdog.register_reader(old_activity, old_conn);
    watchdog.register_reader(new_activity, new_conn);

    std::thread watchdog_thread{std::ref(watchdog)};
    std::this_thread::sleep_for(std::chrono::milliseconds{300}); // at least one scan

    std::array<std::byte, 16> scratch{};
    auto old_read = old_conn->receive(std::span<std::byte>{scratch.data(), scratch.size()});
    check(!old_read && old_read.error() == SocketError::ConnectionClosed,
          "prune: the oldest connection was shut down");

    new_conn->with_receive_timeout(std::chrono::milliseconds{200});
    auto new_read = new_conn->receive(std::span<std::byte>{scratch.data(), scratch.size()});
    check(!new_read && new_read.error() == SocketError::WouldBlock,
          "prune: the newest connection was left open");

    watchdog.stop();
    watchdog_thread.join();

    (void)old_client->shutdown(TcpConnection::Shutdown::Both);
    (void)new_client->shutdown(TcpConnection::Shutdown::Both);
}

// The listener enforces its cap: the oldest idle connection is pruned, and the
// dropped peer can reconnect with the id it was already assigned.
void test_listener_prunes_over_cap() {
    constexpr unsigned kPort = 9982;
    constexpr uint64_t kPeerName = 7;
    constexpr std::size_t kCap = 1;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kCap};

    std::thread listener_thread{std::ref(listener)};

    // First peer connects and names itself; the reader mints its local id.
    auto first = dial(kPort);
    check(first.has_value(), "cap: first client connected");
    if (!first) {
        listener.stop();
        listener_thread.join();
        return;
    }
    auto hello = wire::encode(wire::make_connect(
        kPeerName, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    check(hello.has_value(), "cap: first hello encodes");
    if (hello) { check(send_all(*first, *hello), "cap: first hello sent"); }

    const auto local_id = wait_first_peer(store_queue, std::chrono::milliseconds{1000});
    check(local_id.has_value(), "cap: first peer got a local id");

    // A second peer pushes the listener past its cap of one.
    auto second = dial(kPort);
    check(second.has_value(), "cap: second client connected");
    if (second) {
        auto hello2 = wire::encode(wire::make_connect(
            kPeerName + 1, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
        if (hello2) { (void)send_all(*second, *hello2); }

        // The idle first connection is pruned: its client sees the socket close.
        first->with_receive_timeout(std::chrono::milliseconds{2000});
        std::array<std::byte, 64> scratch{};
        auto pruned = first->receive(std::span<std::byte>{scratch.data(), scratch.size()});
        check(!pruned && pruned.error() == SocketError::ConnectionClosed,
              "cap: the idle first connection was pruned");

        // The peer reconnects with the id we assigned; it keeps it (same conversation).
        if (local_id.has_value()) {
            auto returning = dial(kPort);
            check(returning.has_value(), "cap: the pruned peer reconnects");
            if (returning) {
                auto again = wire::encode(wire::make_connect(
                    kPeerName, *local_id, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
                if (again) { (void)send_all(*returning, *again); }
                check(wait_for_peer(store_queue, *local_id, std::chrono::milliseconds{1000}),
                      "cap: the pruned peer re-adopts its assigned id");
                (void)returning->shutdown(TcpConnection::Shutdown::Both);
            }
        }
        (void)second->shutdown(TcpConnection::Shutdown::Both);
    }

    listener.stop();
    listener_thread.join();
}

// A reader that already finished must free its slot even before it is reaped: the
// next accept must not be treated as over-cap, which would prune the LIVE reader and
// drop the connection straight back. This is the post-reconnect churn: the peer's
// previous reader had exited, so the fresh connection must not be pruned.
void test_finished_reader_frees_slot() {
    constexpr unsigned kPort = 9972;
    constexpr std::size_t kCap = 1;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kCap};
    std::thread listener_thread{std::ref(listener)};

    // First peer connects, then leaves: its reader FINISHES (reaping only happens on
    // the next accept, so its handle is still in the listener's list).
    auto first = dial(kPort);
    check(first.has_value(), "freed: first client connected");
    if (!first) {
        listener.stop();
        listener_thread.join();
        return;
    }
    auto hello = wire::encode(wire::make_connect(
        11, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    if (hello) { (void)send_all(*first, *hello); }
    const auto id_a = wait_first_peer(store_queue, std::chrono::milliseconds{1000});
    check(id_a.has_value(), "freed: first peer got an id");

    (void)first->shutdown(TcpConnection::Shutdown::Both);
    first.reset();
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{500};
        bool seen = false;
        while (!seen && std::chrono::steady_clock::now() < deadline) {
            while (auto event = store_queue.try_pop()) {
                const auto* gone = std::get_if<EventPeerDisconnected>(&*event);
                if (gone != nullptr && id_a.has_value() && gone->peer_id == *id_a) { seen = true; }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
        check(seen, "freed: the first reader reported its disconnect");
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{100}); // let the reader finish

    // Second peer connects: the finished reader's slot is already free, so this must
    // NOT prune the second (live) reader.
    auto second = dial(kPort);
    check(second.has_value(), "freed: second client connected");
    if (second) {
        auto hello2 = wire::encode(wire::make_connect(
            12, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
        if (hello2) { (void)send_all(*second, *hello2); }

        uint64_t id_b = 0;
        bool pruned = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{500};
        while (std::chrono::steady_clock::now() < deadline) {
            while (auto event = store_queue.try_pop()) {
                if (const auto* connected = std::get_if<EventPeerConnected>(&*event)) {
                    id_b = connected->peer_id;
                } else if (const auto* gone = std::get_if<EventPeerDisconnected>(&*event)) {
                    if (id_b != 0 && gone->peer_id == id_b) { pruned = true; }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
        check(id_b != 0, "freed: the second peer got an id");
        check(!pruned, "freed: the finished reader's slot freed the cap");
        if (id_b != 0) {
            const auto entry = peers.lookup(id_b);
            check(entry && entry->status == p2p::PeerTable::Status::Connected,
                  "freed: the second peer stays Connected");
        }
        (void)second->shutdown(TcpConnection::Shutdown::Both);
    }

    listener.stop();
    listener_thread.join();
}

// A cap prune is a real disconnect: the pruned peer is marked Disconnected and is
// reported, so the peer re-handshakes and is then reported reconnected. The
// regression it guards is the *extra* spurious disconnect a later send used to
// cause by reusing the socket the previous session left behind.
void test_cap_prune_reports_disconnect() {
    constexpr unsigned kPort = 9974;
    constexpr uint64_t kPeerName = 7;
    constexpr std::size_t kCap = 1;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kCap};
    std::thread listener_thread{std::ref(listener)};

    auto first = dial(kPort);
    check(first.has_value(), "prune: first client connected");
    if (!first) {
        listener.stop();
        listener_thread.join();
        return;
    }
    auto hello = wire::encode(wire::make_connect(
        kPeerName, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    if (hello) { (void)send_all(*first, *hello); }

    const auto local_id = wait_first_peer(store_queue, std::chrono::milliseconds{1000});
    check(local_id.has_value(), "prune: first peer got a local id");

    // A second peer pushes past the cap, so the first connection is pruned.
    auto second = dial(kPort);
    check(second.has_value(), "prune: second client connected");
    if (second) {
        auto hello2 = wire::encode(wire::make_connect(
            kPeerName + 1, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
        if (hello2) { (void)send_all(*second, *hello2); }
        check(wait_first_peer(store_queue, std::chrono::milliseconds{1000}).has_value(),
              "prune: second peer got a local id");

        // The cap shuts the first socket down and the pruned peer is reported gone.
        first->with_receive_timeout(std::chrono::milliseconds{2000});
        std::array<std::byte, 64> scratch{};
        auto pruned = first->receive(std::span<std::byte>{scratch.data(), scratch.size()});
        check(!pruned && pruned.error() == SocketError::ConnectionClosed,
              "prune: the first connection was pruned");

        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{1000};
        bool announced = false;
        while (!announced && std::chrono::steady_clock::now() < deadline) {
            while (auto event = store_queue.try_pop()) {
                const auto* gone = std::get_if<EventPeerDisconnected>(&*event);
                if (gone != nullptr && local_id.has_value() && gone->peer_id == *local_id) {
                    announced = true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds{10});
        }
        check(announced, "prune: the cap prune reports the disconnect");
        if (local_id.has_value()) {
            const auto entry = peers.lookup(*local_id);
            check(entry && entry->status == p2p::PeerTable::Status::Disconnected,
                  "prune: the pruned peer is marked Disconnected");
        }
        (void)second->shutdown(TcpConnection::Shutdown::Both);
    }

    (void)first->shutdown(TcpConnection::Shutdown::Both);
    listener.stop();
    listener_thread.join();
}

// A reader whose inbound connection is superseded by a reconnect (a newer
// connection adopts the same peer id) must not announce a disconnect when it
// closes: the peer is still live on the newer connection. Without this, the store
// forwards the stale disconnect to the sender, which tears the fresh write socket
// down and makes the peer reconnect again -- the "reconnected then immediately
// disconnected" storm.
void test_superseded_reader_stays_silent() {
    constexpr unsigned kPort = 9975;
    constexpr uint64_t kFirstName = 71;
    constexpr uint64_t kReturnName = 72;

    ThreadQueue<Event> store_queue;
    p2p::PeerTable peers;
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers, kUncappedReaders};
    std::thread listener_thread{std::ref(listener)};

    // First contact: mint our local name for the peer and keep the connection OPEN.
    auto first = dial(kPort);
    check(first.has_value(), "supersede: first client connected");
    if (!first) {
        listener.stop();
        listener_thread.join();
        return;
    }
    auto hello = wire::encode(wire::make_connect(
        kFirstName, 0, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
    if (hello) { check(send_all(*first, *hello), "supersede: first hello sent"); }

    uint64_t local_id = 0;
    if (auto event = store_queue.pop()) {
        if (const auto* connected = std::get_if<EventPeerConnected>(&*event)) {
            local_id = connected->peer_id;
        }
    }
    check(local_id != 0, "supersede: the peer got a local name");

    // A second connection re-presents that name; its reader takes ownership while
    // the first connection is still open (the worst-case ordering).
    auto second = dial(kPort);
    check(second.has_value(), "supersede: second client connected");
    if (second && local_id != 0) {
        auto again = wire::encode(wire::make_connect(
            kReturnName, local_id, std::vector<wire::Endpoint>{{"127.0.0.1", kPort}}));
        if (again) { check(send_all(*second, *again), "supersede: returning hello sent"); }
        check(wait_for_peer(store_queue, local_id, std::chrono::seconds{2}),
              "supersede: the returning peer is reported on the adopted id");
    }

    // The first connection now closes. Its reader was superseded, so it must stay
    // silent and leave the peer connected.
    if (first) {
        (void)first->shutdown(TcpConnection::Shutdown::Both);
        first.reset();
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{400};
    bool spurious = false;
    while (!spurious && std::chrono::steady_clock::now() < deadline) {
        while (auto event = store_queue.try_pop()) {
            const auto* gone = std::get_if<EventPeerDisconnected>(&*event);
            if (gone != nullptr && gone->peer_id == local_id) { spurious = true; }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds{10});
    }
    check(!spurious, "supersede: the superseded reader stays silent");
    const auto entry = peers.lookup(local_id);
    check(entry && entry->status == p2p::PeerTable::Status::Connected,
          "supersede: the peer is still connected");

    if (second) {
        (void)second->shutdown(TcpConnection::Shutdown::Both);
        second.reset();
    }
    listener.stop();
    listener_thread.join();
}

} // namespace

int main() {
    test_peer_table();
    test_peer_table_connection_owner();
    test_watchdog_stops_stalled_reader();
    test_watchdog_prunes_oldest_reader();
    test_listener_roundtrip();
    test_listener_reuses_assigned_id();
    test_listener_prunes_over_cap();
    test_finished_reader_frees_slot();
    test_cap_prune_reports_disconnect();
    test_superseded_reader_stays_silent();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
