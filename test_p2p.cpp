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

    auto frozen = std::make_shared<p2p::ReaderTick>(0); // never advances
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
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers};

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
    p2p::ListenerWorker listener{"127.0.0.1", kPort, store_queue, peers};

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

} // namespace

int main() {
    test_peer_table();
    test_watchdog_stops_stalled_reader();
    test_listener_roundtrip();
    test_listener_reuses_assigned_id();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
