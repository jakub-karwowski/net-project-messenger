// test_sender.cpp - harness for p2p::SenderWorker.
//
// Plain assertions (no framework): every failed check is printed and the process
// exits non-zero if anything failed. Covers:
//   * a successful send: the sender names the peer with a ConnectInfo handshake,
//     then delivers a framed ChatMessage, reusing the same cached connection for
//     later messages
//   * a peer-connection report: the sender dials the advertised endpoint and
//     answers with a ConnectInfoAck carrying both local names
//   * a render-initiated dial: the sender opens the connection and reports the
//     outcome (EventPeerDialed / EventPeerDisconnected) to the store; a message
//     after a successful dial reuses that connection (the peer is now known by
//     address) instead of being reported unreachable
//   * an unreachable peer: retries are exhausted, EventPeerDisconnected is
//     reported on the render queue and the peer is marked Disconnected (not
//     erased from the table)
//   * an unknown peer id: reported immediately, without dialing
//   * a disconnect dropping the cached write connection, so a returning peer is
//     dialled afresh instead of through a dead socket

#include "ThreadWorkers.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <functional>
#include <iostream>
#include <optional>
#include <span>
#include <string>
#include <thread>
#include <utility>

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

// Reads from `connection` until one complete frame decodes or `timeout` passes,
// reusing `decoder` so a message coalesced with a previous frame is not lost.
// A bounded receive timeout (set by the caller) is treated as "nothing yet".
std::optional<wire::Message> receive_message(TcpConnection& connection,
                                             wire::Decoder& decoder,
                                             std::chrono::milliseconds timeout) {
    std::array<std::byte, 4096> scratch{};
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (;;) {
        auto item = decoder.next();
        if (!item) { return std::nullopt; }  // malformed frame
        if (*item) { return std::move(**item); }
        if (std::chrono::steady_clock::now() >= deadline) { return std::nullopt; }

        auto received = connection.receive(std::span<std::byte>{scratch.data(), scratch.size()});
        if (!received) {
            if (received.error() == SocketError::WouldBlock) { continue; }
            return std::nullopt;
        }
        decoder.feed(std::span<const std::byte>{scratch.data(), *received});
    }
}

// ---------------------------------------------------------------------------

void test_delivers_and_reuses_connection() {
    constexpr unsigned kPort = 9992;
    constexpr uint64_t kPeerId = 7;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});

    p2p::SenderWorker sender{store_queue, sender_queue, peers,
                             LocalEndpoints{{{"127.0.0.1", kPort}}}};
    std::thread worker{std::ref(sender)};

    // The sender must dial only when a message needs to go out.
    check(sender_queue.emplace(EventMessage{kPeerId, kPeerId, "first"}),
          "deliver: message queued");

    auto server = listener.accept_client();
    check(server.has_value(), "deliver: sender dialled the peer");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;

    // The sender names the peer (what ConnectInfo carries) before any message.
    auto hello = receive_message(*server, decoder, std::chrono::seconds{2});
    check(hello.has_value(), "deliver: handshake received");
    if (hello) {
        const auto* connect = wire::as_connect(*hello);
        check(connect != nullptr, "deliver: first frame is a ConnectInfo");
        if (connect) {
            check(connect->peer_id == kPeerId, "deliver: handshake names the peer");
            check(connect->assigned_peer_id == 0,
                  "deliver: a peer that never named us gets 0");
            check(connect->endpoints.size() == 1 &&
                      connect->endpoints.front() == wire::Endpoint{"127.0.0.1", kPort},
                  "deliver: handshake carries our endpoint");
        }
    }

    auto first = receive_message(*server, decoder, std::chrono::seconds{2});
    check(first.has_value(), "deliver: first message received");
    if (first) {
        const auto* chat = wire::as_message(*first);
        check(chat != nullptr, "deliver: first frame is a ChatMessage");
        if (chat) {
            check(chat->sender_id == 0, "deliver: chat sender id is unused");
            check(chat->body == "first", "deliver: payload matches");
        }
    }

    // A second message must arrive on the same accepted socket; a fresh dial
    // would stay queued in the listener backlog and the receive would time out.
    check(sender_queue.emplace(EventMessage{kPeerId, kPeerId, "second"}),
          "deliver: second message queued");
    auto second = receive_message(*server, decoder, std::chrono::seconds{2});
    check(second.has_value(), "deliver: second message reuses the cached connection");
    if (second) {
        const auto* chat = wire::as_message(*second);
        check(chat != nullptr && chat->body == "second", "deliver: second payload matches");
    }

    auto delivered_entry = peers.lookup(kPeerId);
    check(delivered_entry.has_value() &&
              delivered_entry->status == p2p::PeerTable::Status::Connected,
          "deliver: peer stays connected");

    sender_queue.graceful_shutdown();
    worker.join();
}

void test_unreachable_peer_is_reported() {
    constexpr unsigned kPort = 9991;
    constexpr uint64_t kPeerId = 8;

    // Claim the port and release it, so the dial below is reliably refused.
    { TcpListener probe("127.0.0.1", kPort, 1); }

    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});

    p2p::SenderWorker sender{store_queue, sender_queue, peers};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventMessage{kPeerId, 1, "undeliverable"}),
          "unreachable: message queued");

    auto event = store_queue.pop(); // blocks until the sender gives up
    check(event.has_value(), "unreachable: a render event was reported");
    if (event) {
        const auto* disconnected = std::get_if<EventPeerDisconnected>(&*event);
        check(disconnected != nullptr, "unreachable: event is EventPeerDisconnected");
        if (disconnected) {
            check(disconnected->conversation_id == 1,
                  "unreachable: conversation id reported");
            check(disconnected->peer_id == kPeerId, "unreachable: peer id matches");
        }
    }

    auto entry = peers.lookup(kPeerId);
    check(entry.has_value() && entry->status == p2p::PeerTable::Status::Disconnected,
          "unreachable: peer marked disconnected, not erased");

    sender_queue.graceful_shutdown();
    worker.join();
}

void test_unknown_peer_is_reported_without_dialing() {
    constexpr uint64_t kPeerId = 99;

    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers; // no address for kPeerId

    p2p::SenderWorker sender{store_queue, sender_queue, peers};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventMessage{kPeerId, 1, "nowhere"}),
          "unknown: message queued");

    auto event = store_queue.pop();
    check(event.has_value(), "unknown: a render event was reported");
    if (event) {
        const auto* disconnected = std::get_if<EventPeerDisconnected>(&*event);
        check(disconnected != nullptr, "unknown: event is EventPeerDisconnected");
        if (disconnected) {
            check(disconnected->conversation_id == 1, "unknown: conversation id reported");
            check(disconnected->peer_id == kPeerId, "unknown: peer id matches");
        }
    }
    check(peers.lookup(kPeerId) == std::nullopt, "unknown: nothing was added to the table");

    sender_queue.graceful_shutdown();
    worker.join();
}

// A ConnectInfo from a peer triggers a dial-back carrying the two local names.
void test_connect_ack_on_peer_connected() {
    constexpr unsigned kPort = 9989;
    constexpr uint64_t kPeerId = 21;
    constexpr uint64_t kAssigned = 200;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers; // the event itself carries the endpoint to dial

    p2p::SenderWorker sender{store_queue, sender_queue, peers};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(
              EventPeerConnected{kPeerId, kAssigned, {{"127.0.0.1", kPort}}}),
          "ack: peer-connected queued");

    auto server = listener.accept_client();
    check(server.has_value(), "ack: sender dialled the peer's endpoint");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;
    auto frame = receive_message(*server, decoder, std::chrono::seconds{2});
    check(frame.has_value(), "ack: handshake received");
    if (frame) {
        const auto* ack = wire::as_connect_ack(*frame);
        check(ack != nullptr, "ack: first frame is a ConnectInfoAck");
        if (ack) {
            check(ack->peer_id == kPeerId, "ack: carries our name for the peer");
            check(ack->ack_peer_id == kAssigned, "ack: echoes the peer's name for us");
        }
    }

    sender_queue.graceful_shutdown();
    worker.join();
}

// A render-initiated dial: name the peer, open the connection, report success.
void test_dial_peer_opens_and_reports() {
    constexpr unsigned kPort = 9988;
    constexpr uint64_t kPeerId = 31;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers; // the dial request carries the address

    p2p::SenderWorker sender{store_queue, sender_queue, peers,
                             LocalEndpoints{{{"127.0.0.1", 9990}}}};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventDialPeer{"127.0.0.1", kPort, kPeerId}),
          "dial: request queued");

    auto server = listener.accept_client();
    check(server.has_value(), "dial: sender dialled the address");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;
    auto frame = receive_message(*server, decoder, std::chrono::seconds{2});
    check(frame.has_value(), "dial: handshake received");
    if (frame) {
        const auto* connect = wire::as_connect(*frame);
        check(connect != nullptr, "dial: first frame is a ConnectInfo");
        if (connect) {
            check(connect->peer_id == kPeerId, "dial: handshake names the peer");
            check(connect->assigned_peer_id == 0,
                  "dial: a never-met peer gets no re-presented id");
            check(connect->endpoints.size() == 1 &&
                      connect->endpoints.front() == wire::Endpoint{"127.0.0.1", 9990},
                  "dial: handshake carries our endpoint");
        }
    }

    auto report = store_queue.pop();
    check(report.has_value(), "dial: outcome reported to the store");
    if (report) {
        const auto* dialed = std::get_if<EventPeerDialed>(&*report);
        check(dialed != nullptr, "dial: report is an EventPeerDialed");
        check(dialed && dialed->conversation_id == 0, "dial: report has no conversation yet");
        check(dialed && dialed->peer_id == kPeerId, "dial: report carries the peer id");
    }

    sender_queue.graceful_shutdown();
    worker.join();
}

// A peer we already handshook with is re-presented the id it gave us, so it can
// recognise us (e.g. after our address changed).
void test_handshake_re_presents_assigned_id() {
    constexpr unsigned kPort = 9985;
    constexpr uint64_t kPeerId = 12;
    constexpr uint64_t kAssignedId = 555;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});
    check(peers.set_assigned_id(kPeerId, kAssignedId),
          "re-present: the remembered name is stored");

    p2p::SenderWorker sender{store_queue, sender_queue, peers,
                             LocalEndpoints{{{"127.0.0.1", kPort}}}};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventMessage{kPeerId, kPeerId, "hello again"}),
          "re-present: message queued");

    auto server = listener.accept_client();
    check(server.has_value(), "re-present: sender dialled the peer");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;
    auto hello = receive_message(*server, decoder, std::chrono::seconds{2});
    check(hello.has_value(), "re-present: handshake received");
    if (hello) {
        const auto* connect = wire::as_connect(*hello);
        check(connect != nullptr, "re-present: first frame is a ConnectInfo");
        if (connect) {
            check(connect->peer_id == kPeerId, "re-present: handshake names the peer");
            check(connect->assigned_peer_id == kAssignedId,
                  "re-present: the id the peer gave us is echoed back");
        }
    }

    sender_queue.graceful_shutdown();
    worker.join();
}

// A disconnect report drops the cached write connection, so a returning peer gets
// a fresh dial-back instead of a stale socket.
void test_disconnect_drops_cached_connection() {
    constexpr unsigned kPort = 9984;
    constexpr uint64_t kPeerId = 21;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;
    peers.insert(kPeerId, {"127.0.0.1", kPort});

    p2p::SenderWorker sender{store_queue, sender_queue, peers,
                             LocalEndpoints{{{"127.0.0.1", kPort}}}};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventMessage{kPeerId, kPeerId, "first"}),
          "drop: message queued");

    auto server = listener.accept_client();
    check(server.has_value(), "drop: sender dialled the peer");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;
    check(receive_message(*server, decoder, std::chrono::seconds{2}).has_value(),
          "drop: handshake received");
    check(receive_message(*server, decoder, std::chrono::seconds{2}).has_value(),
          "drop: message received");

    // Something must be readable once the sender lets go of the socket. Until then
    // every read just times out (WouldBlock).
    check(sender_queue.emplace(EventPeerDisconnected{0, kPeerId}), "drop: disconnect queued");
    std::array<std::byte, 64> scratch{};
    bool released = false;
    for (int i = 0; i < 200 && !released; ++i) {
        auto got = server->receive(std::span<std::byte>{scratch.data(), scratch.size()});
        if (!got) {
            if (got.error() == SocketError::WouldBlock) { continue; }
            released = true; // reset: the socket is gone too
            break;
        }
        released = (*got == 0); // EOF: the sender sent FIN
    }
    check(released, "drop: the cached write connection is released");

    sender_queue.graceful_shutdown();
    worker.join();
}

// A failed dial is reported to the store as a disconnect.
void test_dial_peer_reports_failure() {
    constexpr unsigned kPort = 9987;
    constexpr uint64_t kPeerId = 32;

    { TcpListener probe("127.0.0.1", kPort, 1); } // claim then release the port

    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;

    p2p::SenderWorker sender{store_queue, sender_queue, peers};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventDialPeer{"127.0.0.1", kPort, kPeerId}),
          "dial-fail: request queued");

    auto report = store_queue.pop();
    check(report.has_value(), "dial-fail: outcome reported to the store");
    if (report) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*report);
        check(gone != nullptr, "dial-fail: report is an EventPeerDisconnected");
        check(gone && gone->conversation_id == 0, "dial-fail: no conversation yet");
        check(gone && gone->peer_id == kPeerId, "dial-fail: report carries the peer id");
    }

    sender_queue.graceful_shutdown();
    worker.join();
}

// After a render-initiated dial the peer is known by address, so a later message
// is sent over the cached connection instead of being reported unreachable.
void test_message_after_dial_reuses_connection() {
    constexpr unsigned kPort = 9986;
    constexpr uint64_t kPeerId = 41;

    TcpListener listener("127.0.0.1", kPort, 1);
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    p2p::PeerTable peers;

    p2p::SenderWorker sender{store_queue, sender_queue, peers};
    std::thread worker{std::ref(sender)};

    check(sender_queue.emplace(EventDialPeer{"127.0.0.1", kPort, kPeerId}),
          "dial-msg: request queued");

    auto server = listener.accept_client();
    check(server.has_value(), "dial-msg: sender dialled the peer");
    if (!server) {
        sender_queue.graceful_shutdown();
        worker.join();
        return;
    }
    server->with_receive_timeout(std::chrono::milliseconds{100});

    wire::Decoder decoder;
    auto hello = receive_message(*server, decoder, std::chrono::seconds{2});
    check(hello.has_value() && wire::as_connect(*hello) != nullptr,
          "dial-msg: handshake received");

    auto report = store_queue.pop();
    check(report.has_value(), "dial-msg: dial outcome reported");
    check(report && std::holds_alternative<EventPeerDialed>(*report),
          "dial-msg: dial reported as EventPeerDialed");

    check(peers.lookup(kPeerId).has_value(), "dial-msg: peer is known by address");

    // The message must reuse the cached connection, not be reported unreachable.
    check(sender_queue.emplace(EventMessage{kPeerId, kPeerId, "hi"}), "dial-msg: message queued");
    auto frame = receive_message(*server, decoder, std::chrono::seconds{2});
    check(frame.has_value(), "dial-msg: message delivered after dial");
    if (frame) {
        const auto* chat = wire::as_message(*frame);
        check(chat != nullptr && chat->body == "hi", "dial-msg: payload matches");
    }

    sender_queue.graceful_shutdown();
    worker.join();
}

} // namespace

int main() {
    test_handshake_re_presents_assigned_id();
    test_disconnect_drops_cached_connection();
    test_delivers_and_reuses_connection();
    test_unreachable_peer_is_reported();
    test_unknown_peer_is_reported_without_dialing();
    test_connect_ack_on_peer_connected();
    test_dial_peer_opens_and_reports();
    test_dial_peer_reports_failure();
    test_message_after_dial_reuses_connection();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
