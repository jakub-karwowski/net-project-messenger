// test_io_worker.cpp - the IO worker on its own thread, over loopback sockets.
//
// The test plays the Router (it owns the pool, pushes PtCommands and calls
// pool.interrupt()) and the remote peer (a plain TcpConnection that writes and
// reads encoded frames). It checks what the worker reports to the Router, the
// Store and the Balancer, and what reaches the peer.
//
// Plain assertions, no framework. Every wait is bounded.

#include "IoWorker.hpp"
#include "Worker.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <variant>
#include <vector>

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;
using msg::LinkDownReason;

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

// ------------------------------------------------------------------ the remote peer

struct Listening {
    TcpListener listener{"127.0.0.1", 0, 16};
    wire::Endpoint endpoint() const { return {"127.0.0.1", listener.local_port().value_or(0)}; }
};

// A port nothing listens on: bound, then released.
wire::Endpoint refused_endpoint() {
    Listening l;
    return l.endpoint();
}

// A connected pair: `worker_side` goes to the IO worker, `peer` is the test's end.
struct Pair {
    TcpConnection worker_side;
    TcpConnection peer;
};

Pair make_pair() {
    Listening l;
    auto peer = TcpConnection::connect("127.0.0.1", l.endpoint().port);
    auto accepted = l.listener.accept_client();
    if (!peer || !accepted) {
        std::cout << "FAIL: no loopback pair\n";
        std::exit(1);
    }
    peer->with_receive_timeout(50ms);
    return {std::move(*accepted), std::move(*peer)};
}

msg::Bytes frame_bytes(const wire::Message& m) {
    auto bytes = wire::encode(m);
    return bytes ? std::move(*bytes) : msg::Bytes{};
}

bool write_bytes(TcpConnection& c, std::span<const std::byte> bytes) {
    return c.send(bytes).has_value();
}

bool write_frame(TcpConnection& c, const wire::Message& m) {
    return write_bytes(c, frame_bytes(m));
}

// The next frame from `c`, or nullopt after `limit`.
std::optional<wire::Message> read_frame(TcpConnection& c, wire::Decoder& dec, std::chrono::milliseconds limit = 2s) {
    const auto until = Clock::now() + limit;
    std::array<std::byte, 4096> buf{};
    while (Clock::now() < until) {
        if (auto next = dec.next(); auto* m = std::get_if<wire::Message>(&next)) { return std::move(*m); }
        const auto n = c.receive(buf);
        if (n) { dec.feed(std::span<const std::byte>(buf).first(*n)); continue; }
        if (n.error() != SocketError::WouldBlock && n.error() != SocketError::Interrupted) { return std::nullopt; }
    }
    return std::nullopt;
}

// True once `c` sees the connection end (EOF or reset) within `limit`.
bool sees_close(TcpConnection& c, std::chrono::milliseconds limit = 2s) {
    const auto until = Clock::now() + limit;
    std::array<std::byte, 4096> buf{};
    while (Clock::now() < until) {
        const auto n = c.receive(buf);
        if (n) { continue; }   // data still in flight: skip it
        if (n.error() != SocketError::WouldBlock && n.error() != SocketError::Interrupted) { return true; }
    }
    return false;
}

// ------------------------------------------------------------------ the worker under test

template <class V>
std::optional<V> next_of(ThreadQueue<V>& q, std::chrono::milliseconds limit) {
    const auto until = Clock::now() + limit;
    while (Clock::now() < until) {
        if (auto m = q.try_pop()) { return m; }
        std::this_thread::sleep_for(1ms);
    }
    return std::nullopt;
}

struct Harness {
    IoConfig config;
    EpollConnectionPool pool;
    ThreadQueue<msg::PtCommand> commands;
    ThreadQueue<msg::RouterMsg> router;
    ThreadQueue<msg::StoreMsg> store;
    ThreadQueue<msg::BalancerMsg> balancer;
    std::optional<IoWorker> io;
    std::unique_ptr<WorkerThread> thread;   // last: joined before the rest goes away

    explicit Harness(IoConfig c = {}) : config(c) {
        io.emplace(config, pool, commands, router, store, balancer, Logger{});
        thread = std::make_unique<WorkerThread>("pt-test", [this] { io->run(); }, [this] { pool.stop(); });
    }

    // What the Router does: queue the command, then wake the worker.
    void command(msg::PtCommand c) {
        std::ignore = commands.push(std::move(c));
        pool.interrupt();
    }

    template <class T>
    std::optional<T> to_router(std::chrono::milliseconds limit = 2s) {
        auto m = next_of(router, limit);
        if (!m) { return std::nullopt; }
        if (auto* t = std::get_if<T>(&*m)) { return std::move(*t); }
        std::cout << "  (router got another message, index " << m->index() << ")\n";
        return std::nullopt;
    }

    std::optional<msg::ChatIn> to_store(std::chrono::milliseconds limit = 2s) {
        auto m = next_of(store, limit);
        if (!m) { return std::nullopt; }
        if (auto* t = std::get_if<msg::ChatIn>(&*m)) { return std::move(*t); }
        return std::nullopt;
    }

    // Adopts `conn` as inbound link `link` and completes the handshake for
    // `peer` (`dec` keeps the remote's read state). Returns the pool
    // connection id, which the Router would use for pool.send().
    std::optional<std::uint64_t> establish_inbound(LinkId link, PeerId peer, TcpConnection conn,
                                                   TcpConnection& remote, wire::Decoder& dec) {
        command(msg::Adopt{link, kNoId, msg::Role::Inbound, std::move(conn), {}, {}, msg::HandshakeState::AwaitConnect});
        auto adopted = to_router<msg::LinkAdopted>();
        if (!adopted) { return std::nullopt; }
        if (!write_frame(remote, wire::ConnectInfo{55, kNoId, {}})) { return std::nullopt; }
        if (!to_router<msg::LinkHello>()) { return std::nullopt; }
        command(msg::Bind{link, peer, wire::ConnectInfoAck{peer, 55}});
        auto ack = read_frame(remote, dec);
        if (!ack || !std::holds_alternative<wire::ConnectInfoAck>(*ack)) { return std::nullopt; }
        return adopted->pool_conn_id;
    }

    // The Router's side of sending: queue data until the TX buffer is full.
    bool fill_tx(std::uint64_t id) {
        const std::vector<std::byte> chunk(64 * 1024, std::byte{'x'});
        for (int i = 0; i < 2000; ++i) {
            const auto r = pool.send(id, chunk);
            if (!r && r.error() == SocketError::BufferFull) { return true; }
        }
        return false;
    }
};

// ------------------------------------------------------------------ handshake

void test_inbound_handshake_and_chat() {
    Harness h;
    auto [conn, remote] = make_pair();
    h.command(msg::Adopt{1, kNoId, msg::Role::Inbound, std::move(conn), {}, {}, msg::HandshakeState::AwaitConnect});
    auto adopted = h.to_router<msg::LinkAdopted>();
    check(adopted && adopted->link == 1 && adopted->pt == h.config.pt && adopted->pool_conn_id != 0,
          "adopt: LinkAdopted with the pool connection id");

    check(write_frame(remote, wire::ConnectInfo{55, 0, {wire::Endpoint{"10.0.0.9", 7000}}}), "peer sends hello");
    auto hello = h.to_router<msg::LinkHello>();
    check(hello && hello->link == 1 && hello->info.peer_id == 55 && hello->info.endpoints.size() == 1,
          "hello: LinkHello to the Router with the decoded ConnectInfo");

    h.command(msg::Bind{1, 9, wire::ConnectInfoAck{9, 55}});
    wire::Decoder dec;
    auto ack = read_frame(remote, dec);
    auto* a = ack ? std::get_if<wire::ConnectInfoAck>(&*ack) : nullptr;
    check(a && a->peer_id == 9 && a->ack_peer_id == 55, "bind: the worker encodes and sends the Ack");

    check(write_frame(remote, wire::ChatMessage{0, 9, "hi there"}), "peer sends chat");
    auto chat = h.to_store();
    check(chat && chat->peer == 9 && chat->link == 1 && chat->msg.body == "hi there",
          "established: ChatIn straight to the Store, with the bound peer");
    check(!next_of(h.router, 50ms), "chat does not go through the Router");
}

void test_outbound_dial_and_chat() {
    Harness h;
    Listening l;
    h.command(msg::Dial{2, 7, {l.endpoint()}, wire::ConnectInfo{7, 0, {}}});
    auto accepted = l.listener.accept_client();
    check(accepted.has_value(), "the worker dials the endpoint");
    if (!accepted) { return; }
    accepted->with_receive_timeout(50ms);

    wire::Decoder dec;
    auto hello = read_frame(*accepted, dec);
    auto* info = hello ? std::get_if<wire::ConnectInfo>(&*hello) : nullptr;
    check(info && info->peer_id == 7 && info->assigned_peer_id == 0, "connected: the worker encodes and sends the hello");

    check(write_frame(*accepted, wire::ConnectInfoAck{70, 7}), "peer acks");
    auto acked = h.to_router<msg::LinkAcked>();
    check(acked && acked->link == 2 && acked->ack.peer_id == 70 && acked->remote == l.endpoint() && acked->pool_conn_id != 0,
          "ack: LinkAcked with the Ack, the endpoint that answered and the pool connection");
    check(acked && h.pool.send(acked->pool_conn_id, frame_bytes(wire::ChatMessage{0, 70, "from the router"})).has_value(),
          "the Router can send on that pool connection");
    auto routed = read_frame(*accepted, dec);
    check(routed && std::holds_alternative<wire::ChatMessage>(*routed), "the peer receives it");

    check(write_frame(*accepted, wire::ChatMessage{0, 7, "yo"}), "peer sends chat");
    auto chat = h.to_store();
    check(chat && chat->peer == 7 && chat->link == 2 && chat->msg.body == "yo", "outbound established: ChatIn to the Store");
}

void test_dial_tries_endpoints_in_order() {
    Harness h;
    Listening l;
    const auto refused = refused_endpoint();
    h.command(msg::Dial{3, 7, {wire::Endpoint{"not-an-address", 1}, refused, l.endpoint()}, wire::ConnectInfo{7, 0, {}}});
    auto accepted = l.listener.accept_client();
    check(accepted.has_value(), "after an invalid and a refused endpoint, the third is dialled");
    if (!accepted) { return; }
    accepted->with_receive_timeout(50ms);
    wire::Decoder dec;
    check(read_frame(*accepted, dec).has_value(), "hello arrives on the third endpoint");
    check(write_frame(*accepted, wire::ConnectInfoAck{70, 7}), "peer acks");
    auto acked = h.to_router<msg::LinkAcked>();
    check(acked && acked->link == 3 && acked->remote == l.endpoint(),
          "one link id across attempts; no LinkDown for the failed ones; remote is the one that answered");
}

void test_dial_all_refused() {
    Harness h;
    h.command(msg::Dial{4, 7, {refused_endpoint(), refused_endpoint()}, wire::ConnectInfo{7, 0, {}}});
    auto down = h.to_router<msg::LinkDown>();
    check(down && down->link == 4 && down->why == LinkDownReason::SocketError && down->err == SocketError::ConnectionRefused,
          "every endpoint refused: one LinkDown with the last error");
    check(!next_of(h.router, 100ms), "and nothing else");

    h.command(msg::Dial{5, 7, {}, wire::ConnectInfo{7, 0, {}}});
    down = h.to_router<msg::LinkDown>();
    check(down && down->link == 5 && down->err == SocketError::InvalidAddress, "no endpoint at all: LinkDown at once");
}

void test_handshake_timeout() {
    IoConfig c;
    c.handshake_timeout = 150ms;
    Harness h(c);
    auto [conn, remote] = make_pair();
    h.command(msg::Adopt{6, kNoId, msg::Role::Inbound, std::move(conn), {}, {}, msg::HandshakeState::AwaitConnect});
    std::ignore = h.to_router<msg::LinkAdopted>();
    const auto start = Clock::now();
    auto down = h.to_router<msg::LinkDown>(2s);
    check(down && down->link == 6 && down->why == LinkDownReason::HandshakeFailed && down->err == SocketError::ReadTimedOut,
          "a silent peer: HandshakeFailed after the handshake timeout");
    check(Clock::now() - start >= 100ms, "not before the timeout");
}

// ------------------------------------------------------------------ protocol errors

void test_chat_before_handshake() {
    Harness h;
    auto [conn, remote] = make_pair();
    h.command(msg::Adopt{7, kNoId, msg::Role::Inbound, std::move(conn), {}, {}, msg::HandshakeState::AwaitConnect});
    std::ignore = h.to_router<msg::LinkAdopted>();
    check(write_frame(remote, wire::ChatMessage{0, 1, "too early"}), "peer sends chat first");
    auto down = h.to_router<msg::LinkDown>();
    check(down && down->link == 7 && down->why == LinkDownReason::ProtocolError, "chat before hello: ProtocolError");
    check(sees_close(remote), "the peer is disconnected");
    check(!next_of(h.store, 50ms), "nothing reaches the Store");

    // Chat after the hello but before the Router's Bind.
    auto [conn2, remote2] = make_pair();
    h.command(msg::Adopt{8, kNoId, msg::Role::Inbound, std::move(conn2), {}, {}, msg::HandshakeState::AwaitConnect});
    std::ignore = h.to_router<msg::LinkAdopted>();
    check(write_frame(remote2, wire::ConnectInfo{55, 0, {}}) && write_frame(remote2, wire::ChatMessage{0, 1, "early"}),
          "peer sends hello and chat at once");
    std::ignore = h.to_router<msg::LinkHello>();
    down = h.to_router<msg::LinkDown>();
    check(down && down->link == 8 && down->why == LinkDownReason::ProtocolError, "chat before Bind: ProtocolError");

    // A Bind for that link afterwards is harmless.
    h.command(msg::Bind{8, 9, wire::ConnectInfoAck{9, 55}});
    check(!next_of(h.router, 100ms), "a Bind for a link already down is ignored");
}

void test_bad_and_unknown_frames() {
    Harness h;
    auto [conn, remote] = make_pair();
    h.command(msg::Adopt{9, kNoId, msg::Role::Inbound, std::move(conn), {}, {}, msg::HandshakeState::AwaitConnect});
    std::ignore = h.to_router<msg::LinkAdopted>();

    // Frames of an unknown type are skipped without counting against the link.
    auto unknown = frame_bytes(wire::ConnectInfoAck{1, 2});
    unknown[3] = std::byte{99};   // the type byte
    msg::Bytes burst;
    for (int i = 0; i < 10; ++i) { burst.insert(burst.end(), unknown.begin(), unknown.end()); }
    check(write_bytes(remote, burst), "peer sends 10 frames of an unknown type");
    check(write_frame(remote, wire::ConnectInfo{55, 0, {}}), "then a hello");
    auto hello = h.to_router<msg::LinkHello>();
    check(hello && hello->link == 9, "unknown types are skipped; the hello still works");

    // Malformed frames are tolerated up to the limit, then the link is dropped.
    auto bad_version = frame_bytes(wire::ConnectInfoAck{1, 2});
    bad_version[2] = std::byte{200};   // the version byte
    burst.clear();
    for (int i = 0; i < h.config.max_bad_frames + 1; ++i) { burst.insert(burst.end(), bad_version.begin(), bad_version.end()); }
    check(write_bytes(remote, burst), "peer sends one malformed frame more than tolerated");
    auto down = h.to_router<msg::LinkDown>();
    check(down && down->link == 9 && down->why == LinkDownReason::ProtocolError, "too many malformed frames: ProtocolError");
}

// ------------------------------------------------------------------ moving links

void test_release_and_adopt_keep_half_a_frame() {
    Harness a;
    IoConfig cb;
    cb.pt = 1;
    Harness b(cb);
    auto [conn, remote] = make_pair();
    wire::Decoder remote_dec;
    check(a.establish_inbound(10, 9, std::move(conn), remote, remote_dec).has_value(), "link established on worker A");

    const auto frame = frame_bytes(wire::ChatMessage{0, 9, "a message split across the move"});
    const auto half = frame.size() / 2;
    check(write_bytes(remote, std::span(frame).first(half)), "peer sends the first half");
    std::this_thread::sleep_for(50ms);   // let A read it into its decoder

    a.command(msg::Release{10});
    auto released = a.to_router<msg::LinkReleased>();
    check(released && released->link == 10, "release: LinkReleased with the connection and the decoder");
    if (!released) { return; }

    b.command(msg::Adopt{10, 9, msg::Role::Inbound, std::move(released->rc.connection), std::move(released->rc.unsent_tx),
                         std::move(released->dec), msg::HandshakeState::Established});
    auto adopted = b.to_router<msg::LinkAdopted>();
    check(adopted && adopted->link == 10 && adopted->pt == 1, "adopt on worker B");

    check(write_bytes(remote, std::span(frame).subspan(half)), "peer sends the second half");
    auto chat = b.to_store();
    check(chat && chat->link == 10 && chat->peer == 9 && chat->msg.body == "a message split across the move",
          "the message arrives whole on B");
    check(!next_of(a.store, 50ms), "and not on A");
    a.command(msg::Probe{1});
    auto m = next_of(a.balancer, 2s);
    auto* report = m ? std::get_if<msg::PtReport>(&*m) : nullptr;
    check(report && report->links.size() == 1 && report->links[0].link == 10 && report->links[0].gone
              && report->links[0].rx_bytes > 0,
          "A's next report has the moved link's final counters, marked gone");
    check(a.io->link_count() == 0 && b.io->link_count() == 1, "A no longer holds the link, B does");

    a.command(msg::Release{10});
    auto down = a.to_router<msg::LinkDown>();
    check(down && down->link == 10 && down->err == SocketError::ConnectionNotFound, "release of a link A no longer has: LinkDown");
}

// ------------------------------------------------------------------ reports, close, stop

void test_probe_reports_counters() {
    Harness h;
    auto [conn, remote] = make_pair();
    wire::Decoder dec;
    check(h.establish_inbound(11, 9, std::move(conn), remote, dec).has_value(), "link established");
    check(write_frame(remote, wire::ChatMessage{0, 9, "counted"}), "peer sends chat");
    std::ignore = h.to_store();

    h.command(msg::Probe{42});
    auto m = next_of(h.balancer, 2s);
    auto* report = m ? std::get_if<msg::PtReport>(&*m) : nullptr;
    check(report && report->pt == h.config.pt && report->seq == 42 && report->links.size() == 1,
          "probe: PtReport with the sequence number and one link");
    if (report && !report->links.empty()) {
        const auto& s = report->links[0];
        check(s.link == 11 && s.peer == 9 && s.rx_bytes > 0 && s.rx_frames == 2 && s.tx_frames == 1 && s.tx_bytes > 0,
              "the sample counts the hello and the chat in, the Ack out");
    }
    h.command(msg::Probe{43});
    m = next_of(h.balancer, 2s);
    report = m ? std::get_if<msg::PtReport>(&*m) : nullptr;
    check(report && report->links.size() == 1 && report->links[0].rx_bytes == 0 && report->links[0].rx_frames == 0,
          "counters restart after each report");
}

void test_close_and_peer_close() {
    Harness h;
    auto [conn, remote] = make_pair();
    wire::Decoder dec;
    check(h.establish_inbound(12, 9, std::move(conn), remote, dec).has_value(), "link established");
    h.command(msg::Close{12, true, LinkDownReason::BalancerEvicted});
    auto down = h.to_router<msg::LinkDown>();
    check(down && down->link == 12 && down->why == LinkDownReason::BalancerEvicted, "close: LinkDown with the given reason");
    check(sees_close(remote), "the peer is disconnected");
    h.command(msg::Close{12, true, LinkDownReason::BalancerEvicted});
    check(!next_of(h.router, 100ms), "closing it again: nothing");

    auto [conn2, remote2] = make_pair();
    wire::Decoder dec2;
    check(h.establish_inbound(13, 9, std::move(conn2), remote2, dec2).has_value(), "second link established");
    std::ignore = remote2.shutdown(TcpConnection::Shutdown::Both);
    down = h.to_router<msg::LinkDown>();
    check(down && down->link == 13 && down->why == LinkDownReason::PeerClosed, "peer closes: PeerClosed");
}

void test_stop_without_links() {
    Harness h;
    h.command(msg::Stop{Clock::now() + 2s});
    auto stopped = h.to_router<msg::PtStopped>(1s);
    check(stopped && stopped->pt == h.config.pt, "stop with nothing to flush: PtStopped at once");
    h.thread->join();
    check(h.thread->finished(), "the thread returned");
}

void test_stop_flushes_then_stops() {
    Harness h;
    auto [conn, remote] = make_pair();
    wire::Decoder dec;
    const auto id = h.establish_inbound(14, 9, std::move(conn), remote, dec);
    check(id.has_value(), "link established");
    check(id && h.fill_tx(*id), "the TX buffer is full while the peer is not reading");

    // The peer starts reading: everything queued must still arrive before PtStopped.
    std::atomic<std::size_t> received{0};
    std::thread reader([&] {
        remote.with_receive_timeout(200ms);
        std::array<std::byte, 64 * 1024> sink{};
        for (;;) {
            const auto n = remote.receive(sink);
            if (n) { received += *n; continue; }
            if (n.error() != SocketError::WouldBlock) { return; }
        }
    });
    const auto start = Clock::now();
    h.command(msg::Stop{start + 5s});
    auto down = h.to_router<msg::LinkDown>();
    check(down && down->link == 14 && down->why == LinkDownReason::Shutdown, "stop: LinkDown{Shutdown} for the link");
    auto stopped = h.to_router<msg::PtStopped>(6s);
    const auto took = Clock::now() - start;
    reader.join();
    check(stopped.has_value(), "PtStopped once flushed");
    check(took < 4s, "it does not wait for the deadline once everything is flushed");
    check(received > 1024 * 1024 / 2, "the peer received the queued data");
}

void test_stop_deadline() {
    Harness h;
    auto [conn, remote] = make_pair();
    wire::Decoder dec;
    const auto id = h.establish_inbound(15, 9, std::move(conn), remote, dec);
    check(id.has_value(), "link established");
    check(id && h.fill_tx(*id), "the TX buffer is full and the peer never reads");

    const auto start = Clock::now();
    h.command(msg::Stop{start + 300ms});
    auto stopped = std::optional<msg::PtStopped>{};
    for (int i = 0; i < 3 && !stopped; ++i) {
        auto m = next_of(h.router, 3s);
        if (m && std::holds_alternative<msg::PtStopped>(*m)) { stopped = std::get<msg::PtStopped>(*m); }
    }
    const auto took = Clock::now() - start;
    check(stopped.has_value(), "PtStopped although the data could not be flushed");
    check(took >= 250ms && took < 2s, "at the deadline, not before and not much after");
}

void test_pool_stop_ends_the_worker() {
    Harness h;
    auto [conn, remote] = make_pair();
    wire::Decoder dec;
    check(h.establish_inbound(16, 9, std::move(conn), remote, dec).has_value(), "link established");
    h.thread->request_stop();   // what the Supervisor's emergency does: pool.stop()
    auto stopped = h.to_router<msg::PtStopped>(1s);
    h.thread->join();
    check(stopped.has_value() && h.thread->finished(), "pool.stop(): PtStopped and the thread returns");
}

}  // namespace

int main() {
    std::cout << "test_io_worker\n";
    test_inbound_handshake_and_chat();
    test_outbound_dial_and_chat();
    test_dial_tries_endpoints_in_order();
    test_dial_all_refused();
    test_handshake_timeout();
    test_chat_before_handshake();
    test_bad_and_unknown_frames();
    test_release_and_adopt_keep_half_a_frame();
    test_probe_reports_counters();
    test_close_and_peer_close();
    test_stop_without_links();
    test_stop_flushes_then_stops();
    test_stop_deadline();
    test_pool_stop_ends_the_worker();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
