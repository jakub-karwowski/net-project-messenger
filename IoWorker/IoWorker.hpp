#pragma once

// An IO worker (one per pool thread): runs the handshake, decodes inbound
// frames, dials outbound links, and hands connections over when the Router
// moves them.
//
// It owns no connection policy. The Router decides which links it holds and
// tells it through a command queue (PtCommand), waking it with pool.interrupt().
// It reports facts:
//   to the Router    link lifecycle (LinkAdopted, LinkHello, LinkAcked, LinkDown,
//                    LinkReleased, PtStopped);
//   to the Store     decoded chat messages (ChatIn), directly, so inbound traffic
//                    does not queue behind the Router;
//   to the Balancer  a PtReport for every Probe.
//
// Chat is sent by the Router straight through pool.send(); this worker writes
// only the handshake frames it encodes itself (hello, Ack).
//
// Single-threaded: run() is the thread body, and every handler runs on that
// thread. The pool belongs to the caller (the Router), which also calls
// pool.send() from its own thread.

#include "Logger.hpp"
#include "Messages.hpp"
#include "TcpSocket.hpp"
#include "ThreadQueue.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <span>
#include <string_view>
#include <unordered_map>
#include <vector>

struct IoConfig {
    PtId pt = 0;
    // Before a link is established, nothing may stay silent longer than this.
    std::chrono::milliseconds handshake_timeout{5000};
    // Per dial attempt; each endpoint of a Dial gets its own.
    std::chrono::milliseconds connect_timeout{5000};
    // Queued data must reach the kernel within this, in every state.
    std::chrono::milliseconds write_timeout{5000};
    // Malformed frames tolerated on one link before it is dropped. Frames of an
    // unknown type do not count: they are skipped, so newer peers can add types.
    int max_bad_frames = 3;
    std::size_t rx_buffer_size = 64 * 1024;
    // While shutting down, how often to check whether every link has flushed.
    std::chrono::milliseconds shutdown_poll{20};
};

class IoWorker {
public:
    IoWorker(IoConfig config, EpollConnectionPool& pool, ThreadQueue<msg::PtCommand>& commands,
             ThreadQueue<msg::RouterMsg>& router, ThreadQueue<msg::StoreMsg>& store,
             ThreadQueue<msg::BalancerMsg>& balancer, Logger log);

    // The thread body. Returns after Stop has flushed every link (or its deadline
    // passed), or at once when the pool is stopped; either way PtStopped is sent.
    void run();

    // Commands, handled on the worker thread (between receive() calls).
    void on(msg::Adopt& m);
    void on(msg::Dial& m);
    void on(msg::Bind& m);
    void on(msg::Release& m);
    void on(msg::Close& m);
    void on(msg::Probe& m);
    void on(msg::Stop& m);

    std::size_t link_count() const noexcept { return by_conn_.size(); }

private:
    using Pool = EpollConnectionPool;
    using Clock = std::chrono::steady_clock;

    enum class Hs : std::uint8_t {
        Connecting,     // outbound: a dial attempt is in flight
        AwaitConnect,   // inbound: waiting for the peer's ConnectInfo
        AwaitBind,      // inbound: ConnectInfo passed up, waiting for the Router's Bind
        AwaitAck,       // outbound: hello sent, waiting for the ConnectInfoAck
        Established,
    };

    struct LinkState {
        LinkId link = kNoId;
        PeerId peer = kNoId;
        msg::Role role = msg::Role::Inbound;
        Hs hs = Hs::AwaitConnect;
        wire::Decoder dec;
        // outbound only
        wire::ConnectInfo hello;
        std::vector<wire::Endpoint> to_try;   // endpoints not attempted yet
        wire::Endpoint dialing;               // the endpoint being attempted
        // since the last report
        std::uint64_t rx_frames = 0;
        std::uint64_t tx_frames = 0;
        int bad_frames = 0;
    };

    // How close_link() takes a connection out of the pool.
    enum class Removal : std::uint8_t {
        AlreadyGone,   // the pool removed it and reported why
        Drop,          // discard queued data, close now
        Flush,         // send queued data first, then close
    };

    void drain_commands();
    void on_connected(std::uint64_t id);
    void on_bytes(std::uint64_t id, std::span<const std::byte> data);
    void on_connection_error(std::uint64_t id, SocketError err);
    // false when the frame ended the link
    bool handle_frame(std::uint64_t id, LinkState& st, wire::Message frame);

    // Starts the next attempt of an outbound link, or reports it down with
    // `last_error` when no endpoint is left.
    void dial_next(LinkState st, SocketError last_error);
    // false when sending failed (the link has then been closed and reported)
    bool send_frame(std::uint64_t id, LinkState& st, const wire::Message& frame);
    void close_link(std::uint64_t id, Removal how, msg::LinkDownReason why, SocketError err);
    void add(std::uint64_t id, LinkState st);
    std::optional<std::uint64_t> conn_of(LinkId link) const;
    msg::LinkSample sample(const LinkState& st, const Pool::ConnectionStats& stats, bool gone = false) const;
    void finish();

    Pool::Timeouts handshake_timeouts() const noexcept { return {config_.handshake_timeout, config_.write_timeout}; }
    Pool::Timeouts established_timeouts() const noexcept { return {std::chrono::milliseconds{0}, config_.write_timeout}; }

    void send_router(msg::RouterMsg m);
    void send_store(msg::StoreMsg m);
    void send_balancer(msg::BalancerMsg m);

    IoConfig config_;
    Pool& pool_;
    ThreadQueue<msg::PtCommand>& commands_;
    ThreadQueue<msg::RouterMsg>& router_;
    ThreadQueue<msg::StoreMsg>& store_;
    ThreadQueue<msg::BalancerMsg>& balancer_;
    Logger log_;

    std::unordered_map<std::uint64_t, LinkState> by_conn_;   // pool connection id → link
    std::unordered_map<LinkId, std::uint64_t> by_link_;
    std::vector<msg::LinkSample> departed_;   // final counters of links gone since the last report

    bool stopping_ = false;
    Clock::time_point stop_deadline_{};
    bool done_ = false;
};
