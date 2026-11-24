#pragma once

// The Router: owns every link and every IO worker.
//
// - Places new links on IO workers (AssignmentPolicy, then the least loaded),
//   starting workers as needed, and moves links between them on request.
// - Sends chat itself: encodes the message and calls pool.send() on the
//   link's worker pool, so a send costs no extra hop.
// - Resolves duplicate links to one peer (a reconnect, or both sides dialing
//   at once) the same way on both nodes.
// - Reports peers, not links, to the Store: PeerUp/DialDone name the link now
//   in use; PeerDown means the peer has no live link and no dial in progress.
//   A link dropped as a duplicate is never reported.
//
// Single-threaded: run() is the thread body. Only the Router changes a link's
// status; IO workers report facts.

#include "AssignmentPolicy.hpp"
#include "IoWorker.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "TcpSocket.hpp"
#include "ThreadQueue.hpp"
#include "Worker.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

struct RouterConfig {
    std::size_t min_pts = 1;               // IO workers started up front
    std::size_t max_pts = 4;
    std::size_t max_links = 1024;          // over this, new links are refused
    std::size_t max_links_per_pt = 1024;   // a full worker gets no new links (default: no cap below max_links)
    IoConfig io;                           // template for every IO worker (pt is set per worker)
    EpollConnectionPool::Timeouts pool_timeouts{std::chrono::milliseconds{0}, std::chrono::seconds{5}};
    std::size_t max_tx_buffer = 1024 * 1024;
};

class RouterWorker {
public:
    // `stop_intake` is called once, on Shutdown, to stop accepting connections
    // (normally listener.stop()). `fatal` receives exceptions escaping an IO
    // worker thread.
    RouterWorker(RouterConfig config, ThreadQueue<msg::RouterMsg>& inbox, ThreadQueue<msg::StoreMsg>& store,
                 ThreadQueue<msg::BalancerMsg>& balancer, FatalHandler* fatal, Logger log,
                 std::function<void()> stop_intake = {});
    ~RouterWorker();

    RouterWorker(const RouterWorker&) = delete;
    RouterWorker& operator=(const RouterWorker&) = delete;

    // The thread body: handles the inbox until it closes or the shutdown has
    // finished, then stops and joins every IO worker.
    void run();
    bool done() const noexcept { return done_; }

    // from the Listener
    void on(msg::InboundAccepted& m);
    // from the Store
    void on(msg::RouteSend& m);
    void on(msg::RouteDial& m);
    void on(msg::BindLink& m);
    void on(msg::RejectLink& m);
    void on(msg::ClosePeer& m);
    void on(msg::Shutdown& m);
    // from IO workers
    void on(msg::LinkHello& m);
    void on(msg::LinkAcked& m);
    void on(msg::LinkDown& m);
    void on(msg::LinkReleased& m);
    void on(msg::LinkAdopted& m);
    void on(msg::PtStopped& m);
    // from the Balancer
    void on(msg::SetPolicy& m);
    void on(msg::MoveLink& m);
    void on(msg::EvictLink& m);
    void on(msg::SpawnPt& m);
    void on(msg::RetirePt& m);
    void on(msg::ProbeAll& m);

    // Safe from any thread; for tests and diagnostics.
    std::size_t link_count() const noexcept { return link_count_.load(std::memory_order_relaxed); }
    std::size_t pt_count() const noexcept { return pt_count_.load(std::memory_order_relaxed); }
    std::size_t moves_completed() const noexcept { return moves_completed_.load(std::memory_order_relaxed); }

private:
    using Clock = std::chrono::steady_clock;

    struct Link {
        enum class Status : std::uint8_t { Dialing, Handshaking, Connected, Moving, Closing };

        LinkId id = kNoId;
        PeerId peer = kNoId;              // 0 for an inbound link until it is bound
        PtId pt = 0;                      // the worker holding it (the target while moving)
        std::uint64_t pool_conn = 0;      // 0 until adopted / acked
        Status status = Status::Handshaking;
        msg::Role role = msg::Role::Inbound;
        wire::Endpoint remote;            // inbound: where it came from
        bool quiet = false;               // dropped as a duplicate: never reported to the Store
        bool ever_connected = false;
        Clock::time_point since{};        // when its handshake started
        Clock::time_point connected_at{};
        std::deque<msg::RouteSend> parked;   // sends that arrived while it was moving
        std::optional<msg::Close> close_after_move;   // a close requested while it was moving
    };

    // The Router's view of one peer: at most one link in use and one dial.
    struct PeerLinks {
        LinkId current = kNoId;
        LinkId dialing = kNoId;
    };

    struct PtHandle {
        PtId id = 0;
        std::unique_ptr<EpollConnectionPool> pool;
        std::unique_ptr<ThreadQueue<msg::PtCommand>> commands;
        std::unique_ptr<IoWorker> worker;
        std::unordered_set<LinkId> links;
        bool retiring = false;
        bool stopping = false;
        bool stopped = false;
        std::unique_ptr<WorkerThread> thread;   // last: stopped and joined first
    };

    // IO workers
    PtId spawn_pt();
    std::optional<PtId> choose_pt();
    bool usable(const PtHandle& pt) const;
    void command(PtId pt, msg::PtCommand c);
    void retire_if_empty(PtId pt);

    // Links
    Link& new_link(Link::Status status, msg::Role role, PtId pt);
    void close(Link& link, bool graceful, msg::LinkDownReason why, bool quiet);
    void link_gone(LinkId id, msg::LinkDownReason why, SocketError err);
    void send_now(const Link& link, msg::RouteSend& m);
    void fail_send(const msg::RouteSend& m, SocketError err);
    // Simultaneous dial: true when the link this node initiated wins.
    static bool keep_ours(PeerId peer, PeerId host) noexcept;

    void finish_if_stopped();
    void send_store(msg::StoreMsg m);
    void send_balancer(msg::BalancerMsg m);
    void update_counts() noexcept;

    RouterConfig config_;
    ThreadQueue<msg::RouterMsg>& inbox_;
    ThreadQueue<msg::StoreMsg>& store_;
    ThreadQueue<msg::BalancerMsg>& balancer_;
    FatalHandler* fatal_;
    Logger base_log_;
    Logger log_;
    std::function<void()> stop_intake_;

    std::unordered_map<LinkId, Link> links_;
    std::unordered_map<PeerId, PeerLinks> peers_;
    std::map<PtId, PtHandle> pts_;
    AssignmentPolicy policy_;
    LinkId next_link_ = 1;
    PtId next_pt_ = 0;

    bool stopping_ = false;
    bool done_ = false;
    std::atomic<std::size_t> link_count_{0};
    std::atomic<std::size_t> pt_count_{0};
    std::atomic<std::size_t> moves_completed_{0};
};
