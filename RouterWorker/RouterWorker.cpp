#include "RouterWorker.hpp"

#include <algorithm>
#include <format>
#include <tuple>
#include <utility>
#include <vector>

namespace {

using msg::LinkDownReason;

}  // namespace

RouterWorker::RouterWorker(RouterConfig config, ThreadQueue<msg::RouterMsg>& inbox, ThreadQueue<msg::StoreMsg>& store,
                           ThreadQueue<msg::BalancerMsg>& balancer, FatalHandler* fatal, Logger log,
                           std::function<void()> stop_intake)
    : config_(std::move(config)),
      inbox_(inbox),
      store_(store),
      balancer_(balancer),
      fatal_(fatal),
      base_log_(log),
      log_(log.with_component("router")),
      stop_intake_(std::move(stop_intake)) {
    config_.min_pts = std::max<std::size_t>(config_.min_pts, 1);
    config_.max_pts = std::max(config_.max_pts, config_.min_pts);
    for (std::size_t i = 0; i < config_.min_pts; ++i) { spawn_pt(); }
}

RouterWorker::~RouterWorker() {
    // Normally run() has already stopped them; this covers a Router that never ran.
    for (auto& [id, pt] : pts_) { pt.thread->request_stop(); }
    for (auto& [id, pt] : pts_) { pt.thread->join(); }
}

void RouterWorker::run() {
    worker::run_inbox(*this, inbox_);
    // Rule for workers that own threads: stop and join them before returning.
    for (auto& [id, pt] : pts_) { pt.thread->request_stop(); }
    for (auto& [id, pt] : pts_) { pt.thread->join(); }
}

// ------------------------------------------------------------------ inbound links

void RouterWorker::on(msg::InboundAccepted& m) {
    const auto& from = m.conn.remote_endpoint();
    if (stopping_) {
        log_.debug("inbound connection from {}:{} refused: shutting down", from.address, from.port);
        return;   // the connection closes with the message
    }
    if (links_.size() >= config_.max_links) {
        log_.warn("inbound connection from {}:{} refused: {} links", from.address, from.port, links_.size());
        return;
    }
    const auto pt = choose_pt();
    if (!pt) {
        log_.warn("inbound connection from {}:{} refused: every IO worker is full", from.address, from.port);
        return;
    }
    Link& link = new_link(Link::Status::Handshaking, msg::Role::Inbound, *pt);
    link.remote = wire::Endpoint{from.address, from.port};
    command(*pt, msg::Adopt{link.id, kNoId, msg::Role::Inbound, std::move(m.conn), {}, {},
                            msg::HandshakeState::AwaitConnect});
}

void RouterWorker::on(msg::LinkHello& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Handshaking) { return; }
    send_store(msg::PeerHello{m.link, std::move(m.info), it->second.remote});
}

void RouterWorker::on(msg::BindLink& m) {
    PeerLinks& peer = peers_[m.peer];
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Handshaking) {
        // The link died between its hello and this answer; its LinkDown could not
        // name a peer yet. Tell the Store, unless the peer has another way in.
        if (peer.current == kNoId && peer.dialing == kNoId) {
            send_store(msg::PeerDown{m.peer, m.link, LinkDownReason::HandshakeFailed, SocketError::Unknown});
        }
        return;
    }
    Link& in = it->second;
    in.peer = m.peer;
    const PeerId host = m.ack.ack_peer_id;   // the peer's name for us

    if (peer.current != kNoId) {
        Link& cur = links_.at(peer.current);
        // The existing link was connected before this handshake began: the peer
        // has reconnected, so the old link is the stale one. Otherwise both sides
        // dialled at once and the tie rule picks the same TCP connection on both.
        const bool earlier_session = cur.connected_at < in.since;
        if (!earlier_session && cur.role == msg::Role::Outbound && keep_ours(m.peer, host)) {
            log_.info("peer {:016x}: keeping our link {}, dropping their link {}", m.peer, cur.id, in.id);
            close(in, false, LinkDownReason::Superseded, true);
            return;
        }
        log_.info("peer {:016x}: link {} supersedes link {}", m.peer, in.id, cur.id);
        close(cur, true, LinkDownReason::Superseded, true);
        peer.current = kNoId;
    } else if (peer.dialing != kNoId) {
        Link& dial = links_.at(peer.dialing);
        if (keep_ours(m.peer, host)) {
            // Our dial wins; the Store waits for its outcome.
            log_.info("peer {:016x}: both sides dialled; keeping our dial {}, dropping their link {}",
                      m.peer, dial.id, in.id);
            close(in, false, LinkDownReason::Superseded, true);
            return;
        }
        log_.info("peer {:016x}: both sides dialled; keeping their link {}, dropping our dial {}",
                  m.peer, in.id, dial.id);
        close(dial, false, LinkDownReason::Superseded, true);
        peer.dialing = kNoId;
    }

    in.status = Link::Status::Connected;
    in.connected_at = Clock::now();
    in.ever_connected = true;
    peer.current = in.id;
    command(in.pt, msg::Bind{in.id, m.peer, m.ack});
    send_store(msg::PeerUp{m.peer, in.id});
}

void RouterWorker::on(msg::RejectLink& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Handshaking) { return; }
    close(it->second, false, LinkDownReason::HandshakeFailed, true);
}

// ------------------------------------------------------------------ outbound links

void RouterWorker::on(msg::RouteDial& m) {
    PeerLinks& peer = peers_[m.peer];
    if (stopping_) {
        send_store(msg::PeerDown{m.peer, kNoId, LinkDownReason::Shutdown, SocketError::Unknown});
        return;
    }
    if (peer.current != kNoId) {
        send_store(msg::PeerUp{m.peer, peer.current});   // already connected: nothing to dial
        return;
    }
    if (peer.dialing != kNoId) { return; }   // that dial's outcome will answer this one too

    const auto pt = links_.size() < config_.max_links ? choose_pt() : std::nullopt;
    if (!pt) {
        log_.warn("dial to peer {:016x} refused: no room for another link", m.peer);
        send_store(msg::PeerDown{m.peer, kNoId, LinkDownReason::SocketError, SocketError::TooManyOpenFiles});
        return;
    }
    Link& link = new_link(Link::Status::Dialing, msg::Role::Outbound, *pt);
    link.peer = m.peer;
    peer.dialing = link.id;
    command(*pt, msg::Dial{link.id, m.peer, std::move(m.endpoints), std::move(m.hello)});
}

void RouterWorker::on(msg::LinkAcked& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Dialing) { return; }   // closed meanwhile
    Link& link = it->second;
    PeerLinks& peer = peers_[link.peer];
    if (peer.current != kNoId && peer.current != link.id) {
        // Not reachable through the Store (a dial is only started for a peer
        // without a link), but never leave two links in use.
        close(links_.at(peer.current), true, LinkDownReason::Superseded, true);
    }
    link.pool_conn = m.pool_conn_id;
    link.status = Link::Status::Connected;
    link.connected_at = Clock::now();
    link.ever_connected = true;
    peer.current = link.id;
    peer.dialing = kNoId;
    send_store(msg::DialDone{link.id, link.peer, m.ack, m.remote});
}

void RouterWorker::on(msg::ClosePeer& m) {
    const auto it = peers_.find(m.peer);
    const bool any = it != peers_.end() && (it->second.current != kNoId || it->second.dialing != kNoId);
    if (!any) {
        // Nothing to close here (it went down meanwhile): still answer, so the
        // Store does not wait for an outcome that will never come.
        send_store(msg::PeerDown{m.peer, kNoId, LinkDownReason::PeerClosed, SocketError::Unknown});
        return;
    }
    if (it->second.current != kNoId) { close(links_.at(it->second.current), true, LinkDownReason::PeerClosed, false); }
    if (it->second.dialing != kNoId) { close(links_.at(it->second.dialing), false, LinkDownReason::PeerClosed, false); }
}

// ------------------------------------------------------------------ link lifecycle

void RouterWorker::on(msg::LinkAdopted& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end()) { return; }
    Link& link = it->second;
    link.pool_conn = m.pool_conn_id;
    if (link.status != Link::Status::Moving) { return; }   // a new inbound link: its handshake goes on

    // A move has completed.
    if (link.close_after_move) {
        // Closed while in transit: the new worker holds it now, so close it there.
        for (const auto& send : link.parked) { fail_send(send, SocketError::ConnectionClosed); }
        link.parked.clear();
        link.status = Link::Status::Closing;
        command(link.pt, *link.close_after_move);
        return;
    }
    // Back in service, parked sends first, in order.
    link.status = Link::Status::Connected;
    moves_completed_.fetch_add(1, std::memory_order_relaxed);
    log_.debug("link {} moved to pt-{}; flushing {} parked sends", link.id, link.pt, link.parked.size());
    auto parked = std::exchange(link.parked, {});
    for (auto& send : parked) { send_now(link, send); }
}

void RouterWorker::on(msg::LinkDown& m) {
    link_gone(m.link, m.why, m.err);
}

void RouterWorker::link_gone(LinkId id, msg::LinkDownReason why, SocketError err) {
    const auto it = links_.find(id);
    if (it == links_.end()) { return; }
    Link link = std::move(it->second);
    links_.erase(it);
    if (const auto pt = pts_.find(link.pt); pt != pts_.end()) { pt->second.links.erase(id); }
    update_counts();

    for (const auto& send : link.parked) { fail_send(send, SocketError::ConnectionClosed); }
    if (link.ever_connected) { send_balancer(msg::LinkGone{id}); }

    if (link.peer != kNoId) {
        PeerLinks& peer = peers_[link.peer];
        bool was_in_use = false;
        if (peer.current == id) { peer.current = kNoId; was_in_use = true; }
        if (peer.dialing == id) { peer.dialing = kNoId; was_in_use = true; }
        const bool nothing_left = peer.current == kNoId && peer.dialing == kNoId;
        if (was_in_use && nothing_left && !link.quiet && !stopping_) {
            send_store(msg::PeerDown{link.peer, id, why, err});
        }
    }
    // An unbound inbound link was never named to the Store as a peer. If its
    // hello reached the Store, the Store's BindLink/RejectLink finds it gone.
    log_.debug("link {} gone: {}", id, msg::to_string(why));
    retire_if_empty(link.pt);
}

void RouterWorker::on(msg::PtStopped& m) {
    const auto it = pts_.find(m.pt);
    if (it == pts_.end()) { return; }
    PtHandle& pt = it->second;
    pt.stopped = true;
    pt.thread->join();

    // Links still recorded there went down with the worker (stopped from outside).
    const std::vector<LinkId> left(pt.links.begin(), pt.links.end());
    for (const LinkId id : left) { link_gone(id, LinkDownReason::Shutdown, SocketError::Stopped); }

    if (!stopping_) {
        log_.info("pt-{} stopped{}", m.pt, pt.retiring ? " (retired)" : " unexpectedly");
        pts_.erase(m.pt);
        update_counts();
        send_balancer(msg::TopologyChanged{m.pt, false});
        if (pts_.empty()) { spawn_pt(); }   // never run without a worker
        return;
    }
    finish_if_stopped();
}

// ------------------------------------------------------------------ sending

void RouterWorker::on(msg::RouteSend& m) {
    const auto peer = peers_.find(m.peer);
    const LinkId id = peer == peers_.end() ? kNoId : peer->second.current;
    const auto it = links_.find(id);
    if (it == links_.end()) { return fail_send(m, SocketError::ConnectionNotFound); }
    Link& link = it->second;
    switch (link.status) {
        case Link::Status::Connected: send_now(link, m); return;
        case Link::Status::Moving: link.parked.push_back(std::move(m)); return;
        default: fail_send(m, SocketError::ConnectionClosed); return;
    }
}

void RouterWorker::send_now(const Link& link, msg::RouteSend& m) {
    auto bytes = wire::encode(m.msg);
    if (!bytes) { return fail_send(m, SocketError::Unknown); }
    const auto pt = pts_.find(link.pt);
    if (pt == pts_.end()) { return fail_send(m, SocketError::ConnectionNotFound); }
    const auto sent = pt->second.pool->send(link.pool_conn, *bytes);
    send_store(msg::SendResult{m.peer, m.tag, sent.has_value(), sent ? SocketError::Unknown : sent.error()});
}

void RouterWorker::fail_send(const msg::RouteSend& m, SocketError err) {
    send_store(msg::SendResult{m.peer, m.tag, false, err});
}

// ------------------------------------------------------------------ balancer requests

void RouterWorker::on(msg::SetPolicy& m) {
    policy_.set(std::move(m.next_assignments), m.epoch);
}

void RouterWorker::on(msg::MoveLink& m) {
    const auto it = links_.find(m.link);
    const auto to = pts_.find(m.to);
    if (it == links_.end() || it->second.status != Link::Status::Connected || to == pts_.end()
        || !usable(to->second) || it->second.pt == m.to) {
        log_.debug("move of link {} to pt-{} skipped", m.link, m.to);
        return;
    }
    Link& link = it->second;
    const PtId from = link.pt;
    link.status = Link::Status::Moving;
    link.pt = m.to;   // where it will be; membership moves when it is released
    command(from, msg::Release{link.id});
    pts_.at(from).links.erase(link.id);
    pts_.at(m.to).links.insert(link.id);
}

void RouterWorker::on(msg::LinkReleased& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Moving) { return; }   // the connection closes here
    Link& link = it->second;
    auto target = pts_.find(link.pt);
    if (target == pts_.end() || !usable(target->second)) {
        // The target went away during the move: any other worker will do.
        const auto other = choose_pt();
        if (!other) { return link_gone(link.id, LinkDownReason::SocketError, SocketError::ConnectionNotFound); }
        if (target != pts_.end()) { target->second.links.erase(link.id); }
        link.pt = *other;
        pts_.at(*other).links.insert(link.id);
    }
    command(link.pt, msg::Adopt{link.id, link.peer, link.role, std::move(m.rc.connection), std::move(m.rc.unsent_tx),
                                std::move(m.dec), msg::HandshakeState::Established});
}

void RouterWorker::on(msg::EvictLink& m) {
    const auto it = links_.find(m.link);
    if (it == links_.end() || it->second.status != Link::Status::Connected) { return; }
    close(it->second, true, LinkDownReason::BalancerEvicted, false);
}

void RouterWorker::on(msg::SpawnPt&) {
    if (stopping_ || pts_.size() >= config_.max_pts) { return; }
    spawn_pt();
}

void RouterWorker::on(msg::RetirePt& m) {
    const auto it = pts_.find(m.pt);
    const auto others = std::ranges::count_if(pts_, [&](const auto& e) { return e.first != m.pt && usable(e.second); });
    if (stopping_ || it == pts_.end() || it->second.retiring || others == 0) { return; }
    it->second.retiring = true;
    log_.info("retiring pt-{} ({} links to move)", m.pt, it->second.links.size());
    const std::vector<LinkId> on_it(it->second.links.begin(), it->second.links.end());
    for (const LinkId id : on_it) {
        if (links_.at(id).status != Link::Status::Connected) { continue; }   // handshakes finish where they are
        if (const auto to = choose_pt()) {
            msg::MoveLink move{id, *to};
            on(move);
        }
    }
    retire_if_empty(m.pt);
}

void RouterWorker::on(msg::ProbeAll& m) {
    for (auto& [id, pt] : pts_) {
        if (!pt.stopped) { command(id, msg::Probe{m.seq}); }
    }
}

// ------------------------------------------------------------------ shutdown

void RouterWorker::on(msg::Shutdown& m) {
    if (stopping_) { return; }
    stopping_ = true;
    log_.info("shutting down: {} links on {} IO workers", links_.size(), pts_.size());
    if (stop_intake_) { stop_intake_(); }
    send_balancer(msg::StopBalancer{});
    const auto deadline = Clock::now() + m.grace;
    for (auto& [id, pt] : pts_) {
        if (pt.stopped) { continue; }
        pt.stopping = true;
        command(id, msg::Stop{deadline});
    }
    finish_if_stopped();
}

void RouterWorker::finish_if_stopped() {
    if (!stopping_ || done_) { return; }
    if (std::ranges::any_of(pts_, [](const auto& e) { return !e.second.stopped; })) { return; }
    done_ = true;
    log_.info("all IO workers stopped");
    send_store(msg::RouterStopped{});
}

// ------------------------------------------------------------------ IO workers

PtId RouterWorker::spawn_pt() {
    const PtId id = next_pt_++;
    PtHandle handle;
    handle.id = id;
    handle.pool = std::make_unique<EpollConnectionPool>(config_.pool_timeouts, config_.max_tx_buffer);
    handle.commands = std::make_unique<ThreadQueue<msg::PtCommand>>();
    IoConfig io = config_.io;
    io.pt = id;
    handle.worker = std::make_unique<IoWorker>(io, *handle.pool, *handle.commands, inbox_, store_, balancer_,
                                               base_log_.for_thread(std::format("pt-{}", id)));
    PtHandle& pt = pts_.emplace(id, std::move(handle)).first->second;
    pt.thread = std::make_unique<WorkerThread>(std::format("pt-{}", id), [w = pt.worker.get()] { w->run(); },
                                               [p = pt.pool.get()] { p->stop(); }, fatal_);
    update_counts();
    log_.info("started pt-{}", id);
    send_balancer(msg::TopologyChanged{id, true});
    return id;
}

bool RouterWorker::usable(const PtHandle& pt) const {
    return !pt.retiring && !pt.stopping && !pt.stopped && pt.links.size() < config_.max_links_per_pt;
}

std::optional<PtId> RouterWorker::choose_pt() {
    if (const auto listed = policy_.next([this](PtId id) {
            const auto it = pts_.find(id);
            return it != pts_.end() && usable(it->second);
        })) {
        return listed;
    }
    // No usable entry in the Balancer's list: the least loaded usable worker.
    std::optional<PtId> best;
    std::size_t fewest = 0;
    for (const auto& [id, pt] : pts_) {
        if (!usable(pt)) { continue; }
        if (!best || pt.links.size() < fewest) {
            best = id;
            fewest = pt.links.size();
        }
    }
    if (best) { return best; }
    if (!stopping_ && pts_.size() < config_.max_pts) { return spawn_pt(); }
    return std::nullopt;
}

void RouterWorker::command(PtId id, msg::PtCommand c) {
    const auto it = pts_.find(id);
    if (it == pts_.end()) { return; }
    std::ignore = it->second.commands->push(std::move(c));   // false: the worker is stopping anyway
    it->second.pool->interrupt();
}

void RouterWorker::retire_if_empty(PtId id) {
    const auto it = pts_.find(id);
    if (it == pts_.end() || !it->second.retiring || it->second.stopping || !it->second.links.empty()) { return; }
    it->second.stopping = true;
    command(id, msg::Stop{Clock::now()});
}

// ------------------------------------------------------------------ links

RouterWorker::Link& RouterWorker::new_link(Link::Status status, msg::Role role, PtId pt) {
    const LinkId id = next_link_++;
    Link link;
    link.id = id;
    link.status = status;
    link.role = role;
    link.pt = pt;
    link.since = Clock::now();
    pts_.at(pt).links.insert(id);
    Link& stored = links_.emplace(id, std::move(link)).first->second;
    update_counts();
    return stored;
}

void RouterWorker::close(Link& link, bool graceful, msg::LinkDownReason why, bool quiet) {
    link.quiet = link.quiet || quiet;
    if (link.status == Link::Status::Closing) { return; }
    if (link.status == Link::Status::Moving) {
        // Neither worker can close it now: the source has released it and the
        // target does not hold it yet. Done when the move completes.
        if (!link.close_after_move) { link.close_after_move = msg::Close{link.id, graceful, why}; }
        return;
    }
    link.status = Link::Status::Closing;
    command(link.pt, msg::Close{link.id, graceful, why});
}

bool RouterWorker::keep_ours(PeerId peer, PeerId host) noexcept {
    // Each node compares (its name for the other, the other's name for it).
    // The other node sees the same two values swapped, so exactly one of them
    // keeps the link it initiated, and both keep the same TCP connection.
    return std::pair{peer, host} > std::pair{host, peer};
}

// ------------------------------------------------------------------ output

void RouterWorker::send_store(msg::StoreMsg m) {
    std::ignore = store_.push(std::move(m));   // false: the Store is gone
}

void RouterWorker::send_balancer(msg::BalancerMsg m) {
    std::ignore = balancer_.push(std::move(m));   // false: no Balancer (or it stopped)
}

void RouterWorker::update_counts() noexcept {
    link_count_.store(links_.size(), std::memory_order_relaxed);
    pt_count_.store(pts_.size(), std::memory_order_relaxed);
}
