#include "IoWorker.hpp"

#include "Worker.hpp"

#include <algorithm>
#include <format>
#include <iterator>
#include <stdexcept>
#include <tuple>
#include <utility>

namespace {

using msg::LinkDownReason;

std::string_view frame_name(const wire::Message& m) {
    switch (wire::message_type(m)) {
        case wire::MessageType::Connect: return "ConnectInfo";
        case wire::MessageType::ConnectAck: return "ConnectInfoAck";
        case wire::MessageType::Message: return "ChatMessage";
    }
    return "unknown";
}

std::string endpoint_text(const wire::Endpoint& ep) {
    return std::format("{}:{}", ep.address, ep.port);
}

// Every dialled socket: non-blocking (the pool needs it), no Nagle delay for
// chat, and keepalive, which is how an idle established link notices a dead peer.
TcpConnection::Options dial_options() {
    TcpConnection::Options o;
    o.non_blocking = true;
    o.tcp_nodelay = true;
    o.keep_alive = true;
    return o;
}

}  // namespace

IoWorker::IoWorker(IoConfig config, EpollConnectionPool& pool, ThreadQueue<msg::PtCommand>& commands,
                   ThreadQueue<msg::RouterMsg>& router, ThreadQueue<msg::StoreMsg>& store,
                   ThreadQueue<msg::BalancerMsg>& balancer, Logger log)
    : config_(config),
      pool_(pool),
      commands_(commands),
      router_(router),
      store_(store),
      balancer_(balancer),
      log_(log.with_component(std::format("pt-{}", config.pt))) {}

// ------------------------------------------------------------------ loop

void IoWorker::run() {
    std::vector<std::byte> rx(config_.rx_buffer_size);
    drain_commands();   // whatever was queued before the thread started

    while (!done_) {
        // While reads are paused nothing will wake us when the queues drain, so
        // poll for that.
        std::optional<Clock::time_point> deadline;
        if (stopping_) { deadline = std::min(stop_deadline_, Clock::now() + config_.shutdown_poll); }
        if (reads_paused_) { deadline = std::min(deadline.value_or(Clock::time_point::max()), Clock::now() + config_.resume_poll); }
        const auto r = deadline ? pool_.receive(rx, *deadline) : pool_.receive(rx);
        if (r) {
            if (r->kind == Pool::ReceiveResult::Kind::Connected) {
                on_connected(r->connection_id);
            } else {
                on_bytes(r->connection_id, std::span<const std::byte>(rx).first(r->bytes_read));
                // Checked per read, not per message: a read is at most
                // rx_buffer_size bytes, which bounds the overshoot.
                if (!reads_paused_ && downstream_congested()) { pause_reads(); }
            }
        } else if (r.error().connection_id != 0) {
            on_connection_error(r.error().connection_id, r.error().error);
        } else {
            switch (r.error().error) {
                case SocketError::Interrupted: drain_commands(); break;
                case SocketError::Stopped: finish(); break;
                case SocketError::WouldBlock: break;   // a shutdown poll tick
                default:
                    // epoll_wait() itself failed: the pool is unusable.
                    throw std::runtime_error(std::format("pt-{}: receive() failed: {}", config_.pt,
                                                         to_string(r.error().error)));
            }
        }
        if (reads_paused_ && !downstream_congested()) { resume_reads(); }
        if (stopping_ && !done_ && (pool_.connection_count() == 0 || Clock::now() >= stop_deadline_)) {
            finish();
        }
    }
}

void IoWorker::drain_commands() {
    while (!done_) {
        auto command = commands_.try_pop();
        if (!command) { return; }
        worker::dispatch(*this, *command);
    }
}

void IoWorker::finish() {
    if (done_) { return; }
    if (pool_.connection_count() > 0) {
        log_.warn("stopping with {} connections not flushed", pool_.connection_count());
    }
    pool_.stop();
    done_ = true;
    send_router(msg::PtStopped{config_.pt});
}

// ------------------------------------------------------------------ pool events

void IoWorker::on_connected(std::uint64_t id) {
    const auto it = by_conn_.find(id);
    if (it == by_conn_.end() || it->second.hs != Hs::Connecting) { return; }
    LinkState& st = it->second;
    log_.debug("link {} connected to {}", st.link, endpoint_text(st.dialing));
    st.hs = Hs::AwaitAck;
    std::ignore = send_frame(id, st, st.hello);   // on failure the link is already closed and reported
}

void IoWorker::on_connection_error(std::uint64_t id, SocketError err) {
    const auto it = by_conn_.find(id);
    if (it == by_conn_.end()) { return; }

    if (it->second.hs == Hs::Connecting) {
        LinkState st = std::move(it->second);
        by_link_.erase(st.link);
        by_conn_.erase(it);
        log_.info("link {}: dial to {} failed: {}", st.link, endpoint_text(st.dialing), to_string(err));
        dial_next(std::move(st), err);
        return;
    }

    const bool established = it->second.hs == Hs::Established;
    LinkDownReason why = LinkDownReason::SocketError;
    switch (err) {
        case SocketError::ConnectionClosed:
            why = established ? LinkDownReason::PeerClosed : LinkDownReason::HandshakeFailed;
            break;
        case SocketError::ReadTimedOut:
            why = established ? LinkDownReason::ReadTimedOut : LinkDownReason::HandshakeFailed;
            break;
        case SocketError::WriteTimedOut:
            why = LinkDownReason::WriteTimedOut;
            break;
        default:
            break;
    }
    log_.info("link {} down: {} ({})", it->second.link, msg::to_string(why), to_string(err));
    close_link(id, Removal::AlreadyGone, why, err);
}

void IoWorker::on_bytes(std::uint64_t id, std::span<const std::byte> data) {
    const auto it = by_conn_.find(id);
    if (it == by_conn_.end()) { return; }
    LinkState& st = it->second;
    if (!data.empty()) { st.dec.feed(data); }

    for (;;) {
        wire::Decoded next = st.dec.next();
        if (std::holds_alternative<wire::NeedMore>(next)) { return; }
        if (const auto* err = std::get_if<wire::WireError>(&next)) {
            if (*err == wire::WireError::UnknownType) {
                log_.debug("link {}: skipped a frame of unknown type", st.link);
                continue;
            }
            ++st.bad_frames;
            log_.warn("link {}: {} ({} of {} tolerated)", st.link, wire::to_string(*err),
                      st.bad_frames, config_.max_bad_frames);
            if (st.bad_frames > config_.max_bad_frames) {
                close_link(id, Removal::Drop, LinkDownReason::ProtocolError, SocketError::Unknown);
                return;
            }
            continue;   // the decoder resynchronises on the next frame marker
        }
        ++st.rx_frames;
        if (!handle_frame(id, st, std::get<wire::Message>(std::move(next)))) { return; }
    }
}

bool IoWorker::handle_frame(std::uint64_t id, LinkState& st, wire::Message frame) {
    switch (st.hs) {
        case Hs::AwaitConnect:
            if (auto* info = std::get_if<wire::ConnectInfo>(&frame)) {
                st.hs = Hs::AwaitBind;
                send_router(msg::LinkHello{st.link, std::move(*info)});
                return true;
            }
            break;
        case Hs::AwaitAck:
            if (auto* ack = std::get_if<wire::ConnectInfoAck>(&frame)) {
                st.hs = Hs::Established;
                std::ignore = pool_.set_timeouts(id, established_timeouts());   // fails only for an unknown id
                send_router(msg::LinkAcked{st.link, *ack, st.dialing, id});
                on_established(id);
                return true;
            }
            break;
        case Hs::Established:
            if (auto* chat = std::get_if<wire::ChatMessage>(&frame)) {
                send_store(msg::ChatIn{st.peer, st.link, std::move(*chat)});
                return true;
            }
            break;
        case Hs::Connecting:
        case Hs::AwaitBind:
            break;
    }
    // Includes a chat message before the handshake is complete: the initiator
    // must wait for the Ack, and the responder for its own Bind.
    log_.warn("link {}: unexpected {} frame during the handshake or after it; dropped", st.link, frame_name(frame));
    close_link(id, Removal::Drop, LinkDownReason::ProtocolError, SocketError::Unknown);
    return false;
}

// ------------------------------------------------------------------ commands

void IoWorker::on(msg::Adopt& m) {
    if (stopping_) {
        send_router(msg::LinkDown{m.link, LinkDownReason::Shutdown, SocketError::Unknown});
        return;
    }
    Hs hs = Hs::AwaitConnect;
    switch (m.hs) {
        case msg::HandshakeState::Established: hs = Hs::Established; break;
        case msg::HandshakeState::AwaitAck: hs = Hs::AwaitAck; break;
        case msg::HandshakeState::AwaitConnect:
        case msg::HandshakeState::Connecting: hs = Hs::AwaitConnect; break;
    }
    const auto timeouts = hs == Hs::Established ? established_timeouts() : handshake_timeouts();
    const auto id = pool_.connection_add(std::move(m.conn), timeouts, m.pending_tx);
    if (!id) {
        log_.warn("link {}: the pool refused the connection", m.link);
        send_router(msg::LinkDown{m.link, LinkDownReason::SocketError, SocketError::Unknown});
        return;
    }

    LinkState st;
    st.link = m.link;
    st.peer = m.peer;
    st.role = m.role;
    st.hs = hs;
    st.dec = std::move(m.dec);
    add(*id, std::move(st));
    if (hs == Hs::Established) { on_established(*id); }
    send_router(msg::LinkAdopted{m.link, config_.pt, *id});
    // A decoder handed over by another worker holds at most the start of a
    // frame, but whatever it can already produce is handled now, not after the
    // next read.
    on_bytes(*id, {});
}

void IoWorker::on(msg::Dial& m) {
    if (stopping_) {
        send_router(msg::LinkDown{m.link, LinkDownReason::Shutdown, SocketError::Unknown});
        return;
    }
    LinkState st;
    st.link = m.link;
    st.peer = m.peer;
    st.role = msg::Role::Outbound;
    st.hs = Hs::Connecting;
    st.hello = std::move(m.hello);
    st.to_try = std::move(m.endpoints);
    dial_next(std::move(st), SocketError::InvalidAddress);
}

void IoWorker::on(msg::Bind& m) {
    const auto id = conn_of(m.link);
    if (!id) { return; }   // the link went down meanwhile; that has been reported
    LinkState& st = by_conn_.at(*id);
    if (st.hs != Hs::AwaitBind) {
        log_.warn("link {}: Bind outside the inbound handshake", m.link);
        close_link(*id, Removal::Drop, LinkDownReason::ProtocolError, SocketError::Unknown);
        return;
    }
    st.peer = m.peer;
    if (!send_frame(*id, st, m.ack)) { return; }
    st.hs = Hs::Established;
    std::ignore = pool_.set_timeouts(*id, established_timeouts());   // fails only for an unknown id
    on_established(*id);
}

void IoWorker::on(msg::Release& m) {
    const auto id = conn_of(m.link);
    if (!id) {
        // Gone already; answering again lets the Router stop waiting for the
        // release without tracking which LinkDown came first.
        send_router(msg::LinkDown{m.link, LinkDownReason::SocketError, SocketError::ConnectionNotFound});
        return;
    }
    if (by_conn_.at(*id).hs != Hs::Established) {
        log_.warn("link {}: only an established link can move", m.link);
        close_link(*id, Removal::Drop, LinkDownReason::SocketError, SocketError::NotConnected);
        return;
    }
    auto released = pool_.connection_release(*id);
    if (!released) {
        close_link(*id, Removal::Drop, LinkDownReason::SocketError, released.error());
        return;
    }
    LinkState st = std::move(by_conn_.at(*id));
    by_conn_.erase(*id);
    by_link_.erase(m.link);
    // The counters so far belong to this worker's report; the next owner starts at zero.
    departed_.push_back(sample(st, released->stats, true));
    released->stats = {};
    send_router(msg::LinkReleased{m.link, std::move(*released), std::move(st.dec)});
}

void IoWorker::on(msg::Close& m) {
    const auto id = conn_of(m.link);
    if (!id) { return; }   // already down and reported
    const bool flush = m.graceful && by_conn_.at(*id).hs == Hs::Established;
    close_link(*id, flush ? Removal::Flush : Removal::Drop, m.why, SocketError::Unknown);
}

void IoWorker::on(msg::Probe& m) {
    msg::PtReport report;
    report.pt = config_.pt;
    report.seq = m.seq;
    report.at = Clock::now();
    report.cmd_backlog = commands_.size();
    Clock::duration paused = paused_unreported_;
    if (reads_paused_) {
        paused += report.at - paused_since_;
        paused_since_ = report.at;
    }
    paused_unreported_ = {};
    report.paused_ms = static_cast<std::uint32_t>(std::chrono::ceil<std::chrono::milliseconds>(paused).count());
    report.links.reserve(by_conn_.size() + departed_.size());
    for (auto& [id, st] : by_conn_) {
        const auto stats = pool_.take_stats(id);
        report.links.push_back(sample(st, stats.value_or(Pool::ConnectionStats{})));
        st.rx_frames = 0;
        st.tx_frames = 0;
    }
    std::ranges::move(departed_, std::back_inserter(report.links));
    departed_.clear();
    send_balancer(std::move(report));
}

void IoWorker::on(msg::Stop& m) {
    if (stopping_) { return; }
    stopping_ = true;
    stop_deadline_ = m.deadline;
    std::vector<std::uint64_t> ids;
    ids.reserve(by_conn_.size());
    for (const auto& [id, st] : by_conn_) { ids.push_back(id); }
    for (const auto id : ids) {
        const bool flush = by_conn_.at(id).hs == Hs::Established;
        close_link(id, flush ? Removal::Flush : Removal::Drop, LinkDownReason::Shutdown, SocketError::Unknown);
    }
    log_.info("stopping: {} connections left to flush", pool_.connection_count());
}

// ------------------------------------------------------------------ helpers

void IoWorker::dial_next(LinkState st, SocketError last_error) {
    while (!st.to_try.empty()) {
        wire::Endpoint ep = std::move(st.to_try.front());
        st.to_try.erase(st.to_try.begin());
        const auto id = pool_.connection_dial(ep.address, ep.port, dial_options(), handshake_timeouts(),
                                              config_.connect_timeout);
        if (!id) {
            log_.info("link {}: cannot dial {}: invalid address", st.link, endpoint_text(ep));
            last_error = SocketError::InvalidAddress;
            continue;
        }
        log_.debug("link {}: dialing {}", st.link, endpoint_text(ep));
        st.dialing = std::move(ep);
        add(*id, std::move(st));
        return;
    }
    send_router(msg::LinkDown{st.link, LinkDownReason::SocketError, last_error});
}

bool IoWorker::send_frame(std::uint64_t id, LinkState& st, const wire::Message& frame) {
    auto bytes = wire::encode(frame);
    if (!bytes) {
        log_.error("link {}: cannot encode {}: {}", st.link, frame_name(frame), wire::to_string(bytes.error()));
        close_link(id, Removal::Drop, LinkDownReason::ProtocolError, SocketError::Unknown);
        return false;
    }
    const auto sent = pool_.send(id, *bytes);
    if (!sent) {
        close_link(id, Removal::Drop, LinkDownReason::SocketError, sent.error());
        return false;
    }
    ++st.tx_frames;
    return true;
}

void IoWorker::close_link(std::uint64_t id, Removal how, msg::LinkDownReason why, SocketError err) {
    const auto it = by_conn_.find(id);
    if (it == by_conn_.end()) { return; }
    if (how != Removal::AlreadyGone) {
        const auto stats = pool_.take_stats(id);
        departed_.push_back(sample(it->second, stats.value_or(Pool::ConnectionStats{}), true));
        if (how == Removal::Flush) {
            pool_.connection_remove(id);
        } else {
            pool_.connection_drop(id);
        }
    }
    const LinkId link = it->second.link;
    by_link_.erase(link);
    by_conn_.erase(it);
    send_router(msg::LinkDown{link, why, err});
}

void IoWorker::add(std::uint64_t id, LinkState st) {
    by_link_[st.link] = id;
    by_conn_.insert_or_assign(id, std::move(st));
}

std::optional<std::uint64_t> IoWorker::conn_of(LinkId link) const {
    const auto it = by_link_.find(link);
    if (it == by_link_.end()) { return std::nullopt; }
    return it->second;
}

msg::LinkSample IoWorker::sample(const LinkState& st, const Pool::ConnectionStats& stats, bool gone) const {
    return msg::LinkSample{st.link, st.peer, stats.bytes_received, stats.bytes_sent, st.rx_frames, st.tx_frames, gone};
}

// ------------------------------------------------------------------ backpressure

void IoWorker::pause_reads() {
    paused_.clear();
    for (const auto& [id, st] : by_conn_) {
        if (st.hs == Hs::Established) { paused_.push_back(id); }
    }
    pool_.pause_reading(paused_);
    reads_paused_ = true;
    paused_since_ = Clock::now();
    log_.metric("reads=paused links={} store={} router={}", paused_.size(), store_.approx_size(), router_.approx_size());
}

void IoWorker::resume_reads() {
    pool_.resume_reading(paused_);
    const auto now = Clock::now();
    const auto lasted = now - paused_since_;
    paused_unreported_ += lasted;
    log_.metric("reads=resumed links={} after_ms={}", paused_.size(),
                std::chrono::duration_cast<std::chrono::milliseconds>(lasted).count());
    paused_.clear();
    reads_paused_ = false;
}

void IoWorker::on_established(std::uint64_t id) {
    if (!reads_paused_) { return; }
    const std::uint64_t ids[] = {id};
    pool_.pause_reading(ids);
    paused_.push_back(id);
}

void IoWorker::send_router(msg::RouterMsg m) {
    std::ignore = router_.push(std::move(m));   // false: the Router is gone; nothing left to tell
}

void IoWorker::send_store(msg::StoreMsg m) {
    std::ignore = store_.push(std::move(m));    // false: the Store is gone
}

void IoWorker::send_balancer(msg::BalancerMsg m) {
    std::ignore = balancer_.push(std::move(m)); // false: the Balancer is gone
}
