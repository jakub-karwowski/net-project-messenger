#include "StoreWorker.hpp"

#include <algorithm>
#include <format>
#include <iterator>
#include <tuple>
#include <utility>

namespace {

using msg::ConvStatus;
using msg::DeliveryState;
using Inbound = msg::Subscription::Inbound;
using Outbound = msg::Subscription::Outbound;
using Lifecycle = msg::Subscription::Lifecycle;

// A peer must never be dialled on port 0: Linux does not refuse such a
// connect(), it blocks for the whole SYN retry budget.
bool dialable(const wire::Endpoint& ep) {
    return !ep.address.empty() && ep.port != 0;
}

std::string hex(std::uint64_t id) {
    return std::format("{:016x}", id);
}

std::string endpoint_text(const wire::Endpoint& ep) {
    return std::format("{}:{}", ep.address, ep.port);
}

std::size_t unread_count(const Conversation& conv) {
    const auto from = conv.msgs.begin() + static_cast<std::ptrdiff_t>(std::min(conv.read_upto, conv.msgs.size()));
    return static_cast<std::size_t>(std::count_if(from, conv.msgs.end(), [](const msg::StoredMessage& m) {
        return m.state == DeliveryState::Received;
    }));
}

}  // namespace

StoreWorker::StoreWorker(StoreConfig config, ThreadQueue<msg::FrontendMsg>& frontend,
                         ThreadQueue<msg::RouterMsg>& router, Persistence& persistence, Logger log)
    : config_(std::move(config)),
      frontend_(frontend),
      router_(router),
      persistence_(persistence),
      log_(log.with_component("store")),
      ids_(config_.id_seed ? IdMinter(*config_.id_seed) : IdMinter()) {
    subs_.set_default(config_.default_subscription);
    load_state();
}

// ------------------------------------------------------------------ start-up

void StoreWorker::load_state() {
    LoadedState state = persistence_.load();
    for (Peer& p : state.peers) {
        ids_.reserve(p.peer_id);
        // Only the persisted fields are meaningful; every link starts down.
        p.link_state = Peer::Link::Down;
        p.link = kNoId;
        p.ever_up = true;
        p.last_dial_failed = false;
        peers_.insert(std::move(p));
    }
    for (Conversation& c : state.convs) {
        ids_.reserve(c.id);
        c.read_upto = std::min(c.read_upto, c.msgs.size());
        Conversation& conv = convs_.insert(std::move(c));
        // A message that was still on its way when we stopped never left.
        for (const auto& m : conv.msgs) {
            if (m.state == DeliveryState::PendingSend) {
                commit_message_state(conv, m.id, DeliveryState::Failed);
            }
        }
    }
    if (peers_.size() > 0 || convs_.size() > 0) {
        log_.info("loaded {} peers, {} conversations", peers_.size(), convs_.size());
    }
}

// ------------------------------------------------------------------ frontend requests

void StoreWorker::on(msg::SendText& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    Conversation* conv = convs_.find(m.conv);
    if (conv == nullptr) { return reply_error(m.req, "unknown conversation"); }
    if (m.body.empty()) { return reply_error(m.req, "empty message"); }
    if (m.body.size() > wire::kMaxChatBodySize) { return reply_error(m.req, "message too large"); }
    // A message that cannot leave right now is neither stored nor shown.
    const bool all_up = std::ranges::all_of(conv->members, [&](PeerId member) {
        const Peer* p = peers_.find(member);
        return p != nullptr && p->link_state == Peer::Link::Up;
    });
    if (!all_up) { return reply_error(m.req, "not connected"); }

    reply_ok(m.req, conv->id);
    const MsgId id = commit_message(*conv, msg::StoredMessage{kNoId, kNoId, std::move(m.body),
                                                              msg::WallClock::now(), DeliveryState::PendingSend});
    const msg::StoredMessage& stored = conv->msgs[id];
    if (subs_.get(conv->id).outbound != Outbound::Ignore) {
        emit(msg::MsgAppended{conv->id, id, stored});
    }
    for (const PeerId member : conv->members) {
        const Peer& p = *peers_.find(member);
        send_router(msg::RouteSend{member, msg::SendTag{conv->id, id},
                                   wire::ChatMessage{conv->group_id, p.host_id, stored.body}});
    }
}

void StoreWorker::on(msg::DialReq& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    if (!dialable(m.ep)) { return reply_error(m.req, "bad endpoint"); }

    // An endpoint we already know belongs to an existing conversation.
    if (Peer* known = peers_.find_by_endpoint(m.ep)) {
        Conversation* conv = convs_.direct_of(known->peer_id);
        reply_ok(m.req, conv != nullptr ? conv->id : kNoId);
        if (known->link_state == Peer::Link::Down) {
            std::ignore = start_dial(*known, m.ep);   // cannot fail: m.ep is dialable
            emit_state_for_peer(known->peer_id);
        }
        return;
    }

    Peer proto;
    proto.endpoints = {m.ep};
    Peer& peer = create_peer_with_conv(std::move(proto));
    Conversation* conv = convs_.direct_of(peer.peer_id);
    reply_ok(m.req, conv->id);
    std::ignore = start_dial(peer, m.ep);   // cannot fail: m.ep is dialable
    emit_state(conv->id);
}

void StoreWorker::on(msg::ReconnectReq& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    Conversation* conv = convs_.find(m.conv);
    if (conv == nullptr) { return reply_error(m.req, "unknown conversation"); }
    Peer* peer = sole_peer(*conv);
    if (peer == nullptr) { return reply_error(m.req, "conversation has no single peer"); }
    if (peer->link_state != Peer::Link::Down) { return reply_ok(m.req, conv->id); }   // up or on its way
    if (!start_dial(*peer)) { return reply_error(m.req, "no known endpoint"); }

    reply_ok(m.req, conv->id);
    emit_state_for_peer(peer->peer_id);
}

void StoreWorker::on(msg::CloseReq& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    Conversation* conv = convs_.find(m.conv);
    if (conv == nullptr) { return reply_error(m.req, "unknown conversation"); }
    Peer* peer = sole_peer(*conv);
    if (peer == nullptr || peer->link_state == Peer::Link::Down) { return reply_error(m.req, "not connected"); }

    reply_ok(m.req, conv->id);
    send_router(msg::ClosePeer{peer->peer_id});   // also cancels a dial in progress
}

void StoreWorker::on(msg::Subscribe& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    Conversation* conv = convs_.find(m.conv);
    if (conv == nullptr) { return reply_error(m.req, "unknown conversation"); }

    subs_.set(conv->id, m.sub);
    reply_ok(m.req, conv->id);
    if (m.sub.inbound == Inbound::Forward) { emit_snapshot(*conv); }
}

void StoreWorker::on(msg::SetDefaultSubscription& m) {
    subs_.set_default(m.sub);
    if (m.sub.inbound != Inbound::Forward) { return; }
    for (const ConvId id : convs_.order()) { emit_snapshot(*convs_.find(id)); }
}

void StoreWorker::on(msg::ListReq& m) {
    if (quitting_) { return reply_error(m.req, "shutting down"); }
    reply_ok(m.req);

    msg::ConvList list;
    list.convs.reserve(convs_.size());
    for (const ConvId id : convs_.order()) {
        Conversation& conv = *convs_.find(id);
        msg::ConvSummary summary{id, status_of(conv), {}, unread_count(conv)};
        if (const Peer* p = sole_peer(conv)) {
            summary.remote = dialable(p->remote) || p->endpoints.empty() ? p->remote : p->endpoints.front();
        }
        list.convs.push_back(std::move(summary));
    }
    emit(std::move(list));
}

void StoreWorker::on(msg::QuitReq& m) {
    reply_ok(m.req);
    if (std::exchange(quitting_, true)) { return; }
    log_.info("quit requested; stopping the Router (grace {} ms)", config_.shutdown_grace.count());
    send_router(msg::Shutdown{config_.shutdown_grace});
}

// ------------------------------------------------------------------ peer level

void StoreWorker::on(msg::ChatIn& m) {
    Peer* peer = peers_.find(m.peer);
    if (peer == nullptr) {
        log_.warn("chat from unknown peer {} on link {}: dropped", hex(m.peer), m.link);
        return;
    }
    // The sender names us by the id we gave it, so this must be our name for it.
    if (m.msg.sender_id != peer->peer_id) {
        log_.warn("chat on link {} names sender {}, link belongs to {}: dropped",
                  m.link, hex(m.msg.sender_id), hex(peer->peer_id));
        return;
    }

    Conversation* conv = nullptr;
    bool created = false;
    if (m.msg.conversation_id == 0) {
        conv = convs_.direct_of(peer->peer_id);
        if (conv == nullptr) {
            conv = &commit_new_conv(Conversation{ids_.next(), Conversation::Kind::Direct, 0, {peer->peer_id}, {}, 0});
            created = true;
        }
    } else {
        conv = convs_.by_group_id(m.msg.conversation_id);
        if (conv == nullptr || !std::ranges::contains(conv->members, peer->peer_id)) {
            log_.warn("chat from {} for unknown group {}: dropped", hex(peer->peer_id), hex(m.msg.conversation_id));
            return;
        }
    }

    // A message can overtake the PeerUp/DialDone of its own link (it travels on
    // a different queue). A link that delivers a message is up.
    if (peer->link_state == Peer::Link::Dialing || peer->link_state == Peer::Link::Handshaking) {
        mark_up(*peer, m.link);
    }
    if (created) { emit_state(conv->id); }

    const msg::Subscription& sub = subs_.get(conv->id);
    if (sub.outbound == Outbound::Echo) {
        send_router(msg::RouteSend{peer->peer_id, msg::SendTag{},
                                   wire::ChatMessage{conv->group_id, peer->host_id, std::move(m.msg.body)}});
        return;
    }

    const MsgId id = commit_message(*conv, msg::StoredMessage{kNoId, peer->peer_id, std::move(m.msg.body),
                                                              msg::WallClock::now(), DeliveryState::Received});
    switch (sub.inbound) {
        case Inbound::Forward:
            emit(msg::MsgAppended{conv->id, id, conv->msgs[id]});
            conv->read_upto = conv->msgs.size();
            commit_conv(*conv);
            break;
        case Inbound::Notify:
            emit(msg::ConvNotify{conv->id, unread_count(*conv)});
            break;
        case Inbound::Ignore:
            break;
    }
}

void StoreWorker::on(msg::PeerHello& m) {
    if (m.info.peer_id == kNoId || quitting_) {
        log_.warn("inbound link {} from {}: {}", m.link, endpoint_text(m.remote),
                  quitting_ ? "shutting down, rejected" : "hello without a peer id, rejected");
        send_router(msg::RejectLink{m.link});
        return;
    }

    // A peer that re-presents the id we gave it earlier is reconnecting. An id
    // we do not know (we forgot it, or it is made up) gets a fresh peer.
    Peer* peer = m.info.assigned_peer_id != kNoId ? peers_.find(m.info.assigned_peer_id) : nullptr;
    if (peer != nullptr) {
        peer->host_id = m.info.peer_id;
        peer->endpoints = m.info.endpoints;
        peer->remote = m.remote;
        commit_peer(*peer);
    } else {
        Peer proto;
        proto.host_id = m.info.peer_id;
        proto.endpoints = m.info.endpoints;
        proto.remote = m.remote;
        peer = &create_peer_with_conv(std::move(proto));
    }

    // A peer that is already up stays up on its current link: the Router decides
    // which link survives and says so with PeerUp. Otherwise the peer is now
    // connecting (this can also overlap a dial of ours).
    const bool was_up = peer->link_state == Peer::Link::Up;
    if (!was_up) {
        peer->link_state = Peer::Link::Handshaking;
        peer->link = m.link;
    }

    // The Ack names the peer by our id for it and echoes its name for us.
    send_router(msg::BindLink{m.link, peer->peer_id, wire::ConnectInfoAck{peer->peer_id, m.info.peer_id}});
    log_.info("inbound link {} from {} is peer {}", m.link, endpoint_text(m.remote), hex(peer->peer_id));
    if (!was_up) { emit_state_for_peer(peer->peer_id); }
}

void StoreWorker::on(msg::DialDone& m) {
    Peer* peer = peers_.find(m.peer);
    if (peer == nullptr) {
        log_.warn("dial done for unknown peer {}", hex(m.peer));
        return;
    }
    if (m.ack.ack_peer_id != peer->peer_id) {
        log_.warn("peer {} acknowledged our name for it as {}", hex(peer->peer_id), hex(m.ack.ack_peer_id));
    }
    const bool host_changed = peer->host_id != m.ack.peer_id;
    const bool remote_changed = dialable(m.remote) && !(peer->remote == m.remote);
    if (host_changed || remote_changed) {
        peer->host_id = m.ack.peer_id;
        if (remote_changed) { peer->remote = m.remote; }
        commit_peer(*peer);
    }
    mark_up(*peer, m.link);
}

void StoreWorker::on(msg::PeerUp& m) {
    Peer* peer = peers_.find(m.peer);
    if (peer == nullptr) {
        log_.warn("peer up for unknown peer {}", hex(m.peer));
        return;
    }
    mark_up(*peer, m.link);
}

void StoreWorker::on(msg::PeerDown& m) {
    // The Router reports peers, not links: PeerDown means the peer has no live
    // link and no dial in progress. Links it dropped as duplicates are never
    // reported, so there is no stale PeerDown to filter out here.
    Peer* peer = peers_.find(m.peer);
    if (peer == nullptr) { return; }

    const bool was_connecting = peer->link_state == Peer::Link::Dialing
                             || peer->link_state == Peer::Link::Handshaking;
    peer->link = kNoId;
    peer->link_state = Peer::Link::Down;
    // Closing a dial on purpose is not a failure.
    peer->last_dial_failed = was_connecting && m.why != msg::LinkDownReason::PeerClosed;

    std::string detail(msg::to_string(m.why));
    if (m.err != SocketError::Unknown) { detail += std::format(": {}", to_string(m.err)); }
    log_.info("peer {} down: {}", hex(peer->peer_id), detail);
    emit_state_for_peer(peer->peer_id, std::move(detail));
}

void StoreWorker::on(msg::SendResult& m) {
    if (m.tag.conv == kNoId) {   // an echoed message: not stored, nothing to update
        if (!m.ok) { log_.debug("echo to {} failed: {}", hex(m.peer), to_string(m.err)); }
        return;
    }
    Conversation* conv = convs_.find(m.tag.conv);
    if (conv == nullptr || m.tag.msg >= conv->msgs.size()) {
        log_.warn("send result for unknown message {}/{}", hex(m.tag.conv), m.tag.msg);
        return;
    }
    const DeliveryState state = m.ok ? DeliveryState::Sent : DeliveryState::Failed;
    if (!m.ok) {
        log_.info("send to {} failed: {}", hex(m.peer), to_string(m.err));
    }
    if (conv->msgs[m.tag.msg].state == state) { return; }
    commit_message_state(*conv, m.tag.msg, state);
    if (subs_.get(conv->id).outbound != Outbound::Ignore) {
        emit(msg::MsgStatus{conv->id, m.tag.msg, state});
    }
}

void StoreWorker::on(msg::RouterStopped&) {
    persistence_.flush();
    emit(msg::ShutdownDone{});
    done_ = true;
}

// ------------------------------------------------------------------ status

msg::ConvStatus StoreWorker::status_of(const Conversation& conv) const {
    if (conv.kind == Conversation::Kind::Group) {
        const bool any_up = std::ranges::any_of(conv.members, [&](PeerId id) {
            const Peer* p = peers_.find(id);
            return p != nullptr && p->link_state == Peer::Link::Up;
        });
        return any_up ? ConvStatus::Connected : ConvStatus::Disconnected;
    }
    const Peer* p = conv.members.empty() ? nullptr : peers_.find(conv.members.front());
    if (p == nullptr) { return ConvStatus::New; }
    switch (p->link_state) {
        case Peer::Link::Up: return ConvStatus::Connected;
        case Peer::Link::Dialing:
        case Peer::Link::Handshaking: return ConvStatus::Dialing;
        case Peer::Link::Down: break;
    }
    if (p->last_dial_failed) { return ConvStatus::Failed; }
    return p->ever_up ? ConvStatus::Disconnected : ConvStatus::New;
}

// ------------------------------------------------------------------ output

void StoreWorker::reply_ok(ReqId req, ConvId conv) {
    if (req == 0) { return; }
    emit(msg::Reply{req, true, conv, {}});
}

void StoreWorker::reply_error(ReqId req, std::string error) {
    log_.debug("request {} failed: {}", req, error);
    if (req == 0) { return; }
    emit(msg::Reply{req, false, kNoId, std::move(error)});
}

void StoreWorker::emit(msg::FrontendMsg m) {
    // false: the frontend is shutting down and no longer reads.
    std::ignore = frontend_.push(std::move(m));
}

void StoreWorker::send_router(msg::RouterMsg m) {
    // false: the Router is shutting down; whatever this was is moot.
    std::ignore = router_.push(std::move(m));
}

void StoreWorker::emit_state(ConvId id, std::string detail) {
    const Conversation* conv = convs_.find(id);
    if (conv == nullptr || subs_.get(id).lifecycle != Lifecycle::Notify) { return; }
    emit(msg::ConvState{id, status_of(*conv), std::move(detail)});
}

void StoreWorker::emit_state_for_peer(PeerId peer, std::string detail) {
    for (const ConvId id : convs_.with_member(peer)) { emit_state(id, detail); }
}

void StoreWorker::emit_snapshot(Conversation& conv) {
    emit(msg::ConvSnapshot{conv.id, conv.msgs, status_of(conv)});
    if (conv.read_upto != conv.msgs.size()) {
        conv.read_upto = conv.msgs.size();
        commit_conv(conv);
    }
}

// ------------------------------------------------------------------ commits

Peer& StoreWorker::commit_new_peer(Peer peer) {
    Peer& stored = peers_.insert(std::move(peer));
    persistence_.save_peer(stored);
    return stored;
}

void StoreWorker::commit_peer(const Peer& peer) {
    persistence_.save_peer(peer);
}

Conversation& StoreWorker::commit_new_conv(Conversation conv) {
    Conversation& stored = convs_.insert(std::move(conv));
    persistence_.save_conv(stored);
    return stored;
}

void StoreWorker::commit_conv(const Conversation& conv) {
    persistence_.save_conv(conv);
}

MsgId StoreWorker::commit_message(Conversation& conv, msg::StoredMessage m) {
    m.id = conv.msgs.size();
    conv.msgs.push_back(std::move(m));
    persistence_.append_message(conv.id, conv.msgs.back());
    return conv.msgs.back().id;
}

void StoreWorker::commit_message_state(Conversation& conv, MsgId id, msg::DeliveryState state) {
    if (id >= conv.msgs.size()) { return; }
    conv.msgs[id].state = state;
    persistence_.update_message(conv.id, id, state);
}

// ------------------------------------------------------------------ helpers

Peer& StoreWorker::create_peer_with_conv(Peer proto) {
    proto.peer_id = ids_.next();
    Peer& peer = commit_new_peer(std::move(proto));
    commit_new_conv(Conversation{ids_.next(), Conversation::Kind::Direct, 0, {peer.peer_id}, {}, 0});
    return peer;
}

bool StoreWorker::start_dial(Peer& peer, const std::optional<wire::Endpoint>& first) {
    std::vector<wire::Endpoint> targets;
    if (first && dialable(*first)) { targets.push_back(*first); }
    std::ranges::copy_if(peer.endpoints, std::back_inserter(targets), [&](const wire::Endpoint& ep) {
        return dialable(ep) && !std::ranges::contains(targets, ep);
    });
    if (targets.empty()) { return false; }

    peer.link_state = Peer::Link::Dialing;
    peer.link = kNoId;
    peer.last_dial_failed = false;
    log_.info("dialing peer {} ({} endpoints, first {})", hex(peer.peer_id), targets.size(), endpoint_text(targets.front()));
    // The hello names the peer by our id for it and re-presents its name for
    // us (0 until it has told us one).
    send_router(msg::RouteDial{peer.peer_id, std::move(targets),
                               wire::ConnectInfo{peer.peer_id, peer.host_id, config_.advertised}});
    return true;
}

void StoreWorker::mark_up(Peer& peer, LinkId link) {
    // Already up (possibly on a link that is now being replaced): no status change.
    const bool already = peer.link_state == Peer::Link::Up;
    peer.link_state = Peer::Link::Up;
    peer.link = link;
    peer.ever_up = true;
    peer.last_dial_failed = false;
    if (!already) {
        log_.info("peer {} up on link {}", hex(peer.peer_id), link);
        emit_state_for_peer(peer.peer_id);
    }
}

Peer* StoreWorker::sole_peer(const Conversation& conv) {
    if (conv.kind != Conversation::Kind::Direct || conv.members.size() != 1) { return nullptr; }
    return peers_.find(conv.members.front());
}
