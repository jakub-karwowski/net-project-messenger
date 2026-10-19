// FrameChannel.cpp - the per-connection protocol state machine behind every
// inbound transport (see ReaderWorker for the thread-per-connection driver).

#include "FrameChannel.hpp"

#include "ThreadWorkers.hpp"

#include <string>
#include <utility>

namespace p2p {

FrameChannel::FrameChannel(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger,
                           ChannelRef ref, bool duplex)
    : store_queue_(store_queue),
      peers_(peers),
      logger_(std::move(logger)),
      ref_(ref),
      duplex_(duplex) {
    if (ref_.connection_id == 0) { ref_.connection_id = next_local_peer_id(); }
}

void FrameChannel::remember_assigned_id(uint64_t peer_id, uint64_t assigned_id) {
    // The peer told us the name it uses for us (either by naming us in its own
    // ConnectInfo, or by answering ours with a ConnectInfoAck). Keep it in the peer
    // table: the sender echoes it in the next ConnectInfo it sends that peer, which
    // is what lets the peer recognise us when our address changes.
    if (peer_id == 0 || assigned_id == 0) { return; }
    if (peers_.set_assigned_id(peer_id, assigned_id)) {
        logger_.info("peer#" + std::to_string(peer_id) + " calls us #"
                     + std::to_string(assigned_id));
    } else {
        logger_.warn("assigned id dropped: peer#" + std::to_string(peer_id) + " unknown");
    }
}

bool FrameChannel::on_bytes(std::span<const std::byte> bytes) {
    if (metrics_) { metrics_->bytes_in.fetch_add(bytes.size(), std::memory_order_relaxed); }
    decoder_.feed(bytes);

    for (;;) {
        auto item = decoder_.next();
        if (!item) {
            logger_.warn("malformed frame: closing connection");
            return false;
        }
        if (!*item) { return true; } // need more bytes

        if (metrics_) { metrics_->frames_in.fetch_add(1, std::memory_order_relaxed); }
        wire::Message& message = **item;
        if (const auto* connect = wire::as_connect(message)) {
            // The peer dialled us and named us via its ConnectInfo. It may also
            // re-present an id we handed it earlier: adopting that id instead of
            // minting a new one is what lets a peer that changed address resume
            // its old conversation. Otherwise we name the peer ourselves. Either
            // way, remember its endpoints and ask the sender to open our own
            // write channel and acknowledge (echoing the peer's name for us).
            const uint64_t returning = connect->assigned_peer_id;
            const bool adopted = returning != 0 && peers_.lookup(returning).has_value();
            if (adopted) {
                peer_id_ = returning;
                logger_.info("peer returned as #" + std::to_string(peer_id_));
            } else {
                peer_id_ = next_local_peer_id();
            }
            logger_.info("peer connected: local#" + std::to_string(peer_id_)
                         + " assigned#" + std::to_string(connect->peer_id));
            for (const wire::Endpoint& endpoint : connect->endpoints) {
                peers_.insert(peer_id_, PeerTable::Address{endpoint.address, endpoint.port});
            }
            (void)peers_.attach_rx(peer_id_, ref_);
            int superseded = 0;
            if (duplex_ && ref_.handle != 0) {
                // The stream we were dialled on is also our write channel. A
                // different socket previously in the tx slot is stale: hand it to
                // the sender to close.
                if (const auto old = peers_.attach_tx(peer_id_, ref_);
                    old && old->handle != 0 && old->handle != ref_.handle) {
                    superseded = old->handle;
                }
            }
            remember_assigned_id(peer_id_, connect->peer_id);
            if (!store_queue_.emplace(EventPeerConnected{
                    peer_id_, connect->peer_id, connect->endpoints, adopted, superseded})) {
                return false; // consumer gone: nothing left to deliver to
            }
        } else if (const auto* ack = wire::as_connect_ack(message)) {
            // The peer answered our ConnectInfo: `ack_peer_id` is the name we
            // gave the peer, so it is how we refer to the peer from now on; its
            // `peer_id` is the name it gave us, kept to re-present later.
            peer_id_ = ack->ack_peer_id;
            logger_.info("handshake complete: peer#" + std::to_string(peer_id_));
            (void)peers_.attach_rx(peer_id_, ref_);
            // Duplex: this is the socket we dialled and already write on. Re-attach it
            // as tx under this channel's id (same handle: nothing to close) so that one
            // close releases both slots.
            if (duplex_ && ref_.handle != 0) { (void)peers_.attach_tx(peer_id_, ref_); }
            remember_assigned_id(peer_id_, ack->peer_id);
        } else if (const auto* chat = wire::as_message(message)) {
            // The conversation is local to this node, so the wire's
            // conversation_id is meaningless here: route by the peer identity
            // learned from ConnectInfo. A message that arrives before any
            // identity cannot be attributed to a peer, so it is dropped.
            if (peer_id_ == 0) {
                logger_.warn("dropped message before peer identity");
                continue;
            }
            if (!store_queue_.emplace(EventMessage{peer_id_, 0, chat->body})) {
                return false; // consumer gone: nothing left to deliver to
            }
        }
    }
}

void FrameChannel::on_closed() {
    if (peer_id_ != 0 && peers_.release(peer_id_, ref_.connection_id)) {
        // Keep the peer in the table so its address stays usable for a later
        // reconnect; only the status records that it is gone. The store worker
        // resolves the local conversation and routes the error to its subscriber.
        // A superseded session (a newer connection adopted the same peer id) stays
        // silent: the peer is still live on that newer connection.
        logger_.info("peer#" + std::to_string(peer_id_) + " disconnected");
        (void)store_queue_.emplace(EventPeerDisconnected{0, peer_id_});
    }
}

} // namespace p2p
