#pragma once

// The Store: the only worker that knows conversations, identities and history.
// It translates between the conversation level (frontend requests and events)
// and the peer level (Router and IO workers), and it mints every peer and
// conversation id.
//
// Driven entirely by its inbox (StoreMsg). Writes FrontendMsg to the frontend
// and RouterMsg to the Router. Single-threaded: run it with
// worker::run_inbox(store, store_inbox), or worker::drain() in tests.
//
// Request contract: every frontend request with req != 0 gets exactly one
// Reply, sent before any other event the request causes.
//
// The Store decides what happens (who to connect to, under which names, what
// to send) and hands the Router plain values. It never encodes frames, and it
// does not run dial attempts: the endpoints of a peer go down in one RouteDial,
// and one DialDone or PeerDown comes back.
//
// The Router reports peers, not links: PeerUp/DialDone name the link now in
// use, PeerDown means the peer has no link and no dial left. Links it drops as
// duplicates are never reported, so the Store takes these at face value.

#include "ConvStore.hpp"
#include "Ids.hpp"
#include "Logger.hpp"
#include "Messages.hpp"
#include "PeerTable.hpp"
#include "Persistence.hpp"
#include "Subscriptions.hpp"
#include "ThreadQueue.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

struct StoreConfig {
    // Endpoints we accept connections on, sent to every peer in the handshake.
    std::vector<wire::Endpoint> advertised;
    // How long the Router may spend flushing links when the app quits.
    std::chrono::milliseconds shutdown_grace{1000};
    // Initial subscription for every conversation.
    msg::Subscription default_subscription{};
    // Fixed seed for the id minter (tests); random when empty.
    std::optional<std::uint64_t> id_seed;
};

class StoreWorker {
public:
    StoreWorker(StoreConfig config, ThreadQueue<msg::FrontendMsg>& frontend,
                ThreadQueue<msg::RouterMsg>& router, Persistence& persistence, Logger log);

    // from the frontend
    void on(msg::SendText& m);
    void on(msg::DialReq& m);
    void on(msg::ReconnectReq& m);
    void on(msg::CloseReq& m);
    void on(msg::Subscribe& m);
    void on(msg::SetDefaultSubscription& m);
    void on(msg::ListReq& m);
    void on(msg::QuitReq& m);
    // from the IO workers and the Router
    void on(msg::ChatIn& m);
    void on(msg::PeerHello& m);
    void on(msg::DialDone& m);
    void on(msg::PeerUp& m);
    void on(msg::PeerDown& m);
    void on(msg::SendResult& m);
    void on(msg::RouterStopped& m);

    // True once the Router has stopped and ShutdownDone was sent.
    bool done() const noexcept { return done_; }

    // Read-only views, for tests and diagnostics.
    const PeerTable& peers() const noexcept { return peers_; }
    const ConvStore& convs() const noexcept { return convs_; }
    msg::ConvStatus status_of(const Conversation& conv) const;

private:
    // Replies and events
    void reply_ok(ReqId req, ConvId conv = kNoId);
    void reply_error(ReqId req, std::string error);
    void emit(msg::FrontendMsg m);
    void emit_state(ConvId conv, std::string detail = {});
    void emit_state_for_peer(PeerId peer, std::string detail = {});
    void emit_snapshot(Conversation& conv);
    void send_router(msg::RouterMsg m);

    // Mutations of persisted state; each updates memory, then Persistence.
    Peer& commit_new_peer(Peer peer);
    void commit_peer(const Peer& peer);
    Conversation& commit_new_conv(Conversation conv);
    void commit_conv(const Conversation& conv);
    MsgId commit_message(Conversation& conv, msg::StoredMessage m);
    void commit_message_state(Conversation& conv, MsgId id, msg::DeliveryState state);

    // Helpers
    // Mints a peer id for `proto`, stores it, and creates its direct conversation.
    Peer& create_peer_with_conv(Peer proto);
    // Asks the Router to connect: `first` (when given), then the peer's other
    // dialable endpoints, in order. False when there is nothing to dial.
    [[nodiscard]] bool start_dial(Peer& peer, const std::optional<wire::Endpoint>& first = std::nullopt);
    void mark_up(Peer& peer, LinkId link);
    Peer* sole_peer(const Conversation& conv);
    void load_state();

    StoreConfig config_;
    ThreadQueue<msg::FrontendMsg>& frontend_;
    ThreadQueue<msg::RouterMsg>& router_;
    Persistence& persistence_;
    Logger log_;

    IdMinter ids_;
    PeerTable peers_;
    ConvStore convs_;
    Subscriptions subs_;
    bool quitting_ = false;
    bool done_ = false;
};
