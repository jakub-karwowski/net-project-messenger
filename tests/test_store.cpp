// test_store.cpp - the Store, single-threaded and driven through its inbox.
//
// Each test feeds StoreMsg values, runs worker::drain(), and checks what the
// Store pushed to the frontend and Router queues, and what it committed to a
// recording Persistence fake.
//
// Plain assertions, no framework.

#include "StoreWorker.hpp"
#include "Worker.hpp"

#include <algorithm>
#include <iostream>
#include <map>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

namespace {

int g_failures = 0;
int g_checks = 0;

void check(bool condition, const std::string& what) {
    ++g_checks;
    if (!condition) {
        ++g_failures;
        std::cout << "  FAIL: " << what << '\n';
    }
}

using msg::ConvStatus;
using msg::DeliveryState;
using Sub = msg::Subscription;

// ------------------------------------------------------------------ fakes and helpers

struct RecordingPersistence final : Persistence {
    LoadedState to_load;
    int loads = 0, peers_saved = 0, convs_saved = 0, appended = 0, updated = 0, flushes = 0;
    std::vector<std::tuple<ConvId, MsgId, DeliveryState>> updates;

    LoadedState load() override { ++loads; return to_load; }
    void save_peer(const Peer&) override { ++peers_saved; }
    void save_conv(const Conversation&) override { ++convs_saved; }
    void append_message(ConvId, const msg::StoredMessage&) override { ++appended; }
    void update_message(ConvId c, MsgId id, DeliveryState s) override { ++updated; updates.emplace_back(c, id, s); }
    void flush() override { ++flushes; }
    int total() const { return peers_saved + convs_saved + appended + updated; }
};

const wire::Endpoint kMe{"10.0.0.1", 7000};
const wire::Endpoint kPeerA{"10.0.0.2", 7000};
const wire::Endpoint kPeerA2{"10.0.0.22", 7001};
const wire::Endpoint kPeerB{"10.0.0.3", 7000};

StoreConfig test_config() {
    StoreConfig c;
    c.advertised = {kMe};
    c.id_seed = 1234;
    return c;
}

template <class V>
std::vector<V> take_all(ThreadQueue<V>& q) {
    std::vector<V> out;
    while (auto m = q.try_pop()) { out.push_back(std::move(*m)); }
    return out;
}

template <class T, class V>
std::vector<T*> all_of(std::vector<V>& msgs) {
    std::vector<T*> out;
    for (auto& m : msgs) {
        if (auto* p = std::get_if<T>(&m)) { out.push_back(p); }
    }
    return out;
}

template <class T, class V>
T* first_of(std::vector<V>& msgs) {
    auto all = all_of<T>(msgs);
    return all.empty() ? nullptr : all.front();
}

// The index of the first message of type T, or -1.
template <class T, class V>
int index_of(const std::vector<V>& msgs) {
    for (std::size_t i = 0; i < msgs.size(); ++i) {
        if (std::holds_alternative<T>(msgs[i])) { return static_cast<int>(i); }
    }
    return -1;
}

struct Fixture {
    ThreadQueue<msg::FrontendMsg> fe;
    ThreadQueue<msg::RouterMsg> rt;
    ThreadQueue<msg::StoreMsg> in;
    RecordingPersistence db;
    std::optional<StoreWorker> store;

    explicit Fixture(StoreConfig config = test_config(), LoadedState load = {}) {
        db.to_load = std::move(load);
        store.emplace(std::move(config), fe, rt, db, Logger{});
    }

    void feed(msg::StoreMsg m) {
        std::ignore = in.push(std::move(m));
        worker::drain(*store, in);
    }
    std::vector<msg::FrontendMsg> events() { return take_all(fe); }
    std::vector<msg::RouterMsg> routed() { return take_all(rt); }
    void clear() { events(); routed(); }

    // Dials `ep` and completes the handshake: returns {conv, peer}.
    std::pair<ConvId, PeerId> connect_out(const wire::Endpoint& ep, PeerId their_name_for_us, LinkId link) {
        feed(msg::DialReq{0, ep});
        auto r = routed();
        auto* dial = first_of<msg::RouteDial>(r);
        if (dial == nullptr) { return {kNoId, kNoId}; }
        const PeerId peer = dial->peer;
        feed(msg::DialDone{link, peer, wire::ConnectInfoAck{their_name_for_us, peer}, ep});
        const ConvId conv = store->convs().order().back();
        clear();
        return {conv, peer};
    }

    // An inbound peer says hello and is bound: returns {conv, peer}.
    std::pair<ConvId, PeerId> connect_in(PeerId their_name_for_us, LinkId link, PeerId assigned = kNoId) {
        feed(msg::PeerHello{link, wire::ConnectInfo{their_name_for_us, assigned, {kPeerB}}, wire::Endpoint{"10.0.0.3", 50000}});
        auto r = routed();
        auto* bind = first_of<msg::BindLink>(r);
        if (bind == nullptr) { return {kNoId, kNoId}; }
        const PeerId peer = bind->peer;
        feed(msg::PeerUp{peer, link});
        const ConvId conv = store->convs().order().back();
        clear();
        return {conv, peer};
    }
};

// ------------------------------------------------------------------ dial

void test_dial_flow() {
    Fixture f;
    f.feed(msg::DialReq{1, kPeerA});
    auto ev = f.events();
    auto rt = f.routed();

    auto* reply = first_of<msg::Reply>(ev);
    check(reply && reply->req == 1 && reply->ok && reply->conv != kNoId, "dial: Reply ok with the new conversation");
    check(index_of<msg::Reply>(ev) == 0, "dial: the Reply comes before any other event");
    auto* state = first_of<msg::ConvState>(ev);
    check(state && reply && state->conv == reply->conv && state->status == ConvStatus::Dialing, "dial: ConvState dialing");

    auto* dial = first_of<msg::RouteDial>(rt);
    check(dial && dial->endpoints == std::vector{kPeerA} && dial->peer != kNoId, "dial: RouteDial to the endpoint");
    check(dial && dial->hello.peer_id == dial->peer && dial->hello.assigned_peer_id == kNoId
              && dial->hello.endpoints == std::vector{kMe},
          "dial: hello names the peer by our new id, has no assigned id yet, advertises our endpoints");
    check(f.db.peers_saved == 1 && f.db.convs_saved == 1, "dial: one peer and one conversation committed");

    // Handshake done.
    const PeerId peer = dial ? dial->peer : kNoId;
    f.feed(msg::DialDone{5, peer, wire::ConnectInfoAck{777, peer}, kPeerA});
    ev = f.events();
    state = first_of<msg::ConvState>(ev);
    check(state && state->status == ConvStatus::Connected, "dial done: ConvState connected");
    const Peer* p = f.store->peers().find(peer);
    check(p && p->host_id == 777 && p->remote == kPeerA && p->link == 5,
          "dial done: host id, the endpoint that answered, and the link are recorded");
    check(f.db.peers_saved == 2, "dial done: the peer is committed once more (host id learned)");

    // Dialing the same endpoint again reuses the conversation.
    f.feed(msg::DialReq{2, kPeerA});
    ev = f.events();
    reply = first_of<msg::Reply>(ev);
    check(reply && reply->ok && reply->conv == f.store->convs().order().front() && f.store->convs().size() == 1,
          "dial of a known endpoint: Reply with the existing conversation, nothing new created");
    check(f.routed().empty(), "dial of a connected endpoint: no new RouteDial");
}

void test_dial_rejects() {
    Fixture f;
    f.feed(msg::DialReq{1, wire::Endpoint{"10.0.0.2", 0}});
    f.feed(msg::DialReq{2, wire::Endpoint{"", 7000}});
    auto ev = f.events();
    auto replies = all_of<msg::Reply>(ev);
    check(replies.size() == 2 && !replies[0]->ok && replies[0]->error == "bad endpoint" && !replies[1]->ok,
          "dial: port 0 and an empty address are refused");
    check(ev.size() == 2 && f.routed().empty() && f.db.total() == 0, "dial refused: nothing else happens");
}

void test_dial_failure_and_reconnect() {
    Fixture f;
    f.feed(msg::DialReq{1, kPeerA});
    auto rt = f.routed();
    const PeerId peer = first_of<msg::RouteDial>(rt)->peer;
    const ConvId conv = f.store->convs().order().front();
    f.events();

    f.feed(msg::PeerDown{peer, 0, msg::LinkDownReason::SocketError, SocketError::ConnectionRefused});
    auto ev = f.events();
    auto* state = first_of<msg::ConvState>(ev);
    check(state && state->status == ConvStatus::Failed && state->detail == "socket_error: Connection refused",
          "failed dial: ConvState failed with the reason and the socket error");
    check(f.routed().empty(), "failed dial: the Store does not retry by itself");

    f.feed(msg::ReconnectReq{2, conv});
    ev = f.events();
    rt = f.routed();
    auto* reply = first_of<msg::Reply>(ev);
    auto* dial = first_of<msg::RouteDial>(rt);
    check(reply && reply->ok && reply->conv == conv, "reconnect: Reply ok");
    check(dial && dial->peer == peer && dial->endpoints == std::vector{kPeerA}, "reconnect: the same peer is dialled again");
    check(index_of<msg::Reply>(ev) == 0 && first_of<msg::ConvState>(ev) && first_of<msg::ConvState>(ev)->status == ConvStatus::Dialing,
          "reconnect: Reply, then ConvState dialing");

    f.feed(msg::ReconnectReq{3, conv});
    ev = f.events();
    check(first_of<msg::Reply>(ev) && first_of<msg::Reply>(ev)->ok && f.routed().empty(),
          "reconnect while dialing: ok, no second dial");

    f.feed(msg::ReconnectReq{4, 999});
    ev = f.events();
    check(first_of<msg::Reply>(ev) && first_of<msg::Reply>(ev)->error == "unknown conversation", "reconnect: unknown conversation");
}

void test_reconnect_sends_every_endpoint() {
    // A peer that advertised three endpoints in its hello (one on port 0), then
    // went away.
    Fixture f;
    f.feed(msg::PeerHello{3, wire::ConnectInfo{55, kNoId, {kPeerA, wire::Endpoint{"10.0.0.9", 0}, kPeerA2}},
                          wire::Endpoint{"10.0.0.2", 40000}});
    auto rt = f.routed();
    const PeerId peer = first_of<msg::BindLink>(rt)->peer;
    const ConvId conv = f.store->convs().order().front();
    f.feed(msg::PeerUp{peer, 3});
    f.feed(msg::PeerDown{peer, 3, msg::LinkDownReason::PeerClosed});
    f.clear();

    f.feed(msg::ReconnectReq{1, conv});
    rt = f.routed();
    auto dials = all_of<msg::RouteDial>(rt);
    check(dials.size() == 1 && dials[0]->endpoints == std::vector{kPeerA, kPeerA2},
          "reconnect: one RouteDial with every dialable endpoint in order (port 0 left out)");
    check(dials.size() == 1 && dials[0]->hello.peer_id == peer && dials[0]->hello.assigned_peer_id == 55,
          "reconnect: hello re-presents the name the peer gave us");

    f.feed(msg::PeerDown{peer, 0, msg::LinkDownReason::SocketError, SocketError::ConnectTimedOut});
    auto ev = f.events();
    auto states = all_of<msg::ConvState>(ev);
    check(f.routed().empty() && !states.empty() && states.back()->status == ConvStatus::Failed,
          "one PeerDown after the dial: failed, no further dial");

    // Dialing a specific known endpoint puts it first.
    f.feed(msg::DialReq{2, kPeerA2});
    rt = f.routed();
    auto* dial = first_of<msg::RouteDial>(rt);
    check(dial && dial->peer == peer && dial->endpoints == std::vector{kPeerA2, kPeerA},
          "dial of a known endpoint: that endpoint first, the others after it");

    // A peer that never advertised a dialable endpoint cannot be reconnected.
    f.feed(msg::PeerHello{4, wire::ConnectInfo{66, kNoId, {wire::Endpoint{"10.0.0.8", 0}}}, wire::Endpoint{"10.0.0.8", 40001}});
    rt = f.routed();
    const PeerId listen_only = first_of<msg::BindLink>(rt)->peer;
    f.feed(msg::PeerDown{listen_only, 4, msg::LinkDownReason::PeerClosed});
    f.clear();
    f.feed(msg::ReconnectReq{3, f.store->convs().order().back()});
    ev = f.events();
    check(first_of<msg::Reply>(ev) && first_of<msg::Reply>(ev)->error == "no known endpoint" && f.routed().empty(),
          "reconnect without a dialable endpoint: refused");
}

// ------------------------------------------------------------------ inbound handshake

void test_inbound_new_peer() {
    Fixture f;
    const wire::Endpoint from{"10.0.0.3", 50000};
    f.feed(msg::PeerHello{9, wire::ConnectInfo{4242, kNoId, {kPeerB}}, from});
    auto rt = f.routed();
    auto ev = f.events();

    auto* bind = first_of<msg::BindLink>(rt);
    check(bind && bind->link == 9 && bind->peer != kNoId, "hello: BindLink for the link with a freshly minted peer id");
    check(bind && bind->ack.peer_id == bind->peer && bind->ack.ack_peer_id == 4242,
          "hello: Ack carries our name for them and echoes their name for us");
    auto* state = first_of<msg::ConvState>(ev);
    check(state && state->status == ConvStatus::Dialing, "hello: the new conversation is announced (connecting)");
    const Peer* p = bind ? f.store->peers().find(bind->peer) : nullptr;
    check(p && p->host_id == 4242 && p->endpoints == std::vector{kPeerB} && p->remote == from,
          "hello: the peer's name for us, endpoints and address are recorded");
    check(f.db.peers_saved == 1 && f.db.convs_saved == 1, "hello: one peer and one conversation committed");

    f.feed(msg::PeerUp{bind->peer, 9});
    ev = f.events();
    state = first_of<msg::ConvState>(ev);
    check(state && state->status == ConvStatus::Connected, "peer up: ConvState connected");
}

void test_inbound_known_peer() {
    Fixture f;
    auto [conv, peer] = f.connect_in(100, 1);
    f.feed(msg::PeerDown{peer, 1, msg::LinkDownReason::PeerClosed});
    f.clear();
    const int saved_before = f.db.peers_saved;

    // The peer comes back and re-presents the id we gave it.
    f.feed(msg::PeerHello{2, wire::ConnectInfo{100, peer, {kPeerB}}, wire::Endpoint{"10.0.0.3", 50001}});
    auto rt = f.routed();
    auto* bind = first_of<msg::BindLink>(rt);
    check(bind && bind->peer == peer, "known peer: recognised by the id it re-presents");
    check(f.store->convs().size() == 1 && f.store->peers().size() == 1, "known peer: no new peer or conversation");
    check(f.db.peers_saved == saved_before + 1 && f.db.convs_saved == 1, "known peer: one peer commit, no new conversation");

    // An id we never gave out gets a fresh peer.
    f.feed(msg::PeerHello{3, wire::ConnectInfo{200, 0xdeadbeef, {}}, wire::Endpoint{"10.0.0.4", 50002}});
    rt = f.routed();
    bind = first_of<msg::BindLink>(rt);
    check(bind && bind->peer != peer && bind->peer != 0xdeadbeef && f.store->convs().size() == 2,
          "unknown assigned id: a fresh peer and conversation");
}

void test_inbound_garbage() {
    Fixture f;
    f.feed(msg::PeerHello{7, wire::ConnectInfo{0, 0, {}}, wire::Endpoint{"10.0.0.5", 1}});
    auto rt = f.routed();
    auto* reject = first_of<msg::RejectLink>(rt);
    check(reject && reject->link == 7 && rt.size() == 1, "hello without a peer id: RejectLink");
    check(f.events().empty() && f.db.total() == 0, "rejected hello: nothing stored or shown");
}

void test_stale_peer_down_ignored() {
    Fixture f;
    auto [conv, peer] = f.connect_in(100, 1);
    // The peer reconnects before the old link is known to be dead.
    f.feed(msg::PeerHello{2, wire::ConnectInfo{100, peer, {kPeerB}}, wire::Endpoint{"10.0.0.3", 50001}});
    f.feed(msg::PeerUp{peer, 2});
    f.clear();
    f.feed(msg::PeerDown{peer, 1, msg::LinkDownReason::Superseded});
    check(f.events().empty(), "down for a superseded link: no event");
    check(f.store->status_of(*f.store->convs().find(conv)) == ConvStatus::Connected, "down for a superseded link: still connected");
    f.feed(msg::PeerDown{peer, 2, msg::LinkDownReason::PeerClosed});
    auto ev = f.events();
    check(first_of<msg::ConvState>(ev) && first_of<msg::ConvState>(ev)->status == ConvStatus::Disconnected,
          "down for the current link: disconnected");
}

// ------------------------------------------------------------------ sending

void test_send() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    f.feed(msg::Subscribe{1, conv, Sub{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify}});
    f.clear();
    const int appended_before = f.db.appended;

    f.feed(msg::SendText{2, conv, "hello there, a body longer than SSO"});
    auto ev = f.events();
    auto rt = f.routed();
    check(index_of<msg::Reply>(ev) == 0 && std::get<msg::Reply>(ev[0]).ok && std::get<msg::Reply>(ev[0]).conv == conv,
          "send: Reply ok first");
    auto* appended = first_of<msg::MsgAppended>(ev);
    check(appended && appended->conv == conv && appended->id == 0 && appended->msg.state == DeliveryState::PendingSend
              && appended->msg.sender == kNoId,
          "send: MsgAppended as pending, from me");
    auto* route = first_of<msg::RouteSend>(rt);
    check(route && route->peer == peer && route->tag.conv == conv && route->tag.msg == 0, "send: RouteSend to the peer, tagged");
    check(route && route->msg.conversation_id == 0 && route->msg.sender_id == 777
              && route->msg.body == "hello there, a body longer than SSO",
          "send: a direct ChatMessage naming us by the peer's name for us");
    check(f.db.appended == appended_before + 1, "send: one message commit");

    f.feed(msg::SendResult{peer, msg::SendTag{conv, 0}, true, SocketError::Unknown});
    ev = f.events();
    auto* status = first_of<msg::MsgStatus>(ev);
    check(status && status->id == 0 && status->state == DeliveryState::Sent, "send result ok: MsgStatus sent");
    f.feed(msg::SendResult{peer, msg::SendTag{conv, 0}, true, SocketError::Unknown});
    check(f.events().empty(), "a repeated result with the same state: no event");

    f.feed(msg::SendText{3, conv, "second"});
    f.clear();
    f.feed(msg::SendResult{peer, msg::SendTag{conv, 1}, false, SocketError::BufferFull});
    ev = f.events();
    status = first_of<msg::MsgStatus>(ev);
    check(status && status->id == 1 && status->state == DeliveryState::Failed, "send result failed: MsgStatus failed");
    check(f.db.updated == 2, "two state commits (sent, failed)");
}

void test_send_rejects() {
    Fixture f;
    f.feed(msg::SendText{1, 42, "x"});
    f.feed(msg::DialReq{2, kPeerA});
    const ConvId conv = f.store->convs().order().front();
    f.feed(msg::SendText{3, conv, "not yet"});
    f.feed(msg::SendText{4, conv, ""});
    auto ev = f.events();
    auto replies = all_of<msg::Reply>(ev);
    check(replies.size() == 4, "every request got a reply");
    check(replies.size() == 4 && replies[0]->error == "unknown conversation", "send: unknown conversation");
    check(replies.size() == 4 && replies[2]->error == "not connected", "send while dialing: not connected");
    check(replies.size() == 4 && replies[3]->error == "empty message", "send: empty message");
    check(f.store->convs().find(conv)->msgs.empty() && f.db.appended == 0, "refused sends are not stored");
    auto rt = f.routed();
    check(all_of<msg::RouteSend>(rt).empty(), "refused sends are not routed");
}

void test_send_size_limit() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    f.feed(msg::SendText{1, conv, std::string(wire::kMaxChatBodySize + 1, 'x')});
    f.feed(msg::SendText{2, conv, std::string(wire::kMaxChatBodySize, 'x')});
    auto ev = f.events();
    auto replies = all_of<msg::Reply>(ev);
    check(replies.size() == 2 && replies[0]->error == "message too large" && replies[1]->ok,
          "send: one byte over the frame limit is refused, the limit itself is accepted");
    check(f.store->convs().find(conv)->msgs.size() == 1, "only the accepted message is stored");
}

void test_outbound_ignore_hides_own_messages() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);   // default subscription: outbound Ignore
    f.feed(msg::SendText{1, conv, "quiet"});
    auto ev = f.events();
    check(ev.size() == 1 && std::holds_alternative<msg::Reply>(ev[0]), "outbound Ignore: only the Reply");
    f.feed(msg::SendResult{peer, msg::SendTag{conv, 0}, true, SocketError::Unknown});
    check(f.events().empty(), "outbound Ignore: no MsgStatus");
}

// ------------------------------------------------------------------ receiving

void test_receive_and_subscriptions() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);

    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer, "one"}});
    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer, "two"}});
    auto ev = f.events();
    auto notes = all_of<msg::ConvNotify>(ev);
    check(notes.size() == 2 && notes[1]->conv == conv && notes[1]->unread == 2, "default (Notify): ConvNotify with the unread count");
    check(all_of<msg::MsgAppended>(ev).empty(), "default (Notify): no message bodies");

    f.feed(msg::Subscribe{1, conv, Sub{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify}});
    ev = f.events();
    auto* snap = first_of<msg::ConvSnapshot>(ev);
    check(index_of<msg::Reply>(ev) == 0 && snap && snap->msgs.size() == 2 && snap->msgs[0].body == "one"
              && snap->msgs[1].state == DeliveryState::Received && snap->status == ConvStatus::Connected,
          "subscribe Forward: Reply, then a snapshot of the history");
    check(f.store->convs().find(conv)->read_upto == 2, "snapshot marks the history read");

    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer, "three"}});
    ev = f.events();
    auto* appended = first_of<msg::MsgAppended>(ev);
    check(appended && appended->id == 2 && appended->msg.body == "three" && appended->msg.sender == peer,
          "Forward: MsgAppended for a new message");

    f.feed(msg::Subscribe{2, conv, Sub{Sub::Inbound::Ignore, Sub::Outbound::Ignore, Sub::Lifecycle::Ignore}});
    f.events();
    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer, "four"}});
    check(f.events().empty(), "Ignore: nothing is sent");
    check(f.store->convs().find(conv)->msgs.size() == 4, "Ignore: the message is still stored");

    f.feed(msg::ListReq{3});
    ev = f.events();
    auto* list = first_of<msg::ConvList>(ev);
    check(index_of<msg::Reply>(ev) == 0 && list && list->convs.size() == 1 && list->convs[0].conv == conv
              && list->convs[0].unread == 1 && list->convs[0].status == ConvStatus::Connected && list->convs[0].remote == kPeerA,
          "list: Reply, then one entry with status, address and unread count");
}

void test_receive_drops() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer + 1, "spoofed"}});
    f.feed(msg::ChatIn{peer + 1000, 5, wire::ChatMessage{0, peer + 1000, "stranger"}});
    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0xabc, peer, "unknown group"}});
    check(f.events().empty() && f.store->convs().find(conv)->msgs.empty(),
          "dropped: sender mismatch, unknown peer, unknown group");
}

void test_chat_overtakes_peer_up() {
    Fixture f;
    f.feed(msg::PeerHello{4, wire::ConnectInfo{100, kNoId, {}}, wire::Endpoint{"10.0.0.3", 50000}});
    auto rt = f.routed();
    const PeerId peer = first_of<msg::BindLink>(rt)->peer;
    f.clear();
    f.feed(msg::ChatIn{peer, 4, wire::ChatMessage{0, peer, "early"}});
    auto ev = f.events();
    auto* state = first_of<msg::ConvState>(ev);
    check(state && state->status == ConvStatus::Connected, "chat before PeerUp: the peer counts as up");
    f.feed(msg::PeerUp{peer, 4});
    auto late = f.events();
    check(all_of<msg::ConvState>(late).empty(), "the late PeerUp changes nothing");
}

void test_echo() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    f.feed(msg::Subscribe{1, conv, Sub{Sub::Inbound::Forward, Sub::Outbound::Echo, Sub::Lifecycle::Notify}});
    f.clear();
    f.feed(msg::ChatIn{peer, 5, wire::ChatMessage{0, peer, "ping"}});
    auto rt = f.routed();
    auto* route = first_of<msg::RouteSend>(rt);
    check(route && route->msg.body == "ping" && route->msg.sender_id == 777 && route->tag.conv == kNoId,
          "echo: the message goes straight back, untagged");
    check(f.events().empty() && f.store->convs().find(conv)->msgs.empty(), "echo: not stored, not shown");
    f.feed(msg::SendResult{peer, msg::SendTag{}, false, SocketError::BufferFull});
    check(f.events().empty(), "echo: its send result is ignored");
}

void test_default_subscription() {
    Fixture f;
    f.feed(msg::SetDefaultSubscription{Sub{Sub::Inbound::Forward, Sub::Outbound::Notify, Sub::Lifecycle::Notify}});
    auto [conv, peer] = f.connect_in(100, 1);
    f.feed(msg::ChatIn{peer, 1, wire::ChatMessage{0, peer, "hi"}});
    auto ev = f.events();
    check(first_of<msg::MsgAppended>(ev) != nullptr, "default Forward: messages of a new conversation are forwarded");
}

// ------------------------------------------------------------------ close, quit

void test_close() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    f.feed(msg::CloseReq{1, conv});
    auto rt = f.routed();
    auto ev = f.events();
    check(first_of<msg::Reply>(ev) && first_of<msg::Reply>(ev)->ok, "close: Reply ok");
    check(first_of<msg::ClosePeer>(rt) && first_of<msg::ClosePeer>(rt)->peer == peer, "close: ClosePeer to the Router");
    f.feed(msg::PeerDown{peer, 5, msg::LinkDownReason::PeerClosed});
    f.feed(msg::CloseReq{2, conv});
    ev = f.events();
    auto replies = all_of<msg::Reply>(ev);
    check(replies.size() == 1 && replies[0]->error == "not connected", "close when down: not connected");
}

void test_quit() {
    Fixture f;
    f.feed(msg::QuitReq{1});
    auto rt = f.routed();
    auto ev = f.events();
    check(first_of<msg::Reply>(ev) && first_of<msg::Reply>(ev)->ok, "quit: Reply ok");
    check(first_of<msg::Shutdown>(rt) && first_of<msg::Shutdown>(rt)->grace == test_config().shutdown_grace,
          "quit: Shutdown to the Router with the grace period");

    f.feed(msg::DialReq{2, kPeerA});
    f.feed(msg::QuitReq{3});
    f.feed(msg::PeerHello{1, wire::ConnectInfo{5, 0, {}}, wire::Endpoint{"10.0.0.3", 1}});
    ev = f.events();
    rt = f.routed();
    auto replies = all_of<msg::Reply>(ev);
    check(replies.size() == 2 && replies[0]->error == "shutting down" && replies[1]->ok,
          "while quitting: requests are refused, a second quit is fine");
    check(all_of<msg::Shutdown>(rt).empty() && first_of<msg::RejectLink>(rt), "while quitting: no second Shutdown, inbound links rejected");

    check(!f.store->done(), "not done before the Router stopped");
    f.feed(msg::RouterStopped{});
    ev = f.events();
    check(first_of<msg::ShutdownDone>(ev) && f.db.flushes == 1 && f.store->done(), "router stopped: flush, ShutdownDone, done()");
}

// ------------------------------------------------------------------ contract and persistence

void test_exactly_one_reply_per_request() {
    Fixture f;
    auto [conv, peer] = f.connect_out(kPeerA, 777, 5);
    std::vector<msg::StoreMsg> requests = {
        msg::SendText{10, conv, "a"},  msg::SendText{11, 999, "b"},   msg::DialReq{12, kPeerB},
        msg::DialReq{13, wire::Endpoint{"x", 0}}, msg::ReconnectReq{14, conv}, msg::ReconnectReq{15, 999},
        msg::Subscribe{16, conv, Sub{}}, msg::Subscribe{17, 999, Sub{}}, msg::ListReq{18},
        msg::CloseReq{19, 999}, msg::CloseReq{20, conv}, msg::SendText{0, conv, "no reply wanted"},
        msg::QuitReq{21}, msg::SendText{22, conv, "late"},
    };
    for (auto& r : requests) { f.feed(std::move(r)); }
    auto ev = f.events();
    std::map<ReqId, int> count;
    for (auto* r : all_of<msg::Reply>(ev)) { ++count[r->req]; }
    bool exactly_one = count.size() == 13;
    for (ReqId id = 10; id <= 22; ++id) { exactly_one = exactly_one && count[id] == 1; }
    check(exactly_one, "every request with req != 0 got exactly one Reply, req 0 got none");
}

void test_load_from_persistence() {
    // The ids the Store's minter would hand out first (same seed): loading them
    // must take them out of circulation.
    IdMinter probe(*test_config().id_seed);
    const PeerId loaded_peer = probe.next();
    const ConvId loaded_conv = probe.next();

    LoadedState load;
    Peer p;
    p.peer_id = loaded_peer;
    p.host_id = 0x2222;
    p.endpoints = {kPeerA};
    p.link_state = Peer::Link::Up;   // must be reset: links never survive a restart
    load.peers.push_back(p);
    Conversation c{loaded_conv, Conversation::Kind::Direct, 0, {loaded_peer}, {}, 1};
    c.msgs.push_back(msg::StoredMessage{0, loaded_peer, "old", {}, DeliveryState::Received});
    c.msgs.push_back(msg::StoredMessage{1, kNoId, "never sent", {}, DeliveryState::PendingSend});
    load.convs.push_back(c);

    Fixture f(test_config(), load);
    check(f.db.loads == 1, "load() is called once at start-up");
    const Conversation* conv = f.store->convs().find(loaded_conv);
    check(conv && conv->msgs.size() == 2 && conv->msgs[1].state == DeliveryState::Failed,
          "a message still pending at load time becomes failed");
    check(f.db.updates.size() == 1 && std::get<2>(f.db.updates[0]) == DeliveryState::Failed, "and that is committed");
    check(conv && f.store->status_of(*conv) == ConvStatus::Disconnected, "a loaded peer starts disconnected");

    f.feed(msg::ReconnectReq{1, loaded_conv});
    auto rt = f.routed();
    auto* dial = first_of<msg::RouteDial>(rt);
    check(dial && dial->hello.peer_id == loaded_peer && dial->hello.assigned_peer_id == 0x2222,
          "reconnect after a restart re-presents the persisted names");

    f.feed(msg::DialReq{2, kPeerB});
    const ConvId new_conv = f.store->convs().order().back();
    const PeerId new_peer = f.store->convs().find(new_conv)->members.front();
    check(f.store->peers().size() == 2 && f.store->convs().size() == 2 && new_peer != loaded_peer
              && new_peer != loaded_conv && new_conv != loaded_peer && new_conv != loaded_conv,
          "new ids never collide with loaded ones");
}

}  // namespace

int main() {
    std::cout << "test_store\n";
    test_dial_flow();
    test_dial_rejects();
    test_dial_failure_and_reconnect();
    test_reconnect_sends_every_endpoint();
    test_inbound_new_peer();
    test_inbound_known_peer();
    test_inbound_garbage();
    test_stale_peer_down_ignored();
    test_send();
    test_send_rejects();
    test_send_size_limit();
    test_outbound_ignore_hides_own_messages();
    test_receive_and_subscriptions();
    test_receive_drops();
    test_chat_overtakes_peer_up();
    test_echo();
    test_default_subscription();
    test_close();
    test_quit();
    test_exactly_one_reply_per_request();
    test_load_from_persistence();
    std::cout << g_checks - g_failures << "/" << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
