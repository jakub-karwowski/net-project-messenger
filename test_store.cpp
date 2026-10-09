// test_store.cpp - harness for StoreWorker.
//
// Plain assertions (no framework): every failed check is printed and the process
// exits non-zero if anything failed. Covers:
//   * combinable subscription flags
//   * a copy subscription: empty snapshot on subscribe, full snapshot on update
//   * a delta subscription: primed with history, then only newly appended messages
//   * content subscriptions being exclusive: the viewed conversation holds it and
//     every other one falls back to announcing, so a muted one stays silent
//   * an Error subscription receiving a peer disconnect
//   * unsubscribe stopping further updates
//   * a composed message being stored, echoed to subscribers and forwarded to the
//     sender queue
//   * a snapshot trimmed to the subscription's message count
//   * listing conversations with the peers bound to each
//   * a local conversation id being allocated for an unseen peer, resumed on
//     reconnect, and resolved from a reader-style disconnect
//   * a peer-connection report being forwarded to the sender so it can dial back
//   * a render dial request: the store mints the peer id, prepares a conversation
//     and forwards it; the outcome is reported to the render, and a failed dial
//     that never carried a message is erased again
//   * a worker error being forwarded to the render
//   * an inbound peer connection binding a conversation and notifying the render,
//     and the same peer id coming back being reported as a reconnect
//   * the notification default: a conversation nobody subscribed to announces its
//     messages, reads nothing, and reports what is left unread on /conversations

#include "Events.hpp"
#include "ThreadQueue.hpp"
#include "ThreadWorkers.hpp"

#include <iostream>
#include <optional>
#include <string>
#include <thread>

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

// A worker wired to three throwaway queues, running until the store queue is
// shut down. Tests push into `store` and read from `render` / `sender`.
struct Harness {
    ThreadQueue<Event> render;
    ThreadQueue<Event> store;
    ThreadQueue<Event> sender;
    StoreWorker worker{render, store, sender};
    std::thread thread{[this] { worker(); }};

    ~Harness() {
        store.graceful_shutdown();
        if (thread.joinable()) { thread.join(); }
    }
};

const ConversationStore* as_snapshot(const Event& event) {
    const auto* snapshot = std::get_if<EventConversationSnapshot>(&event);
    return (snapshot == nullptr || !snapshot->store) ? nullptr : snapshot->store.get();
}

const EventConversationDelta* as_delta(const Event& event) {
    return std::get_if<EventConversationDelta>(&event);
}

// The store subscribes every new conversation in notification mode, so a message
// for a conversation with no content subscription is announced, not delivered.
bool expect_notification(ThreadQueue<Event>& queue, uint64_t conversation_id,
                         const std::string& what) {
    auto event = queue.pop();
    const auto* notice = event ? std::get_if<EventConversationNotification>(&*event) : nullptr;
    check(notice != nullptr, what + ": announced instead of delivered");
    check(notice != nullptr && notice->conversation_id == conversation_id,
          what + ": announcement names the conversation");
    return notice != nullptr;
}

// The unread count a list reports for `conversation_id`, or a sentinel when the
// conversation is missing from the list.
constexpr uint64_t kNotListed = ~uint64_t{0};
uint64_t unread_of(const EventConversationList& list, uint64_t conversation_id) {
    for (const ConversationInfo& info : list.conversations) {
        if (info.conversation_id == conversation_id) { return info.unread; }
    }
    return kNotListed;
}

// ---------------------------------------------------------------------------

void test_subscription_flags() {
    check(has_flag(AllSubscriptions, SubscriptionFlag::Message), "flags: ALL has Message");
    check(has_flag(AllSubscriptions, SubscriptionFlag::Error), "flags: ALL has Error");
    check(has_flag(AllSubscriptions, SubscriptionFlag::UseDelta), "flags: ALL has UseDelta");

    const auto combined = SubscriptionFlag::Message | SubscriptionFlag::Error;
    check(has_flag(combined, SubscriptionFlag::Message), "flags: combined has Message");
    check(has_flag(combined, SubscriptionFlag::Error), "flags: combined has Error");
    check(!has_flag(combined, SubscriptionFlag::UseDelta), "flags: combined lacks UseDelta");
    check(!has_flag(AllSubscriptions, SubscriptionFlag::Notification),
          "flags: ALL is content only, never Notification");
}

void test_copy_updates() {
    Harness harness;

    check(harness.store.emplace(EventSubscribe{5, SubscriptionFlag::Message, 0}),
          "copy: subscribe queued");
    auto initial = harness.render.pop();
    check(initial.has_value(), "copy: initial snapshot delivered");
    if (initial) {
        const ConversationStore* store = as_snapshot(*initial);
        check(store != nullptr, "copy: initial event is a snapshot");
        check(store && store->conversation_id == 5, "copy: snapshot id matches");
        check(store && store->store.empty(), "copy: snapshot starts empty");
    }

    check(harness.store.emplace(EventMessage{5, 5, "hello"}), "copy: message queued");
    auto updated = harness.render.pop();
    check(updated.has_value(), "copy: update delivered");
    if (updated) {
        const ConversationStore* store = as_snapshot(*updated);
        check(store != nullptr, "copy: update is a snapshot");
        check(store && store->store.size() == 1, "copy: snapshot holds one message");
        if (store && !store->store.empty()) {
            check(store->store.back().payload == "hello", "copy: payload matches");
            check(store->store.back().sender_id == 5, "copy: author is the peer");
            check(store->peer_id.size() == 1 && store->peer_id.front() == 5,
                  "copy: peer recorded");
        }
    }
}

void test_delta_updates() {
    Harness harness;

    // Seed the conversation before subscribing so the automatic copy has content.
    check(harness.store.emplace(EventMessage{6, 6, "a"}), "delta: seed queued");
    check(harness.store.emplace(
              EventSubscribe{6, SubscriptionFlag::Message | SubscriptionFlag::UseDelta, 0}),
          "delta: subscribe queued");

    // The seed created the conversation (notification mode), so it was announced
    // before the subscription took over.
    expect_notification(harness.render, 6, "delta: seed");

    // A subscription change is answered with a copy, never a delta.
    auto primed = harness.render.pop();
    check(primed.has_value(), "delta: priming update delivered");
    if (primed) {
        const ConversationStore* store = as_snapshot(*primed);
        check(store != nullptr, "delta: priming event is a copy");
        check(store && store->conversation_id == 6, "delta: priming id matches");
        check(store && store->store.size() == 1, "delta: priming holds the history");
        check(store && !store->store.empty() && store->store.front().payload == "a",
              "delta: priming payload matches");
    }

    // Once primed, later updates are deltas.
    check(harness.store.emplace(EventMessage{6, 6, "b"}), "delta: second message queued");
    auto next = harness.render.pop();
    check(next.has_value(), "delta: follow-up delivered");
    if (next) {
        const auto* delta = as_delta(*next);
        check(delta != nullptr, "delta: follow-up is a delta");
        check(delta && delta->messages.size() == 1, "delta: follow-up holds only new content");
        check(delta && !delta->messages.empty() && delta->messages.front().payload == "b",
              "delta: follow-up payload matches");
    }
}

// Content is the conversation being viewed: subscribing to one returns any other
// content subscription to the background default.
void test_concurrent_subscriptions() {
    Harness harness;

    check(harness.store.emplace(EventMessage{10, 10, "x"}), "concurrent: seed 10");
    check(harness.store.emplace(EventMessage{11, 11, "y"}), "concurrent: seed 11");
    check(harness.store.emplace(EventSubscribe{10, SubscriptionFlag::Message, 0}),
          "concurrent: subscribe 10");
    check(harness.store.emplace(EventSubscribe{11, SubscriptionFlag::Message, 0}),
          "concurrent: subscribe 11");

    expect_notification(harness.render, 10, "concurrent: seed 10");
    expect_notification(harness.render, 11, "concurrent: seed 11");

    auto first = harness.render.pop();
    const ConversationStore* a = first ? as_snapshot(*first) : nullptr;
    check(a != nullptr && a->conversation_id == 10, "concurrent: first update is conversation 10");
    check(a != nullptr && !a->store.empty() && a->store.front().payload == "x",
          "concurrent: conversation 10 content");

    auto second = harness.render.pop();
    const ConversationStore* b = second ? as_snapshot(*second) : nullptr;
    check(b != nullptr && b->conversation_id == 11, "concurrent: second update is conversation 11");
    check(b != nullptr && !b->store.empty() && b->store.front().payload == "y",
          "concurrent: conversation 11 content");

    // Both got their initial copy, but 10 lost the content subscription to 11.
    check(harness.store.emplace(EventMessage{10, 10, "x2"}), "concurrent: 10 message queued");
    expect_notification(harness.render, 10, "concurrent: 10 is background again");

    check(harness.store.emplace(EventMessage{11, 11, "y2"}), "concurrent: 11 message queued");
    auto shown = harness.render.pop();
    const ConversationStore* c = shown ? as_snapshot(*shown) : nullptr;
    check(c != nullptr && c->conversation_id == 11, "concurrent: 11 still shows content");
}

// A muted conversation stays silent even while another one takes the content
// subscription, and its messages keep counting as unread.
void test_muted_conversation_stays_silent() {
    Harness harness;

    check(harness.store.emplace(EventMessage{80, 80, "a"}), "muted: seed 80 queued");
    expect_notification(harness.render, 80, "muted: seed 80");

    check(harness.store.emplace(EventUnsubscribe{80}), "muted: 80 muted");
    check(harness.store.emplace(EventSubscribe{81, SubscriptionFlag::Message, 0}),
          "muted: 81 subscribed");
    check(harness.render.pop().has_value(), "muted: 81 initial copy delivered");

    check(harness.store.emplace(EventMessage{80, 80, "b"}), "muted: message for 80 queued");
    check(harness.store.emplace(EventListConversations{}), "muted: list queued");

    auto reply = harness.render.pop();
    check(reply.has_value(), "muted: sentinel delivered");
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        check(list != nullptr, "muted: a muted conversation emits nothing");
        check(list != nullptr && unread_of(*list, 80) == 2,
              "muted: its messages are still unread");
    }
}

void test_error_subscription() {
    Harness harness;

    // Error-only subscription: registering it must not emit a message update.
    check(harness.store.emplace(EventSubscribe{12, SubscriptionFlag::Error, 0}),
          "error: subscribe queued");
    check(harness.store.emplace(EventPeerDisconnected{12, 12}), "error: disconnect queued");

    auto event = harness.render.pop();
    check(event.has_value(), "error: disconnect delivered");
    if (event) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*event);
        check(gone != nullptr, "error: event is a disconnect");
        check(gone && gone->conversation_id == 12, "error: disconnect conversation matches");
        check(gone && gone->peer_id == 12, "error: disconnect peer id matches");
    }
}

void test_unsubscribe_stops_updates() {
    Harness harness;

    check(harness.store.emplace(EventSubscribe{13, SubscriptionFlag::Message, 0}),
          "unsub: subscribe queued");
    check(harness.render.pop().has_value(), "unsub: initial snapshot delivered");

    check(harness.store.emplace(EventMessage{13, 13, "m"}), "unsub: message queued");
    check(harness.render.pop().has_value(), "unsub: subscribed update delivered");

    check(harness.store.emplace(EventUnsubscribe{13}), "unsub: unsubscribe queued");
    check(harness.store.emplace(EventMessage{13, 13, "n"}), "unsub: trailing message queued");
    // Sentinel: once this subscription's snapshot arrives, every earlier event
    // has been processed -- a stray update for 13 would have been delivered first.
    check(harness.store.emplace(EventSubscribe{99, SubscriptionFlag::Message, 0}),
          "unsub: sentinel subscribe queued");

    auto sentinel = harness.render.pop();
    check(sentinel.has_value(), "unsub: sentinel delivered");
    if (sentinel) {
        const ConversationStore* store = as_snapshot(*sentinel);
        check(store != nullptr && store->conversation_id == 99,
              "unsub: no update after unsubscribe");
    }
}

void test_compose_stores_echoes_and_forwards() {
    Harness harness;

    check(harness.store.emplace(EventMessage{20, 20, "hi"}), "compose: inbound seed queued");
    check(harness.store.emplace(EventSubscribe{20, SubscriptionFlag::Message, 0}),
          "compose: subscribe queued");
    expect_notification(harness.render, 20, "compose: seed");
    check(harness.render.pop().has_value(), "compose: initial snapshot delivered");

    check(harness.store.emplace(EventComposeMessage{20, "out"}), "compose: composed queued");

    auto echoed = harness.render.pop();
    check(echoed.has_value(), "compose: subscriber updated");
    if (echoed) {
        const ConversationStore* store = as_snapshot(*echoed);
        check(store != nullptr && store->store.size() == 2, "compose: message appended to store");
        check(store && !store->store.empty() && store->store.back().payload == "out",
              "compose: appended payload matches");
    }

    auto forward = harness.sender.pop();
    check(forward.has_value(), "compose: message forwarded to sender queue");
    if (forward) {
        const auto* message = std::get_if<EventMessage>(&*forward);
        check(message != nullptr, "compose: forwarded event is a message");
        check(message && message->peer_id == 20, "compose: forward peer id matches");
        check(message && message->conversation_id == 20, "compose: forward conversation matches");
        check(message && message->payload == "out", "compose: forward payload matches");
    }
}

void test_snapshot_trimmed_to_count() {
    Harness harness;

    check(harness.store.emplace(EventMessage{30, 30, "a"}), "trim: seed a");
    check(harness.store.emplace(EventMessage{30, 30, "b"}), "trim: seed b");
    check(harness.store.emplace(EventMessage{30, 30, "c"}), "trim: seed c");
    check(harness.store.emplace(EventSubscribe{30, SubscriptionFlag::Message, 2}),
          "trim: subscribe queued");

    expect_notification(harness.render, 30, "trim: seed a");
    expect_notification(harness.render, 30, "trim: seed b");
    expect_notification(harness.render, 30, "trim: seed c");

    auto event = harness.render.pop();
    check(event.has_value(), "trim: snapshot delivered");
    if (event) {
        const ConversationStore* store = as_snapshot(*event);
        check(store != nullptr, "trim: event is a snapshot");
        check(store && store->store.size() == 2, "trim: snapshot holds only the last two");
        if (store && store->store.size() == 2) {
            check(store->store[0].payload == "b", "trim: oldest kept message");
            check(store->store[1].payload == "c", "trim: newest kept message");
        }
    }
}

void test_list_conversations() {
    Harness harness;

    // An empty store answers with an empty list (and nothing else is emitted).
    check(harness.store.emplace(EventListConversations{}), "list: request queued");
    auto empty = harness.render.pop();
    check(empty.has_value(), "list: empty reply delivered");
    if (empty) {
        const auto* list = std::get_if<EventConversationList>(&*empty);
        check(list != nullptr, "list: reply is a conversation list");
        check(list != nullptr && list->conversations.empty(),
              "list: no conversations before any peer exists");
    }

    // Two conversations seeded out of order come back sorted by local id.
    check(harness.store.emplace(EventMessage{42, 42, "a"}), "list: seed 42");
    check(harness.store.emplace(EventMessage{7, 7, "b"}), "list: seed 7");
    check(harness.store.emplace(EventListConversations{}), "list: second request queued");

    // Neither conversation had a content subscription, so each seed was announced
    // and nothing has been read yet.
    expect_notification(harness.render, 42, "list: seed 42");
    expect_notification(harness.render, 7, "list: seed 7");

    auto reply = harness.render.pop();
    check(reply.has_value(), "list: reply delivered");
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        check(list != nullptr, "list: reply is a conversation list");
        check(list != nullptr && list->conversations.size() == 2,
              "list: two conversations reported");
        if (list != nullptr && list->conversations.size() == 2) {
            check(list->conversations[0].conversation_id == 7,
                  "list: ids sorted (smallest first)");
            check(list->conversations[1].conversation_id == 42,
                  "list: ids sorted (largest last)");
            check(unread_of(*list, 7) == 1, "list: conversation 7 counts its unread message");
            check(unread_of(*list, 42) == 1, "list: conversation 42 counts its unread message");
            check(list->conversations[0].peer_id.size() == 1 &&
                      list->conversations[0].peer_id.front() == 7,
                  "list: peer recorded for conversation 7");
        }
    }
}

// A message from an unseen peer (conversation_id 0) gets a fresh local id.
void test_inbound_allocates_local_id() {
    Harness harness;

    check(harness.store.emplace(EventMessage{77, 0, "hi"}), "alloc: message queued");
    check(harness.store.emplace(EventListConversations{}), "alloc: list queued");

    expect_notification(harness.render, 1, "alloc: seed");

    auto reply = harness.render.pop();
    check(reply.has_value(), "alloc: list reply delivered");
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        check(list != nullptr && list->conversations.size() == 1, "alloc: one conversation");
        if (list != nullptr && list->conversations.size() == 1) {
            check(list->conversations[0].conversation_id == 1, "alloc: first local id is 1");
            check(list->conversations[0].peer_id.size() == 1 &&
                      list->conversations[0].peer_id.front() == 77,
                  "alloc: peer bound to the new conversation");
            check(list->conversations[0].unread == 1, "alloc: never delivered, so unread");
        }
    }
}

// The same peer (after a reconnect) maps back to the same local conversation.
void test_reconnect_resumes_conversation() {
    Harness harness;

    check(harness.store.emplace(EventMessage{88, 0, "first"}), "reconnect: first queued");
    check(harness.store.emplace(EventListConversations{}), "reconnect: list queued");
    expect_notification(harness.render, 1, "reconnect: first");
    auto first = harness.render.pop();
    check(first.has_value(), "reconnect: first list delivered");
    uint64_t conversation_id = 0;
    if (first) {
        const auto* list = std::get_if<EventConversationList>(&*first);
        if (list != nullptr && list->conversations.size() == 1) {
            conversation_id = list->conversations[0].conversation_id;
        }
    }
    check(conversation_id != 0, "reconnect: conversation allocated");

    check(harness.store.emplace(EventMessage{88, 0, "second"}), "reconnect: second queued");
    check(harness.store.emplace(EventListConversations{}), "reconnect: list2 queued");
    expect_notification(harness.render, conversation_id, "reconnect: second");
    auto second = harness.render.pop();
    check(second.has_value(), "reconnect: second list delivered");
    if (second) {
        const auto* list = std::get_if<EventConversationList>(&*second);
        check(list != nullptr && list->conversations.size() == 1,
              "reconnect: still one conversation");
        check(list != nullptr && list->conversations.size() == 1 &&
                  list->conversations[0].conversation_id == conversation_id,
              "reconnect: same local conversation resumed");
    }
}

// A reader-style disconnect ({0, peer_id}) resolves to the local conversation.
void test_disconnect_resolves_local_id() {
    Harness harness;

    check(harness.store.emplace(EventMessage{91, 0, "hi"}), "disconnect: seed queued");
    check(harness.store.emplace(EventListConversations{}), "disconnect: list queued");
    expect_notification(harness.render, 1, "disconnect: seed");
    auto reply = harness.render.pop();
    uint64_t conversation_id = 0;
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        if (list != nullptr && list->conversations.size() == 1) {
            conversation_id = list->conversations[0].conversation_id;
        }
    }
    check(conversation_id != 0, "disconnect: conversation allocated");

    check(harness.store.emplace(EventSubscribe{conversation_id, SubscriptionFlag::Error, 0}),
          "disconnect: error subscribe queued");
    check(harness.store.emplace(EventPeerDisconnected{0, 91}),
          "disconnect: reader-style event queued");

    auto event = harness.render.pop();
    check(event.has_value(), "disconnect: disconnect delivered");
    if (event) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*event);
        check(gone != nullptr, "disconnect: event is a disconnect");
        check(gone && gone->conversation_id == conversation_id,
              "disconnect: local conversation id resolved");
        check(gone && gone->peer_id == 91, "disconnect: peer id preserved");
    }
}

// A reader-style disconnect ({0, peer_id}) is passed through the store to the
// sender, so the sender closes its cached write connection for that peer (the
// reader only knows the peer, not the conversation, so the id is still unresolved
// when it reaches the sender).
void test_disconnect_forwarded_to_sender() {
    Harness harness;

    check(harness.store.emplace(EventPeerDisconnected{0, 123}),
          "propagate: reader-style disconnect queued");

    auto forward = harness.sender.pop();
    check(forward.has_value(), "propagate: forwarded to the sender");
    if (forward) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*forward);
        check(gone != nullptr, "propagate: forwarded event is a disconnect");
        check(gone && gone->peer_id == 123, "propagate: the peer id is preserved");
        check(gone && gone->conversation_id == 0,
              "propagate: the unresolved conversation id is preserved");
    }
}

// A peer connection report is passed through to the sender so it can dial back.
void test_peer_connected_forwarded_to_sender() {
    Harness harness;

    check(harness.store.emplace(
              EventPeerConnected{55, 66, {{"10.0.0.1", 4000}, {"10.0.0.2", 4001}}}),
          "connected: event queued");

    auto forward = harness.sender.pop();
    check(forward.has_value(), "connected: forwarded to the sender queue");
    if (forward) {
        const auto* connected = std::get_if<EventPeerConnected>(&*forward);
        check(connected != nullptr, "connected: forwarded event is a peer-connected");
        check(connected && connected->peer_id == 55, "connected: peer id matches");
        check(connected && connected->assigned_peer_id == 66,
              "connected: the peer's name for us is preserved");
        check(connected && connected->endpoints.size() == 2,
              "connected: endpoints preserved");
    }
}

// A render dial request gets a minted peer id, a prepared conversation and is
// forwarded to the sender.
void test_dial_peer_mints_id_and_forwards() {
    Harness harness;

    check(harness.store.emplace(EventDialPeer{"10.0.0.1", 5000, 0}), "dial: request queued");

    auto request = harness.sender.pop();
    check(request.has_value(), "dial: forwarded to the sender");
    uint64_t peer_id = 0;
    if (request) {
        const auto* dial = std::get_if<EventDialPeer>(&*request);
        check(dial != nullptr, "dial: forwarded event is an EventDialPeer");
        if (dial) {
            peer_id = dial->peer_id;
            check(dial->ip == "10.0.0.1" && dial->port == 5000, "dial: address preserved");
            check(peer_id != 0, "dial: a local peer id was minted");
        }
    }
    check(peer_id != 0, "dial: peer id available");

    // The store prepared a conversation for the new peer.
    check(harness.store.emplace(EventListConversations{}), "dial: list queued");
    auto reply = harness.render.pop();
    check(reply.has_value(), "dial: list reply delivered");
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        check(list != nullptr && list->conversations.size() == 1,
              "dial: a conversation was prepared");
        if (list != nullptr && list->conversations.size() == 1) {
            check(list->conversations[0].peer_id.size() == 1 &&
                      list->conversations[0].peer_id.front() == peer_id,
                  "dial: conversation bound to the new peer");
        }
    }
}

// A successful dial resolves the conversation and reports it to the render.
void test_peer_dialed_reports_conversation_to_render() {
    Harness harness;

    check(harness.store.emplace(EventDialPeer{"10.0.0.1", 5000, 0}), "dialed: request queued");
    auto request = harness.sender.pop();
    check(request.has_value(), "dialed: request forwarded");
    const auto* dial = request ? std::get_if<EventDialPeer>(&*request) : nullptr;
    check(dial != nullptr, "dialed: forwarded event is an EventDialPeer");
    if (dial == nullptr) { return; }

    check(harness.store.emplace(EventPeerDialed{0, dial->peer_id}), "dialed: success queued");
    auto reply = harness.render.pop();
    check(reply.has_value(), "dialed: render notified");
    if (reply) {
        const auto* dialed = std::get_if<EventPeerDialed>(&*reply);
        check(dialed != nullptr, "dialed: reply is an EventPeerDialed");
        check(dialed && dialed->peer_id == dial->peer_id, "dialed: peer id matches");
        check(dialed && dialed->conversation_id != 0, "dialed: conversation id assigned");
    }
}

// A failed dial is reported to the render without a subscription.
void test_dial_failure_reports_disconnect_to_render() {
    Harness harness;

    check(harness.store.emplace(EventDialPeer{"10.0.0.1", 5000, 0}), "fail: request queued");
    auto request = harness.sender.pop();
    check(request.has_value(), "fail: request forwarded");
    const auto* dial = request ? std::get_if<EventDialPeer>(&*request) : nullptr;
    check(dial != nullptr, "fail: forwarded event is an EventDialPeer");
    if (dial == nullptr) { return; }

    check(harness.store.emplace(EventPeerDisconnected{0, dial->peer_id}), "fail: report queued");
    auto reply = harness.render.pop();
    check(reply.has_value(), "fail: render notified");
    if (reply) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*reply);
        check(gone != nullptr, "fail: reply is an EventPeerDisconnected");
        check(gone && gone->peer_id == dial->peer_id, "fail: peer id matches");
        check(gone && gone->conversation_id != 0, "fail: conversation id present");
    }

    // The conversation prepared for the dial never received a message, so it is
    // erased again.
    check(harness.store.emplace(EventListConversations{}), "fail: list queued");
    auto list_reply = harness.render.pop();
    check(list_reply.has_value(), "fail: list reply delivered");
    if (list_reply) {
        const auto* list = std::get_if<EventConversationList>(&*list_reply);
        check(list != nullptr && list->conversations.empty(),
              "fail: the empty conversation was erased");
    }
}

// A failed dial keeps a conversation that already carries a message.
void test_dial_failure_keeps_non_empty_conversation() {
    Harness harness;

    check(harness.store.emplace(EventDialPeer{"10.0.0.1", 5000, 0}), "keep: request queued");
    auto request = harness.sender.pop();
    check(request.has_value(), "keep: request forwarded");
    const auto* dial = request ? std::get_if<EventDialPeer>(&*request) : nullptr;
    check(dial != nullptr, "keep: forwarded event is an EventDialPeer");
    if (dial == nullptr) { return; }
    const uint64_t peer_id = dial->peer_id;

    // A message arrives from the peer before the dial outcome is decided.
    check(harness.store.emplace(EventMessage{peer_id, 0, "hi"}), "keep: message queued");
    check(harness.store.emplace(EventPeerDisconnected{0, peer_id}), "keep: failure queued");
    expect_notification(harness.render, 1, "keep: message");
    auto reply = harness.render.pop();
    check(reply.has_value(), "keep: render notified");
    if (reply) {
        const auto* gone = std::get_if<EventPeerDisconnected>(&*reply);
        check(gone != nullptr, "keep: reply is an EventPeerDisconnected");
        check(gone && gone->peer_id == peer_id, "keep: peer id matches");
    }

    // The conversation survives because it holds a message.
    check(harness.store.emplace(EventListConversations{}), "keep: list queued");
    auto list_reply = harness.render.pop();
    check(list_reply.has_value(), "keep: list reply delivered");
    if (list_reply) {
        const auto* list = std::get_if<EventConversationList>(&*list_reply);
        check(list != nullptr && list->conversations.size() == 1,
              "keep: the non-empty conversation is kept");
    }
}

// A worker error is surfaced to the render.
void test_error_forwarded_to_render() {
    Harness harness;

    check(harness.store.emplace(EventError{"listen failed on 127.0.0.1:9000"}),
          "error: event queued");

    auto reply = harness.render.pop();
    check(reply.has_value(), "error: forwarded to the render");
    if (reply) {
        const auto* error = std::get_if<EventError>(&*reply);
        check(error != nullptr, "error: forwarded event is an EventError");
        check(error && error->message == "listen failed on 127.0.0.1:9000",
              "error: message preserved");
    }
}

// An inbound peer connection binds the peer to a conversation and tells the render.
void test_peer_connected_binds_conversation() {
    Harness harness;

    check(harness.store.emplace(EventPeerConnected{7, 9, {{"10.0.0.1", 4000}}}),
          "connect: event queued");

    auto reply = harness.render.pop();
    check(reply.has_value(), "connect: render notified");
    uint64_t conversation_id = 0;
    if (reply) {
        const auto* notice = std::get_if<EventPeerConnection>(&*reply);
        check(notice != nullptr, "connect: reply is an EventPeerConnection");
        check(notice && notice->peer_id == 7, "connect: peer id matches");
        check(notice && !notice->reconnect, "connect: a first connection is not a reconnect");
        conversation_id = (notice != nullptr) ? notice->conversation_id : 0;
        check(conversation_id != 0, "connect: conversation id assigned");
    }

    auto forward = harness.sender.pop();
    check(forward.has_value(), "connect: forwarded to the sender");
    check(forward && std::holds_alternative<EventPeerConnected>(*forward),
          "connect: forward is the connect event");

    check(harness.store.emplace(EventListConversations{}), "connect: list queued");
    auto list_reply = harness.render.pop();
    check(list_reply.has_value(), "connect: list delivered");
    if (list_reply) {
        const auto* list = std::get_if<EventConversationList>(&*list_reply);
        check(list != nullptr && list->conversations.size() == 1, "connect: one conversation");
        if (list != nullptr && list->conversations.size() == 1) {
            check(list->conversations[0].conversation_id == conversation_id,
                  "connect: conversation id matches");
            check(list->conversations[0].peer_id.size() == 1 &&
                      list->conversations[0].peer_id.front() == 7,
                  "connect: peer bound to the conversation");
        }
    }
}

// A peer that reconnects under an id it already used is reported as a reconnect on
// the same conversation (this is what the reader's id adoption produces).
void test_peer_reconnect_reuses_conversation() {
    Harness harness;

    check(harness.store.emplace(EventPeerConnected{7, 9, {{"10.0.0.1", 4000}}}),
          "back: first connection queued");
    auto first = harness.render.pop();
    check(first.has_value(), "back: render notified");
    uint64_t conversation_id = 0;
    if (first) {
        const auto* notice = std::get_if<EventPeerConnection>(&*first);
        check(notice != nullptr, "back: reply is an EventPeerConnection");
        check(notice && !notice->reconnect, "back: the first connection is not a reconnect");
        conversation_id = (notice != nullptr) ? notice->conversation_id : 0;
    }
    (void)harness.sender.pop(); // the forward for the first connection

    check(harness.store.emplace(EventPeerConnected{7, 44, {{"10.0.0.1", 4001}}}),
          "back: second connection queued");
    auto second = harness.render.pop();
    check(second.has_value(), "back: render notified again");
    if (second) {
        const auto* notice = std::get_if<EventPeerConnection>(&*second);
        check(notice != nullptr, "back: reply is an EventPeerConnection");
        check(notice && notice->peer_id == 7, "back: the adopted peer id is kept");
        check(notice && notice->reconnect, "back: the returning peer is a reconnect");
        check(notice && notice->conversation_id == conversation_id,
              "back: the same conversation is resumed");
    }
    (void)harness.sender.pop();
}

// A notification subscription announces every new message and delivers no content
// at all -- not even the initial copy.
void test_notification_subscription_is_content_free() {
    Harness harness;

    check(harness.store.emplace(EventMessage{40, 40, "old"}), "notify: seed queued");
    check(harness.store.emplace(EventSubscribe{40, SubscriptionFlag::Notification, 0}),
          "notify: subscribe queued");
    // The seed arrived while the conversation was on its default subscription.
    expect_notification(harness.render, 40, "notify: seed");

    check(harness.store.emplace(EventMessage{40, 40, "new"}), "notify: message queued");
    expect_notification(harness.render, 40, "notify: message");

    // A listing is the sentinel: if the subscribe had emitted a copy, it would have
    // arrived before this reply.
    check(harness.store.emplace(EventListConversations{}), "notify: list queued");
    auto reply = harness.render.pop();
    check(reply.has_value(), "notify: sentinel delivered");
    if (reply) {
        const auto* list = std::get_if<EventConversationList>(&*reply);
        check(list != nullptr, "notify: no copy was emitted for the subscription");
        check(list != nullptr && unread_of(*list, 40) == 2,
              "notify: announcements never count as read");
    }
}

// Reading a conversation marks it read, and unsubscribing must not un-read it.
void test_read_pointer_survives_unsubscribe() {
    Harness harness;

    check(harness.store.emplace(EventSubscribe{50, SubscriptionFlag::Message, 0}),
          "read: subscribe queued");
    check(harness.render.pop().has_value(), "read: empty copy delivered");

    check(harness.store.emplace(EventMessage{50, 50, "one"}), "read: message queued");
    check(harness.render.pop().has_value(), "read: message delivered");

    check(harness.store.emplace(EventListConversations{}), "read: list queued");
    auto first_list = harness.render.pop();
    check(first_list.has_value(), "read: list delivered");
    if (first_list) {
        const auto* list = std::get_if<EventConversationList>(&*first_list);
        check(list != nullptr && unread_of(*list, 50) == 0, "read: delivered means read");
    }

    // Unsubscribing -- the mute behind /notify-off -- drops the subscription, so
    // later messages are neither shown nor announced, but they are still unread.
    check(harness.store.emplace(EventUnsubscribe{50}), "read: unsubscribe queued");
    check(harness.store.emplace(EventMessage{50, 50, "two"}), "read: trailing message queued");
    check(harness.store.emplace(EventListConversations{}), "read: second list queued");

    auto second_list = harness.render.pop();
    check(second_list.has_value(), "read: second list delivered");
    if (second_list) {
        const auto* list = std::get_if<EventConversationList>(&*second_list);
        check(list != nullptr && unread_of(*list, 50) == 1,
              "read: the earlier message stays read, the trailing one is unread");
    }
}

// Notification is exclusive with the content flags: they are dropped, while Error
// (which is not content) survives.
void test_notification_flag_drops_content_flags() {
    Harness harness;

    check(harness.store.emplace(EventMessage{60, 60, "seed"}), "flags: seed queued");
    check(harness.store.emplace(EventSubscribe{60, SubscriptionFlag::Notification |
                                                       SubscriptionFlag::Message |
                                                       SubscriptionFlag::UseDelta, 0}),
          "flags: contradictory subscribe queued");
    expect_notification(harness.render, 60, "flags: seed");

    check(harness.store.emplace(EventMessage{60, 60, "next"}), "flags: message queued");
    expect_notification(harness.render, 60, "flags: the message flags were dropped");

    check(harness.store.emplace(EventSubscribe{61, SubscriptionFlag::Notification |
                                                       SubscriptionFlag::Error, 0}),
          "flags: notify+error subscribe queued");
    check(harness.store.emplace(EventPeerDisconnected{61, 61}), "flags: disconnect queued");

    auto event = harness.render.pop();
    check(event.has_value(), "flags: disconnect delivered");
    if (event) {
        check(std::holds_alternative<EventPeerDisconnected>(*event),
              "flags: notify+error still reports errors");
    }
}

// A reconnect request reuses the conversation's existing peer id instead of minting
// a new one, and is forwarded to the sender.
void test_reconnect_peer_resolves_and_forwards() {
    Harness harness;

    // Prepare a conversation bound to a peer with a dial request.
    check(harness.store.emplace(EventDialPeer{"10.0.0.1", 5000, 0}), "recon: dial queued");
    auto dial_event = harness.sender.pop();
    const auto* dial = dial_event ? std::get_if<EventDialPeer>(&*dial_event) : nullptr;
    check(dial != nullptr, "recon: dial forwarded");
    if (dial == nullptr) { return; }

    check(harness.store.emplace(EventListConversations{}), "recon: list queued");
    auto list_event = harness.render.pop();
    const auto* list = list_event ? std::get_if<EventConversationList>(&*list_event) : nullptr;
    check(list != nullptr && list->conversations.size() == 1, "recon: one conversation");
    if (list == nullptr || list->conversations.empty()) { return; }
    const uint64_t conversation_id = list->conversations.front().conversation_id;

    check(harness.store.emplace(EventReconnectPeer{conversation_id, 0}), "recon: request queued");
    auto forwarded = harness.sender.pop();
    check(forwarded.has_value(), "recon: forwarded to the sender");
    if (forwarded) {
        const auto* recon = std::get_if<EventReconnectPeer>(&*forwarded);
        check(recon != nullptr, "recon: forwarded event is an EventReconnectPeer");
        check(recon && recon->peer_id == dial->peer_id, "recon: the peer id is reused");
        check(recon && recon->conversation_id == conversation_id,
              "recon: the conversation is preserved");
    }
}

// A reconnect for a conversation with no bound peer is reported to the render.
void test_reconnect_peer_without_peer_reports_error() {
    Harness harness;

    check(harness.store.emplace(EventReconnectPeer{77, 0}), "recon-miss: request queued");
    auto reply = harness.render.pop();
    check(reply.has_value(), "recon-miss: render notified");
    if (reply) {
        check(std::get_if<EventError>(&*reply) != nullptr,
              "recon-miss: reported as EventError");
    }
}

} // namespace

int main() {
    test_subscription_flags();
    test_copy_updates();
    test_delta_updates();
    test_concurrent_subscriptions();
    test_error_subscription();
    test_unsubscribe_stops_updates();
    test_compose_stores_echoes_and_forwards();
    test_snapshot_trimmed_to_count();
    test_list_conversations();
    test_inbound_allocates_local_id();
    test_reconnect_resumes_conversation();
    test_disconnect_resolves_local_id();
    test_disconnect_forwarded_to_sender();
    test_peer_connected_forwarded_to_sender();
    test_peer_connected_binds_conversation();
    test_peer_reconnect_reuses_conversation();
    test_muted_conversation_stays_silent();
    test_notification_subscription_is_content_free();
    test_read_pointer_survives_unsubscribe();
    test_notification_flag_drops_content_flags();
    test_dial_peer_mints_id_and_forwards();
    test_peer_dialed_reports_conversation_to_render();
    test_dial_failure_reports_disconnect_to_render();
    test_dial_failure_keeps_non_empty_conversation();
    test_reconnect_peer_resolves_and_forwards();
    test_reconnect_peer_without_peer_reports_error();
    test_error_forwarded_to_render();

    std::cout << (g_checks - g_failures) << '/' << g_checks << " checks passed\n";
    return g_failures == 0 ? 0 : 1;
}
