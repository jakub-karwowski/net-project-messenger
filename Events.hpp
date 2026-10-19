#pragma once

// Events.hpp - the value types the workers exchange.
//
// All cross-thread communication is one closed set of plain values: no
// virtuals, no behaviour, just data moved through a ThreadQueue<Event>. Each
// alternative notes what it means. The variant is move-only: a history snapshot
// is heap-held so a large store never inflates every value in the queue.

#include <cstdint>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "Wire.hpp" // wire::Endpoint, advertised by EventPeerConnected

// One conversation's history. The StoreWorker owns it for its whole lifetime;
// the render thread only ever receives copies (snapshots) or the appended
// messages (deltas).
struct ConversationStore {
    struct Message {
        std::string payload;
        uint64_t sender_id{}; // author's stable identity (a peer_id, or our own)
    };
    uint64_t conversation_id{}; // local, node-unique id (0 means "unassigned")
    std::vector<Message> store;
    std::vector<uint64_t> peer_id; // peers bound to this conversation (one in p2p)
    // Read pointer owned by the store: how many of `store` have already been handed
    // to the render as part of a subscription. Notifications never move it, so
    // `store.size() - read_id` is how much of the conversation is still unread.
    // It lives with the conversation, so it outlives a subscription.
    uint64_t read_id{};
};

// Selects which update kinds a subscriber receives for one conversation. The
// flags combine: `Message` with a count is a full copy, `Message | UseDelta`
// streams only what is appended after that copy, and all three content flags
// together are AllSubscriptions.
//
// Notification is the odd one out: a notification subscription receives no message
// content at all -- every new message is announced with an
// EventConversationNotification -- so it is exclusive with UseDelta (which only
// describes how content is delivered) and it ignores `Message` too. Such a
// subscription never moves the store's read pointer, so it never marks anything
// read.
enum class SubscriptionFlag : uint8_t {
    Message      = 1u << 0, // message content updates
    Error        = 1u << 1, // error events for the conversation
    UseDelta     = 1u << 2, // after an initial copy, send only appended messages
    Notification = 1u << 3, // content-free: announce each new message instead
};

constexpr SubscriptionFlag operator|(SubscriptionFlag a, SubscriptionFlag b) noexcept {
    return static_cast<SubscriptionFlag>(static_cast<uint8_t>(a) | static_cast<uint8_t>(b));
}

constexpr bool has_flag(SubscriptionFlag set, SubscriptionFlag flag) noexcept {
    return (static_cast<uint8_t>(set) & static_cast<uint8_t>(flag)) != 0;
}

// Every content update kind: full copies, deltas and errors. Notification is not
// included: it replaces content delivery rather than adding to it.
constexpr SubscriptionFlag AllSubscriptions =
    SubscriptionFlag::Message | SubscriptionFlag::Error | SubscriptionFlag::UseDelta;

// A key press decoded from the terminal.
struct EventLocal {
    enum class Type : uint8_t {
        TEXT, ENTER, BACKSPACE, ARROW_LEFT, ARROW_RIGHT, ARROW_UP, ARROW_DOWN, EXIT
    };
    Type type{Type::TEXT};
    std::string payload;
};

// Anything arriving from, or destined for, a peer. `peer_id` is the peer's
// stable, self-announced identity (independent of the address it is reachable
// at), while `conversation_id` is this node's local id for the conversation.
//
// Inbound leg (ReaderWorker -> StoreWorker): `conversation_id` is 0 -- the store
// resolves it from `peer_id`, allocating a fresh local id for a peer it has not
// seen before (and reusing the same id after a reconnect).
// Outbound leg (StoreWorker -> SenderWorker): both are set; `peer_id` is the dial
// target and `conversation_id` is carried so a delivery failure can be reported
// against the right conversation.
struct EventMessage {
    uint64_t peer_id{};
    uint64_t conversation_id{};
    std::string payload;
};

// A peer went away; emitted by whichever worker noticed first. The reader
// reports {0, peer_id} (the store resolves the local conversation); the sender
// reports both ids once it knows them.
struct EventPeerDisconnected {
    uint64_t conversation_id{};
    uint64_t peer_id{};
};

// ReaderWorker -> StoreWorker -> SenderWorker: a newly accepted connection has
// identified itself. `peer_id` is the name this node generated for the peer (used
// to route the peer's messages); `assigned_peer_id` is the name the peer assigned
// to this node (echoed back in the ConnectInfoAck); `endpoints` is where the peer
// accepts connections. The store forwards it and the sender answers with a
// ConnectInfoAck. Simplex: the sender opens this node's own outbound (write)
// connection to the peer's endpoints and sends the Ack there (skipping the dial
// when it already holds a write connection, so the two sides converging on one
// connection cannot loop). Duplex: the accepted socket is already in the peer's
// tx slot, so the Ack goes out on it and nothing is dialed.
struct EventPeerConnected {
    uint64_t peer_id{};
    uint64_t assigned_peer_id{};
    std::vector<wire::Endpoint> endpoints;
    // True when the peer re-presented an id we already knew: this is a reconnect,
    // so the sender re-establishes the write connection fresh instead of reusing the
    // socket left over from the previous session (inbound + outbound are one logical
    // connection, and a stale write socket would swallow the next send).
    bool adopted{false};
    // Duplex transports only: the write handle of a previous channel that this
    // Connect's channel replaced in the peer's tx slot (0 = none). The sender
    // closes it. Simplex: always 0.
    int superseded{0};
};

// RenderWorker -> StoreWorker -> SenderWorker: dial a peer known only by address.
// The render fills {ip, port} and leaves `peer_id` 0; the store mints the local
// peer id (the name this node will use for the new peer) and forwards it; the
// sender dials and opens the write connection.
struct EventDialPeer {
    std::string ip;
    uint16_t port{};
    uint64_t peer_id{};
};

// SenderWorker -> StoreWorker -> render: a dial this node started succeeded (the
// sender reports {0, peer_id}). The store resolves the conversation and forwards
// {conversation_id, peer_id} to the render, which then subscribes to it. A failed
// dial is reported with EventPeerDisconnected instead.
struct EventPeerDialed {
    uint64_t conversation_id{};
    uint64_t peer_id{};
};

// RenderWorker -> StoreWorker -> SenderWorker: re-dial the peer of an existing
// conversation, reusing its peer_id instead of minting a new identity. The render
// fills `conversation_id` (peer_id 0); the store resolves that conversation's peer
// and forwards it. The sender dials the remembered address, re-presenting the
// assigned id so the peer adopts the same peer_id and resumes the conversation.
struct EventReconnectPeer {
    uint64_t conversation_id{};
    uint64_t peer_id{};
};

// StoreWorker -> render: a peer finished connecting and was bound to a
// conversation. `reconnect` tells a peer whose conversation already existed from a
// brand-new one: it is true when the connecting peer re-presented the peer id this
// node assigned it (wire::ConnectInfo::assigned_peer_id), so the reader adopted that
// id and the existing conversation was resumed. The render only shows a system
// message for it.
struct EventPeerConnection {
    uint64_t conversation_id{};
    uint64_t peer_id{};
    bool reconnect{};
};

// Any worker -> StoreWorker -> render: a worker-level failure to surface to the
// user (e.g. the listener could not bind its port). The store forwards it and the
// render shows it as a system message; it is not fatal to the process.
struct EventError {
    std::string message;
};

// RenderWorker -> StoreWorker: subscribe to `conversation_id`. `count` caps how many
// messages a copy carries (0 means no cap); it also bounds the initial payload.
//
// A SubscriptionFlag::Notification level puts the conversation in background mode:
// it announces its messages instead of showing them. A content level (Message) is
// the conversation the render is viewing, so it is exclusive -- subscribing to one
// returns every other conversation to the background default (a muted one, which
// has no subscription at all, stays muted).
struct EventSubscribe {
    uint64_t conversation_id{};
    SubscriptionFlag level{SubscriptionFlag::Message};
    uint32_t count{};
};

// RenderWorker -> StoreWorker: drop the subscription for `conversation_id`
// (a zero id drops every subscription). With no subscription a conversation is
// silent: it neither shows nor announces its messages until it is subscribed
// again. This is the mute behind /notify-off.
struct EventUnsubscribe {
    uint64_t conversation_id{};
};

// RenderWorker -> StoreWorker: a locally authored message to store and send.
struct EventComposeMessage {
    uint64_t conversation_id{};
    std::string payload;
};

// RenderWorker -> StoreWorker: asks for the ids of every conversation the store
// currently holds (the keys of its conversation tables). Answered with
// EventConversationList on the render queue.
struct EventListConversations {};

// StoreWorker -> render: a copy of the conversation, trimmed to the
// subscription's count (oldest first). Heap-held because a history can be large
// and must not inflate every value in the queue; the store is empty for an
// unknown id.
struct EventConversationSnapshot {
    std::unique_ptr<ConversationStore> store;
};

// StoreWorker -> render: the messages appended since the previous update,
// trimmed to the subscription's count.
struct EventConversationDelta {
    uint64_t conversation_id{};
    std::vector<ConversationStore::Message> messages;
};

// StoreWorker -> render: a message arrived for a conversation the render only
// holds a SubscriptionFlag::Notification subscription on. It carries no content,
// just enough to say "this conversation has something new", and it does not count
// as read: the store's read pointer only moves when content is delivered.
struct EventConversationNotification {
    uint64_t conversation_id{};
};

// StoreWorker -> render: the ids of every conversation currently held by the
// store, each with the peers bound to it and how much of it the render has never
// been sent, sorted ascending by conversation id so the answer is deterministic.
// Empty when the store has no conversations yet. (Reply to EventListConversations.)
struct ConversationInfo {
    uint64_t conversation_id{};
    std::vector<uint64_t> peer_id; // peers bound to the conversation (one in p2p)
    uint64_t unread{};             // stored messages never delivered to the render
};

struct EventConversationList {
    std::vector<ConversationInfo> conversations;
};

// render -> StoreWorker: switch echo mode. With echo on the store bounces every
// inbound EventMessage straight back to the sender (same peer, same payload) and
// neither stores it nor publishes anything to the render. Used by the stress rig.
struct EventSetEcho {
    bool on{};
};

// Headless input reader -> render: one line typed on the headless command fd.
// `eof` marks the end of the input (equivalent to /quit); `line` is then empty.
// Travelling through the render queue keeps command lines and store events in a
// single total order.
struct EventCommandLine {
    std::string line;
    bool eof{false};
};

using Event = std::variant<
    EventLocal,
    EventMessage,
    EventPeerDisconnected,
    EventPeerConnected,
    EventDialPeer,
    EventPeerDialed,
    EventReconnectPeer,
    EventPeerConnection,
    EventError,
    EventSubscribe,
    EventUnsubscribe,
    EventComposeMessage,
    EventListConversations,
    EventConversationSnapshot,
    EventConversationDelta,
    EventConversationNotification,
    EventConversationList,
    EventSetEcho,
    EventCommandLine>;