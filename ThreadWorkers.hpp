#pragma once

#include "Events.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"
#include <atomic>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

// Where a node accepts inbound connections, advertised in every ConnectInfo it
// sends so a peer can open a return (write) connection. A node's peer ids are
// generated per connection, except that a peer re-presenting the id it was given
// keeps it: see wire::ConnectInfo::assigned_peer_id and PeerTable::assigned_id.
struct LocalEndpoints {
    std::vector<wire::Endpoint> endpoints;
};

// Monotonic source of locally generated peer ids. For each connection a node
// generates the id by which it refers to the *other* side (see ConnectInfo); this
// single source keeps the ids unique across every reader and the store.
uint64_t next_local_peer_id() noexcept;

// Diagnostics channel, separate from the user-facing EventError (which lands in
// the chat UI). Records are pushed onto one shared queue and consumed by the
// single LoggerWorker, so log lines never interleave and never touch the terminal
// the render thread is drawing on.
enum class LogLevel : uint8_t { Info, Warn, Error };

struct LogRecord {
    LogLevel level{LogLevel::Info};
    std::string message;
};

// A cheap, copyable producer handle onto the shared log queue. A default Logger
// has no queue and silently drops every record, so workers can be built without
// a logger.
class Logger {
public:
    Logger() = default;
    explicit Logger(ThreadQueue<LogRecord>* logger_queue) noexcept : queue_(logger_queue) {}

    void info(std::string message) const { log(LogLevel::Info, std::move(message)); }
    void warn(std::string message) const { log(LogLevel::Warn, std::move(message)); }
    void error(std::string message) const { log(LogLevel::Error, std::move(message)); }
    void log(LogLevel level, std::string message) const {
        if (queue_ != nullptr) { (void)queue_->emplace(LogRecord{level, std::move(message)}); }
    }

private:
    ThreadQueue<LogRecord>* queue_{nullptr};
};

// The single consumer: owns the sink and writes one line per record. Stops once
// the queue is shut down and drained.
struct LoggerWorker {
    LoggerWorker(ThreadQueue<LogRecord>& logger_queue, std::ostream& sink);
    void operator()();
private:
    ThreadQueue<LogRecord>& logger_queue_;
    std::ostream& sink_;
};

struct RenderWorker {
    RenderWorker(
        TerminalUI& terminal,
        ThreadQueue<Event>& render_queue,
        ThreadQueue<Event>& store_queue,
        Logger logger = {}
    );
    void operator()();
private:
    TerminalUI& terminal_;
    ThreadQueue<Event>& render_queue_;
    ThreadQueue<Event>& store_queue_;
    Logger logger_;
};

// Owns every conversation's history and the active subscriptions. Readers feed
// it inbound events; the render thread subscribes and requests sends; it
// forwards outbound messages to the sender. Single-threaded: it is the only
// thread that touches the tables below.
//
// It is also the single owner of the local conversation ids: each peer's stable
// peer_id is mapped to a locally allocated conversation id, and the mapping is
// kept across disconnects so a reconnecting peer resumes the same conversation.
struct StoreWorker {
    StoreWorker(
        ThreadQueue<Event>& render_queue,
        ThreadQueue<Event>& store_queue,
        ThreadQueue<Event>& sender_queue,
        Logger logger = {}
    );
    void operator()();
private:
    struct Subscription {
        SubscriptionFlag level{SubscriptionFlag::Message};
        uint32_t count{}; // messages per payload; 0 means no cap
    };

    // Creates `conversation_id` if it does not exist yet. A brand-new conversation
    // is subscribed in notification mode (unless something already subscribed to
    // that id), so every conversation the render has not opened still announces its
    // new messages -- the default for a background conversation.
    ConversationStore& ensure_conversation(uint64_t conversation_id);
    // Records `peer_id` as a peer of `conversation_id` and remembers the mapping,
    // so a later inbound event carrying only a peer_id resolves to this id.
    void bind_peer(uint64_t conversation_id, uint64_t peer_id);
    // Returns the local conversation id for `peer_id`, allocating a fresh one the
    // first time the peer is seen (and reusing it afterwards). Returns 0 for a
    // zero peer id.
    uint64_t conversation_for_peer(uint64_t peer_id);
    // Drops `conversation_id` (and every peer mapping pointing at it) when its
    // history is still empty -- used when a dial fails, so a conversation that
    // never carried a message does not linger in the store.
    void erase_conversation_if_empty(uint64_t conversation_id);
    // Sends the current history (snapshot) or the trailing messages (delta) to
    // the subscription for `conversation_id`, following its level and count. Both
    // also advance the read pointer, because they hand content to the render.
    bool emit_snapshot(uint64_t conversation_id, uint32_t count);
    bool emit_delta(uint64_t conversation_id,
                    const std::vector<ConversationStore::Message>& appended,
                    uint32_t count);
    // Marks everything stored for `conversation_id` as delivered (the read pointer
    // lives in the conversation itself, so it outlives a subscription).
    void advance_read(uint64_t conversation_id);
    // Stored messages never delivered to the render: size() - read pointer.
    uint64_t unread_of(uint64_t conversation_id) const;
    // Orders the subscriptions of one conversation: Notification replaces content
    // delivery, so UseDelta/Message are dropped when it is set. Returns the
    // effective level.
    SubscriptionFlag normalize_level(uint64_t conversation_id, SubscriptionFlag level);
    bool publish(uint64_t conversation_id,
                 const std::vector<ConversationStore::Message>& appended);
    bool on_message(const EventMessage& message);
    bool on_compose(const EventComposeMessage& compose);
    // Sets the subscription for a conversation. A content subscription is the one
    // being viewed, so it returns every other conversation to the background default
    // (a muted conversation, which has no subscription, is left alone).
    bool on_subscribe(const EventSubscribe& sub);
    // Drops the subscription, muting the conversation: nothing is shown and nothing
    // is announced until it is subscribed again.
    bool on_unsubscribe(const EventUnsubscribe& unsub);
    bool on_disconnect(const EventPeerDisconnected& gone);
    // Passes a newly learned peer to the sender so it can open the write channel.
    bool on_peer_connected(const EventPeerConnected& connected);
    // Starts an outbound dial: mints the peer id when the render left it 0, makes
    // sure a conversation exists and forwards the request to the sender.
    bool on_dial_peer(const EventDialPeer& dial);
    // Reports a successful dial to the render, which then subscribes to it.
    bool on_peer_dialed(const EventPeerDialed& dialed);
    // Surfaces a worker error to the render as a system message.
    bool on_error(const EventError& error);
    // Answers a list request with the current conversations and their peers.
    bool on_list_conversations();

    ThreadQueue<Event>& render_queue_;
    ThreadQueue<Event>& store_queue_;
    ThreadQueue<Event>& sender_queue_;
    uint64_t next_conversation_id_{1};                            // next local id to hand out
    std::unordered_set<uint64_t> pending_dials_;                  // dials awaiting an outcome
    std::unordered_map<uint64_t, uint64_t> peer_to_conversation_; // peer_id -> local id
    std::unordered_map<uint64_t, ConversationStore> conversations_; // owned history
    std::unordered_map<uint64_t, Subscription> subscriptions_;      // one per conversation
    Logger logger_;
};

struct KeyboardListenerWorker {
    KeyboardListenerWorker(
        const TerminalUI& terminal,
        ThreadQueue<Event>& render_queue,
        Logger logger = {}
    );
    void stop() noexcept; // makes the reader return within one poll period
    void operator()();
private:
    const TerminalUI& terminal_;
    ThreadQueue<Event>& render_queue_;
    Logger logger_;
    std::atomic<bool> running_{true};
};

namespace p2p {
// Address of a reachable peer. In p2p mode a peer is identified by the stable
// `peer_id` it announces (independent of its address), so a peer that moves to a
// new ip:port keeps the same id and is recognised when it reconnects.
struct PeerTable {
    struct Address {
        std::string ip;
        uint16_t port{};
        friend bool operator==(const Address&, const Address&) = default;
    };

    // A peer is remembered even after it goes away: its address stays usable for
    // a later reconnect, so `lookup` keeps returning it and only the status says
    // whether the peer is currently reachable.
    enum struct Status : uint8_t { Connected, Disconnected };

    struct Entry {
        Address address;
        Status status{Status::Connected};
        // The id this peer uses for us, learned from the handshake, or 0 while it
        // has never told us. Like the address it outlives a disconnect; unlike the
        // address it is not a dial target -- it is echoed back in the next
        // ConnectInfo we send that peer, so the peer recognises us.
        uint64_t assigned_id{};
    };

    // Records an address for `peer_id`. Returns false when the id is already
    // present (one live peer per id); an existing entry is refreshed and marked
    // Connected, so a returning peer is reactivated rather than duplicated. A
    // remembered `assigned_id` is always kept.
    bool insert(uint64_t peer_id, Address address);
    std::optional<Entry> lookup(uint64_t peer_id) const;

    // Remembers the name `peer_id` uses for us. Returns false for an unknown id.
    bool set_assigned_id(uint64_t peer_id, uint64_t assigned_id);

    // Flips the status of a known peer. Both return false for an unknown id.
    bool mark_disconnected(uint64_t peer_id);
    bool mark_connected(uint64_t peer_id);

    bool erase(uint64_t peer_id);

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, Entry> peers_;
};

// Builds and runs the whole application on the main thread: it owns the
// terminal, the queues and the peer table, spawns the store / sender / keyboard
// / listener threads, runs the render loop here, and shuts everything down in
// order once the render loop returns.
struct MainWorker {
    MainWorker(std::string local_address = "127.0.0.1",
               unsigned port = 9000,
               std::string log_path = "app.log");
    void operator()();
private:
    std::string local_address_;
    unsigned port_;
    std::string log_path_;
};

// Monotonic heartbeat that a reader ticks as it makes progress. The watchdog
// compares successive snapshots, so a stalled reader (unchanged counter past a
// deadline) is distinguishable from a healthy one -- unlike a plain bool.
using ReaderTick = std::atomic<uint64_t>;

struct ReaderWorker {
    ReaderWorker(
        ThreadQueue<Event>& store_queue,
        std::shared_ptr<TcpConnection> connection,
        std::shared_ptr<ReaderTick> tick,
        PeerTable& peers,
        Logger logger = {}
    );
    void operator()();
private:
    ThreadQueue<Event>& store_queue_;
    std::shared_ptr<TcpConnection> connection_; // reader owns the live connection while it reads
    std::shared_ptr<ReaderTick> tick_;           // reader owns the flag: freed only once the reader is done
    PeerTable& peers_;
    Logger logger_;
    wire::Decoder decoder_;
    uint64_t peer_id_{0}; // our name for the peer: minted on handshake, or the id it re-presents

    // Keeps the name the peer uses for us, so the next ConnectInfo we send it
    // re-presents that name and it recognises us after a reconnect.
    void remember_assigned_id(uint64_t peer_id, uint64_t assigned_id);
};

struct WatchdogWorker {
    WatchdogWorker() = default;
    void register_reader(const std::shared_ptr<ReaderTick>& tick,
                         const std::shared_ptr<TcpConnection>& connection);
    void stop() noexcept;
    void operator()(); //scans registrations, reaps finished readers, stops stalled ones
private:
    struct Entry {
        std::weak_ptr<ReaderTick> tick;          // never owns: the reader controls the flag's lifetime
        std::weak_ptr<TcpConnection> connection; // used to shut down a stalled reader
        uint64_t last_seen = 0;
        unsigned stalled_scans = 0;              // scans since the tick last changed
    };
    std::mutex mutex_;
    std::vector<Entry> watch_;
    std::atomic<bool> running_{true};
};

struct ListenerWorker {
    ListenerWorker(
        std::string local_address,
        unsigned port,
        ThreadQueue<Event>& store_queue,
        PeerTable& peers,
        int backlog = 16,
        Logger logger = {}
    );
    ~ListenerWorker();
    void stop() noexcept; // unblocks accept(), stops the watchdog
    void operator()();    //internally spawns watchdog
private:
    struct ReaderHandle {
        std::thread thread;
        std::shared_ptr<TcpConnection> connection;
        std::shared_ptr<std::atomic<bool>> done; // set by the reader when it exits
    };
    void reap_finished_readers();

    std::string local_address_;
    unsigned port_;
    int backlog_;
    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    WatchdogWorker watchdog_;
    std::thread watchdog_thread_;
    std::mutex readers_mutex_;
    std::vector<ReaderHandle> readers_;
    Logger logger_;
    std::atomic<bool> running_{true};
};

struct SenderWorker {
    SenderWorker(
        ThreadQueue<Event>& store_queue,
        ThreadQueue<Event>& sender_queue,
        PeerTable& peers,
        LocalEndpoints local = {},
        Logger logger = {}
    );
    void operator()();
private:
    // Returns the cached outbound connection for `peer_id`, creating it from the
    // address in `peers_` on first use and sending a ConnectInfo that names the
    // peer. Returns nullptr when the peer is unknown or the dial fails.
    TcpConnection* ensure_connection(uint64_t peer_id);
    // Dials `ip:port`, sends `hello` as the first frame and caches the connection
    // under `peer_id`. Returns nullptr (closing the socket) on any failure.
    TcpConnection* open(uint64_t peer_id,
                        const std::string& ip,
                        uint16_t port,
                        const wire::Message& hello);
    // Handles a render-initiated dial: opens the connection and reports the
    // outcome to the store (EventPeerDialed on success, EventPeerDisconnected on
    // failure).
    void dial_peer(const EventDialPeer& dial);
    // Answers a peer's ConnectInfo: dials its advertised `endpoints` unless a
    // write connection already exists, and replies with a ConnectInfoAck carrying
    // this node's name for the peer (`peer_id`) and the peer's name for this node
    // (`assigned_peer_id`).
    void establish_ack(uint64_t peer_id,
                       uint64_t assigned_peer_id,
                       const std::vector<wire::Endpoint>& endpoints);
    // Reports an unreachable peer: flips it to Disconnected (if known) and tells
    // the store worker the conversation's peer went away.
    void report_unreachable(uint64_t conversation_id, uint64_t peer_id);

    ThreadQueue<Event>& store_queue_;
    ThreadQueue<Event>& sender_queue_;
    PeerTable& peers_;
    LocalEndpoints local_;                                     // endpoints advertised on dials
    std::unordered_map<uint64_t, TcpConnection> connections_; // outbound, one per peer
    Logger logger_;
};
}
