#pragma once

#include "AppConfig.hpp"
#include "Events.hpp"
#include "Logger.hpp"
#include "OutboundTransport.hpp"
#include "PeerSession.hpp"
#include "TerminalUI.hpp"
#include "ThreadQueue.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <functional>
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
        Logger logger = {},
        bool echo = false
    );
    void operator()();
    // Number of inbound messages bounced back in echo mode (metrics wiring later).
    uint64_t echoed() const noexcept { return echoed_.load(std::memory_order_relaxed); }
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
    // Re-dials an existing conversation's peer without minting a new identity:
    // resolves the conversation to its peer id and forwards the request to the
    // sender (an error to the render when the conversation has no peer).
    bool on_reconnect_peer(const EventReconnectPeer& reconnect);
    // Surfaces a worker error to the render as a system message.
    bool on_error(const EventError& error);
    // Answers a list request with the current conversations and their peers.
    bool on_list_conversations();

    ThreadQueue<Event>& render_queue_;
    ThreadQueue<Event>& store_queue_;
    ThreadQueue<Event>& sender_queue_;
    bool echo_{false};                   // echo mode: bounce inbound messages, store nothing
    std::atomic<uint64_t> echoed_{0};
    uint64_t next_conversation_id_{1};                              // next local id to hand out
    std::unordered_set<uint64_t> pending_dials_;                    // dials awaiting an outcome
    std::unordered_map<uint64_t, uint64_t> peer_to_conversation_;   // peer_id -> local id
    std::unordered_map<uint64_t, ConversationStore> conversations_; // owned history
    std::unordered_map<uint64_t, Subscription> subscriptions_;      // one per conversation
    Logger logger_;
};

struct KeyboardListenerWorker {
    // Polling cadence, and how long an escape sequence may pause between its bytes
    // before the lead byte is treated as a standalone key.
    static constexpr int kDefaultIdleTimeoutMs = 100;
    static constexpr int kDefaultSequenceTimeoutMs = 50;

    KeyboardListenerWorker(
        const TerminalUI& terminal,
        ThreadQueue<Event>& render_queue,
        int idle_timeout_ms = kDefaultIdleTimeoutMs,
        int sequence_timeout_ms = kDefaultSequenceTimeoutMs,
        Logger logger = {}
    );
    void stop() noexcept; // makes the reader return within one poll period
    void operator()();
private:
    const TerminalUI& terminal_;
    ThreadQueue<Event>& render_queue_;
    int idle_timeout_ms_;     // how long one poll may block waiting for a byte
    int sequence_timeout_ms_; // how long the rest of a key sequence may take
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

    // Registers `connection_id` (a reader) as the owner of `peer_id`'s inbound
    // connection and marks the peer connected. A later connection from the same
    // peer -- e.g. a reconnect that reused the peer id -- replaces the owner.
    bool set_connection(uint64_t peer_id, uint64_t connection_id);
    // Releases ownership when a reader exits: marks the peer disconnected and
    // returns true only while `connection_id` is still the owner. A superseded
    // reader (whose connection a newer one replaced) gets false, so it must not
    // report the still-live peer as gone.
    bool release_connection(uint64_t peer_id, uint64_t connection_id);

    bool erase(uint64_t peer_id);

private:
    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, Entry> peers_;
    // Which reader currently owns each peer's inbound connection (peer_id ->
    // connection_id). Kept apart from `peers_` so ownership is independent of
    // whether an address is known and so `insert` never disturbs it.
    std::unordered_map<uint64_t, uint64_t> owners_;
};

// Builds and runs the whole application on the main thread: it owns the
// terminal, the queues and the peer table, spawns the store / sender / keyboard
// / listener threads, runs the render loop here, and shuts everything down in
// order once the render loop returns.
struct MainWorker {
    explicit MainWorker(AppConfig config);
    // Threads-mode defaults with just the listen address, port and log file.
    MainWorker(std::string local_address, unsigned port, std::string log_path);
    void operator()();
private:
    AppConfig config_;
};

// Everything the watchdog needs to know about one reader, and the one record the
// reader and the watchdog share -- there is no separate per-reader handle type.
// `tick` is a monotonic counter the reader bumps as it makes progress: the watchdog
// compares successive snapshots, so a stalled reader (unchanged counter past a
// deadline) is distinguishable from a healthy one -- unlike a plain bool.
// `last_read_ns` is the steady-clock time of the last bytes the reader actually
// received, so the watchdog can rank readers by idleness and drop the least
// recently active ones first when over the reader cap. `connection` is the live
// socket the watchdog shuts down to unblock a stalled or evicted reader, and the
// two flags carry the lifecycle: the reader sets `finished` as it returns (the
// watchdog then owns joining its thread), the watchdog sets `stopping` before it
// shuts the socket down so the same reader is never selected twice.
struct ReaderActivity {
    explicit ReaderActivity(std::shared_ptr<TcpConnection> connection) noexcept
        : connection(std::move(connection)) {}

    std::atomic<uint64_t> tick{0};
    std::atomic<int64_t> last_read_ns{0};
    std::shared_ptr<TcpConnection> connection;
    std::atomic<bool> finished{false};
    std::atomic<bool> stopping{false};
};

struct ReaderWorker {
    // Receive buffer size, and how long one receive may block. The tick period is
    // kept well below the watchdog's stall window so a healthy idle reader still
    // ticks every period instead of looking stuck.
    static constexpr std::size_t kDefaultReadChunk = 4096;
    static constexpr std::chrono::milliseconds kDefaultTickPeriod{200};

    ReaderWorker(
        ThreadQueue<Event>& store_queue,
        std::shared_ptr<ReaderActivity> activity,
        PeerTable& peers,
        std::size_t read_chunk = kDefaultReadChunk,
        std::chrono::milliseconds tick_period = kDefaultTickPeriod,
        Logger logger = {}
    );
    void operator()();
private:
    std::shared_ptr<ReaderActivity> activity_; // heartbeat + live connection, shared with the watchdog
    std::size_t read_chunk_;                // bytes per receive
    std::chrono::milliseconds tick_period_; // bounded receive: the reader's poll period
    PeerSession session_;                   // protocol state for this connection
};

// Owns every reader thread and the reader population. The listener hands it a
// freshly spawned ReaderWorker together with the shared ReaderActivity tracking it,
// and the watchdog joins that thread once the reader reports `finished`. Each scan
// also unsticks a reader whose heartbeat stopped advancing, and `adopt` enforces the
// cap by shutting down the least-recently-active readers -- so the listener keeps no
// reader handles and no prune counter of its own.
struct WatchdogWorker {
    // Scan cadence, and how many consecutive scans a reader's tick may stay unchanged
    // before it counts as wedged. The defaults are the production values; both are
    // constructor parameters so tests can drive the watchdog faster.
    static constexpr std::chrono::milliseconds kDefaultScanInterval{100};
    static constexpr unsigned kDefaultStallScanLimit = 10; // 10 * 100 ms = 1 s

    explicit WatchdogWorker(
        std::size_t max_readers,
        std::chrono::milliseconds scan_interval = kDefaultScanInterval,
        unsigned stall_scan_limit = kDefaultStallScanLimit,
        Logger logger = {}
    ) noexcept;
    ~WatchdogWorker();

    // Takes ownership of a freshly spawned reader: its thread handle and its activity
    // record. Joins readers that have already finished (freeing their slots) and, if
    // this one pushes the population past `max_readers`, shuts down enough
    // least-recently-active readers to make room. A reader adopted after stop() is
    // shut down and joined immediately, so adopt never leaves an unjoined thread.
    void adopt(std::shared_ptr<ReaderActivity> activity, std::thread thread);
    void stop() noexcept;
    void operator()(); // scans readers: joins finished ones, unsticks stalled ones
private:
    struct Entry {
        std::shared_ptr<ReaderActivity> activity; // owns the heartbeat and the socket
        std::thread thread;                       // joined once `activity->finished`
        uint64_t last_seen = 0;                   // last observed tick
        unsigned stalled_scans = 0;               // scans since the tick last changed
        int64_t last_read_ns = 0;                 // snapshot at scan/adopt time: the sort key
    };

    // Joins `entry.thread` when it holds a running thread (tests adopt empty handles).
    static void join_reader(Entry& entry);
    // Marks the reader stopping (so it is never picked twice) and shuts its socket
    // down, which unblocks the reader's receive.
    static void shutdown_reader(Entry& entry) noexcept;
    // Joins and forgets every finished reader. The caller holds `mutex_`.
    void reap_finished_locked();
    // Shuts every remaining reader down and joins it. Idempotent.
    void drain();

    std::mutex mutex_;
    std::vector<Entry> watch_;
    std::size_t max_readers_;                 // hard cap on concurrently running readers (>= 1)
    std::chrono::milliseconds scan_interval_; // watchdog scan cadence
    unsigned stall_scan_limit_;               // scans without a tick before a reader is wedged
    Logger logger_;
    std::atomic<bool> running_{true};
};

struct ListenerWorker {
    // Defaults for the application's listen setup.
    static constexpr std::size_t kDefaultMaxReaders = 8;
    static constexpr int kDefaultBacklog = 16;

    ListenerWorker(
        std::string local_address,
        unsigned port,
        ThreadQueue<Event>& store_queue,
        PeerTable& peers,
        std::size_t max_readers = kDefaultMaxReaders,
        int backlog = kDefaultBacklog,
        Logger logger = {}
    );
    ~ListenerWorker();
    void stop() noexcept; // unblocks accept(), stops the watchdog (which joins the readers)
    void operator()();    // spawns the watchdog internally
    // Called once from operator() after bind succeeds ("ip:port", no error) or fails
    // ("", reason). Set before operator() runs.
    void set_ready_callback(std::function<void(std::string, std::optional<std::string>)> cb) {
        ready_ = std::move(cb);
    }
private:
    std::function<void(std::string, std::optional<std::string>)> ready_;
    std::string local_address_;
    unsigned port_;
    std::size_t max_readers_; // hard cap on concurrently running readers (>= 1)
    int backlog_;
    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    WatchdogWorker watchdog_; // owns the reader threads and enforces max_readers_
    std::thread watchdog_thread_;
    Logger logger_;
    std::atomic<bool> running_{true};
};

struct SenderWorker {
    // Outbound dial/send policy: a failed dial or send is retried `send_attempts`
    // times, `retry_delay` apart, and a peer that stops draining its socket cannot
    // park the sender longer than `send_timeout` per send.
    static constexpr unsigned kDefaultSendAttempts = 3;
    static constexpr std::chrono::milliseconds kDefaultRetryDelay{100};
    static constexpr std::chrono::milliseconds kDefaultSendTimeout{500};

    SenderWorker(
        ThreadQueue<Event>& store_queue,
        ThreadQueue<Event>& sender_queue,
        PeerTable& peers,
        LocalEndpoints local = {},
        unsigned send_attempts = kDefaultSendAttempts,
        std::chrono::milliseconds retry_delay = kDefaultRetryDelay,
        std::chrono::milliseconds send_timeout = kDefaultSendTimeout,
        Logger logger = {}
    );
    // Same policy over a caller-owned transport, which must outlive the sender
    // (the constructor above owns a BlockingOutbound with `send_timeout`).
    SenderWorker(
        ThreadQueue<Event>& store_queue,
        ThreadQueue<Event>& sender_queue,
        PeerTable& peers,
        OutboundTransport& transport,
        LocalEndpoints local = {},
        unsigned send_attempts = kDefaultSendAttempts,
        std::chrono::milliseconds retry_delay = kDefaultRetryDelay,
        Logger logger = {}
    );
    void operator()();
private:
    // Returns the cached outbound connection for `peer_id`, creating it from the
    // address in `peers_` on first use and sending a ConnectInfo that names the
    // peer. Returns nullopt when the peer is unknown or the dial fails.
    std::optional<OutboundTransport::Handle> ensure_connection(uint64_t peer_id);
    // Dials `ip:port`, sends `hello` as the first frame and caches the connection
    // under `peer_id`. Returns nullopt (nothing left open) on any failure.
    std::optional<OutboundTransport::Handle> open(uint64_t peer_id,
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
    // (`assigned_peer_id`). `reestablish` forces a fresh dial (dropping whatever
    // socket the previous session left), used when the peer is a reconnect: reusing
    // a stale socket would swallow the next send and look like a disconnect.
    void establish_ack(uint64_t peer_id,
                       uint64_t assigned_peer_id,
                       const std::vector<wire::Endpoint>& endpoints,
                       bool reestablish);
    // Reports an unreachable peer: flips it to Disconnected (if known) and tells
    // the store worker the conversation's peer went away.
    void report_unreachable(uint64_t conversation_id, uint64_t peer_id);
    // Closes and forgets the cached write connection for `peer_id`. Returns false
    // when none was held. Used when a peer goes away and, before a send, to force a
    // fresh dial for a peer whose connection was closed.
    bool drop_connection(uint64_t peer_id);
    // Re-dials an existing peer (identity reused, no new id minted): drops any
    // cached socket, dials the remembered address re-presenting the assigned id,
    // and reports the outcome to the store (EventPeerDialed / EventPeerDisconnected).
    void reconnect_peer(const EventReconnectPeer& reconnect);

    ThreadQueue<Event>& store_queue_;
    ThreadQueue<Event>& sender_queue_;
    PeerTable& peers_;
    LocalEndpoints local_;                                     // endpoints advertised on dials
    unsigned send_attempts_;                                   // dial/send tries per message
    std::chrono::milliseconds retry_delay_;                    // pause between attempts
    std::unique_ptr<OutboundTransport> owned_transport_;       // set when the sender built its own
    OutboundTransport* transport_;                             // socket ops only; policy stays here
    std::unordered_map<uint64_t, OutboundTransport::Handle> connections_; // outbound, one per peer
    Logger logger_;
};
}
