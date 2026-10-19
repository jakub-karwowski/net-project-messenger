// ThreadWorkers.cpp - implementation of the terminal keyboard listener and the
// p2p listener / reader / watchdog trio.
//
// KeyboardListenerWorker decodes terminal bytes into EventLocal values and
// forwards them to a render queue. ESC, Ctrl+C (ISIG is off, so it arrives as
// 0x03) and EOF all quit: an EXIT event is pushed, the queue is gracefully shut
// down, and the worker returns.
//
// One TCP connection is one conversation. ListenerWorker accepts peers and spawns
// one ReaderWorker per connection, handing each thread straight to the single
// WatchdogWorker, which owns the reader threads and the reader population: it joins
// a reader once its heartbeat reports `finished`, shuts down a reader whose tick
// stopped advancing (thread wedged), and -- when a new reader pushes past the cap --
// drops the least-recently-read readers to make room. The reader and the watchdog
// share one ReaderActivity record (heartbeat, live socket, lifecycle flags), so
// there is no per-reader handle for the listener to track.

#include "ThreadWorkers.hpp"

#include "Chat.hpp"
#include "Element.hpp"
#include "Transport.hpp"
#include "Utils.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <format>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

uint64_t next_local_peer_id() noexcept {
    static std::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
}

// --- LoggerWorker -----------------------------------------------------------

namespace {
    std::string_view log_level_name(LogLevel level) {
        switch (level) {
            case LogLevel::Info:  return "INFO";
            case LogLevel::Warn:  return "WARN";
            case LogLevel::Error: return "ERROR";
        }
        return "INFO";
    }

    std::string log_timestamp() {
        const auto now = std::chrono::floor<std::chrono::seconds>(
            std::chrono::system_clock::now());
        return std::format("{:%Y-%m-%d %H:%M:%S}", now);
    }
} // namespace

LoggerWorker::LoggerWorker(ThreadQueue<LogRecord>& logger_queue, std::ostream& sink)
    : logger_queue_(logger_queue), sink_(sink) {}

void LoggerWorker::operator()() {
    // The single writer of the sink: one line per record, flushed so a crash does
    // not lose the tail of the log.
    while (auto record = logger_queue_.pop()) {
        sink_ << '[' << log_timestamp() << "] [" << log_level_name(record->level) << "] "
              << record->message << '\n';
        sink_.flush();
    }
}

// --- KeyboardListenerWorker -------------------------------------------------

namespace {
    // Decodes one key press introduced by `lead`. Returns nullopt for input that
    // carries no EventLocal (a non-arrow CSI sequence, or a control byte that is
    // not a shortcut), so the caller keeps reading.
    std::optional<EventLocal> decode_key(const TerminalUI& terminal, unsigned char lead,
                                         int sequence_timeout_ms) {
        if (lead == 0x1B) { // ESC or an ANSI CSI sequence
            unsigned char next = 0;
            if (terminal.read_byte(next, sequence_timeout_ms) != TerminalUI::ReadResult::Byte
                || next != '[') {
                return EventLocal{EventLocal::Type::EXIT, {}};
            }
            unsigned char code = 0;
            if (terminal.read_byte(code, sequence_timeout_ms) == TerminalUI::ReadResult::Byte) {
                if (code == 'A') { return EventLocal{EventLocal::Type::ARROW_UP, {}}; }
                if (code == 'B') { return EventLocal{EventLocal::Type::ARROW_DOWN, {}}; }
                if (code == 'C') { return EventLocal{EventLocal::Type::ARROW_RIGHT, {}}; }
                if (code == 'D') { return EventLocal{EventLocal::Type::ARROW_LEFT, {}}; }
            }
            return std::nullopt;
        }
        if (lead == 0x03) { return EventLocal{EventLocal::Type::EXIT, {}}; } // Ctrl+C (ISIG off)
        if (lead == 0x7F || lead == 0x08) { return EventLocal{EventLocal::Type::BACKSPACE, {}}; }
        if (lead == '\r' || lead == '\n') { return EventLocal{EventLocal::Type::ENTER, {}}; }
        if (lead < 0x20) { return std::nullopt; } // other control bytes
        return EventLocal{EventLocal::Type::TEXT,
                          terminal.read_utf8_char(lead, sequence_timeout_ms)};
    }
} // namespace

KeyboardListenerWorker::KeyboardListenerWorker(
    const TerminalUI& terminal,
    ThreadQueue<Event>& render_queue,
    int idle_timeout_ms,
    int sequence_timeout_ms,
    Logger logger)
    : terminal_(terminal),
      render_queue_(render_queue),
      idle_timeout_ms_(idle_timeout_ms),
      sequence_timeout_ms_(sequence_timeout_ms),
      logger_(std::move(logger)) {}

void KeyboardListenerWorker::stop() noexcept {
    running_.store(false, std::memory_order_relaxed);
}

void KeyboardListenerWorker::operator()() {
    while (running_.load(std::memory_order_relaxed)) {
        unsigned char lead = 0;
        const TerminalUI::ReadResult result = terminal_.read_byte(lead, idle_timeout_ms_);

        if (result == TerminalUI::ReadResult::Timeout) { continue; } // idle: keep polling

        std::optional<EventLocal> key;
        if (result == TerminalUI::ReadResult::Eof) {
            key = EventLocal{EventLocal::Type::EXIT, {}}; // EOF quits
        } else {
            key = decode_key(terminal_, lead, sequence_timeout_ms_);
            if (!key) { continue; }
        }

        const bool quit = (key->type == EventLocal::Type::EXIT);
        if (!render_queue_.emplace(std::move(*key))) {
            return; // consumer gone: nothing left to deliver to
        }
        if (quit) {
            logger_.info("keyboard: quit");
            render_queue_.graceful_shutdown();
            return;
        }
    }
}

namespace {
    // Monotonic nanoseconds on the steady clock, used to stamp and rank a reader's
    // last real read (see ReaderActivity).
    int64_t steady_now_ns() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }
}

namespace p2p {

// --- PeerTable --------------------------------------------------------------

bool PeerTable::insert(uint64_t peer_id, Address address) {
    std::lock_guard lock(mutex_);
    if (const auto it = peers_.find(peer_id); it != peers_.end()) {
        // A known peer coming back: refresh its address and reactivate it.
        it->second.address = std::move(address);
        it->second.status = Status::Connected;
        return false;
    }
    peers_.emplace(peer_id, Entry{std::move(address), Status::Connected});
    return true;
}

std::optional<PeerTable::Entry> PeerTable::lookup(uint64_t peer_id) const {
    std::lock_guard lock(mutex_);
    const auto it = peers_.find(peer_id);
    if (it == peers_.end()) { return std::nullopt; }
    return it->second;
}

bool PeerTable::set_assigned_id(uint64_t peer_id, uint64_t assigned_id) {
    std::lock_guard lock(mutex_);
    const auto it = peers_.find(peer_id);
    if (it == peers_.end()) { return false; }
    it->second.assigned_id = assigned_id;
    return true;
}

bool PeerTable::mark_disconnected(uint64_t peer_id) {
    std::lock_guard lock(mutex_);
    const auto it = peers_.find(peer_id);
    if (it == peers_.end()) { return false; }
    it->second.status = Status::Disconnected;
    return true;
}

bool PeerTable::mark_connected(uint64_t peer_id) {
    std::lock_guard lock(mutex_);
    const auto it = peers_.find(peer_id);
    if (it == peers_.end()) { return false; }
    it->second.status = Status::Connected;
    return true;
}

bool PeerTable::set_connection(uint64_t peer_id, uint64_t connection_id) {
    if (peer_id == 0 || connection_id == 0) { return false; }
    std::lock_guard lock(mutex_);
    // A newer connection for the same peer takes over ownership; whoever held it
    // before is superseded and must not report the peer gone when it exits.
    owners_[peer_id] = connection_id;
    if (const auto it = peers_.find(peer_id); it != peers_.end()) {
        it->second.status = Status::Connected;
    }
    return true;
}

bool PeerTable::release_connection(uint64_t peer_id, uint64_t connection_id) {
    std::lock_guard lock(mutex_);
    const auto it = owners_.find(peer_id);
    if (it == owners_.end() || it->second != connection_id) {
        return false; // superseded by a newer connection (or never owned it)
    }
    owners_.erase(it);
    if (const auto peer = peers_.find(peer_id); peer != peers_.end()) {
        peer->second.status = Status::Disconnected;
    }
    return true;
}

bool PeerTable::erase(uint64_t peer_id) {
    std::lock_guard lock(mutex_);
    return peers_.erase(peer_id) > 0;
}

// --- ReaderWorker -----------------------------------------------------------

ReaderWorker::ReaderWorker(
    ThreadQueue<Event>& store_queue,
    std::shared_ptr<ReaderActivity> activity,
    PeerTable& peers,
    std::size_t read_chunk,
    std::chrono::milliseconds tick_period,
    Logger logger)
    : activity_(std::move(activity)),
      // A zero buffer or timeout would be unusable: fall back to the defaults.
      read_chunk_(read_chunk == 0 ? kDefaultReadChunk : read_chunk),
      tick_period_(tick_period.count() > 0 ? tick_period : kDefaultTickPeriod),
      session_(store_queue, peers, std::move(logger)) {}

void ReaderWorker::operator()() {
    // Report completion on every exit path (including an early return): the watchdog
    // joins this thread as soon as it observes the flag, so the listener needs no
    // handle to join it with.
    struct FinishGuard {
        std::atomic<bool>& flag;
        ~FinishGuard() { flag.store(true, std::memory_order_release); }
    } finish_guard{activity_->finished};

    TcpConnection& connection = *activity_->connection;
    // The reader owns its polling cadence: a bounded receive keeps the heartbeat
    // advancing even when no data arrives.
    connection.with_receive_timeout(tick_period_);
    std::vector<std::byte> scratch(read_chunk_);

    for (;;) {
        // Heartbeat first: an idle-but-healthy reader still proves liveness each
        // iteration, so only a genuinely wedged thread stops ticking.
        activity_->tick.fetch_add(1, std::memory_order_relaxed);

        auto received = connection.receive(std::span<std::byte>{scratch.data(), scratch.size()});
        if (!received) {
            if (received.error() == SocketError::WouldBlock) {
                continue; // bounded receive timed out; nothing arrived yet
            }
            break; // EOF, reset, or the watchdog shut the connection down
        }
        if (*received == 0) { continue; }

        // A real read: stamp the reader so the watchdog ranks it above idle ones.
        activity_->last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);

        if (!session_.on_bytes(std::span<const std::byte>{scratch.data(), *received})) {
            break; // malformed frame, or the consumer is gone
        }
    }

    session_.on_closed();
}

// --- WatchdogWorker ---------------------------------------------------------

WatchdogWorker::WatchdogWorker(std::size_t max_readers,
                               std::chrono::milliseconds scan_interval,
                               unsigned stall_scan_limit,
                               Logger logger) noexcept
    : max_readers_(max_readers == 0 ? 1 : max_readers),
      // Clamp the configuration like a zero cap: a zero interval would spin, and a
      // zero limit would declare every reader stalled on its first scan.
      scan_interval_(scan_interval.count() > 0 ? scan_interval
                                               : std::chrono::milliseconds{1}),
      stall_scan_limit_(stall_scan_limit == 0 ? 1 : stall_scan_limit),
      logger_(std::move(logger)) {}

WatchdogWorker::~WatchdogWorker() {
    // Belt and braces: operator() already drains on its way out, but a watchdog that
    // never ran must not leave a joinable thread in an Entry (that would terminate).
    stop();
    drain();
}

void WatchdogWorker::stop() noexcept {
    running_.store(false, std::memory_order_relaxed);
}

void WatchdogWorker::join_reader(Entry& entry) {
    if (entry.thread.joinable()) { entry.thread.join(); }
}

void WatchdogWorker::shutdown_reader(Entry& entry) noexcept {
    // Mark first: a later scan (or the prune loop) must not pick this reader again
    // while its thread is still winding down.
    entry.activity->stopping.store(true, std::memory_order_relaxed);
    if (entry.activity->connection) {
        std::ignore = entry.activity->connection->shutdown(TcpConnection::Shutdown::Both);
    }
}

void WatchdogWorker::reap_finished_locked() {
    std::erase_if(watch_, [](Entry& entry) {
        if (!entry.activity->finished.load(std::memory_order_acquire)) { return false; }
        join_reader(entry);
        return true;
    });
}

void WatchdogWorker::adopt(std::shared_ptr<ReaderActivity> activity, std::thread thread) {
    Entry entry;
    entry.activity = std::move(activity);
    entry.thread = std::move(thread);
    entry.last_seen = entry.activity->tick.load(std::memory_order_relaxed);
    // Stamp the ranking key now: the listener set last_read_ns at accept time, so a
    // brand-new reader is the most recently active one and is never its own victim.
    entry.last_read_ns = entry.activity->last_read_ns.load(std::memory_order_relaxed);

    std::lock_guard lock(mutex_);
    if (!running_.load(std::memory_order_relaxed)) {
        // Stopped while this reader was being spawned: take it down with the rest
        // rather than leaving an unjoined thread behind.
        shutdown_reader(entry);
        join_reader(entry);
        return;
    }

    // A reader that already finished frees its slot before we count, so a stale entry
    // cannot make a live reader look like it is over the cap.
    reap_finished_locked();
    watch_.push_back(std::move(entry));

    // Population control. Only readers still running count: an evicted reader keeps
    // its entry until its thread exits (so it can be joined), and counting it again
    // would evict a second, live reader.
    std::size_t live = 0;
    for (const Entry& existing : watch_) {
        if (!existing.activity->stopping.load(std::memory_order_relaxed)) { ++live; }
    }
    if (live <= max_readers_) { return; }

    std::size_t need = live - max_readers_;
    // Oldest reads sort first. The cached timestamps are a stable key, so the
    // comparator never locks and the order cannot shift mid-sort.
    std::sort(watch_.begin(), watch_.end(), [](const Entry& a, const Entry& b) {
        return a.last_read_ns < b.last_read_ns;
    });
    for (Entry& existing : watch_) {
        if (need == 0) { break; }
        if (existing.activity->stopping.load(std::memory_order_relaxed)) { continue; }
        shutdown_reader(existing);
        logger_.info("watchdog: pruned idle reader");
        --need;
    }
}

void WatchdogWorker::operator()() {
    while (running_.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(scan_interval_);

        std::lock_guard lock(mutex_);

        // One pass over the readers: a finished one is joined here (freeing its slot
        // immediately), and one whose heartbeat has not advanced for a full window is
        // shut down. A shut-down reader keeps its entry until the thread really
        // returns, so it can still be joined -- and `stopping` keeps later scans from
        // selecting it again.
        std::erase_if(watch_, [&](Entry& entry) {
            if (entry.activity->finished.load(std::memory_order_acquire)) {
                join_reader(entry);
                return true;
            }
            if (entry.activity->stopping.load(std::memory_order_relaxed)) {
                return false; // already on its way out: wait for `finished`
            }
            entry.last_read_ns = entry.activity->last_read_ns.load(std::memory_order_relaxed);
            const uint64_t now = entry.activity->tick.load(std::memory_order_relaxed);
            if (now != entry.last_seen) {
                entry.last_seen = now;
                entry.stalled_scans = 0;
                return false;
            }
            if (++entry.stalled_scans < stall_scan_limit_) { return false; }
            shutdown_reader(entry);
            logger_.warn("watchdog: stalled reader shut down");
            return false; // kept: the thread still has to be joined
        });
    }

    drain();
}

void WatchdogWorker::drain() {
    std::vector<Entry> entries;
    {
        std::lock_guard lock(mutex_);
        entries.swap(watch_);
    }
    // Shut every socket down first: a reader blocked in receive only returns once its
    // connection is gone, so joining before shutting down could wait forever.
    for (Entry& entry : entries) { shutdown_reader(entry); }
    for (Entry& entry : entries) { join_reader(entry); }
}

// --- ListenerWorker ---------------------------------------------------------

ListenerWorker::ListenerWorker(
    std::string local_address,
    unsigned port,
    ThreadQueue<Event>& store_queue,
    PeerTable& peers,
    std::size_t max_readers,
    int backlog,
    Logger logger)
    : local_address_(std::move(local_address)),
      port_(port),
      max_readers_(max_readers == 0 ? 1 : max_readers),
      backlog_(backlog),
      store_queue_(store_queue),
      peers_(peers),
      watchdog_(max_readers_,
                WatchdogWorker::kDefaultScanInterval,
                WatchdogWorker::kDefaultStallScanLimit,
                logger),
      logger_(std::move(logger)) {}

ListenerWorker::~ListenerWorker() {
    stop();
    // The watchdog joins the reader threads as part of shutting down, so there is
    // nothing left for the listener to reap here.
    if (watchdog_thread_.joinable()) { watchdog_thread_.join(); }
}

void ListenerWorker::stop() noexcept {
    if (!running_.exchange(false)) { return; }
    logger_.info("listener stopping");
    // The watchdog stops scanning and drains its readers (shuts their sockets down,
    // then joins the threads) when its loop exits.
    watchdog_.stop();
    // Break a pending accept(): dial our own listener. The loop observes
    // running_ == false and exits without spawning a reader for the wake socket.
    if (auto wake = TcpConnection::connect(local_address_, port_)) {
        std::ignore = wake->shutdown(TcpConnection::Shutdown::Both);
    }
}

void ListenerWorker::operator()() {
    std::optional<TcpListener> listener;
    try {
        listener.emplace(local_address_, port_, backlog_);
        logger_.info("listening on " + local_address_ + ":" + std::to_string(port_));
    } catch (const std::exception& error) {
        // A busy port (or a bad address) must not take the process down: report it
        // and stop accepting. Outbound dials still work, so the app keeps running.
        const std::string detail = "listen failed on " + local_address_ + ":"
            + std::to_string(port_) + ": " + error.what();
        logger_.error(detail);
        (void)store_queue_.emplace(EventError{detail});
        running_.store(false, std::memory_order_relaxed);
        return;
    }

    watchdog_thread_ = std::thread([this] { watchdog_(); });

    while (running_.load(std::memory_order_relaxed)) {
        auto accepted = listener->accept_client();
        if (!accepted) {
            if (!running_.load(std::memory_order_relaxed)) { break; }
            continue; // transient accept failure
        }
        if (!running_.load(std::memory_order_relaxed)) { break; } // drop the wake socket

        accepted->with_keep_alive(true)
                .with_tcp_nodelay(true);

        // One shared record per reader: heartbeat, live socket and lifecycle flags.
        // The listener keeps no handle of its own -- the thread goes straight to the
        // watchdog, which owns it from here on and joins it when the reader reports
        // `finished`.
        auto activity = std::make_shared<ReaderActivity>(
            std::make_shared<TcpConnection>(std::move(*accepted)));
        // A connection that never reads still needs an ordering floor, so start its
        // stamp at accept time instead of 0.
        activity->last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);

        ReaderWorker reader{store_queue_, activity, peers_,
                            ReaderWorker::kDefaultReadChunk,
                            ReaderWorker::kDefaultTickPeriod,
                            logger_};
        watchdog_.adopt(
            std::move(activity),
            std::thread{[reader = std::move(reader)]() mutable { reader(); }});
    }

    watchdog_.stop();
    if (watchdog_thread_.joinable()) { watchdog_thread_.join(); }
}

// --- SenderWorker -----------------------------------------------------------

SenderWorker::SenderWorker(
    ThreadQueue<Event>& store_queue,
    ThreadQueue<Event>& sender_queue,
    PeerTable& peers,
    LocalEndpoints local,
    unsigned send_attempts,
    std::chrono::milliseconds retry_delay,
    std::chrono::milliseconds send_timeout,
    Logger logger)
    : store_queue_(store_queue),
      sender_queue_(sender_queue),
      peers_(peers),
      local_(std::move(local)),
      send_attempts_(send_attempts == 0 ? 1 : send_attempts),
      retry_delay_(retry_delay),
      owned_transport_(std::make_unique<BlockingOutbound>(
          // A zero timeout would mean "block forever", which is exactly what this
          // bound exists to prevent.
          send_timeout.count() > 0 ? send_timeout : kDefaultSendTimeout)),
      transport_(owned_transport_.get()),
      logger_(std::move(logger)) {}

SenderWorker::SenderWorker(
    ThreadQueue<Event>& store_queue,
    ThreadQueue<Event>& sender_queue,
    PeerTable& peers,
    OutboundTransport& transport,
    LocalEndpoints local,
    unsigned send_attempts,
    std::chrono::milliseconds retry_delay,
    Logger logger)
    : store_queue_(store_queue),
      sender_queue_(sender_queue),
      peers_(peers),
      local_(std::move(local)),
      send_attempts_(send_attempts == 0 ? 1 : send_attempts),
      retry_delay_(retry_delay),
      transport_(&transport),
      logger_(std::move(logger)) {}

std::optional<OutboundTransport::Handle> SenderWorker::ensure_connection(uint64_t peer_id) {
    if (const auto it = connections_.find(peer_id); it != connections_.end()) {
        return it->second; // reuse the connection opened for an earlier message
    }

    const auto entry = peers_.lookup(peer_id);
    if (!entry) { return std::nullopt; } // no known address: nothing to dial

    // Name the peer (that is what ConnectInfo carries), re-present the name the peer
    // gave us (0 until it has told us) and advertise where we accept connections,
    // before sending any message.
    return open(peer_id, entry->address.ip, entry->address.port,
                wire::make_connect(peer_id, entry->assigned_id, local_.endpoints));
}

std::optional<OutboundTransport::Handle> SenderWorker::open(uint64_t peer_id,
                                                            const std::string& ip,
                                                            uint16_t port,
                                                            const wire::Message& hello) {
    const auto frame = wire::encode(hello);
    if (!frame) { return std::nullopt; }
    auto handle = transport_->open(ip, port, *frame);
    if (!handle) { return std::nullopt; }
    connections_[peer_id] = *handle;
    return *handle;
}

void SenderWorker::establish_ack(uint64_t peer_id,
                                 uint64_t assigned_peer_id,
                                 const std::vector<wire::Endpoint>& endpoints,
                                 bool reestablish) {
    // A peer dialled us and named us: open our own write channel to it and answer
    // the handshake, telling it the name we gave it (`peer_id`) and echoing the name
    // it gave us (`assigned_peer_id`). On a reconnect (reestablish) the write side is
    // re-established fresh: the inbound and outbound are one logical connection, so
    // the socket the previous session left behind is dropped rather than reused (a
    // stale socket would swallow the next send and surface as a spurious disconnect).
    if (peer_id == 0) { return; }
    if (reestablish) { drop_connection(peer_id); }
    if (connections_.find(peer_id) != connections_.end()) { return; }
    if (endpoints.empty()) { return; }
    const wire::Endpoint& endpoint = endpoints.front();
    const bool sent = open(peer_id, endpoint.address, endpoint.port,
                           wire::make_connect_ack(peer_id, assigned_peer_id)).has_value();
    if (sent) {
        logger_.info("handshake answer sent: peer#" + std::to_string(peer_id));
    } else {
        logger_.warn("handshake answer failed: peer#" + std::to_string(peer_id));
    }
}

void SenderWorker::dial_peer(const EventDialPeer& dial) {
    // A render-initiated dial: name the new peer and open the write connection,
    // then tell the store so it can hand the conversation to the render.
    bool connected = connections_.find(dial.peer_id) != connections_.end();
    if (!connected) {
        // A render-initiated dial usually names a peer we have never met, so there
        // is no remembered name to re-present; a peer we already know supplies one.
        const auto known = peers_.lookup(dial.peer_id);
        const uint64_t assigned_id = known ? known->assigned_id : 0;
        connected = open(dial.peer_id, dial.ip, dial.port,
                         wire::make_connect(dial.peer_id, assigned_id, local_.endpoints)).has_value();
    }
    if (connected) {
        // Remember where the peer lives: unlike an inbound peer (recorded by the
        // reader) a dialled peer has no address in the table yet, and the compose
        // path looks it up before reusing the cached connection.
        (void)peers_.insert(dial.peer_id, PeerTable::Address{dial.ip, dial.port});
        logger_.info("dial ok: peer#" + std::to_string(dial.peer_id));
        (void)store_queue_.emplace(EventPeerDialed{0, dial.peer_id});
    } else {
        logger_.warn("dial failed: peer#" + std::to_string(dial.peer_id));
        (void)store_queue_.emplace(EventPeerDisconnected{0, dial.peer_id});
    }
}

bool SenderWorker::drop_connection(uint64_t peer_id) {
    const auto it = connections_.find(peer_id);
    if (it == connections_.end()) { return false; }
    transport_->close(it->second);
    connections_.erase(it);
    return true;
}

void SenderWorker::reconnect_peer(const EventReconnectPeer& reconnect) {
    // Re-dial an existing peer without minting a new identity: drop any cached socket
    // so the dial is fresh, and re-present the assigned id so the peer adopts the
    // same peer_id and resumes the conversation.
    const auto entry = peers_.lookup(reconnect.peer_id);
    if (!entry) {
        logger_.warn("reconnect failed: peer#" + std::to_string(reconnect.peer_id)
                     + " unknown");
        report_unreachable(reconnect.conversation_id, reconnect.peer_id);
        return;
    }
    drop_connection(reconnect.peer_id);
    const auto connection =
        open(reconnect.peer_id, entry->address.ip, entry->address.port,
             wire::make_connect(reconnect.peer_id, entry->assigned_id, local_.endpoints));
    if (connection) {
        peers_.mark_connected(reconnect.peer_id);
        logger_.info("reconnected: peer#" + std::to_string(reconnect.peer_id));
        (void)store_queue_.emplace(EventPeerDialed{reconnect.conversation_id, reconnect.peer_id});
    } else {
        logger_.warn("reconnect failed: peer#" + std::to_string(reconnect.peer_id));
        report_unreachable(reconnect.conversation_id, reconnect.peer_id);
    }
}

void SenderWorker::report_unreachable(uint64_t conversation_id, uint64_t peer_id) {
    logger_.warn("peer#" + std::to_string(peer_id) + " unreachable (conversation "
                 + std::to_string(conversation_id) + ")");
    peers_.mark_disconnected(peer_id);
    (void)store_queue_.emplace(EventPeerDisconnected{conversation_id, peer_id});
}

void SenderWorker::operator()() {
    while (auto event = sender_queue_.pop()) {
        if (const auto* dial = std::get_if<EventDialPeer>(&*event)) {
            dial_peer(*dial);
            continue;
        }
        if (const auto* connected = std::get_if<EventPeerConnected>(&*event)) {
            establish_ack(connected->peer_id, connected->assigned_peer_id,
                          connected->endpoints, connected->adopted);
            continue;
        }
        if (const auto* gone = std::get_if<EventPeerDisconnected>(&*event)) {
            // A peer went away: forget its write connection so a returning peer is
            // dialled afresh instead of being answered through a dead socket.
            if (drop_connection(gone->peer_id)) {
                logger_.info("write connection dropped: peer#" + std::to_string(gone->peer_id));
            }
            continue;
        }
        if (const auto* reconnect = std::get_if<EventReconnectPeer>(&*event)) {
            reconnect_peer(*reconnect);
            continue;
        }
        const auto* message = std::get_if<EventMessage>(&*event);
        if (message == nullptr) { continue; } // only handshakes and messages go on the wire

        // No address means the peer was never learned (or was never reachable):
        // retrying cannot help, so fail immediately.
        const auto peer = peers_.lookup(message->peer_id);
        if (!peer) {
            report_unreachable(message->conversation_id, message->peer_id);
            continue;
        }

        // A message on a conversation whose peer was closed (e.g. its unused reader
        // was pruned) reconnects first: forget any cached socket so the dial below is
        // fresh rather than writing into a dead one. The reconnect is silent -- the
        // message is simply delivered on the new connection.
        const bool reconnecting = peer->status == PeerTable::Status::Disconnected;
        if (reconnecting && drop_connection(message->peer_id)) {
            logger_.info("reconnect: dropped stale connection to peer#"
                         + std::to_string(message->peer_id));
        }

        // The peer ignores the wire conversation_id and sender_id (conversations
        // and identities are local); it attributes the message to the connection.
        const auto frame = wire::encode(
            wire::make_chat_message(0, 0, message->payload));

        bool delivered = false;
        for (unsigned attempt = 0; frame && attempt < send_attempts_; ++attempt) {
            const auto connection = ensure_connection(message->peer_id);
            if (connection && transport_->send(*connection, *frame)) {
                peers_.mark_connected(message->peer_id);
                if (reconnecting) {
                    logger_.info("reconnected: peer#" + std::to_string(message->peer_id));
                }
                delivered = true;
                break;
            }
            // The socket is suspect (or the dial failed): drop it so the next
            // attempt starts from a fresh connection.
            drop_connection(message->peer_id);
            if (attempt + 1 < send_attempts_) {
                std::this_thread::sleep_for(retry_delay_);
            }
        }

        if (!delivered) { report_unreachable(message->conversation_id, message->peer_id); }
    }
}

// --- MainWorker -------------------------------------------------------------

MainWorker::MainWorker(AppConfig config) : config_(std::move(config)) {}

MainWorker::MainWorker(std::string local_address, unsigned port, std::string log_path)
    : config_([&] {
          AppConfig config;
          config.address = std::move(local_address);
          config.port = port;
          config.log_path = std::move(log_path);
          return config;
      }()) {}

void MainWorker::operator()() {
    // The main thread owns the terminal and the render loop; every other worker
    // runs on its own thread and talks only through the queues.
    TerminalUI terminal;
    ThreadQueue<Event> render_queue;
    ThreadQueue<Event> store_queue;
    ThreadQueue<Event> sender_queue;
    PeerTable peers;

    // Logging: one queue, one consumer that owns the sink. The file is opened
    // before the logger thread starts and outlives it; stderr is the fallback.
    ThreadQueue<LogRecord> logger_queue;
    std::ofstream log_file{config_.log_path};
    std::ostream& log_sink = log_file.is_open() ? log_file : std::cerr;
    Logger logger{&logger_queue};
    LoggerWorker logger_worker{logger_queue, log_sink};

    StoreWorker store{render_queue, store_queue, sender_queue, logger};
    // TODO(headless/echo/metrics): config_.headless, config_.echo and
    // config_.metrics_path are parsed but not wired yet; a later task adds them.
    NetConfig net;
    net.address = config_.address;
    net.port = config_.port;
    net.max_peers = config_.max_peers;
    IoStack io = make_io_stack(config_.io, net, store_queue, peers, logger);

    SenderWorker sender{store_queue, sender_queue, peers, *io.outbound,
                        LocalEndpoints{{wire::Endpoint{config_.address,
                                                       static_cast<uint16_t>(config_.port)}}},
                        SenderWorker::kDefaultSendAttempts,
                        SenderWorker::kDefaultRetryDelay,
                        logger};
    KeyboardListenerWorker keyboard{
        terminal, render_queue,
        KeyboardListenerWorker::kDefaultIdleTimeoutMs,
        KeyboardListenerWorker::kDefaultSequenceTimeoutMs,
        logger};

    logger.info("main: starting on " + config_.address + ":" + std::to_string(config_.port)
                + " io=" + std::string{to_string(config_.io)});

    std::thread logger_thread{[&logger_worker] { logger_worker(); }};
    std::thread store_thread{[&store] { store(); }};
    std::thread sender_thread{[&sender] { sender(); }};
    std::thread keyboard_thread{[&keyboard] { keyboard(); }};
    std::thread listener_thread{[&io] { io.inbound->run(); }};

    // Render on the main thread; it returns once the keyboard (Esc / Ctrl+C / EOF)
    // or a /quit command ends it.
    RenderWorker render{terminal, render_queue, store_queue, logger};
    render();

    // Orderly shutdown: stop the readers, then drain the store and sender so the
    // remaining threads finish and can be joined.
    logger.info("main: shutting down");
    keyboard.stop();
    io.inbound->stop();
    store_queue.graceful_shutdown();
    sender_queue.graceful_shutdown();
    render_queue.graceful_shutdown();

    keyboard_thread.join();
    store_thread.join();
    sender_thread.join();
    listener_thread.join();

    // The log queue is shut down last, after every producer has stopped, so no
    // record is dropped on the way out.
    logger_queue.graceful_shutdown();
    logger_thread.join();
}

} // namespace p2p

// --- StoreWorker ------------------------------------------------------------

namespace {
    // The last `n` messages of `messages` (all of them when n == 0).
    std::vector<ConversationStore::Message> tail_of(
        const std::vector<ConversationStore::Message>& messages, uint32_t n) {
        if (n == 0 || messages.size() <= n) { return messages; }
        return {messages.end() - static_cast<std::ptrdiff_t>(n), messages.end()};
    }
} // namespace

StoreWorker::StoreWorker(
    ThreadQueue<Event>& render_queue,
    ThreadQueue<Event>& store_queue,
    ThreadQueue<Event>& sender_queue,
    Logger logger)
    : render_queue_(render_queue),
      store_queue_(store_queue),
      sender_queue_(sender_queue),
      logger_(std::move(logger)) {}

ConversationStore& StoreWorker::ensure_conversation(uint64_t conversation_id) {
    const auto [it, inserted] = conversations_.try_emplace(conversation_id);
    ConversationStore& store = it->second;
    if (inserted) {
        store.conversation_id = conversation_id;
        // A new conversation is a background one: announce its messages until the
        // render subscribes for content (or mutes it). A subscription the render
        // already asked for is never overwritten.
        if (!subscriptions_.contains(conversation_id)) {
            subscriptions_[conversation_id] = Subscription{SubscriptionFlag::Notification, 0};
        }
    }
    return store;
}

void StoreWorker::bind_peer(uint64_t conversation_id, uint64_t peer_id) {
    if (peer_id == 0) { return; }
    ConversationStore& store = ensure_conversation(conversation_id);
    if (std::find(store.peer_id.begin(), store.peer_id.end(), peer_id) == store.peer_id.end()) {
        store.peer_id.push_back(peer_id);
    }
    peer_to_conversation_[peer_id] = conversation_id;
    // Keep freshly allocated ids above any locally chosen one.
    if (conversation_id >= next_conversation_id_) { next_conversation_id_ = conversation_id + 1; }
}

uint64_t StoreWorker::conversation_for_peer(uint64_t peer_id) {
    if (peer_id == 0) { return 0; }
    if (const auto it = peer_to_conversation_.find(peer_id); it != peer_to_conversation_.end()) {
        return it->second; // known peer: resume the same local conversation
    }
    const uint64_t conversation_id = next_conversation_id_++;
    bind_peer(conversation_id, peer_id);
    logger_.info("new conversation " + std::to_string(conversation_id)
                 + " for peer#" + std::to_string(peer_id));
    return conversation_id;
}

void StoreWorker::erase_conversation_if_empty(uint64_t conversation_id) {
    const auto it = conversations_.find(conversation_id);
    if (it == conversations_.end() || !it->second.store.empty()) { return; }
    // Forget every peer mapping that points at this store, then drop it.
    for (const uint64_t peer_id : it->second.peer_id) {
        if (const auto mapped = peer_to_conversation_.find(peer_id);
            mapped != peer_to_conversation_.end() && mapped->second == conversation_id) {
            peer_to_conversation_.erase(mapped);
        }
    }
    conversations_.erase(it);
}

bool StoreWorker::emit_snapshot(uint64_t conversation_id, uint32_t count) {
    auto snapshot = std::make_unique<ConversationStore>();
    if (const auto it = conversations_.find(conversation_id); it != conversations_.end()) {
        const ConversationStore& source = it->second;
        snapshot->conversation_id = source.conversation_id;
        snapshot->peer_id = source.peer_id;
        // Copy only the surviving tail, so a trimmed snapshot is a single copy.
        const std::size_t keep = (count == 0 || source.store.size() <= count)
            ? source.store.size() : count;
        snapshot->store.assign(
            source.store.end() - static_cast<std::ptrdiff_t>(keep), source.store.end());
    } else {
        snapshot->conversation_id = conversation_id; // unknown: empty history
    }
    advance_read(conversation_id); // the render has now been sent the history
    return render_queue_.emplace(EventConversationSnapshot{std::move(snapshot)});
}

bool StoreWorker::emit_delta(uint64_t conversation_id,
                             const std::vector<ConversationStore::Message>& appended,
                             uint32_t count) {
    advance_read(conversation_id); // the render has now been sent these messages
    return render_queue_.emplace(
        EventConversationDelta{conversation_id, tail_of(appended, count)});
}

void StoreWorker::advance_read(uint64_t conversation_id) {
    if (const auto it = conversations_.find(conversation_id); it != conversations_.end()) {
        it->second.read_id = it->second.store.size();
    }
}

uint64_t StoreWorker::unread_of(uint64_t conversation_id) const {
    const auto it = conversations_.find(conversation_id);
    if (it == conversations_.end()) { return 0; }
    const uint64_t stored = it->second.store.size();
    return stored > it->second.read_id ? stored - it->second.read_id : 0;
}

SubscriptionFlag StoreWorker::normalize_level(uint64_t conversation_id,
                                              SubscriptionFlag level) {
    if (!has_flag(level, SubscriptionFlag::Notification)) { return level; }
    // A notification subscription is content-free, so the content flags mean
    // nothing: drop them (UseDelta is explicitly exclusive with Notification).
    if (has_flag(level, SubscriptionFlag::UseDelta) || has_flag(level, SubscriptionFlag::Message)) {
        logger_.warn("subscribe: conversation " + std::to_string(conversation_id)
                     + " notification drops the message flags");
    }
    return has_flag(level, SubscriptionFlag::Error)
        ? SubscriptionFlag::Notification | SubscriptionFlag::Error
        : SubscriptionFlag::Notification;
}

bool StoreWorker::publish(uint64_t conversation_id,
                          const std::vector<ConversationStore::Message>& appended) {
    const auto sub = subscriptions_.find(conversation_id);
    if (sub == subscriptions_.end()) { return true; } // nobody listening (or muted)

    if (has_flag(sub->second.level, SubscriptionFlag::Notification)) {
        // Content-free announcement: the render is told there is something new but
        // is sent nothing, and the read pointer stays where it is.
        return render_queue_.emplace(EventConversationNotification{conversation_id});
    }
    if (!has_flag(sub->second.level, SubscriptionFlag::Message)) { return true; }

    if (!has_flag(sub->second.level, SubscriptionFlag::UseDelta)) {
        return emit_snapshot(conversation_id, sub->second.count); // copy subscription
    }

    // Delta subscription: stream only the new messages, but fall back to a copy
    // when a single update carries more than the subscriber's window -- a copy of
    // that window is then the more compact, self-contained answer.
    const auto it = conversations_.find(conversation_id);
    const std::size_t stored = (it == conversations_.end()) ? 0 : it->second.store.size();
    const std::size_t copy_size =
        (sub->second.count == 0 || stored <= sub->second.count) ? stored : sub->second.count;
    if (copy_size != 0 && appended.size() > copy_size) {
        return emit_snapshot(conversation_id, sub->second.count);
    }
    return emit_delta(conversation_id, appended, sub->second.count);
}

bool StoreWorker::on_message(const EventMessage& message) {
    // An inbound event carries only the peer's identity (conversation_id 0): its
    // known local conversation is reused, else a fresh local id is allocated. A
    // direct emitter may pin a conversation id, which then binds the peer to it.
    uint64_t conversation_id = message.conversation_id;
    if (conversation_id == 0) {
        conversation_id = conversation_for_peer(message.peer_id);
        if (conversation_id == 0) { return true; } // no identity: nothing to route
    } else {
        bind_peer(conversation_id, message.peer_id);
    }

    ConversationStore& store = ensure_conversation(conversation_id);
    store.store.push_back(ConversationStore::Message{message.payload, message.peer_id});
    return publish(conversation_id, {store.store.back()});
}

bool StoreWorker::on_compose(const EventComposeMessage& compose) {
    ConversationStore& store = ensure_conversation(compose.conversation_id);
    store.store.push_back(ConversationStore::Message{compose.payload, 0}); // 0 = authored here
    logger_.info("compose: conversation " + std::to_string(compose.conversation_id));

    if (!publish(compose.conversation_id, {store.store.back()})) { return false; }
    if (store.peer_id.empty()) {
        // Stored, but no peer is bound to this conversation yet, so there is
        // nowhere to send it. Only reachable before the first inbound message.
        logger_.warn("compose kept local: conversation " + std::to_string(compose.conversation_id)
                     + " has no peer");
        return true;
    }

    // Forward as an outbound message; the sender resolves the address from peer_id.
    return sender_queue_.emplace(
        EventMessage{store.peer_id.back(), compose.conversation_id, compose.payload});
}

bool StoreWorker::on_subscribe(const EventSubscribe& sub) {
    const SubscriptionFlag level = normalize_level(sub.conversation_id, sub.level);
    subscriptions_[sub.conversation_id] = Subscription{level, sub.count};
    logger_.info("subscribe: conversation " + std::to_string(sub.conversation_id));

    if (has_flag(level, SubscriptionFlag::Message)) {
        // Content is the conversation being viewed, and only one can be viewed at a
        // time: every other conversation falls back to the background default. A
        // muted conversation has no subscription, so it is left muted.
        for (auto& entry : subscriptions_) {
            if (entry.first != sub.conversation_id &&
                has_flag(entry.second.level, SubscriptionFlag::Message)) {
                entry.second = Subscription{SubscriptionFlag::Notification, 0};
                logger_.info("notify on (background): conversation " +
                             std::to_string(entry.first));
            }
        }
    } else {
        // A notification subscription is content-free, so there is no initial copy
        // either -- and nothing is marked read.
        return true;
    }

    // A content subscription starts from a full copy; a delta subscriber then
    // receives only the messages appended after it. An unknown conversation yields
    // an empty copy, so the view still initialises.
    return emit_snapshot(sub.conversation_id, sub.count);
}

bool StoreWorker::on_unsubscribe(const EventUnsubscribe& unsub) {
    logger_.info("unsubscribe: " + (unsub.conversation_id == 0
                                         ? std::string{"all"}
                                         : std::to_string(unsub.conversation_id)));
    if (unsub.conversation_id == 0) { subscriptions_.clear(); }
    else { subscriptions_.erase(unsub.conversation_id); }
    return true;
}

bool StoreWorker::on_disconnect(const EventPeerDisconnected& gone) {
    // The sender holds one write connection per peer: tell it the peer is gone so
    // it drops the socket and dials afresh if the peer comes back. This is sent for
    // every disconnect, before the subscription gate below can swallow it.
    if (gone.peer_id != 0) { (void)sender_queue_.emplace(gone); }

    uint64_t conversation_id = gone.conversation_id;
    if (conversation_id == 0) {
        if (pending_dials_.erase(gone.peer_id) > 0) {
            // A dial we started failed: the render has not subscribed yet, so
            // report it directly instead of via the subscription gate below. A
            // conversation that never carried a message is dropped again.
            conversation_id = conversation_for_peer(gone.peer_id);
            erase_conversation_if_empty(conversation_id);
            return render_queue_.emplace(EventPeerDisconnected{conversation_id, gone.peer_id});
        }
        // The reader reports only a peer id; resolve it to the local conversation.
        const auto mapped = peer_to_conversation_.find(gone.peer_id);
        if (mapped == peer_to_conversation_.end()) { return true; } // no conversation
        conversation_id = mapped->second;
    }
    const auto sub = subscriptions_.find(conversation_id);
    if (sub == subscriptions_.end()) { return true; }
    if (!has_flag(sub->second.level, SubscriptionFlag::Error)) { return true; }
    logger_.info("peer#" + std::to_string(gone.peer_id) + " gone from conversation "
                 + std::to_string(conversation_id));
    return render_queue_.emplace(EventPeerDisconnected{conversation_id, gone.peer_id});
}

bool StoreWorker::on_peer_connected(const EventPeerConnected& connected) {
    // A peer connected: bind it to a conversation and tell the render, which shows a
    // system message. The sender still owns the write channel and decides whether a
    // dial is needed. A peer that re-presented the id we assigned it keeps that id,
    // so it lands on its existing conversation and is reported as a reconnect.
    if (connected.peer_id != 0) {
        const bool reconnect =
            peer_to_conversation_.find(connected.peer_id) != peer_to_conversation_.end();
        const uint64_t conversation_id = conversation_for_peer(connected.peer_id);
        logger_.info("peer#" + std::to_string(connected.peer_id) + " "
                     + (reconnect ? "reconnected" : "connected") + " -> conversation "
                     + std::to_string(conversation_id));
        if (!render_queue_.emplace(
                EventPeerConnection{conversation_id, connected.peer_id, reconnect})) {
            return false;
        }
    }
    return sender_queue_.emplace(connected);
}

bool StoreWorker::on_dial_peer(const EventDialPeer& dial) {
    // The render knows only the address. Mint the peer's local name (unless one
    // was supplied), make sure a conversation exists, and hand the dial to the
    // sender. The conversation id is reported to the render once the dial lands.
    const uint64_t peer_id = (dial.peer_id != 0) ? dial.peer_id : next_local_peer_id();
    logger_.info("dial request: peer#" + std::to_string(peer_id) + " at " + dial.ip + ":"
                 + std::to_string(dial.port));
    (void)conversation_for_peer(peer_id);
    pending_dials_.insert(peer_id);
    return sender_queue_.emplace(EventDialPeer{dial.ip, dial.port, peer_id});
}

bool StoreWorker::on_peer_dialed(const EventPeerDialed& dialed) {
    // The sender reports a successful dial with conversation_id 0; resolve the
    // conversation and tell the render, which then subscribes.
    pending_dials_.erase(dialed.peer_id);
    const uint64_t conversation_id = conversation_for_peer(dialed.peer_id);
    logger_.info("dial succeeded: conversation " + std::to_string(conversation_id));
    return render_queue_.emplace(EventPeerDialed{conversation_id, dialed.peer_id});
}

bool StoreWorker::on_reconnect_peer(const EventReconnectPeer& reconnect) {
    // Re-dial an existing conversation's peer reusing its id: the render only knows
    // the conversation, so resolve it here. No conversation (or no bound peer) means
    // there is nothing to dial, which the render is told as an error.
    const auto it = conversations_.find(reconnect.conversation_id);
    if (it == conversations_.end() || it->second.peer_id.empty()) {
        logger_.warn("reconnect: conversation " + std::to_string(reconnect.conversation_id)
                     + " has no peer");
        return render_queue_.emplace(EventError{
            "conversation " + std::to_string(reconnect.conversation_id)
            + " has no peer to reconnect"});
    }
    const uint64_t peer_id = it->second.peer_id.back();
    logger_.info("reconnect request: conversation " + std::to_string(reconnect.conversation_id)
                 + " -> peer#" + std::to_string(peer_id));
    return sender_queue_.emplace(EventReconnectPeer{reconnect.conversation_id, peer_id});
}

bool StoreWorker::on_error(const EventError& error) {
    // Worker failures are surfaced to the render as-is.
    logger_.error(error.message);
    return render_queue_.emplace(error);
}

bool StoreWorker::on_list_conversations() {
    std::vector<ConversationInfo> conversations;
    conversations.reserve(conversations_.size());
    for (const auto& entry : conversations_) {
        conversations.push_back(
            ConversationInfo{entry.first, entry.second.peer_id, unread_of(entry.first)});
    }
    logger_.info("list: " + std::to_string(conversations.size()) + " conversation(s)");
    std::sort(conversations.begin(), conversations.end(),
              [](const ConversationInfo& a, const ConversationInfo& b) {
                  return a.conversation_id < b.conversation_id;
              });
    return render_queue_.emplace(EventConversationList{std::move(conversations)});
}

void StoreWorker::operator()() {
    while (auto event = store_queue_.pop()) {
        const bool delivered = std::visit([this](const auto& value) -> bool {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, EventMessage>) {
                return on_message(value);
            } else if constexpr (std::is_same_v<Value, EventComposeMessage>) {
                return on_compose(value);
            } else if constexpr (std::is_same_v<Value, EventSubscribe>) {
                return on_subscribe(value);
            } else if constexpr (std::is_same_v<Value, EventUnsubscribe>) {
                return on_unsubscribe(value);
            } else if constexpr (std::is_same_v<Value, EventPeerDisconnected>) {
                return on_disconnect(value);
            } else if constexpr (std::is_same_v<Value, EventPeerConnected>) {
                return on_peer_connected(value);
            } else if constexpr (std::is_same_v<Value, EventDialPeer>) {
                return on_dial_peer(value);
            } else if constexpr (std::is_same_v<Value, EventPeerDialed>) {
                return on_peer_dialed(value);
            } else if constexpr (std::is_same_v<Value, EventReconnectPeer>) {
                return on_reconnect_peer(value);
            } else if constexpr (std::is_same_v<Value, EventError>) {
                return on_error(value);
            } else if constexpr (std::is_same_v<Value, EventListConversations>) {
                return on_list_conversations();
            } else {
                return true; // not addressed to the store worker
            }
        }, *event);

        if (!delivered) { return; } // render consumer gone: nothing left to deliver to
    }
}

// --- RenderWorker -----------------------------------------------------------

namespace {
    // The terminal is laid out at this design size and scaled to the real one.
    constexpr int kRenderWidth = 80;
    constexpr int kRenderHeight = 24;
    constexpr int kInputHeight = 5;
    constexpr int kChatHeight = kRenderHeight - kInputHeight;

    // A fixed-capacity history of submitted input lines. It is a circular buffer:
    // once full, each push overwrites the oldest entry, so the buffer never grows
    // and always holds the kCapacity most recent lines. Index 0 is the oldest,
    // size() - 1 the newest.
    template<size_t kCapacity>
    class InputHistory {
    public:
        void push(std::string entry) {
            entries_[head_] = std::move(entry);
            head_ = (head_ + 1) % kCapacity;
            if (size_ < kCapacity) { ++size_; }
        }

        std::size_t size() const noexcept { return size_; }

        const std::string& at(std::size_t index) const noexcept {
            // Oldest entry sits `size_` slots behind the next write slot.
            const std::size_t oldest = (head_ + kCapacity - size_) % kCapacity;
            return entries_[(oldest + index) % kCapacity];
        }

    private:
        std::array<std::string, kCapacity> entries_{};
        std::size_t head_{0}; // next write slot
        std::size_t size_{0};
    };

    // Display label for a message author: 0 is this node (a locally authored
    // message), anything else is the local name of the peer it came from.
    std::string sender_label(uint64_t sender_id) {
        return sender_id == 0 ? std::string{"me"} : "peer#" + std::to_string(sender_id);
    }

    bool parse_u64(std::string_view text, uint64_t& out) {
        const auto result = std::from_chars(text.data(), text.data() + text.size(), out);
        return result.ec == std::errc{} && result.ptr == text.data() + text.size();
    }

    bool parse_port(std::string_view text, uint16_t& out) {
        unsigned value = 0;
        const auto result = std::from_chars(text.data(), text.data() + text.size(), value);
        if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) { return false; }
        if (value > 0xFFFFu) { return false; }
        out = static_cast<uint16_t>(value);
        return true;
    }

    // The render thread's terminal session: it owns the widgets and translates
    // render-queue events into model mutations and store-queue requests. All
    // terminal output happens here; no other worker touches the UI.
    class RenderSession {
    public:
        RenderSession(TerminalUI& terminal,
                      ThreadQueue<Event>& render_queue,
                      ThreadQueue<Event>& store_queue,
                      Logger logger)
            : terminal_(terminal),
              render_queue_(render_queue),
              store_queue_(store_queue),
              logger_(std::move(logger)),
              chat_model_(std::make_shared<ChatModel>()),
              input_model_(std::make_shared<InputModel>()),
              chat_window_(std::make_shared<ChatWindow>(
                  Element::Position{0, 0}, kRenderWidth, kChatHeight)),
              input_box_(std::make_shared<InputBox>(
                  Element::Position{0, kChatHeight}, kRenderWidth, kInputHeight)),
              layout_(Element::Position{0, 0}, kRenderWidth, kRenderHeight) {
            chat_window_->set_model(chat_model_);
            input_box_->set_model(input_model_);
            input_box_->set_border({Element::Border::BorderType::Asterisk, 1, 1, 0, 0});
            input_box_->set_margin({1, 1, 0, 0});
            layout_.add_element(chat_window_);
            layout_.add_element(input_box_);
        }

        void run() {
            logger_.info("render: started");
            system_message("welcome: /help for commands");
            terminal_.clear();
            render();
            while (running_) {
                auto event = render_queue_.pop();
                if (!event) { break; } // queue shut down and drained
                handle(*event);
                if (running_) { render(); }
            }
            logger_.info("render: stopped");
        }

    private:
        void render() { terminal_.render(layout_); }

        void system_message(std::string text) {
            chat_model_->add_message(std::chrono::system_clock::now(), ">", std::move(text));
        }

        void handle(const Event& event) {
            if (const auto* key = std::get_if<EventLocal>(&event)) {
                handle_key(*key);
            } else {
                handle_store(event);
            }
        }

        void handle_key(const EventLocal& key) {
            switch (key.type) {
                case EventLocal::Type::EXIT:
                    running_ = false;
                    return;
                case EventLocal::Type::TEXT: {
                    const size_t cursor = input_model_->get_cursor();
                    input_model_->apply(std::vector<InputModel::Edit>{
                        Input::put(cursor, key.payload),
                        Input::set_cursor(cursor + key.payload.size())});
                    history_pos_ = history_.size(); // editing detaches the recall
                    return;
                }
                case EventLocal::Type::BACKSPACE: {
                    const size_t cursor = input_model_->get_cursor();
                    if (cursor == 0) { return; }
                    const std::string_view text = std::get<0>(input_model_->get_content());
                    const size_t previous = utils::utf8::prev(text, cursor);
                    input_model_->apply(std::vector<InputModel::Edit>{
                        Input::remove(previous, cursor - previous),
                        Input::set_cursor(previous)});
                    history_pos_ = history_.size(); // editing detaches the recall
                    return;
                }
                case EventLocal::Type::ARROW_LEFT: {
                    const std::string_view text = std::get<0>(input_model_->get_content());
                    input_model_->apply(std::vector<InputModel::Edit>{
                        Input::set_cursor(utils::utf8::prev(text, input_model_->get_cursor()))});
                    return;
                }
                case EventLocal::Type::ARROW_RIGHT: {
                    const std::string_view text = std::get<0>(input_model_->get_content());
                    input_model_->apply(std::vector<InputModel::Edit>{
                        Input::set_cursor(utils::utf8::next(text, input_model_->get_cursor()))});
                    return;
                }
                case EventLocal::Type::ARROW_UP:
                    history_prev();
                    return;
                case EventLocal::Type::ARROW_DOWN:
                    history_next();
                    return;
                case EventLocal::Type::ENTER:
                    submit();
                    return;
            }
        }

        // Enter: an empty line is ignored, a leading '/' is a command, anything
        // else is a message for the current conversation.
        void submit() {
            const std::string line{std::get<0>(input_model_->get_content())};
            input_model_->apply(std::vector<InputModel::Edit>{
                Input::remove(0, line.size()),
                Input::set_cursor(0)});
            if (!line.empty()) { history_.push(line); }
            history_pos_ = history_.size(); // a fresh line starts a new recall
            if (line.empty()) { return; }
            if (line.front() == '/') { run_command(line); }
            else { compose(line); }
        }

        void run_command(std::string_view line) {
            std::vector<std::string_view> parts;
            for (size_t i = 0; i < line.size();) {
                while (i < line.size() && line[i] == ' ') { ++i; }
                const size_t start = i;
                while (i < line.size() && line[i] != ' ') { ++i; }
                if (i > start) { parts.push_back(line.substr(start, i - start)); }
            }
            const std::string_view command = parts.empty() ? std::string_view{} : parts[0];
            logger_.info("command: " + std::string(line));

            if (command == "/quit" || command == "/exit") {
                running_ = false;
            } else if (command == "/help") {
                system_message("/conversations             list known conversations");
                system_message("/current                   show the conversation being viewed");
                system_message("/switch <conversation id>  subscribe to a conversation");
                system_message("/notify-on <conversation id>  announce its new messages");
                system_message("/notify-off <conversation id> stay silent about it");
                system_message("/connect <ip> <port>       dial a new peer");
                system_message("/reconnect <conversation id>  re-dial an existing peer");
                system_message("/quit, /exit               leave");
            } else if (command == "/conv" || command == "/conversations") {
                if (!store_queue_.emplace(EventListConversations{})) { running_ = false; }
            } else if (command == "/current") {
                // Purely informational: the render already knows which conversation
                // it is viewing, so nothing has to be asked of the store.
                if (current_conversation_id_ == 0) {
                    system_message("no conversation selected");
                } else {
                    system_message("current conversation: "
                                   + std::to_string(current_conversation_id_));
                }
            } else if (command == "/switch") {
                uint64_t conversation_id = 0;
                if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
                    system_message("usage: /switch <conversation id>");
                } else {
                    switch_conversation(conversation_id);
                }
            } else if (command == "/notify-on") {
                uint64_t conversation_id = 0;
                if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
                    system_message("usage: /notify-on <conversation id>");
                } else {
                    set_notifications(conversation_id, true);
                }
            } else if (command == "/notify-off") {
                uint64_t conversation_id = 0;
                if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
                    system_message("usage: /notify-off <conversation id>");
                } else {
                    set_notifications(conversation_id, false);
                }
            } else if (command == "/connect") {
                uint16_t port = 0;
                if (parts.size() < 3 || !parse_port(parts[2], port)) {
                    system_message("usage: /connect <ip> <port>");
                } else {
                    system_message("connecting to " + std::string(parts[1]) + ":"
                                   + std::string(parts[2]) + "...");
                    if (!store_queue_.emplace(EventDialPeer{std::string(parts[1]), port, 0})) {
                        running_ = false;
                    }
                }
            } else if (command == "/reconnect") {
                uint64_t conversation_id = 0;
                if (parts.size() < 2 || !parse_u64(parts[1], conversation_id)) {
                    system_message("usage: /reconnect <conversation id>");
                } else {
                    system_message("reconnecting conversation "
                                   + std::to_string(conversation_id) + "...");
                    if (!store_queue_.emplace(EventReconnectPeer{conversation_id, 0})) {
                        running_ = false;
                    }
                }
            } else {
                system_message("unknown command: " + std::string(command));
            }
        }

        void switch_conversation(uint64_t conversation_id) {
            if (conversation_id == 0) {
                system_message("conversation id must be non-zero");
                return;
            }
            if (conversation_id == current_conversation_id_) { return; }
            current_conversation_id_ = conversation_id;
            // The store answers with a full snapshot, and returns whichever
            // conversation was being viewed to the background default.
            if (!store_queue_.emplace(EventSubscribe{conversation_id, AllSubscriptions, 0})) {
                running_ = false;
            }
        }

        // Background conversations announce their messages by default (the store
        // subscribes every new one that way), so this only switches the exception on
        // and off. The conversation being viewed keeps its content subscription.
        void set_notifications(uint64_t conversation_id, bool enabled) {
            if (conversation_id == 0) {
                system_message("conversation id must be non-zero");
                return;
            }
            if (conversation_id == current_conversation_id_) {
                system_message("conversation " + std::to_string(conversation_id)
                               + " is open: notifications follow the view");
                return;
            }
            const bool queued = enabled
                ? store_queue_.emplace(
                      EventSubscribe{conversation_id, SubscriptionFlag::Notification, 0})
                : store_queue_.emplace(EventUnsubscribe{conversation_id});
            if (!queued) { running_ = false; return; }
            system_message(std::string{"notifications "} + (enabled ? "on: " : "off: ")
                           + "conversation " + std::to_string(conversation_id));
        }

        // Overwrites the input field with `text` (rather than appending) and puts
        // the cursor at the end, so a recalled history line is ready to edit.
        void set_input(std::string_view text) {
            const size_t length = std::get<0>(input_model_->get_content()).size();
            input_model_->apply(std::vector<InputModel::Edit>{
                Input::remove(0, length),
                Input::put(0, text),
                Input::set_cursor(text.size())});
        }

        // Up: step towards the oldest remembered line. `history_pos_` is the number
        // of entries from the newest, so decrementing walks backwards in time and 0
        // is the oldest line (or no history at all).
        void history_prev() {
            if (history_pos_ == 0) { return; }
            --history_pos_;
            set_input(history_.at(history_pos_));
        }

        // Down: step towards the newest line; past the newest entry the fresh,
        // empty line is restored.
        void history_next() {
            if (history_pos_ >= history_.size()) { return; }
            ++history_pos_;
            set_input(history_pos_ == history_.size()
                          ? std::string_view{}
                          : std::string_view{history_.at(history_pos_)});
        }

        void compose(const std::string& payload) {
            if (current_conversation_id_ == 0) {
                system_message("no conversation selected: /conv then /switch, or /connect");
                return;
            }
            // The store echoes the message back through the subscription, so it is
            // not added to the model here.
            (void)store_queue_.emplace(EventComposeMessage{current_conversation_id_, payload});
        }

        void handle_store(const Event& event) {
            if (const auto* snapshot = std::get_if<EventConversationSnapshot>(&event)) {
                apply_snapshot(*snapshot);
            } else if (const auto* delta = std::get_if<EventConversationDelta>(&event)) {
                apply_delta(*delta);
            } else if (const auto* notice = std::get_if<EventConversationNotification>(&event)) {
                // A background conversation has something new; the content itself is
                // only fetched on /switch (which sends a fresh copy).
                system_message("new message: conversation "
                               + std::to_string(notice->conversation_id));
            } else if (const auto* list = std::get_if<EventConversationList>(&event)) {
                show_list(*list);
            } else if (const auto* dialed = std::get_if<EventPeerDialed>(&event)) {
                system_message("connected: conversation " + std::to_string(dialed->conversation_id));
                switch_conversation(dialed->conversation_id);
            } else if (const auto* connection = std::get_if<EventPeerConnection>(&event)) {
                system_message("peer#" + std::to_string(connection->peer_id)
                               + (connection->reconnect ? " reconnected" : " connected")
                               + ": conversation "
                               + std::to_string(connection->conversation_id));
            } else if (const auto* gone = std::get_if<EventPeerDisconnected>(&event)) {
                system_message("peer#" + std::to_string(gone->peer_id) + " disconnected"
                               + (gone->conversation_id != 0
                                      ? " (conversation " + std::to_string(gone->conversation_id) + ")"
                                      : std::string{}));
            } else if (const auto* error = std::get_if<EventError>(&event)) {
                system_message("error: " + error->message);
            }
            // Other events are not addressed to the render thread.
        }

        void apply_snapshot(const EventConversationSnapshot& snapshot) {
            if (!snapshot.store) { return; }
            const ConversationStore& source = *snapshot.store;
            if (source.conversation_id != current_conversation_id_) { return; } // stale
            chat_model_->clear();
            if (source.store.empty()) {
                system_message("conversation " + std::to_string(source.conversation_id)
                               + " has no messages yet");
                return;
            }
            for (const auto& message : source.store) {
                chat_model_->add_message(std::chrono::system_clock::now(),
                                         sender_label(message.sender_id), message.payload);
            }
        }

        void apply_delta(const EventConversationDelta& delta) {
            if (delta.conversation_id != current_conversation_id_) { return; }
            for (const auto& message : delta.messages) {
                chat_model_->add_message(std::chrono::system_clock::now(),
                                         sender_label(message.sender_id), message.payload);
            }
        }

        void show_list(const EventConversationList& list) {
            if (list.conversations.empty()) {
                system_message("no conversations");
                return;
            }
            system_message("conversations:");
            for (const ConversationInfo& info : list.conversations) {
                std::string line = "  #" + std::to_string(info.conversation_id);
                for (const uint64_t peer : info.peer_id) {
                    line += " peer#" + std::to_string(peer);
                }
                // A fully read conversation shows no count at all.
                if (info.unread != 0) {
                    line += " (" + std::to_string(info.unread) + " unread)";
                }
                system_message(std::move(line));
            }
        }

        TerminalUI& terminal_;
        ThreadQueue<Event>& render_queue_;
        ThreadQueue<Event>& store_queue_;
        Logger logger_;
        std::shared_ptr<ChatModel> chat_model_;
        std::shared_ptr<InputModel> input_model_;
        std::shared_ptr<ChatWindow> chat_window_;
        std::shared_ptr<InputBox> input_box_;
        AbsoluteLayout layout_;
        uint64_t current_conversation_id_{0};
        // Submitted-line history and the recall cursor into it. `history_pos_` is
        // the number of entries from the newest: history_.size() means the fresh
        // line is being edited, 0 means the oldest remembered line.
        InputHistory<32ULL> history_;
        std::size_t history_pos_{0};
        bool running_{true};
    };
} // namespace

RenderWorker::RenderWorker(
    TerminalUI& terminal,
    ThreadQueue<Event>& render_queue,
    ThreadQueue<Event>& store_queue,
    Logger logger)
    : terminal_(terminal),
      render_queue_(render_queue),
      store_queue_(store_queue),
      logger_(std::move(logger)) {}

void RenderWorker::operator()() {
    RenderSession session{terminal_, render_queue_, store_queue_, logger_};
    session.run();
}
