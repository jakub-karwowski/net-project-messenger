// PeerThreadIo.cpp - one owner thread per peer behind --io=peer-threads.

#include "PeerThreadIo.hpp"

#include "ThreadName.hpp"

#include <chrono>
#include <exception>
#include <limits>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace p2p {

namespace {
int64_t steady_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// pool.receive() blocks until data or close, so an idle owner never ticks: a stall
// scan would shut every idle peer down. Disable it (see PeerThreadIo.hpp).
constexpr unsigned kNoStallDetection = std::numeric_limits<unsigned>::max();
} // namespace

PeerThreadIo::Link::Link(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger,
                         std::shared_ptr<TcpConnection> connection, int handle_, Metrics* metrics,
                         std::size_t max_tx_buffer_bytes)
    : handle(handle_),
      activity(std::make_shared<ReaderActivity>(connection)),
      pool(1, kIdleTimeout, max_tx_buffer_bytes),
      pool_id(pool.add_connection(std::move(connection))),
      channel(store_queue, peers, std::move(logger), ChannelRef{next_local_peer_id(), handle_}, true) {
    channel.set_metrics(metrics);
    // A connection that never reads still needs an ordering floor for the watchdog.
    activity->last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);
}

PeerThreadIo::PeerThreadIo(const NetConfig& net, ThreadQueue<Event>& store_queue, PeerTable& peers,
                           Logger logger, std::size_t max_tx_buffer_bytes)
    : net_(net),
      store_queue_(store_queue),
      peers_(peers),
      logger_(logger),
      max_tx_buffer_bytes_(max_tx_buffer_bytes),
      watchdog_(net.max_peers, WatchdogWorker::kDefaultScanInterval, kNoStallDetection, logger) {
    watchdog_.set_metrics(net_.metrics);
    watchdog_thread_ = std::thread{[this] { set_thread_name("watchdog"); watchdog_(); }};
}

PeerThreadIo::~PeerThreadIo() {
    stopping_.store(true);
    watchdog_.stop();
    join_watchdog(); // its drain shuts every socket down and joins every owner thread
}

void PeerThreadIo::join_watchdog() {
    std::lock_guard lock(join_mutex_);
    if (watchdog_thread_.joinable()) { watchdog_thread_.join(); }
}

std::shared_ptr<PeerThreadIo::Link> PeerThreadIo::find(int handle) {
    std::lock_guard lock(registry_mutex_);
    const auto it = registry_.find(handle);
    return it == registry_.end() ? nullptr : it->second;
}

void PeerThreadIo::publish_tx_stats(Link& link) noexcept {
    if (!net_.metrics) { return; }
    const TxStats now = link.pool.stats();
    // Unsigned deltas: a racing publisher may briefly add a "negative" (wrapped)
    // amount, which the next exchange cancels exactly.
    net_.metrics->tx_queued_sends.fetch_add(
        now.queued_sends - link.published_queued.exchange(now.queued_sends), std::memory_order_relaxed);
    net_.metrics->tx_flushes.fetch_add(
        now.flushes - link.published_flushes.exchange(now.flushes), std::memory_order_relaxed);
}

// --- Attaching ---------------------------------------------------------------

std::shared_ptr<PeerThreadIo::Link> PeerThreadIo::attach(TcpConnection&& connection, bool accepted) {
    // The pool makes the socket non-blocking and applies the options eagerly when it
    // takes it, before the owner thread or the sender can touch it.
    connection.with_tcp_nodelay(true).with_keep_alive(true);
    std::shared_ptr<Link> link;
    try {
        link = std::make_shared<Link>(store_queue_, peers_, logger_,
                                      std::make_shared<TcpConnection>(std::move(connection)),
                                      next_handle_.fetch_add(1), net_.metrics, max_tx_buffer_bytes_);
    } catch (const std::exception&) {
        return nullptr; // descriptor exhaustion: the socket closes with the link
    }
    if (link->pool_id < 0) { return nullptr; }

    {
        std::lock_guard lock(registry_mutex_);
        registry_.emplace(link->handle, link);
    }
    if (accepted) { bump(net_.metrics, &Metrics::accepts); }
    live_inc(net_.metrics);

    std::thread owner;
    try {
        owner = std::thread{[this, link] {
            set_thread_name("pt", static_cast<std::uint64_t>(link->handle));
            owner_loop(link);
        }};
    } catch (const std::exception&) {
        live_dec(net_.metrics);
        std::lock_guard lock(registry_mutex_);
        registry_.erase(link->handle);
        return nullptr;
    }
    // Not under registry_mutex_: adopt() may join threads that need it.
    watchdog_.adopt(link->activity, std::move(owner));
    return link;
}

// --- The owner thread ---------------------------------------------------------

void PeerThreadIo::owner_loop(const std::shared_ptr<Link>& link_ref) {
    Link& link = *link_ref;
    ReaderActivity& activity = *link.activity;

    // Reports completion on every exit path: the watchdog joins this thread as soon
    // as it sees `finished`.
    struct FinishGuard {
        std::atomic<bool>& flag;
        Metrics* metrics;
        ~FinishGuard() {
            live_dec(metrics);
            flag.store(true, std::memory_order_release);
        }
    } finish_guard{activity.finished, net_.metrics};

    std::vector<std::byte> scratch(kReadChunk);
    for (;;) {
        // Ends with {id, 0} on EOF, a read/write error or a watchdog shutdown, and
        // with ConnectionClosed after close_pool() (close() or our own exit).
        auto received = link.pool.receive(std::span<std::byte>{scratch});
        publish_tx_stats(link);
        if (!received) {
            if (received.error() == SocketError::Interrupted) { continue; }
            break;
        }
        activity.tick.fetch_add(1, std::memory_order_relaxed);
        if (received->bytes_read == 0) { break; }
        activity.last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);
        if (!link.channel.on_bytes(std::span<const std::byte>{scratch.data(), received->bytes_read})) {
            break; // malformed frame, or the consumer is gone
        }
    }

    // Exit, always: the one and only place on_closed() is called. Closing the pool
    // first makes any later send() fail instead of queueing into a dead link.
    link.pool.close_pool();
    std::ignore = activity.connection->shutdown(TcpConnection::Shutdown::Both);
    publish_tx_stats(link);
    {
        std::lock_guard lock(registry_mutex_);
        if (const auto it = registry_.find(link.handle); it != registry_.end() && it->second == link_ref) {
            registry_.erase(it);
        }
    }
    link.channel.on_closed(); // idempotent
}

// --- Inbound ------------------------------------------------------------------

void PeerThreadIo::run() {
    std::optional<TcpListener> listener;
    try {
        listener.emplace(net_.address, net_.port, net_.backlog);
        logger_.info("listening on " + net_.address + ":" + std::to_string(net_.port));
        notify_ready(net_.address + ":" + std::to_string(net_.port));
    } catch (const std::exception& error) {
        const std::string detail = "listen failed on " + net_.address + ":"
            + std::to_string(net_.port) + ": " + error.what();
        logger_.error(detail);
        (void)store_queue_.emplace(EventError{detail});
        notify_ready("", detail);
        return; // outbound keeps working
    }

    // stop() sets the flag before dialling; if its dial raced ahead of our bind it
    // failed, but we see the flag here and do not block in accept.
    while (!stopping_.load()) {
        auto accepted = listener->accept_client();
        if (!accepted) { continue; }     // transient accept failure
        if (stopping_.load()) { break; } // the wake-up dial
        attach(std::move(*accepted), true);
    }
}

void PeerThreadIo::stop() noexcept {
    if (stopping_.exchange(true)) { return; }
    logger_.info("listener stopping");
    // Stops scanning; the watchdog thread then drains (shuts down and joins) every
    // owner thread. Joined by the destructor, never here.
    watchdog_.stop();
    // Break a pending accept(): dial our own listener (same trick as ListenerWorker).
    if (auto wake_dial = TcpConnection::connect(net_.address, net_.port)) {
        std::ignore = wake_dial->shutdown(TcpConnection::Shutdown::Both);
    }
}

// --- Outbound -----------------------------------------------------------------

std::expected<OutboundTransport::Handle, SocketError>
PeerThreadIo::open(const std::string& ip, uint16_t port, std::span<const std::byte> hello) {
    auto connected = TcpConnection::connect(ip, port);
    if (!connected) { return std::unexpected(connected.error()); }
    // Registered (and its owner running) before the hello goes out, so the peer's
    // ConnectAck can never arrive for an unknown link.
    auto link = attach(std::move(*connected), false);
    if (!link) { return std::unexpected(SocketError::ConnectionClosed); }
    if (auto sent = send(link->handle, hello); !sent) {
        close(link->handle);
        return std::unexpected(sent.error());
    }
    return link->handle;
}

std::expected<void, SocketError>
PeerThreadIo::send(Handle handle, std::span<const std::byte> data) {
    const auto link = find(handle);
    if (!link) { return std::unexpected(SocketError::ConnectionNotFound); }
    // The pool's contract: immediate non-blocking write, else queue up to the cap
    // (WouldBlock beyond it, nothing queued); a fatal write drops the connection and
    // is reported to the owner through receive().
    auto sent = link->pool.send(link->pool_id, data);
    publish_tx_stats(*link); // queueing is counted here, not in the owner's receive()
    return sent;
}

void PeerThreadIo::close(Handle handle) noexcept {
    // The owner thread's receive() returns, it exits, releases the peer's slots and
    // reports the close.
    if (const auto link = find(handle)) { link->pool.close_pool(); }
}

} // namespace p2p
