// PeerThreadIo.cpp - one owner thread per peer behind --io=peer-threads.

#include "PeerThreadIo.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <exception>
#include <optional>
#include <string>
#include <system_error>
#include <tuple>
#include <utility>

#include <poll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace p2p {

namespace {
int64_t steady_now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}
} // namespace

PeerThreadIo::Link::Link(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger,
                         std::shared_ptr<TcpConnection> connection, int handle_, Metrics* metrics)
    : handle(handle_),
      conn(std::move(connection)),
      activity(std::make_shared<ReaderActivity>(conn)),
      channel(store_queue, peers, std::move(logger), ChannelRef{next_local_peer_id(), handle_}, true) {
    channel.set_metrics(metrics);
    wake_fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wake_fd < 0) { throw std::system_error(errno, std::generic_category(), "eventfd"); }
    // A connection that never reads still needs an ordering floor for the watchdog.
    activity->last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);
}

PeerThreadIo::Link::~Link() {
    if (wake_fd >= 0) { ::close(wake_fd); }
}

PeerThreadIo::PeerThreadIo(const NetConfig& net, ThreadQueue<Event>& store_queue, PeerTable& peers,
                           Logger logger, std::size_t max_tx_buffer_bytes)
    : net_(net),
      store_queue_(store_queue),
      peers_(peers),
      logger_(logger),
      max_tx_buffer_bytes_(max_tx_buffer_bytes),
      watchdog_(net.max_peers, WatchdogWorker::kDefaultScanInterval,
                WatchdogWorker::kDefaultStallScanLimit, logger) {
    watchdog_.set_metrics(net_.metrics);
    watchdog_thread_ = std::thread{[this] { watchdog_(); }};
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

void PeerThreadIo::wake(Link& link) noexcept {
    const uint64_t one = 1;
    std::ignore = ::write(link.wake_fd, &one, sizeof one); // EAGAIN: already signalled
}

void PeerThreadIo::shut_link(Link& link) noexcept {
    {
        std::lock_guard lock(link.tx_mutex);
        link.closed.store(true);
    }
    std::ignore = link.conn->shutdown(TcpConnection::Shutdown::Both);
    wake(link);
}

std::shared_ptr<PeerThreadIo::Link> PeerThreadIo::find(int handle) {
    std::lock_guard lock(registry_mutex_);
    const auto it = registry_.find(handle);
    return it == registry_.end() ? nullptr : it->second;
}

// --- Attaching ---------------------------------------------------------------

std::shared_ptr<PeerThreadIo::Link> PeerThreadIo::attach(TcpConnection&& connection, bool accepted) {
    // Options first: once the owner thread and the sender share the connection,
    // nothing may call with_* again.
    connection.with_tcp_nodelay(true).with_keep_alive(true).with_non_blocking(true);
    if (!connection.apply_options_now()) { return nullptr; }
    auto shared = std::make_shared<TcpConnection>(std::move(connection));

    std::shared_ptr<Link> link;
    try {
        link = std::make_shared<Link>(store_queue_, peers_, logger_, std::move(shared),
                                      next_handle_.fetch_add(1), net_.metrics);
    } catch (const std::exception&) {
        return nullptr; // eventfd exhaustion: the socket closes with `shared`
    }

    {
        std::lock_guard lock(registry_mutex_);
        registry_.emplace(link->handle, link);
    }
    if (accepted) { bump(net_.metrics, &Metrics::accepts); }
    live_inc(net_.metrics);

    std::thread owner;
    try {
        owner = std::thread{[this, link] { owner_loop(link); }};
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
    TcpConnection& conn = *link.conn;
    const int fd = conn.native_handle();

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
        activity.tick.fetch_add(1, std::memory_order_relaxed);
        if (link.closed.load()) { break; }

        bool pending;
        {
            std::lock_guard lock(link.tx_mutex);
            pending = link.tx_buffer.size() > link.tx_offset;
        }
        pollfd fds[2] = {
            {fd, static_cast<short>(pending ? (POLLIN | POLLOUT) : POLLIN), 0},
            {link.wake_fd, POLLIN, 0},
        };
        const int ready = ::poll(fds, 2, kPollTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) { continue; }
            break;
        }
        if (ready == 0) { continue; }

        if ((fds[1].revents & POLLIN) != 0) {
            uint64_t drained;
            std::ignore = ::read(link.wake_fd, &drained, sizeof drained);
            if (link.closed.load()) { break; }
        }

        if ((fds[0].revents & POLLOUT) != 0) {
            bool ok = true;
            {
                std::lock_guard lock(link.tx_mutex);
                while (link.tx_offset < link.tx_buffer.size()) {
                    const std::span<const std::byte> rest{link.tx_buffer.data() + link.tx_offset,
                                                          link.tx_buffer.size() - link.tx_offset};
                    const auto sent = conn.send_once(rest, MSG_DONTWAIT);
                    if (!sent) {
                        if (sent.error() == SocketError::Interrupted) { continue; }
                        if (sent.error() == SocketError::WouldBlock) { break; }
                        ok = false;
                        break;
                    }
                    if (*sent == 0) { ok = false; break; }
                    link.tx_offset += *sent;
                }
                if (ok && link.tx_offset >= link.tx_buffer.size()) {
                    link.tx_buffer.clear();
                    link.tx_offset = 0;
                }
            }
            bump(net_.metrics, &Metrics::tx_flushes);
            if (!ok) { break; }
        }

        if ((fds[0].revents & POLLIN) != 0) {
            const auto received = conn.receive_once(std::span<std::byte>{scratch}, MSG_DONTWAIT);
            if (received) {
                if (*received == 0) { break; } // orderly EOF
                activity.last_read_ns.store(steady_now_ns(), std::memory_order_relaxed);
                if (!link.channel.on_bytes(std::span<const std::byte>{scratch.data(), *received})) {
                    break; // malformed frame, or the consumer is gone
                }
            } else if (received.error() != SocketError::WouldBlock
                       && received.error() != SocketError::Interrupted) {
                break;
            }
        } else if ((fds[0].revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            break;
        }
    }

    // Exit, always: the one and only place on_closed() is called.
    {
        std::lock_guard lock(link.tx_mutex);
        link.closed.store(true);
        link.tx_buffer.clear();
        link.tx_offset = 0;
    }
    std::ignore = conn.shutdown(TcpConnection::Shutdown::Both);
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
    Link& l = *link;

    std::optional<SocketError> fatal;
    bool queued = false;
    {
        std::lock_guard lock(l.tx_mutex);
        if (l.closed.load()) { return std::unexpected(SocketError::ConnectionNotFound); }
        if (data.empty()) { return {}; }

        const std::size_t pending = l.tx_buffer.size() - l.tx_offset;
        // Reject up front when the buffer cannot be accepted atomically.
        if (data.size_bytes() > max_tx_buffer_bytes_
            || (pending != 0 && data.size_bytes() > max_tx_buffer_bytes_ - pending)) {
            return std::unexpected(SocketError::WouldBlock);
        }

        std::size_t consumed = 0;
        if (pending == 0) {
            l.tx_buffer.clear();
            l.tx_offset = 0;
            while (consumed < data.size_bytes()) {
                const auto sent = l.conn->send_once(data.subspan(consumed), MSG_DONTWAIT);
                if (sent && *sent > 0) { consumed += *sent; continue; }
                if (!sent && sent.error() == SocketError::Interrupted) { continue; }
                if (!sent && sent.error() == SocketError::WouldBlock) { break; }
                fatal = sent ? SocketError::ConnectionClosed : sent.error();
                break;
            }
            if (!fatal && consumed == data.size_bytes()) { return {}; }
        }
        if (!fatal) {
            // Keep the unsent remainder and let the owner finish it on POLLOUT. Compact
            // only once the dead prefix dominates, so appends stay amortized O(1).
            if (l.tx_offset > 0 && l.tx_offset >= l.tx_buffer.size() / 2) {
                l.tx_buffer.erase(l.tx_buffer.begin(),
                                  l.tx_buffer.begin() + static_cast<std::ptrdiff_t>(l.tx_offset));
                l.tx_offset = 0;
            }
            l.tx_buffer.insert(l.tx_buffer.end(),
                               data.begin() + static_cast<std::ptrdiff_t>(consumed), data.end());
            queued = true;
        }
    }
    if (fatal) {
        shut_link(l); // the owner sees the wake / EOF and finishes the close
        return std::unexpected(*fatal);
    }
    if (queued) {
        bump(net_.metrics, &Metrics::tx_queued_sends);
        wake(l);
    }
    return {};
}

void PeerThreadIo::close(Handle handle) noexcept {
    // The owner thread exits, releases the peer's slots and reports the close.
    if (const auto link = find(handle)) { shut_link(*link); }
}

} // namespace p2p
