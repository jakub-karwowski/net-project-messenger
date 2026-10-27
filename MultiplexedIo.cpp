// MultiplexedIo.cpp - shared-pool transport behind --io=poll and --io=epoll.

#include "MultiplexedIo.hpp"

#include "ThreadName.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace p2p {

template <class Pool, class Tx>
MultiplexedIo<Pool, Tx>::MultiplexedIo(const NetConfig& net, ThreadQueue<Event>& store_queue,
                                   PeerTable& peers, Logger logger,
                                   std::size_t max_tx_buffer_bytes, ConnectionModel model)
    : net_(net),
      store_queue_(store_queue),
      peers_(peers),
      logger_(std::move(logger)),
      model_(kSplit ? ConnectionModel::Duplex : model),
      // Dual: inbound and outbound share the pool, hence twice the peer cap. Duplex:
      // one connection per peer (plus slack for handshakes in flight). Liveness comes
      // from EOF / keepalive, so the idle timeout is only a backstop.
      pool_(std::max<std::size_t>(net.max_peers, 1)
                * (model_ == ConnectionModel::Dual ? 2 : 1)
                + (model_ == ConnectionModel::Dual ? 0 : 16),
            kIdleTimeout, max_tx_buffer_bytes),
      tx_(max_tx_buffer_bytes) {
    if constexpr (kSplit) { tx_thread_ = std::thread{[this] { set_thread_name("tx"); tx_.run(); }}; }
    io_thread_ = std::thread{[this] { set_thread_name("io"); io_loop(); }};
}

template <class Pool, class Tx>
MultiplexedIo<Pool, Tx>::~MultiplexedIo() {
    stopping_.store(true);
    pool_.close_pool();
    if constexpr (kSplit) { tx_.close(); }
    if (io_thread_.joinable()) { io_thread_.join(); }
    if (tx_thread_.joinable()) { tx_thread_.join(); }
}

template <class Pool, class Tx>
int MultiplexedIo<Pool, Tx>::attach_locked(std::shared_ptr<TcpConnection> connection) {
    if constexpr (kSplit) {
        const int id = pool_.add_connection(connection);
        if (id < 0) { return -1; }
        if (!tx_.add(id, std::move(connection))) {
            pool_.remove_connection(id);
            return -1;
        }
        return id;
    } else {
        return pool_.add_connection(std::move(connection));
    }
}

template <class Pool, class Tx>
std::shared_ptr<FrameChannel> MultiplexedIo<Pool, Tx>::make_duplex_channel(int id, bool) {
    auto channel = std::make_shared<FrameChannel>(
        store_queue_, peers_, logger_, ChannelRef{next_local_peer_id(), id}, true);
    channel->set_metrics(net_.metrics);
    return channel;
}

// --- Inbound ----------------------------------------------------------------

template <class Pool, class Tx>
void MultiplexedIo<Pool, Tx>::run() {
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
        return; // outbound keeps working: the I/O thread is already running
    }

    // stop() sets the flag before dialling; if its dial raced ahead of our bind
    // it failed, but we see the flag here and do not block in accept.
    while (!stopping_.load()) {
        auto accepted = listener->accept_client();
        if (!accepted) { continue; }     // transient accept failure
        if (stopping_.load()) { break; } // the wake-up dial

        accepted->with_keep_alive(true).with_tcp_nodelay(true);
        auto connection = std::make_shared<TcpConnection>(std::move(*accepted));
        std::lock_guard lock(registry_mutex_);
        const int id = attach_locked(std::move(connection));
        if (id < 0) { continue; } // pool closed: shutting down
        std::shared_ptr<FrameChannel> session;
        if (model_ == ConnectionModel::Duplex) {
            session = make_duplex_channel(id, true);
        } else {
            session = std::make_shared<FrameChannel>(store_queue_, peers_, logger_);
            session->set_metrics(net_.metrics);
        }
        bump(net_.metrics, &Metrics::accepts);
        live_inc(net_.metrics);
        registry_.emplace(id, Registered{std::move(session)});
    }
}

template <class Pool, class Tx>
void MultiplexedIo<Pool, Tx>::stop() noexcept {
    if (stopping_.exchange(true)) { return; }
    logger_.info("listener stopping");
    // Break a pending accept(): dial our own listener (same trick as ListenerWorker).
    if (auto wake = TcpConnection::connect(net_.address, net_.port)) {
        std::ignore = wake->shutdown(TcpConnection::Shutdown::Both);
    }
    // Ends the I/O thread; it reports the remaining sessions closed on its way out.
    pool_.close_pool();
    if constexpr (kSplit) { tx_.close(); }
}

template <class Pool, class Tx>
void MultiplexedIo<Pool, Tx>::publish_tx_stats() noexcept {
    if (!net_.metrics) { return; }
    TxStats st = pool_.stats();
    if constexpr (kSplit) {
        const TxStats t = tx_.stats();
        st.queued_sends += t.queued_sends;
        st.flushes += t.flushes;
    }
    net_.metrics->tx_queued_sends.store(st.queued_sends, std::memory_order_relaxed);
    net_.metrics->tx_flushes.store(st.flushes, std::memory_order_relaxed);
}

template <class Pool, class Tx>
void MultiplexedIo<Pool, Tx>::io_loop() {
    std::vector<std::byte> buffer(kReadChunk);
    for (;;) {
        auto received = pool_.receive(std::span<std::byte>{buffer});
        publish_tx_stats();
        if (!received) {
            if (received.error() == SocketError::ConnectionClosed || stopping_.load()) { break; }
            if (received.error() != SocketError::Interrupted) {
                logger_.error("pool receive failed");
                break;
            }
            continue;
        }
        const int id = received->connection_id;
        if constexpr (kSplit) {
            // Pool-initiated drop: forget the write side too (no shutdown: it is gone).
            if (received->bytes_read == 0) { tx_.remove(id); }
        }

        std::shared_ptr<FrameChannel> session;
        bool known = false;
        {
            std::lock_guard lock(registry_mutex_);
            if (const auto it = registry_.find(id); it != registry_.end()) {
                known = true;
                session = it->second.session;
                if (received->bytes_read == 0) { registry_.erase(it); }
            }
        }
        if (!known) { continue; } // already removed by close() / a failed on_bytes

        if (received->bytes_read == 0) {
            // Pool-initiated drop (EOF, error, LRU prune, idle). An outbound entry
            // is simply gone now, so the next send() says ConnectionNotFound.
            if (session) {
                live_dec(net_.metrics);
                // EOF and prune are indistinguishable here; count as a plain close.
                session->on_closed();
            }
            continue;
        }
        if (!session) {
            if (outbound_bytes_warned_++ % 1000 == 0) {
                logger_.warn("unexpected bytes on an outbound connection: ignored");
            }
            continue;
        }
        if (!session->on_bytes(std::span<const std::byte>{buffer.data(), received->bytes_read})) {
            // remove_connection is not reported by the pool, so finish the close here.
            if constexpr (kSplit) { tx_.remove(id); }
            pool_.remove_connection(id);
            bool erased;
            {
                std::lock_guard lock(registry_mutex_);
                erased = registry_.erase(id) > 0; // close() may have won the race
            }
            if (erased) { live_dec(net_.metrics); }
            session->on_closed(); // idempotent
        }
    }

    // Pool closed: tell the store about every peer still attached, like the
    // threads variant does when its readers are drained.
    std::unordered_map<int, Registered> rest;
    {
        std::lock_guard lock(registry_mutex_);
        rest.swap(registry_);
    }
    for (auto& [id, entry] : rest) {
        if (entry.session) {
            live_dec(net_.metrics);
            entry.session->on_closed();
        }
    }
}

// --- Outbound ---------------------------------------------------------------

template <class Pool, class Tx>
std::expected<OutboundTransport::Handle, SocketError>
MultiplexedIo<Pool, Tx>::open(const std::string& ip, uint16_t port, std::span<const std::byte> hello) {
    auto connected = TcpConnection::connect(ip, port);
    if (!connected) { return std::unexpected(connected.error()); }
    connected->with_tcp_nodelay(true).with_keep_alive(true);
    auto connection = std::make_shared<TcpConnection>(std::move(*connected));
    int id;
    {
        std::lock_guard lock(registry_mutex_);
        id = attach_locked(std::move(connection));
        if (id < 0) { return std::unexpected(SocketError::ConnectionClosed); }
        if (model_ == ConnectionModel::Duplex) {
            // The dialed socket also carries the peer's frames (the ConnectAck): it
            // needs its channel in place before the hello can provoke any reply.
            live_inc(net_.metrics);
            registry_.emplace(id, Registered{make_duplex_channel(id, false)});
        } else {
            registry_.emplace(id, Registered{});
        }
    }
    if (auto sent = send(id, hello); !sent) {
        close(id);
        return std::unexpected(sent.error());
    }
    return id;
}

template <class Pool, class Tx>
std::expected<void, SocketError>
MultiplexedIo<Pool, Tx>::send(Handle handle, std::span<const std::byte> data) {
    if constexpr (kSplit) {
        return tx_.send(handle, data);
    } else {
        return pool_.send(handle, data); // unknown / dropped ids yield ConnectionNotFound
    }
}

template <class Pool, class Tx>
void MultiplexedIo<Pool, Tx>::close(Handle handle) noexcept {
    if constexpr (kSplit) { tx_.remove(handle); }
    pool_.remove_connection(handle);
    std::shared_ptr<FrameChannel> channel;
    {
        std::lock_guard lock(registry_mutex_);
        if (const auto it = registry_.find(handle); it != registry_.end()) {
            channel = std::move(it->second.session);
            registry_.erase(it);
            if (channel) { live_dec(net_.metrics); }
        }
    }
    // Dual stays silent (the rx side is a different socket). In Duplex the pool does
    // not report a connection we removed ourselves, so release its slots here;
    // release is a no-op when the channel was superseded or already released.
    if (model_ == ConnectionModel::Duplex && channel) { channel->on_closed(); }
}

template class MultiplexedIo<TcpConnectionPool>;
template class MultiplexedIo<EpollConnectionPool>;
template class MultiplexedIo<TcpConnectionPool, PollTxPoller>;
template class MultiplexedIo<EpollConnectionPool, EpollTxPoller>;

} // namespace p2p
