// MultiplexedIo.cpp - shared-pool transport behind --io=poll and --io=epoll.

#include "MultiplexedIo.hpp"

#include <algorithm>
#include <exception>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace p2p {

template <class Pool>
MultiplexedIo<Pool>::MultiplexedIo(const NetConfig& net, ThreadQueue<Event>& store_queue,
                                   PeerTable& peers, Logger logger,
                                   std::size_t max_tx_buffer_bytes)
    : net_(net),
      store_queue_(store_queue),
      peers_(peers),
      logger_(std::move(logger)),
      // Inbound and outbound share the pool, hence twice the peer cap. Liveness
      // comes from EOF / keepalive, so the idle timeout is only a backstop.
      pool_(std::max<std::size_t>(net.max_peers, 1) * 2, kIdleTimeout, max_tx_buffer_bytes) {
    io_thread_ = std::thread{[this] { io_loop(); }};
}

template <class Pool>
MultiplexedIo<Pool>::~MultiplexedIo() {
    stopping_.store(true);
    pool_.close_pool();
    if (io_thread_.joinable()) { io_thread_.join(); }
}

// --- Inbound ----------------------------------------------------------------

template <class Pool>
void MultiplexedIo<Pool>::run() {
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
        auto session = std::make_shared<FrameChannel>(store_queue_, peers_, logger_);
        session->set_metrics(net_.metrics);
        std::lock_guard lock(registry_mutex_);
        const int id = pool_.add_connection(std::move(*accepted));
        if (id < 0) { continue; } // pool closed: shutting down
        bump(net_.metrics, &Metrics::accepts);
        live_inc(net_.metrics);
        registry_.emplace(id, Registered{std::move(session)});
    }
}

template <class Pool>
void MultiplexedIo<Pool>::stop() noexcept {
    if (stopping_.exchange(true)) { return; }
    logger_.info("listener stopping");
    // Break a pending accept(): dial our own listener (same trick as ListenerWorker).
    if (auto wake = TcpConnection::connect(net_.address, net_.port)) {
        std::ignore = wake->shutdown(TcpConnection::Shutdown::Both);
    }
    // Ends the I/O thread; it reports the remaining sessions closed on its way out.
    pool_.close_pool();
}

template <class Pool>
void MultiplexedIo<Pool>::io_loop() {
    std::vector<std::byte> buffer(kReadChunk);
    for (;;) {
        auto received = pool_.receive(std::span<std::byte>{buffer});
        if (!received) {
            if (received.error() == SocketError::ConnectionClosed || stopping_.load()) { break; }
            if (received.error() != SocketError::Interrupted) {
                logger_.error("pool receive failed");
                break;
            }
            continue;
        }
        const int id = received->connection_id;

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
            pool_.remove_connection(id);
            {
                std::lock_guard lock(registry_mutex_);
                registry_.erase(id);
            }
            live_dec(net_.metrics);
            session->on_closed();
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

template <class Pool>
std::expected<OutboundTransport::Handle, SocketError>
MultiplexedIo<Pool>::open(const std::string& ip, uint16_t port, std::span<const std::byte> hello) {
    auto connected = TcpConnection::connect(ip, port);
    if (!connected) { return std::unexpected(connected.error()); }
    connected->with_tcp_nodelay(true).with_keep_alive(true);
    int id;
    {
        std::lock_guard lock(registry_mutex_);
            id = pool_.add_connection(std::move(*connected));
            if (id < 0) { return std::unexpected(SocketError::ConnectionClosed); }
        registry_.emplace(id, Registered{});
    }
    if (auto sent = pool_.send(id, hello); !sent) {
        close(id);
        return std::unexpected(sent.error());
    }
    return id;
}

template <class Pool>
std::expected<void, SocketError>
MultiplexedIo<Pool>::send(Handle handle, std::span<const std::byte> data) {
    return pool_.send(handle, data); // unknown / dropped ids yield ConnectionNotFound
}

template <class Pool>
void MultiplexedIo<Pool>::close(Handle handle) noexcept {
    pool_.remove_connection(handle);
    std::lock_guard lock(registry_mutex_);
    registry_.erase(handle);
}

template class MultiplexedIo<TcpConnectionPool>;
template class MultiplexedIo<EpollConnectionPool>;

} // namespace p2p
