#pragma once

// Outbound transport over a connection pool (--io=pool-sender). Dials stay
// blocking TcpConnection::connect; the connected socket is handed to the pool
// and every send goes through Pool::send (non-blocking, per-connection tx
// buffer). Works with TcpConnectionPool and EpollConnectionPool.
//
// A pump thread drives Pool::receive(), which the pool needs for flushing tx
// buffers and for detecting dead sockets. Bytes arriving on an outbound socket
// are not part of the protocol and are discarded; a {id, 0} result (the pool
// dropped the connection) marks the handle dead so the next send reports
// ConnectionNotFound and SenderWorker re-dials.
//
// Backpressure: a full tx buffer makes send() return SocketError::WouldBlock
// and backpressure_is_drop() is true, so SenderWorker drops the message.

#include "Logger.hpp"
#include "OutboundTransport.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_set>
#include <utility>

namespace p2p {

struct PoolOutboundOptions {
    std::size_t max_connections{16};
    // Effectively disabled: liveness comes from EOF/errors, not from idleness.
    std::chrono::milliseconds idle_timeout{std::chrono::hours{24 * 365}};
    std::size_t max_tx_buffer_bytes{1024 * 1024};
};

template <class Pool>
class PoolOutbound final : public OutboundTransport {
public:
    explicit PoolOutbound(PoolOutboundOptions options = {}, Logger logger = {})
        : pool_(options.max_connections, options.idle_timeout, options.max_tx_buffer_bytes),
          logger_(std::move(logger)),
          pump_([this] { pump(); }) {}

    ~PoolOutbound() override { stop(); }

    // Closes the pool and joins the pump thread. Idempotent.
    void stop() noexcept {
        pool_.close_pool();
        if (pump_.joinable()) { pump_.join(); }
    }

    std::expected<Handle, SocketError> open(const std::string& ip, uint16_t port,
                                            std::span<const std::byte> hello) override {
        auto connected = TcpConnection::connect(ip, port);
        if (!connected) { return std::unexpected(connected.error()); }
        connected->with_tcp_nodelay(true).with_keep_alive(true);

        const int id = pool_.add_connection(std::move(*connected));
        if (id < 0) { return std::unexpected(SocketError::ConnectionClosed); }
        {
            std::lock_guard lock{mutex_};
            live_.insert(id);
        }
        if (auto sent = pool_.send(id, hello); !sent) {
            close(id);
            return std::unexpected(sent.error());
        }
        return id;
    }

    std::expected<void, SocketError> send(Handle handle,
                                           std::span<const std::byte> data) override {
        {
            std::lock_guard lock{mutex_};
            if (!live_.contains(handle)) { return std::unexpected(SocketError::ConnectionNotFound); }
        }
        return pool_.send(handle, data);
    }

    void close(Handle handle) noexcept override {
        {
            std::lock_guard lock{mutex_};
            live_.erase(handle);
        }
        pool_.remove_connection(handle);
    }

    bool backpressure_is_drop() const noexcept override { return true; }

private:
    void pump() {
        std::array<std::byte, 4096> buffer{};
        for (;;) {
            auto result = pool_.receive(std::span<std::byte>{buffer});
            if (!result) { return; } // pool closed
            if (result->bytes_read == 0) {
                std::lock_guard lock{mutex_};
                live_.erase(result->connection_id);
                continue;
            }
            // Unexpected on a write-only connection: warn on the 1st, then every 1000th.
            if (unexpected_reads_++ % 1000 == 0) {
                logger_.warn("outbound connection " + std::to_string(result->connection_id)
                             + " received " + std::to_string(result->bytes_read)
                             + " unexpected bytes (ignored)");
            }
        }
    }

    Pool pool_;
    Logger logger_;
    std::mutex mutex_;
    std::unordered_set<int> live_;      // handles not yet reported closed
    std::size_t unexpected_reads_{0};   // pump thread only
    std::thread pump_;                  // last member: starts after everything else is built
};

} // namespace p2p
