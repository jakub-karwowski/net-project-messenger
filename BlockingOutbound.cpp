// BlockingOutbound.cpp - the thread-per-sender outbound transport: blocking
// connect, send_all and SO_SNDTIMEO.

#include "OutboundTransport.hpp"

#include <tuple>
#include <utility>

namespace {
// Pushes every byte of `data`; a partial send resumes where the previous one
// stopped. Returns the error as soon as the socket reports a failure.
std::expected<void, SocketError> send_all(TcpConnection& connection,
                                          std::span<const std::byte> data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        auto result = connection.send(data.subspan(sent));
        if (!result) { return std::unexpected(result.error()); }
        if (*result == 0) { return std::unexpected(SocketError::ConnectionClosed); }
        sent += *result;
    }
    return {};
}
} // namespace

std::expected<OutboundTransport::Handle, SocketError>
BlockingOutbound::open(const std::string& ip, uint16_t port, std::span<const std::byte> hello) {
    auto connected = TcpConnection::connect(ip, port);
    if (!connected) { return std::unexpected(connected.error()); }

    connected->with_tcp_nodelay(true).with_keep_alive(true).with_send_timeout(send_timeout_);

    if (auto sent = send_all(*connected, hello); !sent) {
        std::ignore = connected->shutdown(TcpConnection::Shutdown::Both);
        return std::unexpected(sent.error());
    }

    const Handle handle = next_handle_++;
    connections_.emplace(handle, std::move(*connected));
    return handle;
}

std::expected<void, SocketError>
BlockingOutbound::send(Handle handle, std::span<const std::byte> data) {
    const auto it = connections_.find(handle);
    if (it == connections_.end()) { return std::unexpected(SocketError::ConnectionNotFound); }
    return send_all(it->second, data);
}

void BlockingOutbound::close(Handle handle) noexcept {
    const auto it = connections_.find(handle);
    if (it == connections_.end()) { return; }
    std::ignore = it->second.shutdown(TcpConnection::Shutdown::Both);
    connections_.erase(it);
}
