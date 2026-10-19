#pragma once

// Outbound (write-side) transport used by SenderWorker. The sender keeps all
// policy (handshake content, retries, reconnect, reporting unreachable peers);
// a transport only opens, writes to and closes sockets.

#include "TcpSocket.hpp"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <unordered_map>

class OutboundTransport {
public:
    using Handle = int;
    virtual ~OutboundTransport() = default;
    // Dials ip:port and sends `hello` as the first bytes. On any failure nothing
    // is left open.
    virtual std::expected<Handle, SocketError> open(const std::string& ip, uint16_t port,
                                                    std::span<const std::byte> hello) = 0;
    // Sends all of `data` or fails (a partial send is a failure).
    virtual std::expected<void, SocketError> send(Handle, std::span<const std::byte> data) = 0;
    virtual void close(Handle) noexcept = 0;
};

// Today's behaviour: one blocking TcpConnection per handle, send_all with
// SO_SNDTIMEO bounding each send. Used from the sender thread only.
class BlockingOutbound final : public OutboundTransport {
public:
    explicit BlockingOutbound(std::chrono::milliseconds send_timeout) noexcept
        : send_timeout_(send_timeout) {}

    std::expected<Handle, SocketError> open(const std::string& ip, uint16_t port,
                                            std::span<const std::byte> hello) override;
    std::expected<void, SocketError> send(Handle handle, std::span<const std::byte> data) override;
    void close(Handle handle) noexcept override;

private:
    std::chrono::milliseconds send_timeout_;
    Handle next_handle_{1};
    std::unordered_map<Handle, TcpConnection> connections_;
};
