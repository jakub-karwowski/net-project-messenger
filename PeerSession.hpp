#pragma once

#include "Events.hpp"
#include "Logger.hpp"
#include "ThreadQueue.hpp"
#include "Wire.hpp"
#include <cstddef>
#include <cstdint>
#include <span>

namespace p2p {
struct PeerTable;

// Per-inbound-connection protocol state: decodes the byte stream, learns the
// peer's identity from the handshake and forwards messages to the store. It is
// transport-agnostic: whoever owns the socket feeds it bytes and reports close.
class PeerSession {
public:
    PeerSession(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger = {});

    // Feeds received bytes. Returns false when the connection must be closed
    // (malformed frame, or the store consumer is gone).
    bool on_bytes(std::span<const std::byte> bytes);
    // The connection is gone: releases ownership of the peer and, unless a newer
    // connection superseded this one, tells the store the peer disconnected.
    void on_closed();

private:
    void remember_assigned_id(uint64_t peer_id, uint64_t assigned_id);

    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;
    wire::Decoder decoder_;
    uint64_t peer_id_{0}; // our name for the peer: minted on handshake, or the id it re-presents
    // Unique owner id of this connection. A session that a newer connection
    // superseded must not report the (still live) peer as gone on close.
    uint64_t connection_id_;
};
} // namespace p2p
