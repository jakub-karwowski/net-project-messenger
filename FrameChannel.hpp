#pragma once

#include "ChannelRef.hpp"
#include "Events.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"
#include "ThreadQueue.hpp"
#include "Wire.hpp"
#include <cstddef>
#include <cstdint>
#include <span>

namespace p2p {
struct PeerTable;

// Per-TCP-byte-stream protocol state: decodes the stream, learns the peer's
// identity from the handshake, binds the stream to the peer's rx slot in the
// PeerTable (and, for a duplex channel, its tx slot too) and forwards messages
// to the store. It is transport-agnostic: whoever owns the socket feeds it bytes
// and reports close.
class FrameChannel {
public:
    // `ref` names this stream in the PeerTable slots; connection_id 0 mints a
    // fresh one. `duplex` (with ref.handle != 0) means the stream is also our
    // write channel, so a Connect fills the peer's tx slot as well as rx.
    FrameChannel(ThreadQueue<Event>& store_queue, PeerTable& peers, Logger logger = {},
                 ChannelRef ref = {}, bool duplex = false);

    // Feeds received bytes. Returns false when the connection must be closed
    // (malformed frame, or the store consumer is gone).
    // Optional counters (bytes_in, frames_in); nullptr disables them.
    void set_metrics(Metrics* metrics) noexcept { metrics_ = metrics; }
    bool on_bytes(std::span<const std::byte> bytes);
    // The connection is gone: releases its slot(s) of the peer and, unless a newer
    // connection superseded this one, tells the store the peer disconnected.
    void on_closed();

private:
    void remember_assigned_id(uint64_t peer_id, uint64_t assigned_id);

    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;
    Metrics* metrics_{nullptr};
    wire::Decoder decoder_;
    uint64_t peer_id_{0}; // our name for the peer: minted on handshake, or the id it re-presents
    // This stream's slot identity (unique connection_id). A channel that a newer
    // one superseded must not report the (still live) peer as gone on close.
    ChannelRef ref_;
    bool duplex_;
};
} // namespace p2p
