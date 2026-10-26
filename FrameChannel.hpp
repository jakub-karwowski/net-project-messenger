#pragma once

#include "ChannelRef.hpp"
#include "Events.hpp"
#include "Logger.hpp"
#include "Metrics.hpp"
#include "ThreadQueue.hpp"
#include "Wire.hpp"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <span>

namespace p2p {
struct PeerTable;

// Per-TCP-byte-stream protocol state: decodes the stream, learns the peer's
// identity from the handshake, binds the stream to the peer's rx slot in the
// PeerTable (and, for a duplex channel, its tx slot too) and forwards messages
// to the store. It is transport-agnostic: whoever owns the socket feeds it bytes
// and reports close.
//
// Threads: on_bytes() runs on one I/O thread. on_closed() may run on another (a
// duplex transport's close() is called by the sender) while on_bytes() is still
// inside a handshake frame. The bind (setting the peer id and attaching slots,
// plus the event it emits) and on_closed() therefore run under `state_mutex_`,
// and on_closed() sets `closing_`: a bind that comes after it attaches nothing,
// so a closed channel can never be left in a slot nobody will release. Chat
// frames take no lock: the peer id is written only by the I/O thread itself.
class FrameChannel {
    // Movable wrappers (FrameChannel is moved into ReaderWorker before its thread
    // starts; moves are never concurrent with use).
    struct MovableMutex {
        std::mutex value;
        MovableMutex() = default;
        MovableMutex(MovableMutex&&) noexcept {}
        MovableMutex& operator=(MovableMutex&&) noexcept { return *this; }
    };
    struct MovableFlag {
        std::atomic<bool> value{false};
        MovableFlag() = default;
        MovableFlag(MovableFlag&& other) noexcept : value(other.value.load()) {}
        MovableFlag& operator=(MovableFlag&& other) noexcept { value.store(other.value.load()); return *this; }
    };

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
    // Idempotent; after it, on_bytes() binds nothing and returns false.
    void on_closed();
    bool closing() const noexcept { return closing_.value.load(); }

private:
    void remember_assigned_id(uint64_t peer_id, uint64_t assigned_id);

    ThreadQueue<Event>& store_queue_;
    PeerTable& peers_;
    Logger logger_;
    Metrics* metrics_{nullptr};
    wire::Decoder decoder_;
    // Our name for the peer: minted on handshake, or the id it re-presents. Written
    // by the I/O thread under state_mutex_; read by on_closed() under it.
    uint64_t peer_id_{0};
    MovableMutex state_mutex_;
    MovableFlag closing_; // set once by on_closed()
    // This stream's slot identity (unique connection_id). A channel that a newer
    // one superseded must not report the (still live) peer as gone on close.
    ChannelRef ref_;
    bool duplex_;
};
} // namespace p2p
