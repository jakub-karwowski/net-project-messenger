#pragma once

#include <cstdint>

namespace p2p {
// Names one TCP byte stream in the PeerTable's per-peer rx/tx slots.
// `connection_id` is a node-unique generation (from next_local_peer_id()): a slot
// is only released by the id that currently holds it, which is how a superseded
// channel stays silent. `handle` is the OutboundTransport::Handle (an int) the
// sender can write on, or 0 when the channel is not writable through a transport.
struct ChannelRef {
    uint64_t connection_id{0};
    int handle{0};
    friend bool operator==(const ChannelRef&, const ChannelRef&) = default;
};
} // namespace p2p
