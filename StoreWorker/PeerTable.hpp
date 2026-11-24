#pragma once

// The remote nodes the Store knows, keyed by the id we gave each of them.

#include "Ids.hpp"
#include "Wire.hpp"

#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

struct Peer {
    enum class Link : std::uint8_t {
        Down,          // no link
        Dialing,       // RouteDial sent, waiting for DialDone or PeerDown
        Handshaking,   // inbound hello answered with BindLink, waiting for PeerUp
        Up,
    };

    // Persisted.
    PeerId peer_id = kNoId;                  // our name for them (we minted it)
    PeerId host_id = kNoId;                  // their name for us, learned in the handshake
    std::vector<wire::Endpoint> endpoints;   // where they accept connections; dial targets
    wire::Endpoint remote;                   // the address of the last link

    // Transient: rebuilt from live links, never persisted.
    Link link_state = Link::Down;
    LinkId link = kNoId;                     // the current link; 0 while dialing
    bool ever_up = false;                    // distinguishes Disconnected from New
    bool last_dial_failed = false;           // distinguishes Failed from Disconnected
};

class PeerTable {
public:
    Peer* find(PeerId id) {
        const auto it = peers_.find(id);
        return it == peers_.end() ? nullptr : &it->second;
    }
    const Peer* find(PeerId id) const {
        const auto it = peers_.find(id);
        return it == peers_.end() ? nullptr : &it->second;
    }

    // The peer that advertised `ep`, or that the last link went to.
    Peer* find_by_endpoint(const wire::Endpoint& ep) {
        const auto it = std::ranges::find_if(peers_, [&](const auto& entry) {
            const Peer& peer = entry.second;
            return peer.remote == ep || std::ranges::contains(peer.endpoints, ep);
        });
        return it == peers_.end() ? nullptr : &it->second;
    }

    Peer& insert(Peer peer) {
        const PeerId id = peer.peer_id;
        return peers_.insert_or_assign(id, std::move(peer)).first->second;
    }

    std::size_t size() const noexcept { return peers_.size(); }
    const std::unordered_map<PeerId, Peer>& all() const noexcept { return peers_; }

private:
    std::unordered_map<PeerId, Peer> peers_;
};
