#pragma once

// Identifiers shared by every worker, and the IdMinter that mints the random
// ones (peer and conversation ids).

#include <cstdint>
#include <random>
#include <unordered_set>

using PeerId = std::uint64_t;   // our name for a peer; the key below the Store
using ConvId = std::uint64_t;   // one conversation; never leaves Store/Frontend
using LinkId = std::uint64_t;   // one TCP connection, unchanged across moves
using MsgId  = std::uint64_t;   // index in a conversation's history
using PtId   = std::uint32_t;   // one IO worker thread
using ReqId  = std::uint64_t;   // one frontend request; echoed in its single Reply

inline constexpr std::uint64_t kNoId = 0;   // "unknown / none" for every id

// Random, never-zero, never-repeated 64-bit ids.
//
// - Random rather than sequential, so a peer cannot guess the id another peer
//   was given and present it on reconnect.
// - Unique across restarts once the ids loaded from storage are passed to
//   reserve().
//
// Not thread-safe: one owner (the Store) uses it.
class IdMinter {
public:
    IdMinter() : rng_(seed()) {}
    explicit IdMinter(std::uint64_t seed_value) : rng_(seed_value) {}   // deterministic, for tests

    // A fresh id: not 0 and not returned or reserved before.
    std::uint64_t next() {
        for (;;) {
            const std::uint64_t id = rng_();
            if (id != kNoId && issued_.insert(id).second) { return id; }
        }
    }

    // Marks an id that is already in use (for example loaded from disk), so
    // next() never returns it. Returns false if it was already known; 0 is
    // ignored.
    bool reserve(std::uint64_t id) {
        if (id == kNoId) { return false; }
        return issued_.insert(id).second;
    }

    bool known(std::uint64_t id) const { return issued_.contains(id); }
    std::size_t size() const noexcept { return issued_.size(); }

private:
    static std::uint64_t seed() {
        std::random_device rd;
        return (static_cast<std::uint64_t>(rd()) << 32) ^ rd();
    }

    std::mt19937_64 rng_;
    std::unordered_set<std::uint64_t> issued_;
};
