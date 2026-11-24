#pragma once

// Conversations and their histories. Only the Store and the frontend know
// conversations; everything below the Store deals in peers.

#include "Ids.hpp"
#include "Messages.hpp"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <unordered_map>
#include <vector>

struct Conversation {
    enum class Kind : std::uint8_t { Direct, Group };

    ConvId id = kNoId;
    Kind kind = Kind::Direct;           // only Direct is created today
    std::uint64_t group_id = 0;         // the wire conversation_id; 0 for Direct
    std::vector<PeerId> members;        // Direct: exactly one
    std::vector<msg::StoredMessage> msgs;   // msg_id = index
    std::size_t read_upto = 0;          // msgs before this index have been shown
};

class ConvStore {
public:
    Conversation* find(ConvId id) {
        const auto it = convs_.find(id);
        return it == convs_.end() ? nullptr : &it->second;
    }
    const Conversation* find(ConvId id) const {
        const auto it = convs_.find(id);
        return it == convs_.end() ? nullptr : &it->second;
    }

    Conversation* direct_of(PeerId peer) {
        const auto it = direct_conv_of_.find(peer);
        return it == direct_conv_of_.end() ? nullptr : find(it->second);
    }

    Conversation* by_group_id(std::uint64_t group_id) {
        const auto it = by_group_id_.find(group_id);
        return it == by_group_id_.end() ? nullptr : find(it->second);
    }

    Conversation& insert(Conversation conv) {
        const ConvId id = conv.id;
        if (conv.kind == Conversation::Kind::Direct && !conv.members.empty()) {
            direct_conv_of_[conv.members.front()] = id;
        } else if (conv.kind == Conversation::Kind::Group) {
            by_group_id_[conv.group_id] = id;
        }
        if (!convs_.contains(id)) { order_.push_back(id); }
        return convs_.insert_or_assign(id, std::move(conv)).first->second;
    }

    // Every conversation `peer` is a member of.
    std::vector<ConvId> with_member(PeerId peer) const {
        std::vector<ConvId> out;
        std::ranges::copy_if(order_, std::back_inserter(out), [&](ConvId id) {
            return std::ranges::contains(convs_.at(id).members, peer);
        });
        return out;
    }

    // Conversation ids in creation (or load) order.
    const std::vector<ConvId>& order() const noexcept { return order_; }
    std::size_t size() const noexcept { return convs_.size(); }

private:
    std::unordered_map<ConvId, Conversation> convs_;
    std::unordered_map<PeerId, ConvId> direct_conv_of_;
    std::unordered_map<std::uint64_t, ConvId> by_group_id_;   // empty until groups exist
    std::vector<ConvId> order_;
};
