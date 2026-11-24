#pragma once

// Where the Store keeps peers, conversations and history across restarts.
//
// The Store calls it only from its commit functions, on its own thread, after
// updating its in-memory tables. Transient state (links, statuses,
// subscriptions) is never passed here. NullPersistence keeps nothing, which is
// today's behaviour.

#include "ConvStore.hpp"
#include "PeerTable.hpp"

#include <vector>

struct LoadedState {
    std::vector<Peer> peers;                 // persisted fields only
    std::vector<Conversation> convs;         // with their histories
};

class Persistence {
public:
    virtual ~Persistence() = default;

    virtual LoadedState load() = 0;                                   // once, at start-up
    virtual void save_peer(const Peer& peer) = 0;                     // created or changed
    virtual void save_conv(const Conversation& conv) = 0;             // metadata only, not msgs
    virtual void append_message(ConvId conv, const msg::StoredMessage& m) = 0;
    virtual void update_message(ConvId conv, MsgId id, msg::DeliveryState state) = 0;
    virtual void flush() = 0;                                         // on shutdown
};

class NullPersistence final : public Persistence {
public:
    LoadedState load() override { return {}; }
    void save_peer(const Peer&) override {}
    void save_conv(const Conversation&) override {}
    void append_message(ConvId, const msg::StoredMessage&) override {}
    void update_message(ConvId, MsgId, msg::DeliveryState) override {}
    void flush() override {}
};
