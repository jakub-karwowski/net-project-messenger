#pragma once

// What the Store sends the frontend about each conversation. Transient: rebuilt
// by the frontend after every start.
//
//   inbound   Ignore: nothing; Notify: ConvNotify{unread}; Forward: MsgAppended
//             (and the conversation counts as read).
//   outbound  Ignore: nothing about our own messages; Notify: MsgAppended and
//             MsgStatus for them; Echo: as Notify, and every received message is
//             sent straight back to its peer without being stored (stress mode).
//   lifecycle Ignore: nothing; Notify: ConvState when the status changes.

#include "Ids.hpp"
#include "Messages.hpp"

#include <unordered_map>

class Subscriptions {
public:
    const msg::Subscription& get(ConvId conv) const {
        const auto it = per_conv_.find(conv);
        return it == per_conv_.end() ? default_ : it->second;
    }

    void set(ConvId conv, const msg::Subscription& sub) { per_conv_[conv] = sub; }

    // Replaces the default and every per-conversation setting.
    void set_default(const msg::Subscription& sub) {
        default_ = sub;
        per_conv_.clear();
    }

    const msg::Subscription& default_subscription() const noexcept { return default_; }

private:
    msg::Subscription default_{};
    std::unordered_map<ConvId, msg::Subscription> per_conv_;
};
