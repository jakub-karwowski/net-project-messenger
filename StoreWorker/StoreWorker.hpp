#include <cstdint>
#include <map>
#include <string>

struct ConversationStore {
    struct Message { 
        uint64_t sender_id{}; 
        std::string payload; 
    };
    uint64_t host_id{}; // assigned by the peer
    uint64_t peer_id{}; // assiged by the host
};

class PeerTable {
public:
    struct InboundMapping { uint64_t conversation_id; uint64_t host_id; };
    struct OutboundMapping { uint64_t peer_id; uint64_t host_id; };
private:
    std::map<uint64_t, ConversationStore> conversations_;
    std::map<uint64_t, InboundMapping> inbound_mappings_; // peer_id key
    std::map<uint64_t, OutboundMapping> outbound_mappings_; // conversation_id key
};

class StoreWorker {
    class SubscriptionManager {
    public:
        enum struct OnInboundMessage { Ignore, Notify, Forward };
        enum struct OnOutboundMessage { Ignore, Notify, Echo };
        enum struct OnPeerConnected { Ignore, Notify };
        enum struct OnPeerDisconnected { Ignore, Notify }; 
    private:
        std::map<uint64_t, OnInboundMessage> conversation_on_inbound_;
        std::map<uint64_t, OnOutboundMessage> conversation_on_outbound_;
        std::map<uint64_t, OnPeerConnected> peer_on_peer_connected;
        std::map<uint64_t, OnPeerDisconnected> peer_on_peer_disconnected;
    };
    
};