#pragma once

// Every message that crosses a thread boundary, one std::variant per inbox.
// Plain structs with no behaviour. They stay copyable so tests
// can build them freely, but are always moved between threads.
//
// Everything lives in namespace `msg`: several names (Close, Bind, Stop, Dial)
// are too generic for the global namespace.

#include "Ids.hpp"
#include "TcpSocket.hpp"
#include "Wire.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

namespace msg {

using Clock = std::chrono::steady_clock;
using WallClock = std::chrono::system_clock;
using Bytes = std::vector<std::byte>;

// ---------------------------------------------------------------- shared types

enum class DeliveryState : std::uint8_t { PendingSend, Sent, Failed, Received };
enum class ConvStatus : std::uint8_t { New, Dialing, Connected, Disconnected, Failed };

struct StoredMessage {
    MsgId id = kNoId;
    PeerId sender = kNoId;        // 0 = me
    std::string body;
    WallClock::time_point at{};
    DeliveryState state = DeliveryState::PendingSend;
};

struct ConvSummary {
    ConvId conv = kNoId;
    ConvStatus status = ConvStatus::New;
    wire::Endpoint remote;
    std::size_t unread = 0;
};

// What the Store forwards to the frontend for one conversation. New
// conversations get the default {Notify, Ignore, Notify}: unread counts, no
// echo, status changes.
struct Subscription {
    enum class Inbound : std::uint8_t { Ignore, Notify, Forward };
    enum class Outbound : std::uint8_t { Ignore, Notify, Echo };
    enum class Lifecycle : std::uint8_t { Ignore, Notify };
    Inbound inbound = Inbound::Notify;
    Outbound outbound = Outbound::Ignore;
    Lifecycle lifecycle = Lifecycle::Notify;
    friend bool operator==(const Subscription&, const Subscription&) = default;
};

// Opaque to the Router: echoed back in SendResult so the Store can update the
// right message.
struct SendTag {
    ConvId conv = kNoId;
    MsgId msg = kNoId;
};

enum class LinkDownReason : std::uint8_t {
    PeerClosed, ReadTimedOut, WriteTimedOut, SocketError, HandshakeFailed,
    ProtocolError, Superseded, BalancerEvicted, Shutdown
};

enum class Role : std::uint8_t { Inbound, Outbound };
enum class HandshakeState : std::uint8_t { Connecting, AwaitConnect, AwaitAck, Established };

// Per-link counters since the previous report (Pt-i → Balancer).
struct LinkSample {
    LinkId link = kNoId;
    PeerId peer = kNoId;
    std::uint64_t rx_bytes = 0, tx_bytes = 0, rx_frames = 0, tx_frames = 0;
};

using ReleasedConnection = EpollConnectionPool::ReleasedConnection;

// ---------------------------------------------------------------- Frontend inbox

// from the input thread
struct Key {
    enum Type : std::uint8_t {
        Text, Enter, Backspace, Left, Right, Up, Down, PageUp, PageDown, Tab, Exit
    } type = Text;
    std::string text;
};
struct InputLine { std::uint64_t line_no = 0; std::string text; };   // CLI
struct InputClosed {};                                              // CLI: stdin EOF

// from the Store
struct Reply { ReqId req = 0; bool ok = false; ConvId conv = kNoId; std::string error; };
struct ConvSnapshot { ConvId conv = kNoId; std::vector<StoredMessage> msgs; ConvStatus status{}; };
struct MsgAppended { ConvId conv = kNoId; MsgId id = kNoId; StoredMessage msg; };
struct MsgStatus { ConvId conv = kNoId; MsgId id = kNoId; DeliveryState state{}; };
struct ConvState { ConvId conv = kNoId; ConvStatus status{}; std::string detail; };
struct ConvNotify { ConvId conv = kNoId; std::size_t unread = 0; };
struct ConvList { std::vector<ConvSummary> convs; };
struct SystemNotice { std::string text; };
struct ShutdownDone {};

using FrontendMsg = std::variant<Key, InputLine, InputClosed, Reply, ConvSnapshot, MsgAppended,
                                 MsgStatus, ConvState, ConvNotify, ConvList, SystemNotice,
                                 ShutdownDone>;

// ---------------------------------------------------------------- Store inbox

// from the Frontend (conversation level)
struct SendText { ReqId req = 0; ConvId conv = kNoId; std::string body; };
struct DialReq { ReqId req = 0; wire::Endpoint ep; };
struct ReconnectReq { ReqId req = 0; ConvId conv = kNoId; };
struct CloseReq { ReqId req = 0; ConvId conv = kNoId; };
struct Subscribe { ReqId req = 0; ConvId conv = kNoId; Subscription sub; };
struct SetDefaultSubscription { Subscription sub; };
struct ListReq { ReqId req = 0; };
struct QuitReq { ReqId req = 0; };
// from Pt-i / Router (peer level)
struct ChatIn { PeerId peer = kNoId; LinkId link = kNoId; wire::ChatMessage msg; };
struct PeerHello { LinkId link = kNoId; wire::ConnectInfo info; wire::Endpoint remote; };
struct DialDone { LinkId link = kNoId; PeerId peer = kNoId; wire::ConnectInfoAck ack; };
struct PeerUp { PeerId peer = kNoId; LinkId link = kNoId; };
struct PeerDown { PeerId peer = kNoId; LinkId link = kNoId; LinkDownReason why{}; };
struct SendResult { PeerId peer = kNoId; SendTag tag; bool ok = false; SocketError err = SocketError::Unknown; };
struct RouterStopped {};

using StoreMsg = std::variant<SendText, DialReq, ReconnectReq, CloseReq, Subscribe,
                              SetDefaultSubscription, ListReq, QuitReq, ChatIn, PeerHello,
                              DialDone, PeerUp, PeerDown, SendResult, RouterStopped>;

// ---------------------------------------------------------------- Router inbox

// from the Listener
struct InboundAccepted { TcpConnection conn; };
// from the Store
struct RouteSend { PeerId peer = kNoId; SendTag tag; Bytes frame; };
struct RouteDial { PeerId peer = kNoId; wire::Endpoint ep; Bytes hello_frame; };
struct BindLink { LinkId link = kNoId; PeerId peer = kNoId; Bytes ack_frame; };
struct RejectLink { LinkId link = kNoId; };
struct ClosePeer { PeerId peer = kNoId; };
struct Shutdown { std::chrono::milliseconds grace{1000}; };
// from Pt-i
struct LinkHello { LinkId link = kNoId; wire::ConnectInfo info; };
struct LinkAcked { LinkId link = kNoId; wire::ConnectInfoAck ack; };
struct LinkDown { LinkId link = kNoId; LinkDownReason why{}; SocketError err = SocketError::Unknown; };
struct LinkReleased { LinkId link = kNoId; ReleasedConnection rc; wire::Decoder dec; };
struct LinkAdopted { LinkId link = kNoId; PtId pt = 0; std::uint64_t pool_conn_id = 0; };
struct PtStopped { PtId pt = 0; };
// from the Balancer
struct SetPolicy { std::vector<PtId> next_assignments; std::uint64_t epoch = 0; };
struct MoveLink { LinkId link = kNoId; PtId to = 0; };
struct EvictLink { LinkId link = kNoId; };
struct SpawnPt {};
struct RetirePt { PtId pt = 0; };
struct ProbeAll { std::uint64_t seq = 0; };

using RouterMsg = std::variant<InboundAccepted, RouteSend, RouteDial, BindLink, RejectLink,
                               ClosePeer, Shutdown, LinkHello, LinkAcked, LinkDown, LinkReleased,
                               LinkAdopted, PtStopped, SetPolicy, MoveLink, EvictLink, SpawnPt,
                               RetirePt, ProbeAll>;

// ---------------------------------------------------------------- Pt-i command queue

struct Adopt {
    LinkId link = kNoId; PeerId peer = kNoId; Role role{};
    TcpConnection conn; Bytes pending_tx; wire::Decoder dec; HandshakeState hs{};
};
struct Dial { LinkId link = kNoId; PeerId peer = kNoId; wire::Endpoint ep; Bytes hello_frame; };
struct Bind { LinkId link = kNoId; PeerId peer = kNoId; Bytes ack_frame; };
struct Release { LinkId link = kNoId; };
struct Close { LinkId link = kNoId; bool graceful = true; };
struct Probe { std::uint64_t seq = 0; };
struct Stop { Clock::time_point deadline{}; };

using PtCommand = std::variant<Adopt, Dial, Bind, Release, Close, Probe, Stop>;

// ---------------------------------------------------------------- Balancer inbox

struct PtReport {
    PtId pt = 0; std::uint64_t seq = 0; Clock::time_point at{};
    std::size_t cmd_backlog = 0; std::vector<LinkSample> links;
};
struct TopologyChanged { PtId pt = 0; bool added = false; };
struct LinkGone { LinkId link = kNoId; };
struct StopBalancer {};
struct BalancerTick {};   // from a timer the Balancer registers on its own inbox

using BalancerMsg = std::variant<PtReport, TopologyChanged, LinkGone, StopBalancer, BalancerTick>;

// ---------------------------------------------------------------- size budget

// Every inbox variant must
//  - move without throwing: a throwing move could leave a variant
//    valueless_by_exception in the middle of a push or pop;
//  - fit in kInboxSizeLimit bytes. ThreadQueue stores elements in a std::deque,
//    which allocates 512-byte blocks; above 256 bytes a block holds a single
//    element, so every message would cost an extra malloc/free.
// The variant is as large as its largest alternative. A large alternative that
// is rarely sent can be boxed (std::unique_ptr<Alt>) to keep the common ones
// cheap.
inline constexpr std::size_t kInboxSizeLimit = 256;

template <class V>
inline constexpr bool inbox_ok = std::is_nothrow_move_constructible_v<V>
                              && std::is_nothrow_move_assignable_v<V>
                              && sizeof(V) <= kInboxSizeLimit;

static_assert(inbox_ok<FrontendMsg>,
              "FrontendMsg must move without throwing and be at most kInboxSizeLimit (256) bytes; box a large, rarely sent alternative in std::unique_ptr");
static_assert(inbox_ok<StoreMsg>,
              "StoreMsg must move without throwing and be at most kInboxSizeLimit (256) bytes; box a large, rarely sent alternative in std::unique_ptr");
static_assert(inbox_ok<RouterMsg>,
              "RouterMsg must move without throwing and be at most kInboxSizeLimit (256) bytes; box a large, rarely sent alternative in std::unique_ptr");
static_assert(inbox_ok<PtCommand>,
              "PtCommand must move without throwing and be at most kInboxSizeLimit (256) bytes; box a large, rarely sent alternative in std::unique_ptr");
static_assert(inbox_ok<BalancerMsg>,
              "BalancerMsg must move without throwing and be at most kInboxSizeLimit (256) bytes; box a large, rarely sent alternative in std::unique_ptr");

}  // namespace msg
