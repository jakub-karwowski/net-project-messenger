# TCP p2p messaging app: detailed component design

Expands [project.md](project.md). The code already on the `rewrite` branch
([TcpSocket](TcpSocket/TcpSocket.hpp), [Wire.hpp](Wire.hpp),
[ThreadQueue.hpp](ThreadQueue.hpp), [TerminalUI](TerminalUI/TerminalUI.hpp), the
[RouterWorker](RouterWorker/RouterWorker.hpp) / [StoreWorker](StoreWorker/StoreWorker.hpp)
sketches) is the baseline. Where this document departs from project.md, section 1
says why.

**Scope decisions**

| Topic | In scope now | Designed for later |
|---|---|---|
| Conversations | Direct only: one conversation per peer, one TCP link per peer | Group conversations (§12.1). Ad-hoc routing/relaying is out of scope. |
| Persistence | None: history and ids live in memory | Persistent peers, ids and history behind a Store-only interface (§12.2) |
| Inbound backpressure | Watermarks on the Store and Router inboxes; IO workers stop reading (§16) | Store → Frontend coalescing (§16.6); per-link caps set by the Balancer |

---

## 1. Changes to the original plan

| # | project.md | This design | Why |
|---|---|---|---|
| D1 | One queue per edge (`Store -router q-> Router`, `Pt-i -router q-> Router`, `Balancer -router q-> Router`) | **One inbox per thread.** Each inbox is a `ThreadQueue<std::variant<...>>` of everything that thread accepts | A thread can block on only one `ThreadQueue`. With three inboxes it would have to poll. |
| D2 | Router → Pt-i is a queue | **Two channels.** Router calls `pool.send()` directly: the pool is thread-safe and returns `BufferFull` straight away. Lifecycle commands (adopt, release, dial, probe, shutdown) go through a Pt-i command queue that wakes `receive()` via a new `pool.interrupt()` | Sends skip a hop and a wakeup. Commands that touch per-connection state the Pt-i owns (its decoder) must run on the Pt-i thread. |
| D3 | Inbound messages go Pt-i → Router → Store | **Pt-i → Store directly** for chat frames. Lifecycle events still go through the Router | Otherwise one Router thread serialises all inbound traffic and the Pt-i parallelism is lost. The Store already knows the peer, so ordering is safe (§5.2). |
| D4 | New connection: Listener → Store checks the peer_id | **Listener → Router → Pt-i (handshake) → Router → Store** | The peer_id is only known once the `Connect` frame has been read. Reading it in the Listener would let one slow peer stall `accept()`. |
| D5 | Router → Balancer after every send/receive | **The Balancer samples.** Every tick it probes each Pt-i, and the Pt-i replies with a stats report. That reply is also the heartbeat | One message per chat message to the Balancer is pure overhead. A probe with a deadline detects stalled threads, which a self-sent heartbeat cannot do when the thread is parked in `epoll_wait`. |
| D6 | Dial: "Router under mutex on LoadLog" and the `AssignmentPolicy` mutex | **No shared mutable state between workers.** The Balancer sends `SetPolicy` to the Router | Router and Balancer are single-threaded owners. Message passing removes the lock and the ordering questions that come with it. |
| D7 | Pt-i does a non-blocking TCP connect | Kept, implemented **inside `EpollConnectionPool`** (`connection_dial`) | The pool already has epoll, timers and error reporting. Today `TcpConnection::connect()` blocks for up to the SYN retry budget. |
| D8 | Each conversation is identified by `conv_id` only | Adds a **`link_id`** (one per TCP connection, assigned by the Router, unchanged when the connection moves between Pt-i) | Pool connection ids are per pool and change on a move, and a link exists before its peer is known (during the handshake). |
| D9 | Router/Pt-i tables are keyed by `conv_id` | **Below the Store everything is keyed by `peer_id`.** Conversations exist only in the Store and Frontend | A link connects to a peer, not to a conversation. With groups, one link carries several conversations and one conversation spans several links. Keeping conversations out of the Router means groups need no Router or Pt-i changes (§12.1). |
| D10 | Render reads keys, interprets commands and draws | **Frontend = input thread + `CommandProcessor` + view.** Two frontends share the command layer: the TUI (Keyboard + `TuiView`) and a headless **agent CLI** (`LineReader` + `JsonlWriter`) with no keyboard listener and no rendering (§14) | Command handling becomes a pure, testable layer. The CLI lets scripts and agents drive real nodes end to end, without a TTY. |
| D12 | (first version of this design) The Store encodes every frame and walks a peer's endpoints one dial at a time | **The Store hands down values and intent only.** `RouteDial` carries the ordered endpoint list and a `wire::ConnectInfo`; `BindLink` a `wire::ConnectInfoAck`; `RouteSend` a `wire::ChatMessage`. The thread that writes to the connection encodes: Pt-i for the handshake, the Router for chat. Pt-i tries the endpoints in turn within one link and reports one outcome | The Store stays at the level of identities and conversations; byte formats and connection attempts (timeouts, ordering, retries) live with the code that owns the sockets. Ids are still decided by the Store, as values. |
| D13 | (first version of this design) The Store filters `PeerDown`s of links it no longer uses | **The Router reports peers, not links.** `PeerUp` / `DialDone` name the link now in use; `PeerDown` means the peer has no live link and no dial in progress. A link the Router drops as a duplicate (`Superseded`, `RejectLink`) is never reported | With duplicate links (reconnect, simultaneous dial) the Store cannot tell from link ids alone which one survives: keeping our dial and dropping the inbound link, then the dial failing, left it waiting forever. The Router is the one that decides, so it is the one that reports. |
| D14 | (first version of this design) Every queue unbounded, no inbound backpressure | **IO workers stop reading when the Store's or Router's inbox passes a high watermark**, and resume below a low one. Reading is paused per connection inside the pool, so writes, timers and handshakes go on. The UI's inbox is not watched: a slow UI is handled between the Store and the Frontend (§16) | Under overload the backlog grew without bound: 420–464 MB and 16 s round trips in a 20 s run at 1024 peers, and forced shutdowns. Stopping reads lets TCP slow the senders down instead. |
| D11 | Requests from the UI are fire-and-forget | Every frontend request carries a **`ReqId`**; the Store answers each with exactly one `Reply` | An agent has to know which `conv` its `/dial` created and whether a command failed, without guessing from later events. |

Everything else (thread set, ownership of PeerTable/ConvStore/ConnTable/LoadLog,
the scenario list) stays as in project.md.

---

## 2. Threads and inboxes

```
                    ┌───────────┐  Key | InputLine
   Input ──────────►│ Frontend  │◄──────────────┐ FrontendMsg (events, Reply)
                    └─────┬─────┘               │
                StoreMsg (requests + ReqId)     │
                          ▼                     │
                    ┌───────────┐───────────────┘
      ┌────────────►│   Store   │◄───► Persistence (interface; no-op today, §12.2)
      │ ChatIn      └─────┬─────┘▲
      │       StoreToRouter│      │RouterToStore
      │                   ▼      │
      │             ┌───────────┐◄──── Listener (InboundAccepted)
      │             │  Router   │◄──── Balancer (SetPolicy / Move / Evict / Spawn / Retire / Probe)
      │             └─┬───────┬─┘
      │  pool.send()  │       │ PtCommand queue (+ pool.interrupt())
      │  (direct)     ▼       ▼
      │             ┌───────────┐  PtReport (heartbeat + stats)
      └─────────────│  Pt-0..n  │───────────────────────► Balancer
                    └─────┬─────┘
                          │ PtEvent (link lifecycle)
                          └──────────────────────────────► Router

   Logger: every thread pushes LogRecord; it is the only consumer.
```

| Thread | Blocks on | Inbox type | Owns |
|---|---|---|---|
| Input: Keyboard (TUI) | `TerminalUI::read_byte(timeout)` | none (stop flag) | escape-sequence decoder |
| Input: LineReader (CLI) | `poll(stdin, WakeupEvent)` | none (`stop()` raises the event) | partial-line buffer, line counter |
| Frontend (main thread) | `frontend_q.pop()` | `FrontendMsg` | `CommandProcessor` (active conv, `ReqId`s); TUI: `TerminalUI`, layout, `ChatModel`/`InputModel`; CLI: `JsonlWriter` |
| Store | `store_q.pop()` | `StoreMsg` | `PeerTable`, `ConvStore`, subscriptions, id minting, persistence hooks |
| Router | `router_q.pop()` | `RouterMsg` | `ConnTable`, Pt-i handles and threads, `AssignmentPolicy` cursor |
| Listener | `TcpListener::accept_client()` | none (`stop()` wakes accept) | `TcpListener` |
| Pt-i | `pool.receive()` | `ThreadQueue<PtCommand>`, drained on `Interrupted` | `EpollConnectionPool`, per-link `wire::Decoder` + handshake state |
| Balancer | `balancer_q.pop()`, which also yields its `BalancerTick` (a queue timer, §5.4) | `BalancerMsg` | `LoadLog`, policy computation |
| Logger | `log_q.pop()` | `LogRecord` | log sink |

**Queue rules.**
- Every queue is unbounded, so a `push` never blocks. That keeps the
  Store↔Router and Router↔Pt-i cycles deadlock-free.
- Backpressure is applied where data enters, not by blocking pushes: the pool's
  TX buffer limit (`BufferFull` surfaces as a failed send in the UI), and IO
  workers that stop reading while the Store's or Router's inbox is too deep
  (§16).
- A `push` that returns `false` means the target is shutting down. The sender
  drops the message and logs it at debug level.

---

## 3. Identifiers

| Id | Minted by | Scope / lifetime | Meaning |
|---|---|---|---|
| `peer_id` | Store | local, stable | **our** name for a peer. Sent as `ConnectInfo.peer_id`; the peer re-presents it as `assigned_peer_id`. The key for everything below the Store (D9). |
| `host_id` | the peer | stable | the peer's name **for us**, learned from Connect/Ack and re-presented when we dial it. |
| `conv_id` | Store | local, stable | one conversation. Today each peer has exactly one direct conversation. Never leaves the Store/Frontend. |
| `group_id` | group creator | global | future: identifies a group conversation on the wire (§12.1). `0` means the direct conversation. |
| `msg_id` | Store | per conv, monotonic | index in the conversation history. |
| `link_id` | Router | one TCP connection, survives moves | the routing key below the Router. |
| `pt_id` | Router | lifetime of a Pt-i thread | worker index. |
| `pool_conn_id` | `EpollConnectionPool` | one pool membership | changes when a link moves. Never leaves Router/Pt-i. |

**Id rules.**
- Every id the Store mints (`peer_id`, `conv_id`) is a **random 64-bit value** from
  one `IdMinter` (`mt19937_64` seeded from `std::random_device`, retried on
  collision with the known set), not a counter:
  - a sequential `peer_id` lets anyone who guesses it impersonate a known peer on
    reconnect;
  - random ids stay unique across restarts, which persistence will need (§12.2).
- `0` is reserved as "unknown / none" for every id.

**Wire fields of `ChatMessage`** (this changes what the current
[Wire.hpp](Wire.hpp) comment says):
- `conversation_id` = `group_id`, so `0` for a direct message. It is no longer the
  sender's local conv id, which meant nothing to the receiver.
- `sender_id` = the sender's `host_id` for this peer, which is the receiver's own
  `peer_id` for the sender. The receiver checks that it matches the link's peer
  and drops the frame on mismatch. For relayed group messages this field would
  need a global identity; relaying is out of scope.

---

## 4. Messages (one variant per inbox)

They live in `Messages/Messages.hpp` (its own header-only target, because it needs the socket and wire types), in namespace `msg` (names such as `Close`, `Bind` and `Stop` are too generic for the global namespace). They are plain structs with no behaviour, always moved between threads.

```cpp
using ConvId = uint64_t; using PeerId = uint64_t; using LinkId = uint64_t;
using PtId = uint32_t;   using MsgId = uint64_t;

// Opaque to the Router: echoed back in SendResult so the Store can update the
// right message (per member once groups exist).
struct SendTag { ConvId conv; MsgId msg; };

enum class LinkDownReason { PeerClosed, ReadTimedOut, WriteTimedOut, SocketError,
                            HandshakeFailed, ProtocolError, Superseded,
                            BalancerEvicted, Shutdown };

using ReqId = uint64_t;   // per frontend, 0 = no reply wanted (§14.3)

// ---- Frontend inbox (conversation level); the Store doesn't know which frontend runs
// from the input thread
struct Key        { enum Type { Text, Enter, Backspace, Left, Right, Up, Down,
                                PageUp, PageDown, Tab, Exit } type; std::string text; };  // TUI
struct InputLine  { uint64_t line_no; std::string text; };                                // CLI
struct InputClosed {};                                                                    // CLI: stdin EOF
// from the Store
struct Reply      { ReqId req; bool ok; ConvId conv; std::string error; };  // exactly one per ReqId != 0
struct ConvSnapshot   { ConvId conv; std::vector<StoredMessage> msgs; ConvStatus status; };
struct MsgAppended    { ConvId conv; MsgId id; StoredMessage msg; };
struct MsgStatus      { ConvId conv; MsgId id; DeliveryState state; };
struct ConvState      { ConvId conv; ConvStatus status; std::string detail; };
struct ConvNotify     { ConvId conv; size_t unread; };
struct ConvList       { std::vector<ConvSummary> convs; };
struct SystemNotice   { std::string text; };          // notices not tied to a request
struct ShutdownDone   {};
using FrontendMsg = std::variant<Key, InputLine, InputClosed, Reply, ConvSnapshot, MsgAppended,
                                 MsgStatus, ConvState, ConvNotify, ConvList, SystemNotice,
                                 ShutdownDone>;
// Each frontend gets only one of Key / InputLine+InputClosed; it handles the others
// with an explicit no-op, so the no-catch-all rule (§9) still holds.

// ---- Store inbox
// from the Frontend (conversation level); each carries a ReqId (§14.3)
struct SendText   { ReqId req; ConvId conv; std::string body; };   // Reply.conv = conv, MsgAppended carries the msg_id
struct DialReq    { ReqId req; wire::Endpoint ep; };               // Reply.conv = the new conversation
struct ReconnectReq { ReqId req; ConvId conv; };
struct CloseReq   { ReqId req; ConvId conv; };                     // → ClosePeer to the Router for each member
struct Subscribe  { ReqId req; ConvId conv; Subscription sub; };   // see §6.3
struct SetDefaultSubscription { Subscription sub; };              // applied to existing and new convs
struct ListReq    { ReqId req; };
struct QuitReq    { ReqId req; };
// from Pt-i / Router (peer level)
struct ChatIn     { PeerId peer; LinkId link; wire::ChatMessage msg; };  // Pt-i, D3
struct PeerHello  { LinkId link; wire::ConnectInfo info; wire::Endpoint remote; };
struct DialDone   { LinkId link; PeerId peer; wire::ConnectInfoAck ack; wire::Endpoint remote; };  // remote: the endpoint that answered
struct PeerUp     { PeerId peer; LinkId link; };
struct PeerDown   { PeerId peer; LinkId link; LinkDownReason why; SocketError err; };  // for a dial: after every endpoint failed
struct SendResult { PeerId peer; SendTag tag; bool ok; SocketError err; };
struct RouterStopped {};
using StoreMsg = std::variant<SendText, DialReq, ReconnectReq, CloseReq, Subscribe,
                              SetDefaultSubscription, ListReq, QuitReq,
                              ChatIn, PeerHello, DialDone, PeerUp, PeerDown, SendResult,
                              RouterStopped>;

// ---- Router inbox (peer level only, D9)
struct InboundAccepted { TcpConnection conn; };   // Listener; conn.remote_endpoint() holds the peer address
struct RouteSend   { PeerId peer; SendTag tag; wire::ChatMessage msg; };   // the Router encodes
struct RouteDial   { PeerId peer; std::vector<wire::Endpoint> endpoints; wire::ConnectInfo hello; };  // tried in order
struct BindLink    { LinkId link; PeerId peer; wire::ConnectInfoAck ack; };
struct RejectLink  { LinkId link; };
struct ClosePeer   { PeerId peer; };
struct Shutdown    { std::chrono::milliseconds grace; };
// from Pt-i (PtEvent)
struct LinkHello   { LinkId link; wire::ConnectInfo info; };
struct LinkAcked   { LinkId link; wire::ConnectInfoAck ack; wire::Endpoint remote; uint64_t pool_conn_id; };  // the connection the dial ended on
struct LinkDown    { LinkId link; LinkDownReason why; SocketError err; };
struct LinkReleased{ LinkId link; ReleasedConnection rc; wire::Decoder dec; };
struct LinkAdopted { LinkId link; PtId pt; uint64_t pool_conn_id; };
struct PtStopped   { PtId pt; };
// from Balancer
struct SetPolicy   { std::vector<PtId> next_assignments; uint64_t epoch; };
struct MoveLink    { LinkId link; PtId to; };
struct EvictLink   { LinkId link; };
struct SpawnPt     {};  struct RetirePt { PtId pt; };
struct ProbeAll    { uint64_t seq; };
using RouterMsg = std::variant<...all of the above...>;

// ---- Pt-i command queue
struct Adopt   { LinkId link; PeerId peer; Role role; TcpConnection conn;
                 std::vector<std::byte> pending_tx; wire::Decoder dec; HandshakeState hs; };
struct Dial    { LinkId link; PeerId peer; std::vector<wire::Endpoint> endpoints; wire::ConnectInfo hello; };
struct Bind    { LinkId link; PeerId peer; wire::ConnectInfoAck ack; };
struct Release { LinkId link; };
struct Close   { LinkId link; bool graceful; LinkDownReason why; };   // why: what Pt-i reports in its LinkDown
struct Probe   { uint64_t seq; };
struct Stop    { std::chrono::steady_clock::time_point deadline; };
using PtCommand = std::variant<Adopt, Dial, Bind, Release, Close, Probe, Stop>;

// ---- Balancer inbox
struct PtReport { PtId pt; uint64_t seq; Clock::time_point at; size_t cmd_backlog;
                  std::vector<LinkSample> links; };   // LinkSample: link, peer, rx/tx bytes, rx/tx frames since last,
                                                      // gone (left this worker since the last report: final counters)
struct TopologyChanged { PtId pt; bool added; };        // Router → Balancer
struct LinkGone  { LinkId link; };
struct StopBalancer {};
struct BalancerTick {};                                  // from the Balancer's own queue timer (§5.4)
using BalancerMsg = std::variant<PtReport, TopologyChanged, LinkGone, StopBalancer, BalancerTick>;
```

**Size budget.** Measured with g++ 13.3 (x86-64, libstdc++), using the real
`wire::*`, `TcpConnection`, `ReleasedConnection` and `Decoder` types:

| Inbox | `sizeof` | Largest alternative | Hot alternatives |
|---|---|---|---|
| `FrontendMsg` | 88 | `MsgAppended` 80 | `MsgAppended` 80, `MsgStatus` 24 |
| `StoreMsg` | 96 | `PeerHello` 88 | `ChatIn` 64, `SendResult` 32, `SendText` 48 |
| `RouterMsg` | 184 | `LinkReleased` 176, `InboundAccepted` 96 | `RouteSend` 72 |
| `PtCommand` | 192 | `Adopt` 184 | none (commands are rare, D2) |
| `BalancerMsg` | 64 | `PtReport` 56 | `PtReport` |
| `LogRecord` | 72 | – | – |

- **What drives the sizes:** `TcpConnection` is 96 bytes (`Options` 40,
  `TcpEndpoint` 40), `ReleasedConnection` 136, `Decoder` 32, `std::string` 32,
  `std::vector` 24.
- **A queue hop costs about 120 ns, and the mutex and condition variable dominate
  it.** In a cross-thread `ThreadQueue` benchmark the variant size adds about
  20–40 ns between 72 and 200 bytes.
- **The cliff is 512 bytes.** At that size `std::deque` holds one element per
  512-byte block, so every message costs a malloc and free, and the hop takes about
  250 ns.
- **The Router's inbox pays a little for rare messages.** `RouteSend` takes a
  184-byte slot because of `LinkReleased` and `Adopt`, which costs half a deque
  allocation per send. That is small next to the queue hop and the `send()`
  syscall.
- **Payload allocations cost more than variant sizes.** A chat message costs about
  3 heap allocations on send (body, frame, the frontend's copy) and about 2 on
  receive (the decoded body, the frontend's copy).

**Rules**, enforced in `Messages.hpp`:
```cpp
template <class V> constexpr bool inbox_ok =
    std::is_nothrow_move_constructible_v<V> && std::is_nothrow_move_assignable_v<V>
    && sizeof(V) <= 256;   // at least two per deque block
static_assert(inbox_ok<FrontendMsg> && inbox_ok<StoreMsg> && inbox_ok<RouterMsg>
           && inbox_ok<PtCommand> && inbox_ok<BalancerMsg>);
```
- Moves must not throw, so a push or pop can never leave a variant
  `valueless_by_exception`.
- Messages stay copyable for tests, but are always moved: `push(std::move(m))`,
  and handlers take `X&`.
- If profiling ever shows the Router inbox as the bottleneck, there are two cheap
  fixes. Box the rare large alternatives (`std::unique_ptr<LinkReleased>`,
  `std::unique_ptr<Adopt>`), which brings `RouterMsg` to about 80 bytes and
  `PtCommand` to about 88. Better, give `ThreadQueue` a growable ring buffer
  instead of `std::queue`, which removes steady-state allocations from every
  inbox. Neither is done now.

**Frame encoding rules (D12).**
- The Store never handles bytes. It decides every id and puts them into values:
  `wire::ConnectInfo` (in `RouteDial`), `wire::ConnectInfoAck` (in `BindLink`),
  `wire::ChatMessage` (in `RouteSend`).
- Only the thread that writes to a connection encodes:
  - Pt-i encodes the hello (when a dial connects) and the Ack (on `Bind`);
  - the Router encodes chat, because it calls `pool->send()` itself (D2).
- Pt-i decodes inbound frames and hands the Store the decoded
  `wire::ChatMessage` without interpreting it.
- The Store refuses an over-long message before storing it, using
  `wire::kMaxChatBodySize` (`kMaxPayloadSize` minus the 20 bytes of ids and
  length), so an encode below it cannot fail on size.

---

## 5. Components

### 5.1 Router (`RouterWorker/`)

**State**
```cpp
struct Link {
    LinkId id; PeerId peer;            // 0 for an inbound link until it is bound
    PtId pt; uint64_t pool_conn_id;    // pt: the target while moving; conn: 0 until adopted / acked
    enum class Status { Dialing, Handshaking, Connected, Moving, Closing } status;
    Role role;                         // Inbound | Outbound
    wire::Endpoint remote;             // inbound: where it came from (for PeerHello)
    bool quiet;                        // dropped as a duplicate: never reported (D13)
    bool ever_connected;               // LinkGone to the Balancer only for these
    Clock::time_point since, connected_at;
    std::deque<RouteSend> parked;      // sends that arrived while Moving
    std::optional<Close> close_after_move;   // a close requested while Moving
};
struct PeerLinks { LinkId current; LinkId dialing; };   // the Router's view of a peer
struct PtHandle {
    PtId id;
    std::unique_ptr<EpollConnectionPool> pool;      // Router calls pool->send()
    std::unique_ptr<ThreadQueue<PtCommand>> cmds;   // + pool->interrupt() after push
    std::unique_ptr<IoWorker> worker;
    std::unordered_set<LinkId> links; bool retiring, stopping, stopped;
    std::unique_ptr<WorkerThread> thread;           // last: stopped and joined first
};
std::unordered_map<LinkId, Link> links_;
std::unordered_map<PeerId, PeerLinks> peers_;
std::map<PtId, PtHandle> pts_;
AssignmentPolicy policy_;                            // plain struct, no mutex (D6)
```
This replaces `RoutingTable` in [RouterWorker.hpp](RouterWorker/RouterWorker.hpp).
The map key changes from conversation to peer, and `RoutingEntry::Status` grows
from `{Normal, Moving, Closed}` to the five states above. The Router never sees a
`conv_id`.

**`AssignmentPolicy::next()`**
1. Take the next entry of the Balancer's list. Skip pts that are retiring or
   missing.
2. When the list is exhausted or stale, fall back to the pt with the fewest
   links.
3. When there are no pts at all, spawn one. When every pt is at
   `max_links_per_pt` and fewer than `max_pts` exist, spawn one too.

**Handlers (summary)**

| Message | Action |
|---|---|
| `InboundAccepted` | Over `max_links`: drop the connection and log. Otherwise create a Link (`Handshaking`, Inbound), choose a pt, push `Adopt{role=Inbound, hs=AwaitConnect}` and call `interrupt()`. |
| `RouteDial` | If the peer already has a live link: reply `PeerUp` (idempotent). Otherwise create a Link (`Dialing`), choose a pt, push `Dial{endpoints, hello}`. A final `LinkDown` for a dialing link becomes `PeerDown{why, err}` to the Store. `ClosePeer` for a dialing peer cancels the dial. |
| `LinkHello` | Forward to the Store as `PeerHello{link, info, remote}`. |
| `BindLink` | The link died meanwhile: `PeerDown{HandshakeFailed}` to the Store, unless the peer has a current link or a dial. Otherwise resolve duplicates with the rule in §7.4 (the loser is closed quietly), then `current = link`, push `Bind{ack}`, `Connected`, `PeerUp` to the Store. |
| `RejectLink` | Close the handshaking link quietly. |
| `LinkAcked` | Outbound handshake done: record `pool_conn_id`, `Connected`, `current = link`, `DialDone{remote}` to the Store. |
| `RouteSend` | Look up `peer_to_link_`. `Connected`: encode `msg`, call `pool->send()` and return `SendResult` from its result. `Moving`: park the send (still as a value). Missing or other states: `SendResult{ok=false, ConnectionNotFound}`. |
| `LinkDown` | Erase the link, fail parked sends, `LinkGone` to the Balancer if it was ever connected. If it was the peer's current link or dial, and the peer has neither left, and it is not quiet, and the Router is not stopping: `PeerDown{why, err}` to the Store. |
| `MoveLink` | Only allowed when `Connected` and `to` is alive and not retiring. Set `Moving`, push `Release` to the source pt. |
| `LinkReleased` | Push `Adopt{conn, pending_tx, dec, hs=Established}` to the target pt. |
| `LinkAdopted` | Update pt and `pool_conn_id`, go back to `Connected`, flush parked sends in order. |
| `EvictLink` / `ClosePeer` | Push `Close{graceful=true, why}` (`ClosePeer` also closes a dial in progress). The final state arrives via `LinkDown`. `ClosePeer` for a peer with nothing open answers `PeerDown{PeerClosed}` at once. A close of a link that is moving waits until the move completes. |
| `SpawnPt` / `RetirePt` | Spawn: create pool, queue and thread, send `TopologyChanged{added}`. Retire (refused when no other usable pt exists): mark it retiring, `MoveLink` every connected link elsewhere; once it holds no link, `Stop`; on its `PtStopped`, join, erase and `TopologyChanged{removed}`. |
| `PtStopped` | Join the thread. Links still recorded there are gone (`Shutdown`). Outside a shutdown the pt is erased, and when none is left a new one is started. During a shutdown: `RouterStopped` once every pt has stopped. |
| `ProbeAll` | Push `Probe{seq}` to every pt. |
| `Shutdown` | §7.8. |

**Invariants.**
- Only the Router changes `Link::status`. A Pt-i reports facts; the Router
  decides.
- Per peer, at most one link is in use (`current`) and one dial is in progress.
- The Store hears about peers, not links (D13).
- `run()` ends by stopping and joining every IO worker it started (§9.1, rule 4).

### 5.2 Pt-i, the IO worker (`IoWorker/`)

**State**
```cpp
struct LinkState {
    LinkId link; PeerId peer; Role role;
    enum class Hs { Connecting, AwaitConnect, AwaitBind, AwaitAck, Established } hs;
    wire::Decoder dec;
    wire::ConnectInfo hello;                  // outbound: encoded and sent once connected
    std::vector<wire::Endpoint> to_try;       // outbound: endpoints not attempted yet
    wire::Endpoint dialing;                   // outbound: the endpoint being attempted
    uint64_t rx_frames, tx_frames;   // since the last report; bytes come from pool.take_stats()
    int bad_frames;                  // malformed frames so far
};
std::vector<LinkSample> departed_;   // final counters of links gone since the last report
std::unordered_map<uint64_t /*pool_conn_id*/, LinkState> by_conn_;
std::unordered_map<LinkId, uint64_t> by_link_;
```

**Loop**
```
for (;;) {
    auto r = pool.receive(rx_buf);
    if (r)                                   -> on_bytes(r->connection_id, span)
    else switch (r.error())
      {0, Interrupted}                       -> drain cmds.try_pop() until empty
      {0, Stopped}                           -> report PtStopped, return
      {id, Connected}                        -> dial finished: encode + send hello, hs = AwaitAck   (new event, §8.1)
      {id, err} while Connecting             -> next endpoint in to_try (same link_id); none left: LinkDown{SocketError, err}
      {id, err}                              -> LinkDown{link, map(err)}, erase
}
```

**`on_bytes`**: feed `dec`, then for each decoded frame:

| hs | frame | action |
|---|---|---|
| AwaitConnect | `ConnectInfo` | `LinkHello` → Router, hs = AwaitBind (a second hello is a protocol error). |
| AwaitAck | `ConnectInfoAck` | `LinkAcked` → Router, hs = Established, then `set_timeouts(established)`. |
| Established | `ChatMessage` | `ChatIn{peer, link, msg}` → **Store** (D3). |
| any | anything else (including chat before the handshake is complete) | protocol error: `connection_drop`, `LinkDown{ProtocolError}`. |
| any | `WireError::UnknownType` | skipped, not counted (invariant 5). |
| any | any other `WireError` | counted; the decoder resyncs. More than `max_bad_frames` (3) → protocol error. |

**Ordering argument for D3.** A link's peer_id is set when Pt-i handles `Bind` (or
the Ack). The Store created that peer before it sent `BindLink` or `RouteDial`. So
any `ChatIn` the Store receives names a peer it already has. `ChatIn` may overtake
`PeerUp`; the Store treats a message from a peer that is not "up" yet as implying
it is up. Protocol rule: **the initiator sends no `ChatMessage` before it has
received the `ConnectAck`**. Pt-i sends the Ack itself while handling `Bind`, after
the peer is set, so no reply can arrive before the peer is known.

**Commands**
- `Adopt`: `pool.connection_add(conn, timeouts_for(hs), pending_tx)`, record the
  state, send `LinkAdopted` → Router, then run the decoder once (a decoder handed
  over by another worker is handled at once, not after the next read).
- `Dial`: keep `hello` and the endpoint list, then
  `pool.connection_dial(endpoints[0], connect_timeout)`. A failed attempt moves to
  the next endpoint under the same `link_id` (a new pool connection id each
  time). The hello is encoded and sent when `Connected` arrives; `LinkAcked`
  carries the endpoint that answered. Only when every endpoint has failed does
  the Router get `LinkDown`, with the last attempt's `SocketError`.
- `Bind`: set the peer, encode and `pool.send()` the Ack, hs = Established,
  `set_timeouts(established)`.
- `Release`: `pool.connection_release(id)` returns the `TcpConnection` and unsent
  TX. Move the decoder out and send `LinkReleased` → Router. This runs on the Pt-i
  thread, so no `receive()` is in flight. Only an established link can move. The
  counters so far go into this worker's next report; the new owner starts at
  zero. A link this worker no longer has gets `LinkDown{ConnectionNotFound}`.
- `Close{graceful, why}`: `connection_remove` (established and graceful: flush
  first) or `connection_drop`, then `LinkDown{why}` → Router. The pool reports
  nothing for a removal it was asked to make, so Pt-i reports it itself.
- `Probe`: build a `PtReport`: per link `take_stats()` bytes plus the frame
  counters, plus the final counters of links gone since the last report; reset
  them, push it to the Balancer. `tx_frames` counts only the frames this worker
  writes (hello, Ack): chat is sent by the Router.
- `Stop{deadline}`: every link: established → `connection_remove` (flush),
  otherwise `connection_drop`, each with `LinkDown{Shutdown}`. Then the loop
  calls `receive(deadline)` (a wait bounded by `shutdown_poll`, 20 ms) until
  `connection_count() == 0` or the deadline passes, then `pool.stop()` and
  `PtStopped`. After `Stop`, `Adopt` and `Dial` are answered with
  `LinkDown{Shutdown}`.
- `pool.stop()` from outside (the Supervisor's emergency path): `receive()`
  returns `Stopped`, Pt-i sends `PtStopped` and returns at once.
- A `Bind` or `Close` for a link that is already down is ignored: its `LinkDown`
  has been sent.

**Timeouts per handshake state**

| State | Read timeout | Write timeout |
|---|---|---|
| Connecting, AwaitConnect, AwaitAck | handshake, e.g. 5 s | 5 s |
| Established | 0 (disabled) | 5 s |

Established links rely on `SO_KEEPALIVE` for dead-peer detection, so an idle chat
is not killed. A `Ping` frame can come later if faster detection is needed.

### 5.3 Store (`StoreWorker/`)

The Store is the only component that knows about conversations. It translates
between the conversation level (Frontend) and the peer level (Router, Pt-i).

**State**
```cpp
struct StoredMessage { MsgId id; PeerId sender; /*0 = me*/ std::string body;
                       system_clock::time_point at; DeliveryState state; };
enum class DeliveryState { PendingSend, Sent, Failed, Received };
enum class ConvStatus { New, Dialing, Connected, Disconnected, Failed };

struct Peer {                                    // one remote node
    PeerId peer_id; PeerId host_id;
    std::vector<wire::Endpoint> endpoints;       // last advertised, used for reconnect
    enum class Link { Down, Dialing, Handshaking, Up } link_state; LinkId link;
};
struct Conversation {
    ConvId id;
    enum class Kind { Direct, Group } kind;      // only Direct is created today
    uint64_t group_id;                           // 0 for Direct (the wire conversation_id)
    std::vector<PeerId> members;                 // Direct: exactly one
    std::vector<StoredMessage> msgs; size_t read_upto;
};

class PeerTable {                                // owned by the Store
    std::unordered_map<PeerId, Peer> peers_;                 // recognition: our name → peer
    std::unordered_map<LinkId, PeerId> by_link_;
};
class ConvStore {
    std::unordered_map<ConvId, Conversation> convs_;
    std::unordered_map<PeerId, ConvId> direct_conv_of_;      // peer → its direct conv
    std::unordered_map<uint64_t, ConvId> by_group_id_;       // empty until groups (§12.1)
};
```
This replaces the sketch in [StoreWorker.hpp](StoreWorker/StoreWorker.hpp).

- The sketch's `InboundMapping` and `OutboundMapping` are replaced by the
  `Peer` / `Conversation` split.
- Peer and conversation stay **separate records with a membership list**, even
  though today every conversation has one member. That is the seam groups need.
- A conversation's status is derived from its members' link states:
  - Direct: the status of its single peer.
  - Group: Connected if any member is up.

**Resolving an inbound `ChatIn`**
1. `msg.sender_id != peer`: the frame does not belong to this link. Drop it and
   log.
2. `msg.conversation_id == 0`: append to `direct_conv_of_[peer]`. Create the
   conversation if it is missing.
3. Otherwise look the id up in `by_group_id_`. If found and `peer` is a member,
   append there. Otherwise drop and log. Today the lookup always misses.

**Handshake logic (inbound `PeerHello`)**
1. Look up `info.assigned_peer_id` in `peers_`. If found, this is a known peer
   reconnecting: reuse the record.
2. Otherwise mint a `peer_id` and create the Peer and its direct conversation.
3. Set `host_id = info.peer_id` and `endpoints = info.endpoints`.
4. Reply `BindLink{link, peer_id, ConnectInfoAck{peer_id, host_id}}`.
5. Garbage (`info.peer_id == 0`) gets `RejectLink`.

**Dial (`DialReq` / `ReconnectReq`)**
- `DialReq` creates a Peer (fresh `peer_id`, `host_id = 0`) and its direct
  conversation. `ReconnectReq` resolves the conversation to its peer, or to its
  members for a group.
- `RouteDial{peer, endpoints, ConnectInfo{peer_id, host_id, my_endpoints}}`, one
  per dial. The list is the requested endpoint first (for `DialReq`), then the
  peer's other dialable endpoints in the order it advertised them. Port 0 is
  never dialled. The Store does not retry: the Router answers with one
  `DialDone` or one `PeerDown`.
- `DialDone`: `host_id = ack.peer_id`, `remote = the endpoint that answered`,
  then check `ack.ack_peer_id == peer_id` (log a mismatch), then mark the peer
  Up.

**Send**
1. Append `{PendingSend}` to the conversation.
2. For each member, send
   `RouteSend{member, {conv, id}, ChatMessage{group_id, member.host_id, body}}`.
   Direct conversations have exactly one member.
3. `SendResult` moves the message to `Sent` or `Failed`. With several members the
   state becomes per member; see §12.1.

"Sent" means the transport accepted the message. True delivery needs an ack frame
(§8.4).

**Persistence hooks.** Every mutation of a `Peer`, `Conversation` or
`StoredMessage` goes through one private function per kind:
`commit_peer(const Peer&)`, `commit_conv(const Conversation&)` and
`commit_message(ConvId, const StoredMessage&)`. Each updates the in-memory table
and then calls the `Persistence` interface (§12.2). Today that interface is a
no-op.

**Decisions made in the implementation (step 3)**
- **`DialReq` for a known endpoint** (one a peer advertised, or the address of its
  last link) reuses that peer and its conversation instead of creating a second
  one. It dials only if the peer is down; the `Reply` carries the existing conv.
- **A send that cannot leave is refused, not stored:** unknown conv, empty body,
  a body over `wire::kMaxChatBodySize` ("message too large"), or a member not
  `Up` ("not connected"). Only an
  accepted send is appended as `PendingSend`.
- **While quitting** every frontend request except `QuitReq` gets
  `Reply{ok = false, "shutting down"}`, and inbound hellos get `RejectLink`.
- **Status mapping** for a direct conv, from its peer: `Up` → Connected;
  `Dialing` / `Handshaking` → Dialing; `Down` → Failed if the last attempt
  failed, else Disconnected if it was ever up, else New. Loaded peers count as
  "ever up".
- **One dial, one outcome:** the Store sends every dialable endpoint in one
  `RouteDial` and the conv stays Dialing until `DialDone` (Connected) or
  `PeerDown` (Failed, with `why` and the socket error as detail, for example
  `socket_error: Connection refused`). A peer without a dialable endpoint gets
  "no known endpoint" on `ReconnectReq`.
- **Peer-level reports (D13):** `PeerDown` is taken at face value; the Router never
  reports a link it dropped as a duplicate. A hello from a peer that is already up
  leaves it up (no status event); the Router's `PeerUp` names the link that
  survives. A dial closed on purpose (`PeerClosed`) is not shown as Failed.
- **A `ChatIn` while `Dialing` or `Handshaking` marks the peer up.** It can
  overtake `PeerUp` and `DialDone`, which travel through the Router.
- **Echoed messages** are sent with an empty `SendTag` (`conv = 0`), so their
  `SendResult` is recognised and ignored.
- **`SetDefaultSubscription`** replaces the default and every per-conv setting.
  With `inbound = Forward` it sends a snapshot of every existing conversation.
- **Commits** happen only when a persisted field changes: a new peer or conv,
  `host_id` / `endpoints` / `remote`, `read_upto`, a new message, or a delivery
  state. Link state changes are transient and never committed.
- **`PeerTable` has no `by_link_` index.** Every peer-level message names its
  peer, except `PeerHello`, which creates or recognises the peer itself.

**Subscriptions (§6.3)** decide what goes to Frontend. The current
`SubscriptionManager` enums (`Ignore`/`Notify`/`Forward`/`Echo`) stay. They are
keyed by `conv_id`, with lifecycle notifications fanned out from peer to
conversation. `Echo` bounces inbound messages back without storing them, which is
the stress-test mode used in earlier experiments.

### 5.4 Balancer (`BalancerWorker/`)

**State** (replaces `conversatoin_metrics_`)
```cpp
struct LinkLoad { LinkId link; PeerId peer; PtId pt; double rx_bps, tx_bps, fps;  // EWMA
                  Clock::time_point last_active, last_moved; };
struct PtLoad   { PtId pt; uint64_t last_seq_seen; Clock::time_point last_report;
                  size_t links; double load; bool stalled; size_t cmd_backlog; };
std::unordered_map<LinkId, LinkLoad> links_;
std::map<PtId, PtLoad> pts_;
```

**Where ticks come from.** The Balancer registers a timer on its own inbox in its
constructor: `tick_timer_ = inbox.every(config.period, BalancerTick{})`. It cancels
the timer in its destructor. `pop()` then returns a `BalancerTick` whenever one is
due, ahead of queued reports. A late tick is not made up, so ticks never pile up.
Ticks are ordinary messages, so tests push `BalancerTick{}` by hand and need no
real time (§8.3).

**Tick** (every `balance_period`, default 500 ms):
1. Send `ProbeAll{seq}` to the Router.
2. Fold in the reports that arrived since the last tick:
   EWMA with α = 0.3, `load(pt) = Σ (tx_bps + rx_bps)/byte_cost + links·link_cost`.
3. A pt with no report for `k_missed = 3` ticks is marked **stalled**: it is
   excluded from the policy and logged at warn. Links cannot be released from a
   stalled thread, so this is detection only.
4. Policy: build the next `policy_len = 64` assignments by weighted round robin
   with weights ∝ `max(ε, capacity − load)`. Send `SetPolicy` only when it changed
   noticeably.
5. Scale out: if mean load > `hi_watermark` and `pts < max_pts`, send `SpawnPt`.
6. Scale in: if mean load < `lo_watermark` for `scale_in_ticks` and
   `pts > min_pts`, send `RetirePt` for the least-loaded pt.
7. Rebalance: if `max_load − min_load > imbalance_threshold`, move the hottest
   links from the hottest pt to the coldest. A link moves only if
   `now − last_moved > move_cooldown`, and at most `max_moves_per_tick` links move
   per tick (hysteresis, so links don't ping-pong between pts).
8. Evict: if links exceed `max_links`, `EvictLink` the oldest links idle longer
   than `idle_evict_after` (this policy is optional, off by default).

Logic and decisions live in a pure function,
`Decisions decide(const State&, Config, now)`, so they can be unit-tested without
threads.

**As built (steps 7 and 8)** (`BalancerWorker/`)
- `BalancerPolicy` folds facts into the state (`on_report`, `on_topology`,
  `on_link_gone`, and `end_tick`, which turns the bytes of the past tick into
  EWMA rates and counts missed probes), `decide()` reads it, and `commit()`
  records what was sent: policy and epoch, a pending spawn or retire, and
  `last_moved` together with the assumed new pt.
- **Where a link is:** after a move, the old pt's next report still carries the
  link's final counters. Those samples are marked `gone`. Their bytes count, but
  only a live sample places the link (and sets `last_moved` when the pt
  changed).
- **Stall detection:** a pt's first tick is free (it has not been probed yet), so
  it is stalled after `k_missed` unanswered probes after that.
- **Probe after deciding, and stale reports cannot undo a move** (found by the
  first matrix run). The tick used to send `ProbeAll` before its `MoveLink`s. The
  old pt then answered the probe before handling `Release`, so its report still
  listed the moved links live there, and the Balancer moved them back in its
  model, marked them as just moved, and at the next tick moved as many again:
  64 moves for a 32-link imbalance. Now:
  - the probe goes out after the decisions;
  - a live sample places a link elsewhere only if its report was built after
    the Balancer's last move of that link (`PtReport.at > last_moved`, both on
    the steady clock).
- **Rebalance guard:** a link moves only if its load is below the current gap, so
  the move narrows the gap rather than swapping which pt is busy. One hot link on
  its own therefore never ping-pongs.
- **Scaling:** at most one pending spawn and one pending retire at a time. The
  `TopologyChanged` that answers clears it. A retiring pt gets no new links and
  is neither a source nor a target of moves.
- **Defaults:** period 500 ms (`--balance-period`), alpha 0.3, `byte_cost` 1 MB/s
  per unit of load, `link_cost` 0.002, capacity 1, watermarks 0.75 / 0.25,
  `k_missed` 3, `policy_len` 64, policy sent when a share moves by more than 0.1,
  `scale_in_ticks` 10, imbalance 0.2, cooldown 10 s, 2 moves per tick, no
  eviction.
- **Metric log**, one line per pt per tick:
  `pt=<id> links=<n> load=<x> stalled=<bool> backlog=<n>`.

### 5.5 Listener (`ListenerWorker/`)

```
loop:
    accept_client()
    ok                    -> set options (non_blocking, tcp_nodelay, keep_alive),
                             push InboundAccepted{conn}   (conn.remote_endpoint() is already set)
    TooManyOpenFiles      -> log, sleep 100 ms (the client stays queued)
    Stopped               -> exit
    other                 -> log, retry
```
`accept_client()` itself skips clients that give up while queued, and retries
when interrupted. `stop()` raises a `WakeupEvent` that `accept_client()` polls
next to the listening socket (§8.2).

### 5.6 Frontends (`Frontend/`)

A frontend is three parts (D10, §14): an **input thread**, the shared
**`CommandProcessor`**, and a **view**. This section covers the TUI. §14 covers the
agent CLI and the command layer both use.

**Keyboard thread** (TUI input): `read_byte(timeout = 100 ms)`, decodes UTF-8 (`read_utf8_char`)
and CSI sequences (`ESC [ A/B/C/D/5~/6~`) into `Key`, pushes to `frontend_q`. It
checks a `std::atomic<bool> stop` on every timeout. `0x03` (Ctrl+C) becomes
`Key::Exit`.

**TUI loop** (main thread). `Key`s edit the `InputModel`; Enter hands the line to
`CommandProcessor::submit`, and a parse error becomes a local notice. `Reply`
with `ok = false` is shown as a notice too.
```
while (auto m = frontend_q.pop()) {
    apply(*m);
    while (auto more = render_q.try_pop()) apply(*more);   // coalesce bursts
    if (dirty) ui.render(layout);                             // FrameBuffer diff already skips unchanged rows
    if (shutdown_done) break;
}
```

**Layout** uses the existing `AbsoluteLayout`, `ChatWindow` and `InputBox`, plus a
new one-line `StatusBar` element (active conv, status, unread counts, pts/links).

The command set is in §14.2 and is the same in both frontends; Tab is a TUI key
binding for `/open` of the next conv.

The frontend deals only in conversations and never sees peer ids. A future `/group ...`
command family only needs new Store messages and new parser cases.

The TUI keeps a `ChatModel` only for the active conversation. A switch asks the
Store for a snapshot (`Subscribe` returns `ConvSnapshot`), so the Store stays the
single source of truth.

**As built (step 9)**
- **Screen** (`TuiScreen`, a `Renderable`):
  - row 0: the status bar (active conv id, its state and address, "scrolled
    back N", the other convs with their unread counts; "quitting" comes first,
    since the bar is cut at the right);
  - the chat;
  - a notice row (the last command result or event);
  - the input box (a top border, two lines).

  Rows are fixed, so after every resize the children are placed exactly instead
  of being scaled by `AbsoluteLayout`.
- **Subscriptions:** at start `SetDefaultSubscription{Notify, Ignore, Notify}`.
  `/open` and Tab subscribe the new conv `{Forward, Notify, Notify}` and put the
  old one back to the default (`ProcessorMode::previous`).
- **Automatic opening:** a successful `/dial` opens its conv. The first conv
  that appears while none is active (an inbound peer) is opened too.
- **Keys:** Text, Backspace, Left and Right edit the input (by codepoint);
  Enter submits; Tab goes to the next conv; PgUp and PgDn scroll by 10 messages
  (a new message does not move a scrolled view); Ctrl+C is `/quit`. Up and
  Down are unused. Keys are decoded by pure functions (`Keys.hpp`) from bytes
  read by `KeyboardWorker` (ESC sequences: 50 ms for the rest of the sequence).
- **Labels:** my messages show as "me (sending)", then "me" or "me (failed)"
  (`ChatModel::set_sender`). A peer's messages show its address, which comes
  from a `ListReq{0}` sent when a conv first appears; with no request id it gets
  no `Reply` and is not shown. An explicit `/list` is shown, recognised by the
  `Reply` that precedes its `ConvList`.
- **Rendering:** through a callback, so tests draw into a `FrameBuffer` without
  a terminal. The loop handles everything already queued, then draws once.
- **Terminal safety:** stdin's settings are saved before `TerminalUI` changes
  them (`TerminalState`). The Supervisor's hard exit restores them with
  `tcsetattr` and `write` only. Without a terminal on stdin the node exits 1
  with a message.
- **Conversation ids** are typed in full (16 hex digits, as the status bar shows
  them). Tab covers switching; short prefixes are not supported.

### 5.7 Logger (`Common/Logger.hpp`)

`Logger` is a cheap handle `{ThreadQueue<LogRecord>*, thread_name}` with
`log(level, fmt, args...)`. `LogRecord{steady ts, wall ts, thread, level,
component, text}`. `Logger::for_thread(name)` gives a worker that another thread
starts its own thread name (the Router does this for each IO worker). The worker writes one line per record to a file, since stdout
belongs to the TUI.

A `Metric` level writes key=value lines (`pt=2 links=14 rx_bps=...`) that the
experiment scripts can parse. The level filter is applied before formatting, so
debug logging costs nothing when it is off.

### 5.8 App / main (`App/`)

`AppConfig` holds:
- listen address and port, advertised endpoints
- `min_pts`, `max_pts`, `max_links`, `max_links_per_pt`
- the timeouts from §5.2
- the Balancer parameters from §5.4
- log file and level
- `persistence` (`none` today; a future data-directory option, §12.2)
- `frontend`: `tui` (default) or `cli` (`--cli`, §14)

It is parsed from argv.

Start order: Supervisor (§9.2; it sets the signal mask, so it comes before every
other thread) → Logger → Store (with its `Persistence` implementation) → Router
(spawns `min_pts` Pt-i) → Balancer → Listener → input thread (Keyboard or
LineReader) → frontend loop on the main thread. In CLI mode `TerminalUI` is never
constructed. Shutdown is the reverse, driven by messages (§7.8). `main` joins every
thread in reverse start order, the Logger second to last and the Supervisor last.
If shutdown stalls, the Supervisor escalates it (§9.2).

---

## 6. Data structure notes

### 6.1 ConnTable (Router)
`links_` is keyed by `link_id`, with `peer_to_link_` as a secondary index and
`PtHandle::links` as the per-pt view. All three are updated together, in one
handler.

### 6.2 LoadLog (Balancer)
Per-link EWMA plus a raw delta from the last report. `last_read` from project.md
becomes the report timestamp.

### 6.3 Subscriptions (Store)
One subscription per conv:
`{ inbound: Ignore|Notify|Forward, outbound: Ignore|Notify|Echo, lifecycle: Ignore|Notify }`.

- New convs default to `{Notify, Ignore, Notify}`.
- The active conv gets `{Forward, Notify, Notify}`. Subscribing to it sends a
  `ConvSnapshot` first, then deltas (`MsgAppended`/`MsgStatus`), and advances
  `read_upto`.
- Notify sends `ConvNotify{unread}` and does not advance `read_upto`.
- `PeerUp`/`PeerDown` become a `ConvState` for every conversation the peer is a
  member of. Today that is just its direct conversation.

---

## 7. Scenarios (revised sequences)

Notation: `A → B: Msg [action at B]`.

### 7.1 Send message
1. Frontend → Store: `SendText` [append `PendingSend`. If subscribed: `MsgAppended` → Frontend]
2. Store → Router: `RouteSend` per member [look up `peer_to_link_`]
3. Router: `pool_i->send(frame)` [direct write or TX buffer; synchronous result]
4. Router → Store: `SendResult` [`Sent`/`Failed`. If subscribed: `MsgStatus` → Frontend]

Counters reach the Balancer at the next probe (no per-message traffic). A frame
that later fails to flush shows up as `LinkDown{WriteTimedOut}` (§7.5). Messages
still `Sent` at that point stay `Sent`; acks (§8.4) would tighten this.

### 7.2 Receive message
1. Pt-i: decode → Store: `ChatIn` [resolve the conv (§5.3), append `Received`. If subscribed: forward or notify → Frontend]

### 7.3 New incoming connection
1. Listener → Router: `InboundAccepted` [new Link `Handshaking`, choose pt]
2. Router → Pt-i: `Adopt{AwaitConnect}` [pool add, handshake read timeout]
3. Pt-i → Router: `LinkAdopted`, later `LinkHello`
4. Router → Store: `PeerHello` [recognise or mint, build the Ack]
5. Store → Router: `BindLink` [resolve duplicates, `Connected`] or `RejectLink`
6. Router → Pt-i: `Bind` [set peer, send Ack, established timeouts]
7. Router → Store: `PeerUp` [peer Up] → Frontend: `ConvState` for its conversations
8. Router → Balancer: `TopologyChanged`/link known at the next report

### 7.4 Dial
1. Frontend → Store: `DialReq`/`ReconnectReq` [peer `Dialing`; ordered endpoint list and `ConnectInfo` as values]
2. Store → Router: `RouteDial{endpoints, hello}` [new Link `Dialing`, choose pt]
3. Router → Pt-i: `Dial{endpoints, hello}` [`pool.connection_dial(endpoints[0])`, non-blocking]
4. Pt-i: `Connected` → encode and send the hello, `AwaitAck`. An attempt fails (`ConnectionRefused`, `ConnectTimedOut`, ...) → dial the next endpoint, same link. All failed → Router: `LinkDown{SocketError, err}`
5. Pt-i → Router: `LinkAcked{ack, remote}` [`Connected`]
6. Router → Store: `DialDone{remote}` [record `host_id` and `remote`, peer Up] → Frontend: `ConvState`.
   On failure: `PeerDown{why, err}` → `ConvStatus::Failed`.

**Duplicate links** (simultaneous dial, or a reconnect while the old link is not
yet known to be dead). The Router learns that one peer has two live links.

- If the old link is from an earlier session (it was `Connected` before the new
  one started its handshake), or it is also inbound: keep the **new** link and
  close the old one with `Superseded`. The peer has evidently reconnected.
- If both links are in their handshakes at the same time (a simultaneous dial):
  both sides must keep the *same* TCP connection. Rule: **keep the link that the
  side with the numerically larger `(peer_id, host_id)` initiated.**
  - Each node compares the pair as it sees it: `(our name for them, their name for
    us)`. The other node sees the same two values swapped, so both nodes reach the
    same decision.
  - This needs the `host_id`: the Router decides at `BindLink`, using the Ack the
    Store built (`ack_peer_id` is the peer's name for us). The competitor is our
    dial (still in progress), or our dial that was acked after the inbound
    handshake started.
  - The loser is closed quietly (D13): the Store hears only about the survivor.
    When our dial wins, the Store, which has just bound the inbound hello, waits
    for that dial's `DialDone` or `PeerDown`.
  - The two pairs are equal only if both nodes picked the same 64-bit id for each
    other; then both keep the other's link and both links close. Not handled
    (probability 2^-64): the next reconnect resolves it.
- A simultaneous **first** dial cannot be detected this way. Neither side has
  named the other yet (`assigned_peer_id = 0`), so each side's inbound hello
  creates a second Peer. This is accepted: the user sees two conversations with
  the same endpoint. Merging them would need a global node identity, which is out
  of scope.

### 7.5 Peer disconnection
1. Pool reports `{conn, err}` → Pt-i → Router: `LinkDown`
2. Router → Store: `PeerDown` [peer Down; keep its record and endpoints for reconnect] → Frontend: `ConvState`
3. Router → Balancer: `LinkGone`

### 7.6 Balancer-initiated disconnect
1. Balancer → Router: `EvictLink` [`Closing`]
2. Router → Pt-i: `Close{graceful, why = BalancerEvicted}` [`connection_remove`: flush then close]
3. Pt-i → Router: `LinkDown{BalancerEvicted}` (Pt-i reports it right after removing the link; the flush continues inside the pool) → Store / Balancer as in §7.5

### 7.7 Rebalance (move a link)
1. Balancer → Router: `MoveLink{link, to=j}` [`Moving`, sends are parked from now on]
2. Router → Pt-i: `Release` [`connection_release`, take the decoder]
3. Pt-i → Router: `LinkReleased{conn, pending_tx, decoder}`
4. Router → Pt-j: `Adopt{Established, pending_tx, decoder}`
5. Pt-j → Router: `LinkAdopted` [`Connected`, flush parked sends in order]
6. The Balancer sees the new pt at the next report and records `last_moved`.

Bytes the kernel receives during the move stay in the socket buffer. Unsent TX
travels with the connection. Partial frames travel inside the decoder. Nothing is
lost or reordered.

If Pt-i finds the link gone when `Release` arrives, it answers `LinkDown`. The
Router then fails the parked sends.

### 7.8 App closing
1. Frontend → Store: `QuitReq`
2. Store → Router: `Shutdown{grace = 1 s}`
3. Router:
   - `listener.stop()` and join it
   - Balancer: `StopBalancer`
   - every Pt-i: `Stop{deadline = now + grace}`
4. Pt-i: `connection_remove` on every established link (graceful flush),
   `connection_drop` on the rest. It keeps calling `receive(deadline)` until
   `connection_count()` is 0 or the deadline passes, then calls `pool.stop()`, and
   sends `PtStopped` → Router.
5. Router: join every Pt-i, then → Store: `RouterStopped`
6. Store: `persistence.flush()` (a no-op today), then → Frontend: `ShutdownDone`
7. The frontend writes out every event it still has (CLI: `shutdown_done` last)
   and exits its loop. main stops the input thread, closes every queue
   (`graceful_shutdown`), and joins the Store, Router and Balancer. It then logs
   `all workers stopped; exit code N`, the last line of every complete log, and
   joins the Logger, which drains its queue first.

The Router logs `all IO workers stopped` when the network part is done. The
time from the Supervisor's `shutdown phase=graceful` line to it is the network
shutdown; the rest is the frontend writing out its backlog and the joins.

The per-link `LinkDown{Shutdown}` messages reach the Router while it waits. It
does not forward them to the Store.

---

## 8. Changes needed in existing code

### 8.1 `EpollConnectionPool` ([TcpSocket.hpp](TcpSocket/TcpSocket.hpp)): **done**

| Addition | Purpose |
|---|---|
| `interrupt()`: the next `receive()` returns `ReceiveError{0, Interrupted}` once; calls made before it reports collapse into one | Pt-i command queue (D2) |
| `connection_dial(address, port, Options, Timeouts, connect_timeout)`: non-blocking, built on `TcpConnection::connect_start()` / `connect_status()`. `receive()` reports `ReceiveResult{id, 0, Kind::Connected}` or `{id, ConnectionRefused / HostUnreachable / ConnectTimedOut / ...}`. Only a malformed address fails synchronously | D7 |
| `connection_release(id)` → `ReleasedConnection{TcpConnection, unsent_tx, stats}`, without closing the socket | Rebalance |
| `connection_add(TcpConnection, Timeouts, initial_tx)` | Adopt after a move |
| `set_timeouts(id, Timeouts)` (read timers carry a generation, so the old one is retired) | Handshake → established |
| `take_stats(id)` → `{bytes_received, bytes_sent}` since the last call | Balancer reports |
| `receive(rx, deadline)`: as `receive()`, but returns `ReceiveError{0, WouldBlock}` once the deadline passes with nothing to report (added in step 4) | Pt-i shutdown: bounded wait for flushing links |
| `connection_count()`: connections held, including dials in progress and removals still flushing (added in step 4) | Pt-i shutdown: knowing when everything has flushed |

The pool wakes `receive()` through a `WakeupEvent` (§8.2).

### 8.2 `TcpConnection` / `TcpListener` / `WakeupEvent`: **done**
- **`WakeupEvent`** (now `Common/WakeupEvent.hpp`, its own target, so code outside
  the socket library can use it without depending on sockets): an eventfd wrapper
  with `signal()` (any thread), `clear()` and `fd()`. One rule throughout: an atomic flag next to the event records *why*,
  and the event only wakes the waiting thread. The pool and the listener both
  use it.
- **`TcpEndpoint{address, port}`**: plain C++ types, so `TcpSocket.hpp` includes no
  C networking header and does not depend on `Wire.hpp`. The Router converts it to
  `wire::Endpoint`.
- **`TcpConnection::remote_endpoint()`** → `const TcpEndpoint&`, always set for a live connection.
  The address is recorded when the connection is created:
  - `accept_client()` stores what `accept()` returned;
  - `connect()` / `connect_start()` store the destination they dialled;
  - the raw-descriptor constructor is private, so no connection is created without an address; a moved-from one holds `TcpEndpoint{}`.

  It stays readable after the peer disconnects. It moves with the object, into a
  pool and out again (`connection_release`).
- **`TcpListener::stop()`**: sets a flag and raises a `WakeupEvent`.
  `accept_client()` is built from two helpers:
  - `wait_for_client()`: `poll()` on the non-blocking listening socket plus the
    event;
  - `try_accept()`: one `accept4()`.

  Stop is permanent, idempotent, safe from any thread, and wins over a client
  that is already queued.
- **New `SocketError::TooManyOpenFiles`** (EMFILE/ENFILE), so the Listener can back
  off.
- **`TcpListener::local_port()`**, so tests can bind to port 0.

### 8.3 `ThreadQueue` timers: **done**
- `TimerId every(period, Tick)` and `cancel(TimerId)`. While a timer is due,
  `pop()` / `try_pop()` return `T(tick)` before queued values.
  - A late timer yields one tick, never a burst, and the schedule does not drift.
  - No ticks after either shutdown.
  - Safe from any thread.
- **A tick is an ordinary message.** `every()` requires
  `std::constructible_from<T, Tick>`: the tick type must be one of the consumer's
  inbox alternatives. The timer stores the `Tick`, not a `T`, so move-only inboxes
  work. `pop()` keeps its signature (`std::optional<T>`, `nullopt` = closed).
- **Why not `pop_until(deadline)` or a ticker thread.** Both were considered.
  - `pop_until` puts the deadline bookkeeping (per-message check, catch-up, drift)
    in every consumer, and time becomes a hidden input that tests can't inject.
  - A ticker thread delays ticks behind a backed-up inbox, lets them pile up, and
    adds a thread with lifetime rules.
  - Queue timers have none of these problems.
- **Rules for workers that use timers:**
  1. The consumer registers its timers on its own inbox, in its constructor, and
     cancels them in its destructor. Timer, tick type and handler then sit in one
     class.
  2. Dispatch with `std::visit([this](auto& m) { on(m); }, msg)` over `on(...)`
     overloads. With no catch-all handler, a missing `on(Tick&)`, or a missing
     handler for any new inbox type, fails to compile.

### 8.4 `Wire.hpp`
- **Now:** update the `ChatMessage` comment to the field meanings in §3. The
  layout stays the same, and so does `kVersion`.
- **Later, optional:**
  - `MessageAck{conversation_id, msg_id}` (type 4) for real delivery state.
    `ChatMessage` would gain `msg_id`, which needs a version bump.
  - `Ping`/`Pong` if keepalive is too slow.
  - The group control frames from §12.1.
- New message types take fresh type numbers. An unknown type is already a
  recoverable `WireError::UnknownType`, so an older node drops the frame and keeps
  the link.

### 8.5 Sketches to replace
- `RoutingTable` → §5.1 state: keyed by peer, not conv. Drop the
  `AssignmentPolicy` mutex.
- `BalancerWorker` → §5.4. Fix the `conversatoin_` typo. Move it out of `RouterWorker/`.
- `StoreWorker`/`PeerTable`/`ConversationStore` → §5.3 (`Peer` / `Conversation`
  split, messages stored in the conv, commit hooks).
- All headers need `#pragma once`.

---

## 9. Source layout

```
Common/        ThreadQueue.hpp  WakeupEvent.hpp/.cpp (own target; TcpSocket uses it)  Ids.hpp (IdMinter)
               Logger.hpp/.cpp  Worker.hpp/.cpp (§9.1)  Supervisor.hpp/.cpp (§9.2)
               No networking: Common does not depend on TcpSocket.
Messages/      Messages.hpp (+ a one-line Messages.cpp that compiles it alone; depends on Common, TcpSocket, Wire.hpp)
TcpSocket/     (existing) + dial/release/interrupt
TerminalUI/    cell-based rendering (§13): Unicode, Style, TextLayout, Canvas, Element::draw; + StatusBar element
Wire.hpp       (existing)
StoreWorker/   StoreWorker.hpp/.cpp  PeerTable.hpp  ConvStore.hpp
               Subscriptions.hpp  Persistence.hpp (interface + NullPersistence)
RouterWorker/  RouterWorker.hpp/.cpp  AssignmentPolicy.hpp
IoWorker/      IoWorker.hpp/.cpp
BalancerWorker/BalancerWorker.hpp/.cpp  BalancerPolicy.hpp/.cpp   (pure decide())
ListenerWorker/ListenerWorker.hpp/.cpp
Frontend/      Commands.hpp/.cpp (pure parser)  CommandProcessor.hpp/.cpp
               Tui: TuiFrontend.hpp/.cpp  TuiScreen.hpp/.cpp  KeyboardWorker.hpp/.cpp  Keys.hpp/.cpp
                    TerminalState.hpp/.cpp (saved termios, async-signal-safe restore)
               Cli: CliFrontend.hpp/.cpp  CliFormat.hpp/.cpp (the events)  LineReader.hpp/.cpp
                    JsonlWriter.hpp/.cpp  Json.hpp (escaping, hex ids)
App/           main.cpp (wiring, start/stop order)  AppConfig.hpp/.cpp (argv)   → the `node` executable
tests/         one test_*.cpp per module (CTest); NodeHarness.hpp: complete nodes in one process;
               CliProcess.hpp: a `node --cli` process driven through pipes
```

Each worker exposes `run()` (the thread body) and is constructed with references to
the inboxes it writes to. It never holds another worker's object. This keeps every
worker unit-testable by feeding its inbox and inspecting the queues it writes to.

Every worker handles its inbox the same way:
```cpp
while (auto msg = inbox_.pop()) {
    std::visit([this](auto& m) { on(m); }, *msg);   // one on(X&) overload per inbox type
}
```
There is no catch-all overload (`void on(auto&)`), so a message type without a
handler, ticks included, is a compile error (§8.3). `Common/Worker.hpp` (§9.1)
provides this loop.

### 9.1 `Common/Worker.hpp`

Each worker class is still written by hand. The helper's job is to remove what
can go wrong around that loop: a thread that never finishes joining, an exception
that brings down the process, a reference to a queue that no longer exists, and a
shutdown sequence left half done.

```cpp
// Dispatch: checked at compile time; the error names the missing alternative
template <class W, class M>
concept Handles = requires(W& w, M& m) { w.on(m); };

template <class W, class... Ts>
void dispatch(W& w, std::variant<Ts...>& msg) {
    (check_handles<W, Ts>(), ...);              // static_assert per type: "no on(InputLine&)"
    std::visit([&](auto& m) { w.on(m); }, msg);
}

// Loop: ends when the inbox closes, or when the worker reports done()
template <class W, class T>
void run_inbox(W& w, ThreadQueue<T>& inbox) {
    while (auto m = inbox.pop()) {
        dispatch(w, *m);
        if constexpr (requires { w.done(); }) if (w.done()) break;   // e.g. Pt-i after Stop
    }
}

// For tests: no thread and no waiting. Handles whatever is queued, returns the count.
template <class W, class T>
size_t drain(W& w, ThreadQueue<T>& inbox);

// Thread ownership
class WorkerThread {
public:
    WorkerThread(std::string name, std::function<void()> body,
                 std::function<void()> stop, Supervisor&);
    void request_stop();         // calls `stop` exactly once; safe from any thread
    void join();
    bool finished() const;       // set as the body returns; the Supervisor reports stuck threads
    ~WorkerThread();             // request_stop(); join();
};
```

**`stop` is per worker, because each one blocks on something different:**

| Worker | Blocks on | `stop` |
|---|---|---|
| Store, Router, Balancer, Logger, frontend loop | `inbox.pop()` | `inbox.graceful_shutdown()` |
| Pt-i | `pool.receive()` | `pool.stop()` |
| Listener | `accept_client()` | `listener.stop()` |
| LineReader | `poll(stdin, event)` | `event.signal()` |
| Keyboard | `read_byte(100 ms)` | the atomic flag |

**Rules**
1. **Exceptions end the process deliberately.** The `WorkerThread` body catches
   everything, logs `fatal` with the thread name, and calls
   `Supervisor::fatal()`, which goes straight to the Emergency phase (§9.2).
   Keeping the rest of the process running is not an option: if the Store dies,
   the shutdown chain in §7.8 hangs, because it runs through the Store. Handlers
   are not expected to throw at all. The catch is there only so a bug fails loudly
   instead of silently (an exception escaping a `std::thread` calls
   `std::terminate` with no log line).
2. **Declaration order sets the lifetimes.** `App` declares the queues first, then
   the worker objects, then the `WorkerThread`s. C++ destroys members in reverse
   order, so threads are joined before the objects and queues they use go away.
3. **Exactly one way to exit per worker.** A worker leaves its loop either because
   its inbox closed or because `done()` returned true. It never calls `return`
   from inside a handler, and nothing outside it stops the thread.
4. **A worker that owns threads stops and joins them itself**, in its `run()`
   epilogue after the loop. The Router does this for its Pt-i threads. The
   Supervisor's registry therefore contains only threads that exist before
   start-up finishes, and needs no lock.
5. **Every thread goes through `WorkerThread`**, including the Pt-i threads the
   Router spawns, so the same catch, naming and stop rules apply everywhere.
   Threads are named with `pthread_setname_np` (for example `store` or `pt-3`), so
   gdb, `top -H` and log lines identify each one.
6. **Handlers never block on I/O.** No helper can interrupt a handler that blocks.
   §9.2 turns such a hang into a bounded exit, but not into correct behaviour.

**Not used**
- **A `Worker` base class with a virtual `run()`.** It adds nothing to the
  templates and hides the inbox type.
- **`std::jthread`'s stop_token as the stop mechanism.** A stop_token can't wake
  `pop()`, `receive()` or `poll()` by itself, so the per-worker `stop` function is
  needed anyway. `jthread` is fine as the thread type inside `WorkerThread`.
- **A supervisor that restarts failed workers.** A restarted Store or Router would
  have lost its tables, so a restart can't recover correctly. Fail-fast with a
  clean log is the honest behaviour.

### 9.2 Shutdown watchdog (`Common/Supervisor.hpp`)

**Guarantee:** once shutdown starts, the process exits within a fixed time,
whatever else goes wrong. Exiting cleanly is preferred, and when the exit is not
clean, the logs say why.

```
Running ──quit / SIGTERM / EOF──► Graceful ──deadline──► Emergency ──deadline──► Hard exit
   │                                 │  ▲                    │
   └──worker exception───────────────┼──┼──────────────────► │
                                     │  └ 2nd SIGTERM ───────┘ (skip ahead)
                                     └──all joined──► Done (exit 0)
```

| Phase | What happens | Deadline (default) | Exit code if it ends here |
|---|---|---|---|
| Graceful | §7.8 runs as messages: Pt-i flush, `ShutdownDone`, main joins everything | `grace + 1 s` = 2 s, extended by progress (below) up to `grace + 30 s` | 0 |
| Emergency | `force_shutdown()` on every queue except `log_q`, `request_stop()` on every `WorkerThread`, main joins | 1 s | 3 if graceful timed out, 2 after a worker exception |
| Hard exit | restore the terminal, write one log line with raw `write(2)`, `_exit` | — | 4 |

Startup failures exit with 1 (CLI: a `fatal` event, §14.4). The phases only move
forward, and moving between them is one atomic compare-and-swap, so every trigger
is safe from any thread and safe to fire more than once. `--shutdown-grace` sets
`grace`. The graceful deadline is `grace + 1 s`, and the emergency deadline is
1 s.

**Slow is not stuck (step 10).** The fixed graceful deadline also had to cover
the frontend writing out its backlog. A CLI that had echoed 18 MB of events was
forced into Emergency with `stuck=[]`: every worker had finished, and the
frontend was still writing to a slowly read stdout. Now:
- `Supervisor::progress()` (any thread, cheap) pushes the graceful deadline to
  at least now + `progress_margin` (1 s), never past `graceful_cap` (`grace + 30 s`)
  after the shutdown began. Outside the graceful phase it does nothing.
- The frontends call it after every event delivered (a CLI line written, a TUI
  frame drawn). That covers shutdowns started by a signal, which the frontend
  does not see.
- What still escalates: a stuck worker (no events, so no progress), a frontend
  blocked in `write()` (no progress), and endless progress (the cap).
- `handle_deadline()` re-checks the deadline, since it may have moved while the
  Supervisor was waiting.

**It runs on its own thread, not as a timer on `frontend_q`.** The main thread is
one of the things that can get stuck:
- in CLI mode, an agent that stops reading leaves stdout's pipe full, so
  `JsonlWriter` blocks in `write`;
- main's own `join()` calls can hang.

The Supervisor starts first and is joined last. It also owns signal handling,
which needs a thread for the same reason.

```cpp
class Supervisor {
public:
    explicit Supervisor(ShutdownTimings, Logger);
    // Must be called on main before any other thread starts: blocks SIGINT, SIGTERM
    // and SIGHUP in every thread (they inherit the mask) and ignores SIGPIPE.
    void start();

    void register_queue(std::function<void()> force_shutdown);   // before workers start
    void register_thread(WorkerThread&);                          // before workers start
    void set_graceful_trigger(std::function<void()>);             // pushes QuitReq{0} to store_q
    void set_terminal_restore(void (*)() noexcept);               // TUI only, async-signal-safe

    void begin_shutdown(std::string_view reason);   // frontend: /quit, EOF, Ctrl+C
    void fatal(std::string_view thread, std::string_view what);   // WorkerThread's catch block
    int  finish();                                   // main, after every join: disarm, return exit code
};
```

**Loop.** A `poll` on a `signalfd` and a `WakeupEvent`, with the remaining time to
the current deadline as the timeout:
- **First signal:** call the graceful trigger and arm the Graceful deadline.
- **Second signal:** go straight to Emergency.
- **WakeupEvent:** the phase or deadline changed, so recompute the timeout.
- **Timeout:** escalate one phase.

**Rules**
0. **The Logger thread is not registered with `register_thread`.** It is expected
   to be running (draining) during an emergency, so it would always be listed as
   stuck. Its queue is registered with `register_log_queue` instead.
1. **The Logger's queue drains even in Emergency.** Every other queue is
   force-closed, but `log_q` gets `graceful_shutdown()`, so records already queued
   (including the fatal one) are still written. If that hangs, Hard exit is the
   backstop.
2. **The log names the threads that got stuck.** On each escalation the Supervisor
   logs, for example,
   `shutdown phase=emergency reason=graceful_timeout stuck=[store,pt-2]`, using
   `WorkerThread::finished()` for the threads it registered. A stuck Pt-i shows up
   as the stuck Router, because the Router joins its own Pt-i threads (§9.1, rule 4).
3. **Hard exit uses only async-signal-safe calls:**
   - the TUI restore hook: `tcsetattr` with the saved termios, then `write` of the
     escapes that leave the alternate screen and show the cursor;
   - one `write(2)` to the log file's fd;
   - `_exit(4)`.

   No destructors run and nothing takes a lock. Log records still queued are lost.
4. **CLI output during an escalation is best effort.** The Supervisor `try_lock`s
   the `JsonlWriter` mutex and writes
   `{"ev":"shutdown_forced","phase":"emergency","stuck":[...]}` with a
   non-blocking write. If the lock is held or stdout is full, it writes nothing
   there. The exit code still tells the agent what happened.
5. **EPIPE on stdout counts as a quit.** It is treated like `InputClosed`: the
   agent has gone, so the node shuts down instead of buffering events forever.

**main**
```cpp
int main(int argc, char** argv) {
    App app{parse(argv)};              // queues, then workers, then threads (destroyed in reverse)
    app.supervisor.start();            // signal mask first, before any other thread exists
    app.start_workers();               // registers queues and threads, then starts them
    app.frontend.run();                // returns on ShutdownDone, or when frontend_q is force-closed
    app.join_all();                    // reverse start order; the Logger last
    return app.supervisor.finish();    // 0 / 2 / 3; Hard exit never gets here
}
```
In Emergency, `frontend_q` is force-closed too, so `frontend.run()` returns. Main
then reaches `join_all()` by the same route as in a graceful shutdown. Only one
shutdown sequence exists in main; the phases differ only in how hard the queues
are closed.

**Tests** (`test_supervisor`, with timings shrunk to ~100 ms)

| Case | Expected |
|---|---|
| Normal quit | no escalation, exit 0, no `shutdown_forced` line |
| A Store handler blocks on a test latch the shutdown never releases | Graceful expires, Emergency succeeds once the latch is released, exit 3, `stuck=[store]` |
| A handler blocks and cannot be woken (a `sleep` loop with no wake-up) | Hard exit within graceful + emergency + ε, exit 4, termios restored (checked on a pty) |
| A handler throws | exit 2, the `fatal` log line names the thread, Graceful is skipped |
| SIGTERM, then a second SIGTERM | the first starts Graceful, the second escalates at once |
| CLI: the agent stops reading stdout, then sends SIGTERM | exit within the deadlines, even though main is stuck in `write` |
| CLI: stdout is closed (EPIPE) | normal quit, exit 0 |
| Progress past the graceful deadline, below the cap | not forced, exit 0 |
| Progress that never stops | forced at the cap, exit 3 |

The two CLI rows are tested at the Supervisor level in step 2 (main blocked in
`write` on a full pipe) and end to end in step 6. Under ThreadSanitizer, run with
`TSAN_OPTIONS=atexit_sleep_ms=0` (and `setarch -R` on WSL2): TSan's default 1 s
sleep inside `_exit` otherwise breaks the timing checks of the hard-exit cases.

---

## 10. Build order and tests

| Step | Deliverable | Tests |
|---|---|---|
| 1 ✅ | Pool extensions (§8.1); `WakeupEvent`, `TcpListener::stop`, `remote_endpoint` (§8.2); `ThreadQueue` timers (§8.3) | `test_pool`: interrupt coalescing, dial ok/refused/timeout, release then add on a second pool with no byte loss, `set_timeouts`, stats. `test_listener`: stop (blocked, permanent, beats a queued client), `TooManyOpenFiles`, remote address on both sides and across moves. `test_queue`: FIFO/shutdown, tick timing, ticks first, no burst, several timers, cancel, move-only inbox |
| 2 ✅ | `Common/`: `ThreadQueue.hpp` (moved), `WakeupEvent` (moved out of TcpSocket into its own target), `Ids.hpp` (`IdMinter`), `Logger`, `Worker.hpp`, `Supervisor`. `Messages/Messages.hpp` (every inbox of §4, in namespace `msg`, size budget as `static_assert`s) | `test_ids` 19 checks (id uniqueness, `reserve`, messages through a queue, budget); `test_logger` 24 (level filter skips formatting, escaping incl. UTF-8 C1, line format, 4 threads × 2000 records in per-thread order, drain on graceful shutdown); `test_worker` 21 (dispatch, `done()`, `drain`, ticks, naming, stop once with 8 callers, exception → `fatal`, stop for inbox / pool / listener / poll); `test_supervisor` 27 (every row of the §9.2 table, each scenario in its own child process). Clean under TSan and ASan+UBSan |
| 3 ✅ | Store, single-threaded and fed by its inbox, with `NullPersistence` and a recording fake | `test_store` 90 checks: dial (hello contents, Reply before events, known endpoint reuses its conv and goes first), dial failure → Failed with the socket error and no retry, reconnect re-presents names and sends every dialable endpoint in one `RouteDial` (port 0 left out), size limit at `wire::kMaxChatBodySize`, inbound hello (new / known / unknown assigned id / garbage → `RejectLink`), stale `PeerDown` of a superseded link ignored, send (frame contents, pending → sent / failed, refusals not stored), outbound Ignore, receive with Notify / Forward / Ignore, snapshot marks read, drops (sender mismatch, unknown peer, unknown group), chat overtaking `PeerUp`, echo, default subscription, close, quit (refusals, `RejectLink`, flush, `ShutdownDone`), exactly one `Reply` per `ReqId`, load from persistence (pending → failed, names re-presented, no id collisions). Clean under ASan+UBSan |
| 4 ✅ | Pt-i (`IoWorker/`), plus `receive(deadline)` and `connection_count()` in the pool | `test_io_worker` 68 checks, over loopback with the test as Router and as remote peer: inbound handshake (hello up, Ack encoded and sent, chat straight to the Store), outbound dial (hello encoded and sent, `LinkAcked.remote`), a dial over an invalid, a refused and a good endpoint under one link id, all refused → one `LinkDown` with the last error, no endpoints, handshake timeout, chat before hello / before `Bind` → `ProtocolError`, 10 unknown-type frames skipped, malformed frames over the limit → `ProtocolError`, release on A + adopt on B with half a frame in the decoder (message arrives whole on B), probe counters and reset, close with a reason, peer close, stop with nothing to flush, stop that flushes 1 MiB before the deadline, stop that hits the deadline, `pool.stop()`. `test_pool` +2 tests (97 checks). 20 repeated runs without a failure; clean under TSan and ASan+UBSan |
| 5 ✅ | Router with N Pt-i, without the Balancer (own placement), and the Listener | `test_router` 34 checks, two complete nodes in one process (Store, Router + IO workers, Listener on their own threads) over loopback: dial and chat both ways, close and reconnect from either side keep the conversations, simultaneous reconnect converges on one link on both sides (10 rounds), a refused dial, retiring the worker that holds the link while 50 messages flow (all arrive in order, worker stops, traffic continues), quit (`ShutdownDone`, the other side sees it go). `test_store` +3 (peer-level contract), `test_io_worker` +2 (`pool_conn_id`). 20 runs stable; clean under TSan and ASan+UBSan |
| 6 ✅ | Command layer and agent CLI (§14): `Commands`, `CommandProcessor`, `CliFrontend`, `CliFormat`, `LineReader`, `JsonlWriter`, `Json.hpp`, `App/` (`node --cli`) | `test_commands` 49 checks (every command and its errors, `//`, ids in either case, the processor: request ids, blank/help/quit, `/open` with and without restoring the previous conv, `/echo`, a closed Store queue); `test_jsonl` 45 (escaping: controls, DEL, C1, U+2028, invalid / overlong / surrogate / truncated UTF-8, NUL; ids; every event's exact text; the writer: EPIPE closes it, `try_write` on a full pipe returns at once; the reader: CRLF, blank lines numbered, a line split across reads, no final newline, `stop()` with input open); `test_cli_e2e` 33, real processes over pipes: `ready` with the bound port, dial, chat both ways with text that must be escaped (no raw ESC reaches the output), `/open` with its snapshot, `/list`, `/close` + `/reconnect` keep both convs, errors with line numbers, blank lines counted, end of input → `shutdown_done` + exit 0, `/quit`, `--stay` + SIGTERM, stdout closed, port in use → `fatal` + exit 1. 15 runs stable; clean under TSan and ASan+UBSan, the spawned nodes included |
| 7 ✅ | Balancer `decide()` as a pure function (`BalancerPolicy`) | `test_balancer` 57 checks: smooth weighted round robin (shares, spread, deterministic), EWMA and loads, `gone` samples do not move a link back, policy shares and no resend when unchanged, stall after `k_missed` (first tick free) and recovery, a stalled pt gets no links and no moves, scale out (once until answered, never above `max_pts`), scale in (after `scale_in_ticks`, least loaded, never below `min_pts`, the retiring pt gets no links), rebalance (busiest link to the idlest pt, cooldown, a lone hot link never moves, `max_moves_per_tick`), eviction, the worker driven by hand-pushed ticks |
| 8 ✅ | Balancer thread wired in (Router, `node`) | `test_balancer_e2e` 9 checks: one pt, two peers streaming ~1 MB/s into it: the Balancer starts a second pt and moves a link while 2 × 1500 messages arrive complete and in order, then scales back in once idle, and traffic still flows. 15 runs stable; clean under TSan and ASan+UBSan. A real stalled pt is not tested end to end (nothing can block an IO worker on purpose); stall handling is covered by `test_balancer` |
| 9 ✅ | TUI frontend: `TuiScreen`, `TuiFrontend`, `KeyboardWorker`, `Keys`, `TerminalState`, on the same `CommandProcessor`; TerminalUI gained `StatusBar` and `ChatModel::set_sender` | `test_tui` 52 checks, headless (keys and events fed by hand, frames read back as text): key decoding, layout and resize, typing → the same `StoreMsg` the CLI sends, codepoint editing, a dial opens its conv, snapshot and messages with labels, sending / sent / failed, an ESC byte in a message is drawn not executed, errors as notices, the first conv opens itself, unread counts, Tab (previous conv back to counting, wraps), `/list` shown but address lookups not, Ctrl+C once, `ShutdownDone`, scrolling. `test_tui_e2e` 13 checks, the real node on a pseudo-terminal with a `--cli` peer: start, typed `/dial`, chat both ways, Ctrl+C → exit 0 with the terminal reset, no terminal → exit 1. Repeated runs stable; clean under TSan and ASan+UBSan |
| 10 ✅ | Shutdown hardening: progress-extended graceful deadline (`Supervisor::progress()`, cap), the final log line, `all IO workers stopped` from the Router, `Supervisor::exit_code()` | `test_shutdown` 81 checks, real node processes. A stalled raw peer (handshake, then no reads) with 300 × 60 KB queued: the network side waits exactly the grace (500 ms → 500–502 ms) and gives up, exit 0, not forced, refused sends reported failed. A peer that starts reading only once the shutdown has begun: all 30 messages reported sent are delivered (flushed in ~41 ms), then the node stops without waiting for the deadline. SIGTERM in the middle of two-way traffic: exit 0 in ~100 ms, the other node carries on. TUI on a pseudo-terminal, Ctrl+C and SIGTERM: exit 0 in ~100 ms, cursor restored. Every log: every line well-formed, lines from every thread, the last line written after all workers stopped. `test_supervisor` +2 (progress extends; the cap holds). 10 runs stable; clean under TSan and ASan+UBSan (under TSan the CLI's backlog takes ~7 s to write out and is no longer forced) |

**Why the CLI comes before the Balancer and the TUI.** After step 5 the network
stack works but nothing outside a test binary can drive it. The CLI makes real
nodes scriptable, so later steps are checked end to end by tests, scripts and
agents through the same interface.

The load generator and the runner are described in §15.

---

## 11. Invariants that keep the extensions open

1. **Conversations live only in the Store and Frontend.** The Router, Pt-i, Balancer
   and Listener deal in peers and links. Adding groups or persistence changes no
   thread except the Store (plus Frontend for new UI).
2. **The Store is the only stateful owner of identity and history.** Every other
   table can be rebuilt from live connections, so persistence never needs to
   reach outside the Store.
3. **The Store mutates its tables only through `commit_*`.** Persistence plugs
   into those functions alone.
4. **Ids are random and never reused,** so ids persisted from earlier runs cannot
   collide with new ones.
5. **Unknown wire types are skipped, not fatal.** New frame types can ship
   without breaking older nodes.
6. **Frontends are interchangeable.** The Store sees only `StoreMsg` requests and
   emits only `FrontendMsg` events. Commands are parsed in one place (§14.2), so
   the TUI and the CLI cannot drift apart.

---

## 12. Future extensions (designed for, not built)

### 12.1 Group conversations

**Scope.** Groups among peers that each have a **direct link to every other
member** (full mesh). Delivering to a member who is only reachable through another
peer (ad-hoc routing or relaying) stays out of scope. It would need a global node
identity, forwarding and deduplication, none of which this design provides.

**Already in place**
- `Conversation{kind, group_id, members}` and `ConvStore::by_group_id_`
- the wire `conversation_id` set to `group_id`, `0` meaning the direct conversation
- per-member fan-out in the Store's send, and `SendTag` echoed back per peer
- the Router being peer-only (invariant 1)

**Would still need**
- Wire control frames (new types):
  - `GroupInvite{group_id, name, members as (endpoint)}`
  - `GroupJoin`, `GroupLeave`
  - optionally `GroupMembers` for a sync.
  Members are named by endpoint plus the inviter's vouching. A node does not know
  the other members' names for each other, so it dials any member it has no peer
  record for.
- The Store:
  - mint a random `group_id` (64-bit; collisions are negligible)
  - handle the control frames: Pt-i forwards a new `ControlIn` variant on the same
    D3 path as `ChatIn`
  - store per-member delivery state: `StoredMessage::state` becomes a
    `small_map<PeerId, DeliveryState>` for group convs.
- Frontend: `/group new|invite|leave`, and group status in the status bar.

**Ordering caveat.** Without relaying, two members may see messages from
different senders in different orders. If that matters, display order can use a
(Lamport clock, sender) pair, which would mean adding a field to `ChatMessage`
behind a version bump.

### 12.2 Persistent peers, ids and history

**Interface, owned by the Store and called only from `commit_*`:**
```cpp
struct Persistence {
    virtual ~Persistence() = default;
    virtual LoadedState load() = 0;                          // peers, convs, histories at startup
    virtual void save_peer(const Peer&) = 0;                 // endpoints, peer_id, host_id
    virtual void save_conv(const Conversation&) = 0;         // kind, group_id, members, read_upto
    virtual void append_message(ConvId, const StoredMessage&) = 0;
    virtual void update_message(ConvId, MsgId, DeliveryState) = 0;
    virtual void flush() = 0;                                // called on shutdown (§7.8 step 6)
};
struct NullPersistence final : Persistence { /* all no-ops; load() returns empty */ };
```

**Rules that make it a drop-in later**
- On startup the Store seeds `PeerTable`/`ConvStore` from `load()`, and the
  `IdMinter` from the loaded ids. Every peer starts with `link_state = Down`.
  Transient state is never persisted: link ids, statuses and subscriptions.
- `host_id` and `endpoints` are already kept per peer. After a restart a
  reconnect re-presents the same `assigned_peer_id`, so the remote side
  recognises us with no protocol change.
- `msg_id` stays a per-conversation sequence. After a load the next one is
  `msgs.size()`.
- A message still `PendingSend` at load time becomes `Failed`.
- Calls run on the Store thread. A file-based implementation should make
  `append_message` an append to a per-conversation log and batch its `fsync`, or
  the Store thread will stall. If that turns out too slow, the implementation can
  run its own writer thread behind the same interface, and no caller changes.

---

## 13. TerminalUI: cell-based rendering

A track of its own, independent of the build order in §10. Today the UI passes
text down as `std::string`s with UTF-8 and colour escape sequences mixed in. The
pipeline then decodes the same text up to four times: wrapping, row padding, the
grid overlay, and the cursor. This section replaces that with text decoded
**once** into styled glyphs, and every layer below works in cells.

### 13.1 Problems in the current code

All nine are recorded as `known_bug` cases in `tests/test_terminal_ui.cpp`.

| # | Bug | Cause |
|---|---|---|
| 1 | Coloured text wraps too early | `line_split` counts colour escape bytes as visible columns. After a break it re-measures with `utf8::width`, which skips them, so the two counts disagree. |
| 2 | **A peer's message can send commands to the user's terminal** (OSC 52 clipboard write, clear screen, bell) | Message text is passed through raw; `write_text` stores escapes in cells and emits them |
| 3 | Wrapped lines lose their colour | Only the first piece of a message carries the escape, and every row starts with a reset |
| 4 | Content colour leaks into padding and the right border | Nothing resets after a row's content |
| 5 | The `...` truncation marker can split a glyph or an escape | `ChatWindow` overwrites the first 3 bytes of the top line |
| 6 | An element can draw outside its own box | There is no clipping |
| 7 | `Position::z` has no effect | `Element::render` never puts z into its commands, so the stable sort never reorders anything |
| 8 | A wide glyph is half-drawn at the frame's right edge | `write_text` draws the left half when the right half doesn't fit |
| 9 | Drawing a narrow glyph breaks the wide glyph to its right, so the row is emitted one column too wide | `write_text` always clears the next cell's continuation flag, even for a 1-column glyph |

| 10 | A resize resets every child's z to 0 (found in step 3) | `AbsoluteLayout::set_size` rebuilt each position as `{x, y}` |

**Status: all fixed.** Bugs 2, 8 and 9 were fixed in step 1; 4, 6, 7 and 10 in
step 3; 1, 2b, 3 and 5 in step 4. Every one of them is now a `check`.
Bug 2 was only partly fixed. Control characters can no longer reach the terminal,
but colour codes in a message still become styles. That is recorded as known bug
**2b**, fixed by cleaning text at the input boundary (step 5).

### 13.2 Design

**Style is data, not escape codes.** Text never carries escapes. Untrusted text
is cleaned once, where it becomes styled text.

```cpp
struct Color { enum class Kind : uint8_t { Default, Ansi16, Ansi256, Rgb } kind{}; uint8_t r{}, g{}, b{}; };
struct Style { Color fg, bg; uint8_t attrs = 0; /* Bold, Dim, Italic, Underline, Reverse */ };

// One user-perceived character: a base codepoint plus the combining marks after it,
// stored inline as UTF-8. Fixed size, no heap.
struct Glyph { std::array<char, 15> bytes{}; uint8_t size = 0; uint8_t width = 1; };
struct Cell  { Glyph glyph; Style style; bool continuation = false; };

// Text plus styles: what models and widgets produce. The text is kept as given:
// TextLayout shows control characters and bad bytes as U+FFFD (through next_glyph),
// so display safety doesn't depend on cleaning, and byte offsets stay those of
// the original text (InputBox's cursor offsets need that).
class StyledText { /* runs of {Style, std::string}; add(text, style) */ };
```

There are four layers. Each is the only place that does its job.

1. **`Unicode.hpp`: the only place that decodes UTF-8.**
   - `next_glyph(text, pos)` returns `{Glyph, byte length}`, using the existing
     wide and combining tables.
   - `sanitize(text)` turns C0, DEL and C1 control characters (so `ESC` too) into
     `U+FFFD`, and tabs into spaces.
   - `InputModel` keeps its byte-based, codepoint-aligned editing.
2. **`TextLayout`: shape and wrap once.**
   - `layout(StyledText, width)` returns the glyphs, each with its style and source
     byte offset, plus the wrapped lines (word wrap, hard break for over-long words,
     `'\n'`).
   - Each glyph carries its style, so wrapped lines keep their colour.
   - Widths are known in advance, so wrapping is exact.
   - The source offsets give `InputBox` its cursor column with a lookup.
   - It replaces `line_split` and `line_split_tail`.
3. **`Canvas`: a clipped, offset view of the `FrameBuffer`.**
   - Operations: `sub(rect)`, `put(x, y, Glyph, Style)`, `text(x, y, line)`,
     `fill(rect, ...)`, `set_cursor(x, y)`.
   - Nothing can be drawn outside it.
   - `put` keeps wide glyphs intact: covering either half blanks the whole glyph,
     and a glyph cut by the clip edge becomes a space.
4. **`Element::draw(Canvas&)`**, replacing `begin_render` / `inner_content` /
   `end_render` and `RenderCommand`.
   - Margin, border and padding are cells in the element's own style.
   - `draw_content(Canvas& inner)` is the widget's single hook.
   - `AbsoluteLayout` sorts its children by z and draws each into
     `canvas.sub(child rect)`.

**Terminal output.** Rows are still diffed against the previous frame. While a row
is written, an escape is emitted only where a cell's style differs from the
previous cell's. Every cell states its full style, so a row can later be
repainted in part.

**Widgets.**
- **`ChatWindow`:**
  - Each message becomes `StyledText`: sender and time in the separator style,
    then the body in the message style.
  - Only the messages that reach the window are laid out. It walks back from the
    newest until the window is full, so the work is bounded by its height and
    the result is exact (no byte-stride guess). Those layouts are kept until the
    model, the size or a style changes.
  - The `...` marker is drawn as glyphs over the top row, in the style of what
    it covers. It appears whenever anything is hidden above: part of a message,
    or older messages.
- **`InputBox`** lays out its content once per change and finds the cursor through
  the glyphs' source offsets.
- **Style setters.** `set_main_color(std::string_view)` and `set_separator_color`
  are replaced by `ChatWindow::set_message_style(Style)` and
  `set_separator_style(Style)`. `Element::set_style` covers the element's own
  cells (background, border, padding; for `InputBox` also its text). That also
  ends the risk of a `string_view` pointing into a string the caller owns.
- **`ChatModel::add_message`** stores text unchanged; display safety comes from
  `next_glyph`.

**Out of scope:** emoji joined by ZWJ, and variation selectors. They stay
approximate; supporting them later changes only `next_glyph` and the width table.

**Cost:** a `Cell` goes from about 40 bytes plus a possible heap string to about
28 bytes, with no heap. A 200×60 frame is about 340 KB, and two frames are kept
for the diff. Comparing rows becomes a plain byte comparison.

### 13.3 Steps

| Step | Change | Tests |
|---|---|---|
| 0 ✅ | `tests/test_terminal_ui.cpp`: current behaviour as `check`s, bugs 1–9 as `known_bug`s | 33 checks, 9 known bugs |
| 1 ✅ | `Unicode.hpp` (decode, `Glyph`, `next_glyph`, `sanitize`, the width table, which `Utils.hpp` now uses), `Style`/`Color` with `append_sgr`/`apply_sgr`, the new `Cell`. `FrameBuffer::put` keeps wide glyphs whole, and `row_text` emits SGR only on style changes. The transitional `write_text` turns colour codes into a running style, drops other escapes, and draws glyphs through `put` | `tests/test_cells.cpp` (62 checks: decoding including malformed input, widths, glyphs, cleaning, SGR both ways, `put`, `row_text`). `test_terminal_ui`: 36 checks; bugs 2, 8, 9 fixed |
| 2 ✅ | `StyledText` + `TextLayout` (`TextLayout.hpp/.cpp`): `wrap(text, width)` shapes glyphs once, each with its style, source offset and length, then wraps them. `locate(offset)` gives `(line, column)` | `tests/test_layout.cpp` (40 checks: words, hard breaks, newlines, wide glyphs, styles across breaks, marks across runs, controls, `locate`). Checked against the old `line_split` on 171,467 random texts: identical lines, except the two intended changes (a trailing `'\n'` leaves an empty last line, and empty text is one empty line) |
| 3 ✅ | `Canvas` (`Canvas.hpp/.cpp`): a clipped, offset view over `FrameBuffer::put` with `sub`, `put`, `text`, `fill`, `set_cursor`. `Element::draw(Canvas&)` clears the box in the element's style, draws the border, then calls `draw_content(inner)`. `AbsoluteLayout` draws children by z, each into `inner.sub(child rect)`. `ChatWindow` and `InputBox` still produce strings, so they run on a temporary `LegacyElement` (the old hooks over `Canvas::write_legacy`) | `test_cells`: 78 checks (Canvas: clipping, nested clips, wide glyphs at both clip edges, fill, cursor clamping). `test_terminal_ui`: 45 checks; bugs 4, 6, 7, 10 fixed |
| 4 ✅ | `ChatWindow` and `InputBox` moved onto `draw_content` with `TextLayout`; style setters. Removed: `LegacyElement`, `RenderCommand`, `Canvas::write_legacy`, `apply_sgr`, `Element::set_main_color`, `line_split` / `line_split_tail`, and every display helper in `Utils.hpp` (it now holds only the byte-boundary helpers editing uses). Net −402 lines | all 33 step 0 checks still pass; bugs 1, 2b, 3, 5 fixed. New checks: chat styles, multi-line messages, the marker for hidden older messages and over wide glyphs, the input cursor after a trailing newline and after a control character, the input box's style. `test_terminal_ui`: 59 checks, no known bugs |
| 5 | `unicode::sanitize` where text leaves the UI's display path, e.g. text written to logs. Display needs no cleaning, since `next_glyph` already guarantees it | the remaining uses, if any |

When a known bug is fixed, its `known_bug` reports XPASS and is turned into a
`check`.

---

## 14. Frontends: command layer and agent CLI

### 14.1 Split (D10)

| Part | TUI | CLI (`--cli`) |
|---|---|---|
| Input thread → `frontend_q` | `KeyboardWorker`: bytes → `Key` | `LineReader`: `poll(stdin, WakeupEvent)` → `InputLine{line_no, text}`, then `InputClosed` on EOF |
| Command layer (shared, no I/O) | `CommandProcessor`, given the line on Enter | `CommandProcessor`, given each `InputLine` |
| View (main thread) | `TuiView`: `ChatModel`, layout, `TerminalUI::render` | `JsonlWriter`: one JSON object per line on stdout, flushed per line |

In CLI mode:
- there is no keyboard listener and nothing is rendered;
- `TerminalUI` is never constructed (its constructor changes termios), so a node runs without a TTY;
- stdout carries only events, logs go to the log file, and stderr carries only fatal startup errors.

### 14.2 Command layer

```cpp
// Frontend/Commands.hpp: pure, unit-tested without threads
using Command = std::variant<Blank, SendText, DialReq, ReconnectReq, CloseReq, ListReq, QuitReq,
                             OpenConv, EchoMode, Help>;   // Store requests carry req = 0 here
struct CommandError { std::string text; };
std::expected<Command, CommandError> parse_command(std::string_view line, ConvId active);

// Frontend/CommandProcessor.hpp: session state + dispatch, still no I/O of its own
struct ProcessorMode {
    Subscription active;                    // what /open subscribes the new conv to
    std::optional<Subscription> previous;   // TUI: the old conv goes back to this; CLI: none
};
class CommandProcessor {
public:
    enum class Local { None, Blank, Help, Quit };   // what the frontend still has to do
    CommandProcessor(ThreadQueue<StoreMsg>& store, ProcessorMode mode = {});
    // Parses `line`, stamps `req` and pushes the Store request(s).
    std::expected<Local, CommandError> submit(std::string_view line, ReqId req);
    ConvId active() const;
};
```
A blank line is its own command (`Blank`) rather than an empty optional, so the
result is one plain `expected`.

| Command | Request | Notes |
|---|---|---|
| `/dial <ip> <port>` | `DialReq` | `Reply.conv` = the new conversation |
| `/send <conv> <text>` | `SendText` | explicit target, so no hidden state; the form an agent should use |
| plain text | `SendText` to the active conv | error when no conv is active |
| `//<text>` | `SendText` of `/<text>` to the active conv | to send text that starts with `/` |
| `/open <conv>` | `Subscribe{req, conv, mode.active}`; TUI: also `Subscribe{0, old, mode.previous}` | makes `conv` active. The Store's `Reply` checks the conv exists, and a `ConvSnapshot` (the history) follows. TUI: also Tab |
| `/list` | `ListReq` | answered with `ConvList` |
| `/reconnect [<conv>]` | `ReconnectReq` | default: the active conv |
| `/close [<conv>]` | `CloseReq` | the Store sends `ClosePeer` per member |
| `/echo on\|off [<conv>]` | `Subscribe` with outbound `Echo` (or back to the mode's outbound) | |
| `/help` | local | the frontend replies ok itself, then a `notice` with the command list |
| `/quit`, Ctrl+C, CLI stdin EOF | `QuitReq` | |

`<conv>` is the hex id from `/list`, `Reply` or any event. The TUI may later allow
short prefixes; the CLI requires the full id, so a match is never ambiguous.

### 14.3 Request ids

- **CLI:** `req` is the input line's 1-based number (blank lines count). An agent
  needs no tagging syntax, and two requests can never share an id.
- **TUI:** the `CommandProcessor` assigns a counter.
- **Store contract:** every request with `req != 0` gets **exactly one `Reply`**,
  sent before any event that request causes. For example, `Reply{conv}` for a
  `/dial` arrives before that conv's first `conv_state`.
  - Success: `ok = true`; `conv` is set where the request concerns a conv.
  - Failure: `ok = false` with `error` (unknown conversation, not connected, bad endpoint, …).
  - A send that the transport later rejects is not a failed request. It shows up as
    `msg_status` = `failed`.
- A parse error never reaches the Store. The frontend reports it as a `Reply`
  with `ok = false` for the same `req`.

### 14.4 CLI output (JSONL)

One object per line, with the event name in `ev`. Ids are lowercase hex strings,
because 64-bit values lose precision as JSON numbers. Strings are JSON-escaped,
so control characters and escape sequences in peer text can never reach a terminal.
Invalid UTF-8 becomes U+FFFD.

```
{"ev":"ready","listen":"127.0.0.1:40123","pts":2}
{"ev":"reply","req":1,"ok":true,"conv":"9f3a1c0d2e4b5a60"}
{"ev":"conv_state","conv":"9f3a1c0d2e4b5a60","status":"dialing"}
{"ev":"conv_state","conv":"9f3a1c0d2e4b5a60","status":"connected"}
{"ev":"reply","req":2,"ok":true,"conv":"9f3a1c0d2e4b5a60"}
{"ev":"msg","conv":"9f3a1c0d2e4b5a60","id":0,"from":"me","body":"hi","state":"pending","at":"2026-11-24T10:00:00.123Z"}
{"ev":"msg_status","conv":"9f3a1c0d2e4b5a60","id":0,"state":"sent"}
{"ev":"msg","conv":"9f3a1c0d2e4b5a60","id":1,"from":"peer","body":"hello back","state":"received","at":"…"}
{"ev":"reply","req":3,"ok":false,"error":"unknown conversation"}
{"ev":"convs","convs":[{"conv":"9f3a1c0d2e4b5a60","status":"connected","remote":"127.0.0.1:40200","unread":0}]}
{"ev":"notice","text":"…"}
{"ev":"shutdown_done"}
```

| `FrontendMsg` | `ev` |
|---|---|
| (listener bound) | `ready` (always the first line; `listen` holds the real port when bound to port 0) |
| `Reply` | `reply` |
| `ConvSnapshot` | `snapshot` (`msgs` array) |
| `MsgAppended` | `msg` |
| `MsgStatus` | `msg_status` |
| `ConvState` | `conv_state` (`detail` when present) |
| `ConvNotify` | not emitted: the CLI forwards every conv (§14.5) |
| `ConvList` | `convs` |
| `SystemNotice` | `notice` |
| `ShutdownDone` | `shutdown_done` (always the last line) |
| startup failure | `fatal` with `error`, then exit code 1 |

Adding a field to an event is a compatible change; renaming or removing one is
not.

### 14.5 CLI behaviour

- **Everything is forwarded.** At startup the CLI sends
  `SetDefaultSubscription{Forward, Notify, Notify}`, so every conversation's
  messages and status changes are printed. There is no unread state to poll.
  `/echo` still works per conv.
- **Listening:** `--listen 127.0.0.1:0` binds a free port and `ready` reports it.
  Tests then never need to pick ports at random.
- **Exit:** stdin EOF → `InputClosed` → `QuitReq` → the normal shutdown (§7.8) →
  `shutdown_done` → exit 0. With `--stay`, EOF is ignored and the node runs until
  SIGTERM, which also takes the normal shutdown path. That suits a node started
  in the background with `</dev/null`.
- **Echoing input:** none. An agent correlates through `req`.
- **Shutdown:** writing out the remaining events counts as progress for the
  shutdown watchdog (§9.2), so a long backlog is delivered, not cut off by a
  forced shutdown. An agent that stops reading altogether still gets a bounded
  exit.
- **Order of replies:** a parse error is answered by the frontend at once, so it
  can come before the Store's reply to an earlier line. Each reply carries its
  `req`; nothing else orders them.
- **Blank lines** get no reply but still count toward the line numbers.
- **stdout closed** (EPIPE): the writer stays closed and the node quits through
  the normal shutdown, exit 0.
- **Options** (`node --help`): `--cli`, `--stay`, `--listen HOST:PORT` (default
  `0.0.0.0:7000`, port 0 = any), `--advertise HOST:PORT` (repeatable; default the
  listen address, `127.0.0.1` for `0.0.0.0`, port 0 = the bound port), `--log PATH`
  (default: no log), `--log-level`, `--min-pts` / `--max-pts`, `--shutdown-grace MS`,
  `--handshake-timeout MS`, `--connect-timeout MS`. A bad option prints the usage to
  stderr (with `--cli` also a `fatal` event) and exits 1.
- **`--balance-period MS`** sets the Balancer's tick (default 500). Without
  `--cli` the node runs the terminal UI (§5.6).
- **Stopping `LineReader`:** it blocks in `poll` on stdin plus a `WakeupEvent`, so
  `stop()` wakes it even while stdin is open (the same rule as `TcpListener::stop`).
  It splits on `'\n'` and strips one trailing `'\r'`.

### 14.6 Threads per mode

| Thread | TUI | CLI |
|---|---|---|
| main | frontend loop: `TuiView` + `CommandProcessor` | frontend loop: `JsonlWriter` + `CommandProcessor` |
| input | Keyboard | LineReader |
| Store, Router, Pt-0..n, Balancer, Listener, Logger | §2 | §2 (identical) |

---

## 15. Load generator and runner

### 15.1 Choice

The generator is a C++ tool that speaks the wire protocol itself (`Tools/loadgen/`),
as `stressgen` did before the rewrite. It is not a script that drives `node --cli`
peers.
- One process simulates hundreds to thousands of peers on one thread, so the
  sizes of the earlier S1 and S7 runs (1024 peers) are reachable.
- What is measured is the target node, not the overhead of peer nodes.

### 15.2 How it works

- **Target:** `node --cli --echo` sets the default subscription to
  `{Ignore, Echo, Ignore}`. Every chat message goes straight back to its sender;
  nothing is stored, and nothing is printed per message or per peer (an echo
  node writes only `ready` and `shutdown_done`).
- **Peers:** each dials the target and completes the real handshake: a hello
  with a random name of ours, and an Ack whose `peer_id` becomes the chat
  `sender_id`. There is one connection per peer.
- **Messages:** the body starts with the send time (8 bytes) and a sequence
  number (8 bytes), padded to `--size`. The echo gives the round-trip time; the
  sequence number makes each message count once.
- **Kinds of peers:**
  - active: sends at `--rate`, or keeps `--window` messages in flight with
    `--rate 0` (closed loop);
  - idle: handshake only;
  - slow: sends but never reads. After the handshake it leaves the pool
    (`connection_release`), keeps its own send buffer so frames stay whole, and
    has a small `SO_RCVBUF` (new `TcpConnection::Options::receive_buffer`).
- **Churn:** every `1/--churn` s a random active peer's connection is dropped and
  redialled, re-presenting the name the node gave it, so the node takes its
  reconnect path.
- **Phases:** ramp (all handshakes, at most 64 dials in flight), warm-up,
  measurement, grace (no new sends; late echoes of measured messages still
  count).
- **Accounting** of the messages sent in the measurement window: echoed,
  refused by our own buffer (`send_failed`), abandoned (in flight on a
  connection that churn dropped), or lost. Throughput and RTT percentiles
  (log-bucketed histogram, about 3% resolution) are reported, plus a per-second
  series. The output is one JSON object.
- **The node, for load:** `--echo`, `--no-balancer` (`min_pts` workers, the
  Router's own placement), `--max-links`, `--max-links-per-pt` (default: no cap
  below `--max-links`; the old 256 per pt silently capped a one-pt node),
  `--balance-max-moves`. It also raises `RLIMIT_NOFILE` to its hard limit and
  uses a listen backlog of 4096.
- **Balancer decisions are Metric log lines:** `decision=spawn|retire|move|evict`
  and `topology=added|removed`, key=value, so a `--log-level metric` log has
  them.

### 15.3 Runner (`scripts/run_load.py`)

`run_load.py --out DIR [node options] -- [loadgen options]` runs one load test:
1. starts `node --cli --echo` with a metric log;
2. runs `loadgen` against it, sampling the node from `/proc` every 0.5 s (CPU
   per thread, RSS, threads, open descriptors);
3. ends the node through end of input, and records its exit code and shutdown
   time;
4. parses the metric log: per tick and per pt the links and load, and the
   Balancer's moves, spawns and retirements.

Output: `DIR/node.log`, `DIR/loadgen.json` and `DIR/result.json`, plus a one-line
summary. It needs only the Python standard library and uses `build-release/`.

### 15.4 First measurements (Release build, WSL2, 22 cores; indicative only)

| Load | Result |
|---|---|
| 8 × 200 msg/s, 64 B | everything echoed, RTT p50 0.7 ms |
| 256 × 100 msg/s | 25.6k msg/s, nothing lost, p50 0.4 ms |
| 512 × 100 msg/s, Balancer on or off | 51k msg/s, nothing lost, p50 0.7–1.1 ms, p99 9–24 ms |
| 1024 × 100 msg/s | the node caps at about 43–58k msg/s; RTT grows to seconds (queues build up); the shutdown still exits 0 |
| 8 active + 300 idle + 4 slow peers | the slow peers cost the active ones nothing |
| 64 × 50 msg/s with 20 reconnects/s | nothing lost; reconnect handshake about 1.2 ms |
| closed loop 8 × window 16, 4 KB | about 31k msg/s |

**Findings for the experiments**
1. **At the 1024-peer cap no thread is saturated** (pt-0 about 66%, Router
   about 62%, Store about 44%, the generator 79%). The limit is in the hand-offs,
   not in one busy thread. Candidates to measure:
   - contention on a pool's mutex between its IO worker's `receive()` and the
     Router's `send()`;
   - a condition-variable wake-up per message on the Pt-i → Store → Router →
     pool path.
2. **Mass arrival lands on one pt.** During the ramp there is no traffic, so the
   Balancer sees no load and starts no worker: all links are placed on pt-0. The
   Balancer then spawns workers once traffic starts, and drains pt-0 at
   `max_moves_per_tick` per tick. At the default 2 moves per tick, 512 links
   would take minutes. `--balance-max-moves 32` spreads them over 4 pts in a few
   seconds (96/128/160/128).
3. **There is no backpressure on inbound traffic** (at the time a scope
   decision at the top of this document). When the node is overloaded its queues
   grow without bound, latency follows, and memory with it. §16 designs it.

### 15.5 Scenario matrix (`scripts/run_matrix.py`, `scripts/analyze.py`)

**Variants.** The old matrix compared I/O variants, which the rewrite no longer
has. These compare node configurations instead:

| Variant | Node options |
|---|---|
| `fixed-1` | `--no-balancer --min-pts 1` |
| `fixed-4` | `--no-balancer --min-pts 4` (the Router's least-loaded placement) |
| `balancer` | `--max-pts 4` (default 2 moves per tick) |
| `balancer-fast` | `--max-pts 4 --balance-max-moves 32` |

**Scenarios** (as before the rewrite; 20 s measured after 5 s of warm-up, 3
repetitions):

| | Swept | Other load |
|---|---|---|
| S1 fan-in | active peers 1, 8, 64, 256, 1024 | 100 msg/s, 64 B |
| S2 idle-heavy | idle peers 0, 100, 300, 1000 | 8 × 500 msg/s |
| S3 slow consumers | slow peers 0, 1, 4 | 16 × 200 msg/s |
| S4 churn | reconnects/s 0, 10, 50 | 64 × 50 msg/s |
| S5 throughput ceiling | size 32, 4096, 65536 B | 8 peers, closed loop, window 16 |
| S7 capped fan-in | node `--max-links` 64, 128, 256, 1040 | 1024 × 100 msg/s |

S6 (WAN: netem in a namespace) is not ported yet. The full matrix is 264 cells,
about 2.2 hours.

**Runner.** Per cell, `run_load.run_cell()`:
- the node is pinned to `--node-cpus` (default 0-3) and loadgen to
  `--loadgen-cpus` (8-9);
- CPU is measured over loadgen's measurement window only (it reports the window
  on the monotonic clock);
- outside S7, the node's link cap is set above the load, so it never refuses a
  peer.

The rest is as before the rewrite:
- the layout `<out>/<scenario>/<x>=<value>/<variant>/rep<N>/`;
- `meta.json` with status (`ok`, `dirty`, `failed`), validity and a build stamp
  (commit, uncommitted files, binary hashes, build type);
- resume skips `ok` and `dirty` cells; adding cells from another build is
  refused unless `--force-mixed`;
- a seeded shuffle of the variant order per (scenario, value, rep);
- `--quick` is a smoke run, about 2.5 minutes for all 48 cells;
- `--dry-run` lists the cells and estimates the time.

**Validity** (`analyze.validity`, also stored in `meta.json`). A completed cell
does not count when:
- the node exited non-zero or was forced;
- fewer than 95% of the expected peers connected (in S7, of `min(peers, cap)`);
- loadgen fell behind (then the generator, not the node, set the pace);
- there were protocol errors;
- CPU was sampled over less than half of the measurement.

**Analysis.** `analyze.py RESULTS` writes:
- `summary.csv`: per (scenario, value, variant) the median, Q1 and Q3 of
  throughput, delivered %, RTT p50/p99, refused sends, handshake and reconnect
  times, node CPU and its busiest thread, threads, RSS, descriptors, IO workers
  (counted from the node's `pt-N` threads) and moves;
- `report.html`: per scenario three charts (throughput, RTT p99 on a log axis
  when it spans more than 50×, node CPU; one measure per chart), with IQR
  whiskers, a legend plus direct labels, hover tooltips, the full table, and
  the problems list. It is self-contained, in light and dark, with the variants
  in fixed categorical slots 1–4.

`scripts/test_analyze.py` (in CTest) checks the statistics, the validity rules,
aggregation over a synthetic results directory, the plan (shuffle, reproducible,
caps), and the charts: well-formed SVG, labels inside the chart and apart,
colours fixed per variant.

Not yet done: S6, and figures for the LaTeX report (`summary.csv` is the input
for them).

---

## 16. Inbound backpressure (D14)

### 16.1 Why

In the scenario matrix every 1024-peer cell (about 102k msg/s offered) was
overloaded: about 30k msg/s got through, the rest queued.

| | 1024 peers, 20 s run |
|---|---|
| Node memory | 420–464 MB (4–7 MB below overload); about 1.4M queued messages at ~320 B each |
| Round trip | about 16 s |
| Shutdown | 16 of 24 cells forced (exit 3): the backlog could not drain within the grace |

At that rate an 8 GB machine runs out of memory in about 6 minutes, and the
out-of-memory killer then takes every peer down at once. Unbounded queues also:
- keep control messages (link events, frontend commands, Balancer moves) behind
  the whole backlog in each FIFO inbox;
- spend the node's capacity on messages that are seconds old;
- let one fast or hostile peer fill memory alone;
- defeat TCP's own flow control, because the IO workers always read.

**Where the backlog is** (the `queue_depth` Metric lines of step 1, one run each
at 1024 peers on CPUs 0–3): all of it is in the Router's inbox. The Store's
queue stays at a few hundred messages at most, while the Router's grows by
60–75k messages per second:

| | Store inbox | Router inbox (after ~20 s) | Throughput | Node memory |
|---|---|---|---|---|
| `fixed-1` | 2–127 | 1.5M | 39k msg/s | 732 MB |
| `fixed-4` | 0–282 | 1.8M | 17k msg/s | 719 MB |

The Router is the bottleneck in both, and with four IO workers it moves less
than half as many messages: its `pool.send` competes with each worker's reads
for that pool's mutex. That is the next thing to profile; backpressure bounds
the backlog but does not raise the ceiling.

What is bounded already: the pool's TX buffer per connection (1 MB, then
`BufferFull`; write timeout), frame size (`kMaxChatBodySize`), and connections
(`max_links`, `max_links_per_pt`). The gap is the inbound data path.

### 16.2 What is watched

```
socket ─▶ Pt-i ─ChatIn─▶ Store inbox ─▶ Store ─RouteSend─▶ Router inbox ─▶ Router ─▶ pool.send
                                              └─MsgAppended─▶ Frontend inbox ─▶ TUI / CLI
```

- **The Store's inbox.** Every received message passes through it, so when the
  Store falls behind the only safe answer is to read less from the network.
- **The Router's inbox, for echo.** Received chat never reaches the Router: its
  `RouteSend`s are the messages this node sends, at the rate its user types
  (times the members of a group). In echo mode (the load-test target) every
  received message becomes a `RouteSend`, and that is where the 1024-peer
  backlog built up (§16.1). Watching it costs one atomic load and keeps the
  benchmark's overload cells bounded.
- **Not the Frontend's inbox.** The UI is a view of the Store's history; a slow
  terminal or a slow agent must not slow the network down. It is handled between
  the Store and the Frontend (§16.6).
- **Not the Listener.** Connections are capped by `max_links` and arrive at a
  low rate compared with messages.
- **Not by chaining** (the Store stops taking from its inbox while the Router's
  is full). The Store's inbox is one FIFO: a paused Store would also stop
  commands, `PeerUp` / `PeerDown` and subscription changes, so the UI would
  freeze during overload.

**The queue owns its watermarks** (`QueueDepth`, the base of every
`ThreadQueue`):
- `approx_size()`: a relaxed atomic counter, published under the queue's lock
  after every push, pop and `pop_all`, read without the lock. `size()` takes the
  mutex, which would add exactly the contention the inbox batches removed.
- `set_watermarks(high, low)`, once, by `main`; then `congested()`, a relaxed
  atomic flag. A push that reaches `high` sets it, a pop or `pop_all` that
  brings the queue down to `low` clears it, both under the lock the queue
  already holds. The hysteresis is computed once, at the crossings, so every
  reader sees the same answer.
- Both atomics sit on a cache line of their own: every push and pop writes
  them.

The IO workers already hold the Store's and the Router's queues, so they check
`store.congested() || router.congested()` directly; no other object is
involved. (A first version had a separate `FlowGate` holding a list of queues
and the two numbers, with each worker comparing depths itself; it had one user,
and the workers could briefly disagree.)

### 16.3 Pausing reads in the pool

The IO worker cannot simply stop calling `receive()`: the same loop flushes
queued TX (the replies), runs timers, completes dials and drains commands. So
reading is paused per connection, inside the pool:

- **API.** `pause_reading(span<id>)` / `resume_reading(span<id>)`, one lock for
  the whole set.
- **Interest.** A `reading` flag per `Entry`; while it is false, EPOLLIN is not
  in the connection's epoll interest. `set_interest_locked` is the one place
  that computes interest, from the status, pending TX and `reading`. epoll is
  level-triggered, so merely skipping reads would spin.
- **`ready_` skips paused connections**: data already reported readable stays
  in the kernel, so the overshoot is the read in progress.
- **Writes go on.** EPOLLOUT stays armed while TX is pending.
- **The read timeout is frozen.** A paused connection receives nothing, so
  without this every paused peer would be dropped as `ReadTimedOut`. The
  connection keeps its one read timer: when it comes due on a paused connection
  it re-queues itself a timeout later instead of failing, and resume only sets
  `last_rx = now` (the timer re-checks it when due). Arming a fresh timer on
  each resume, with a generation bump to retire the old one, was the first
  version: the retired nodes stay in the heap until their deadlines, up to
  cycles/s × links × read timeout of them (about 30k at 6 cycles/s, 1024 links
  and 5 s), and without the bump they would never leave.
- **Hang-ups.** EPOLLERR and EPOLLHUP are always reported, whatever the
  interest. A connection that reports one is read even while paused (it would
  otherwise be reported again on every wait), so its last data and its end are
  delivered. A plain FIN only shows as readable, so it is seen on resume.
- **Moves.** A connection released while paused joins its next pool reading;
  the adopting IO worker pauses it again if it must.
- **Cost.** One `epoll_ctl` per connection per pause and per resume. Under
  sustained overload the queue swings between the watermarks about
  `store_rate / (high − low)` times per second (50k / 8k ≈ 6): about 12k
  `epoll_ctl` per second per worker with 1024 links, under 1% of a core. A
  two-level epoll (one syscall per pause) would need a restructured pool.

The backlog moves into the kernel's socket buffers, bounded by its TCP memory
limits; `TcpConnection::Options::receive_buffer` can cap them per connection.

**Memory is bounded, latency is not yet.** Measured in step 4 (`test_loadgen`,
32 peers offering 640k msg/s to a Debug node): the node's deepest inbox was
1983–2224 messages with `--inbox-high 2000`, against 240k–447k without
backpressure (one of those nodes was forced at shutdown). But loadgen's sends
were never refused: the kernel's buffers, which autotune to several MB per
connection on loopback, absorbed the excess, and no message sent in the 2 s
measurement came back within its 0.5 s grace. The queueing delay moved from
the node's memory into the kernel. Two consequences:
- Between our own nodes the sender's side is bounded too: once its kernel send
  buffer is full the sending node's pool TX buffer fills (1 MiB), and further
  sends fail with `BufferFull`, which the user sees as a failed message.
- To bound latency at the receiver, cap `SO_RCVBUF` on accepted sockets (a node
  option, measured in step 6). A smaller buffer makes TCP push back sooner.

### 16.4 IO worker

- **Pause.** After every read, check whether the Store's or the Router's
  queue is `congested()` (two atomic loads). If one is, pause every
  **established** link of this worker. A read is at most `rx_buffer_size`
  (64 KiB), which bounds the overshoot per worker.
- **Handshaking links keep reading.** Their frames go to the Router, are small,
  and the handshake timeout bounds them, so peers can still connect.
- **Resume.** While paused, `receive` runs with a deadline (`resume_poll`, 1 ms)
  instead of blocking; on every return the worker checks that neither queue is
  `congested()` any more and resumes its paused links. About 1000 wake-ups per second, only while paused.
  A wake-up from the consumer instead would give `ThreadQueue` callbacks and the
  Store knowledge of the IO workers.
- **Each worker decides alone.** All read the same flags, so they pause and
  resume within one round of each other.
- **A link adopted while paused** is paused at once if it is established.
- **Fairness while reading: the budget already exists.** Each epoll round the
  pool reads every ready connection once, at most `rx_buffer_size`; one with
  more data stays readable and waits for the next round, behind the others
  (level-triggered epoll moves each reported connection to the back of its
  ready list). So a heavy peer gets one read per round like everybody else (a
  test floods one link and times a message on another).
- **Fairness across pauses: least recently read first.** A pause empties the
  kernel's ready list, and on resume it lists readable connections in the
  order reading is switched back on. In a fixed order, a short cycle (a low
  watermark) reads only the first connections before the queues fill again,
  and the rest starve: the watermark sweep (§16.9) showed a p99 of 18–20 s at
  `--inbox-high` 4096 and below. Every read stamps its link with a sequence
  number, and `resume_reads()` sorts the paused links by it, oldest first, so
  whoever was cut off last time goes first now. One sort of at most the
  worker's links per resume, small next to the `epoll_ctl` calls the resume
  makes. A test reads about one connection per cycle (4 KiB reads against a
  watermark of 16) over 32 links: every link gets at least 30% of the mean;
  without the sort the least served gets nothing. Rotating
  EPOLLIN across connections was rejected: `epoll_ctl` churn for as long as the
  throttling lasts, a wait for every peer including light ones, and turns that
  are not fair to traffic.
- **Sizing `low`.** The Store must not run dry while a worker waits to resume:
  `low` must exceed what the Store drains in one `resume_poll` (about 100
  messages at 100k msg/s).

### 16.5 The Balancer

A paused worker reads less, so its links' rates fall. Taken at face value the
Balancer would see quiet workers and could retire one or move links onto it,
but while the queues are congested the bottleneck is downstream, and adding, removing
or moving workers cannot help.

- `PtReport` gets **`paused_ms`**, the time paused since the last report.
  Paused workers still answer probes, so stall detection is unaffected.
- While any worker paused during the last tick (`paused_ms > 0`, summed over
  the reports `end_tick` folds in) the Balancer **holds**: no spawn, no retire,
  no moves. The scale-in streak is frozen, neither advanced nor reset, so a
  short pause in the middle of a quiet period does not restart the count. It
  keeps updating its load estimates, the placement policy for new links and
  eviction (neither changes the workers), and logs
  `decision=hold reason=backpressure paused_pts=N`.
- **The Balancer does not decide admission.** It acts on 500 ms ticks, while
  the backlog grows by about 70k messages per second under overload; each
  decision would also take a report, a command and a pool call. Admission is
  local and fast (the IO workers); placement is global and slow (the Balancer).
  The Balancer can later set policy for the fast loop: cap or evict a link that
  takes far more than its share, or tune the watermarks and budgets.

### 16.6 Store → Frontend (later)

The Store pushes into the Frontend's inbox, so it can watch that queue and
degrade by itself:
- **Normal:** one `MsgAppended` per message.
- **Over `high`:** no per-message events. Each affected conversation is marked
  dirty with its newest message number, and a timer runs on the Store's own
  inbox (`ThreadQueue::every`). The timer is needed: without it the summaries
  would wait for the next message, and the last ones would never be announced
  if traffic stopped.
- **On a tick below `low`:** one "conversation X has messages up to N" per dirty
  conversation, then back to normal and the timer is cancelled. The summaries go
  out before any per-message event, so the Frontend never sees N+5 before
  learning of N+1..N+4.
- **Needs** a command to read a conversation's messages after a given number,
  and a CLI event for the summary: a change to the CLI contract, documented in
  the JSONL output. Agents that need every message read the history by cursor
  and see larger batches, never gaps.

The Frontend's queue is then bounded by the number of conversations. It only
matters with heavy inbound traffic and a slow UI at once; in echo mode the
Frontend receives nothing.

### 16.7 Configuration

- `--inbox-high N`, default 4096 messages (16384 until the watermark sweep in
  16.9): at the Store's 50–100k msg/s, at most about 40–80 ms of queueing in
  the node and a few MB per watched queue. Lower marks pause so often that the
  backlog moves into the kernel's socket buffers instead (see 16.9).
- `--inbox-low N`, default `high / 2`.

### 16.8 Steps

| Step | Deliverable | Tests |
|---|---|---|
| 1 ✅ | `QueueDepth` (base of `ThreadQueue`, own cache line): `approx_size()`, `set_watermarks()`, `congested()`. `QueueMonitor`: each inbox's depth and its maximum as a Metric line every 500 ms, on its own thread, started only when those lines are logged (a log file at `metric` or finer), not registered with the Supervisor | `test_flow` 28 checks: `approx_size` matches `size()` through `push`, `emplace`, `pop` (through a compaction), `pop_all` (partial and full), ticks and shutdown, bounded with 4 producers; the flag at both crossings, held between them, set at once on a queue already past `high`, through `pop_all`, off with `high` 0, with 4 producers; monitor lines and their maxima; `stop()`. Clean under TSan |
| 2 ✅ | `EpollConnectionPool::pause_reading` / `resume_reading` (spans of ids, one lock); a `reading` flag per entry and `interest_of()`, the one place that computes epoll interest; `timer_count()` for diagnostics | `test_pool` +6 tests (124 checks): data stays in the kernel while paused (unknown ids and a double pause are harmless) and is read on resume; the paused wait sleeps (under 30 ms CPU in 150 ms; it spins if EPOLLIN stays on); 768 KiB of queued TX reaches the peer while paused with nothing read; four timeouts of silence while paused raise no `ReadTimedOut`, the count restarts at resume, still one timer; 50 pause/resume cycles leave one read timer; last data and close of a peer that hangs up while paused; release while paused, another pool reads it. Each of four deliberate breakages (no freeze, a timer per resume, EPOLLIN kept, resume keeping the old count) fails a check. Clean under TSan and ASan+UBSan |
| 3 ✅ | IO worker: `pause_reads()` / `resume_reads()` on the Store's and Router's `congested()`, checked after every read; links established while paused (outbound Ack, inbound `Bind`, adopted) are paused at once; `receive` polls every `resume_poll` (1 ms) while paused; `PtReport::paused_ms`; `reads=paused` / `reads=resumed` Metric lines | `test_io_worker` +5 tests (96 checks): 3000 messages against `high` 64: reads stop, the Store's queue stays put while nobody drains it, `paused_ms` reported; draining resumes, every message once and in order, no paused time afterwards; the sender is blocked by TCP while the Store holds under 5000 (over a million without backpressure); a congested Router queue stops reads too; a new link's handshake completes while paused, its chat waits, an adopted established link is paused at once; a light link's message gets through a flood on another link. Breaking it (never pause, never resume, new links not paused) fails checks. Clean under TSan |
| 4 ✅ | `--inbox-high N` (default 16384, 0 = off) and `--inbox-low N` (default half; must be below high); `main` sets both watermarks on the Store's and Router's inboxes; `NodeHarness::Node(rc, bc, inbox_high)` | `test_router` +1 (38 checks): two in-process nodes, B congests at 2 messages, 2000 messages arrive complete and in order through repeated pauses, B's report shows it paused, the link works both ways after. `test_loadgen` +1 (30 checks): a real `node --echo` flooded by 32 peers: deepest inbox about 2000 with `--inbox-high 2000` (bound 2 × high + one read) and over 5 × high without; no peer dropped; the node with backpressure exits 0. Clean under TSan |
| 5 ✅ | `PtLoad::paused_ms` (per tick), `Decisions::hold`; `decide()` skips scaling and rebalancing while held; the hold Metric line | `test_balancer` +1 test (69 checks): a paused worker stops a scale out, a rebalance and a retire; the streak is frozen and the next quiet tick completes it; the policy and eviction still apply; the next tick without pauses decides normally. Breaking it (no hold on scaling, no hold on moves, a hold that resets the streak) fails checks. Clean under TSan |
| 6 ✅ | Measurement and analysis (§16.9); `--rcvbuf BYTES` on every accepted and dialled peer connection; `run_load` reads inbox maxima, pauses and holds from the node's log; `run_matrix --set backpressure`, `--values`; `analyze.py`: colours in the run's own variant order, a node memory chart, inbox / pauses / holds columns | the 1024-peer cells, then S1 and S7, with and without backpressure and with a capped receive buffer: memory flat, clean shutdowns, and the round trip with the kernel's share of the queueing removed. loadgen's refused sends (`send_failed`) are the expected result, not a degraded cell: `analyze.py` reports them separately |
| 7 | Store → Frontend coalescing (§16.6) | its own step |

### 16.9 Results (commit `8f3dcce`, Release, WSL2, node on CPUs 0–3)

`run_matrix.py --set backpressure --scenarios S1,S7`: 108 cells, 3
repetitions, all valid. Variants: `bp-off` (one IO worker, `--inbox-high 0`),
`bp-on` (one IO worker, the default then, 16384), `bp-rcvbuf` (as `bp-on` with
`--rcvbuf 65536`), `balancer-rcvbuf` (the Balancer, up to 4 workers, with the
same receive buffer). Medians:

| 1024 × 100 msg/s (S1; S7 uncapped alike) | Echoed | RTT p50 / p99 | Memory | Deepest inbox | Shutdown |
|---|---|---|---|---|---|
| `bp-off` | 40k msg/s, 1.2M lost | 13.4 s / 15.1 s | 767 MB | 1.5M | about 800 ms |
| `bp-on` | 102.4k msg/s (all offered), none lost | 172 ms / 294 ms | 17 MB | 16.6k | about 20 ms |
| `bp-rcvbuf` | 102.4k msg/s | 172 ms / 277 ms | 17 MB | 16.7k | about 15 ms |
| `balancer-rcvbuf` | 102.4k msg/s | 172 ms / 268 ms | 16 MB | 16.7k | about 17 ms |

- **Backpressure raised the ceiling, not just bounded the backlog.** One IO
  worker moves 2.5 times as much with it: without it the Router works through
  1.5M queued messages spread over 700+ MB; with the inboxes held near the
  watermark it keeps up with the whole offered load at about 76% of a core. The
  earlier "1024-peer ceiling" (§15.4, and `fixed-4` behind `fixed-1`) was the
  backlog's own cost.
- **The remaining latency is the watermark.** The Router's inbox sits at its
  high mark: about 16k messages at 100k msg/s is about 160 ms of queueing, the
  measured p50. See the sweep below.
- **TCP absorbs the pauses.** About 10 pauses per second, no refused send and no
  lost message in any backpressure cell.
- **The receive buffer cap changes nothing here**: the node keeps up, so nothing
  waits in the kernel. It matters only beyond the node's capacity.
- **Below saturation nothing changes.** 1–256 peers and every S7 cap: the same
  throughput, round trips within noise (0.8–1.5 ms p99), 4–5 MB, no pause.
- **The Balancer rebalances where it can and holds where it cannot help.**
  `balancer-rcvbuf`: 2 workers and 28 moves at 64 peers, 4 and 90 at 128, 4 and
  96 at 256, as in the earlier matrix, with no hold. At 1024 peers it held on
  every tick (49) and stayed at one worker, which reached the full load: the
  Router is the bottleneck, and more workers would not have helped. A hold
  triggered by any pause freezes a whole tick; a threshold (hold only when
  workers were paused for a real share of the tick) is the refinement if a
  helpful scale-out is ever blocked. In echo mode the Router saturates first; in
  normal use it receives no inbound traffic, so holds come only from the Store.

**Watermark sweep** (`run_matrix.py --set watermark --scenarios S1 --values
1024`: one IO worker, `--inbox-high` 256 to 16384, low at half; 3 repetitions,
all valid). Before and after the least-recently-read resume order (§16.4,
commit `914f918`):

| `--inbox-high` | Before: echoed, lost | Before: p50 / p99 | After: echoed, lost | After: p50 / p99 | Pauses (after) |
|---|---|---|---|---|---|
| 256 | 81k msg/s, 421k | 20 ms / 20.4 s | 102.4k, 0 | 147 ms / 386 ms | 6704 |
| 1024 | 101k msg/s, 24k | 34 ms / 20.4 s | 102.4k, 0 | 51 ms / 111 ms | 2176 |
| 4096 | 102.4k, 0 | 62 ms / 18.8 s | 102.4k, 0 | 69 ms / 124 ms | 750 |
| 16384 | 102.4k, 0 | 172 ms / 277 ms | 102.4k, 0 | 172 ms / 239 ms | 206 |

- **Before the fix, low watermarks starved peers**: the p50 fell with the
  watermark while a fixed resume order left the connections at the back unread
  until the load stopped (p99 ≈ the length of the run; the "lost" messages and
  the lower throughput at 256 were those peers' messages). After it, every
  watermark carries the whole load and nothing is lost.
- **The latency is not only the node's queue.** At 1024 the inbox holds about
  10 ms of messages, yet the p50 is 51 ms: the rest waits in the kernel's socket
  buffers while reads are paused. At 256 pauses come so often (about 250 per
  second) that more of the backlog waits there (p50 147 ms). A capped
  `--rcvbuf` may now matter at the low watermarks; not yet measured.
- **1024–4096 is the sweet spot here**: about a third of the latency at
  16384, the same throughput, 10–13 MB. The default is now 4096: close to the
  best latency, a third of the pauses at 1024, and more room for bursts.
