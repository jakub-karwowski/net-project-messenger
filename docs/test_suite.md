# I/O modes and the test suite

The node can move bytes in eight ways, selected with `--io=`. Each one is a
different combination of threads, connections and readiness APIs underneath
the same application pipeline. This document explains how each mode works, how
it builds its threads, how peers and connections are tracked and named, what is
measured, how the terminal and headless front ends share one implementation,
then how the tests and the benchmark harness exercise them.

```
app --io=threads|pool-sender|poll|epoll|poll-duplex|epoll-duplex|poll-split|epoll-split
    [--headless] [--echo] [--max-peers=N] [--backlog=N] [--address=A] [--port=P]
    [--log=PATH] [--metrics=PATH]
```

## 1. What every mode shares

### The pipeline

The application is a set of workers joined by queues. The I/O mode only
changes what sits underneath the store and the sender.

```
             render queue              store queue               sender queue
 Render  <--------------------  Store  <-----------------  ...  ------------------>  Sender
 (UI or          ------------->        ------------------------------------------>   (policy)
 headless)                                ^                                            |
                                          | EventPeerConnected / EventMessage /        | open / send / close
                                          | EventPeerDisconnected                      v
                               InboundTransport (reads)                   OutboundTransport (writes)
                               \__________________ IoStack, built by make_io_stack(IoMode) ___/
```

- **`make_io_stack()`** ([Transport.cpp](../Transport.cpp)) builds an `IoStack`
  holding an `InboundTransport` and an `OutboundTransport`.
  - In the poll and epoll families a single object implements both halves, and
    both `shared_ptr`s point at it.
- **`SenderWorker` owns all write policy:** what the handshake frames contain,
  the retry count, re-dialing, and dropping on backpressure. A transport only
  opens, writes and closes.
- **`FrameChannel`** ([FrameChannel.hpp](../FrameChannel.hpp)) holds the protocol
  state of one TCP byte stream: the frame decoder, identity learned in the
  handshake, and forwarding to the store. It has no socket. Whatever owns the
  socket feeds it with `on_bytes()` and reports the end with `on_closed()`.
  Section 4 explains it, together with `PeerTable`, in plain terms.

### Threads present in every mode

| Thread | Role |
|---|---|
| main | render loop (terminal UI, or `HeadlessRenderWorker`) |
| stdin reader (headless) / keyboard (terminal) | turns input into render events |
| logger | the only writer to the log file |
| store | conversations and subscriptions; echo mode bounces messages |
| sender | `SenderWorker`: every outbound write decision |
| listener | `InboundTransport::run()`: blocking `accept()` loop |
| metrics (with `--metrics`) | one JSONL snapshot per second |

The per-mode sections below list only the threads a mode adds on top of these.
Section 3 explains when and by whom every thread is created and joined.

### The peer link: simplex or duplex

A peer link has an **rx slot** (the stream we read the peer from) and a **tx
slot** (the stream we write to it). Both are stored in `PeerTable`
([ThreadWorkers.hpp](../ThreadWorkers.hpp)) as a `ChannelRef{connection_id, handle}`.

- **Simplex** (threads, pool-sender, poll, epoll): the two slots name **two
  different TCP connections**.
- **Duplex** (`*-duplex`, `*-split`): both slots name **the same connection**.

The wire format is identical in both shapes. Only where the reply travels
differs. Here A dials B:

```
Simplex (two connections)                         Duplex (one connection)

A ──conn 1──> B   Connect{X, assigned, A's endpoints}  A ──conn──> B   Connect{X, assigned, ...}
                  B: rx := conn 1, mints Y                            B: rx := tx := conn, mints Y
A <──conn 2── B   B's sender dials A's endpoint,       A <──conn─── B  B's sender sends the
                  sends ConnectAck{Y, X} on conn 2                     ConnectAck{Y, X} on conn
                  A: rx := conn 2                                      A: rx := tx := conn
A ──conn 1──> B   chat A -> B                          A <──conn──> B  chat both ways
A <──conn 2── B   chat B -> A
```

Rules that hold in both shapes:

- **The newest channel wins a slot.** A reconnect replaces the old ref. A
  release only counts if its `connection_id` is still current, so a superseded
  channel's close stays silent.
- **Exactly one `EventPeerDisconnected` per peer.** Simplex reports it when the
  rx slot is released. Duplex releases both slots in one go.
- **A channel closing during its handshake binds nothing.** In duplex the
  sender's `close()` can run while the I/O thread is still processing the
  handshake frame. The channel guards its bind and `on_closed()` with a small
  lock and a `closing_` flag, so a late Connect or Ack attaches no slot and
  announces nothing. Chat frames don't take the lock.
- **Runs must be homogeneous.** A simplex node and a duplex node cannot talk:
  each would wait for the reply on the wrong socket. Every node in a run, and
  stressgen (via `--duplex`), must use the same shape.

### Sender policy common to all modes

- A failed dial or send is retried 3 times, 100 ms apart. Blocking sends are
  capped by `SO_SNDTIMEO` at 500 ms. After the last attempt the peer is
  reported unreachable.
- **Backpressure:** in the pool-based modes `send()` returns `WouldBlock` once a
  connection's tx buffer (1 MiB) is full. `backpressure_is_drop()` is true for
  these transports, so the sender **drops the message and counts it**, with no
  retry and no sleep.
- A peer with no tx channel and no dialable address (port 0) is reported
  unreachable immediately, without the retry sleeps. Duplex stressgen peers
  advertise port 0.
- **While quitting** (`SenderWorker::begin_shutdown()`, called by `MainWorker`
  before it shuts the sender queue down), nothing is dialled and nothing is
  retried:
  - A queued message gets one attempt on a live write connection.
  - A message for a peer that would need a (re)dial is dropped and counted.

  Without this, a backlog of echoes for peers that had already left kept the node
  busy re-dialling them, at about 0.2 s per message, long after `/quit`.

## 2. The modes

| `--io=` | Connections per peer | Read path | Write path | Extra threads | Readiness API |
|---|---|---|---|---|---|
| `threads` | 2 | 1 blocking reader thread per inbound connection | blocking `send_all` on the sender thread | watchdog + 1 per peer | none (blocking) |
| `pool-sender` | 2 | same as `threads` | `TcpConnectionPool`, non-blocking | watchdog + 1 per peer + pump | `poll()` (writes only) |
| `poll` | 2 | one pool, one I/O thread | same pool | I/O thread | `poll()` |
| `epoll` | 2 | one pool, one I/O thread | same pool | I/O thread | `epoll` |
| `poll-duplex` | 1 | one pool, one I/O thread | same pool, same socket | I/O thread | `poll()` |
| `epoll-duplex` | 1 | one pool, one I/O thread | same pool, same socket | I/O thread | `epoll` |
| `poll-split` | 1 | rx pool + I/O thread | `TxPoller` + tx thread, same socket | I/O + tx thread | `poll()` ×2 |
| `epoll-split` | 1 | rx pool + I/O thread | `TxPoller` + tx thread, same socket | I/O + tx thread | `epoll` ×2 |

### `threads`: one thread per connection (the original design)

- **Inbound:** `ThreadedInbound` wraps `ListenerWorker`, which accepts in a
  blocking loop and spawns a `ReaderWorker` thread for each connection.
  - Each reader blocks in `receive()` with a 200 ms timeout, feeds its
    `FrameChannel`, and bumps a heartbeat counter.
- **Watchdog** (`WatchdogWorker`): scans every 100 ms and joins finished readers.
  - A reader whose heartbeat hasn't moved for 10 scans (1 s) is treated as
    wedged: the watchdog shuts its socket down to unblock it.
  - It enforces the reader cap, `--max-peers` (default 8). Once the cap is
    exceeded, the least recently active readers are shut down first.
- **Outbound:** `BlockingOutbound`, with one blocking `TcpConnection` per
  handle, used only from the sender thread. A peer that stops reading stalls the
  sender for up to 500 ms per attempt.
- **Cost:** threads grow linearly with peers, and each peer uses two fds.

### `pool-sender`: threaded reads, pooled writes

- **Inbound:** identical to `threads`.
- **Outbound:** `PoolOutbound<TcpConnectionPool>` ([PoolOutbound.hpp](../PoolOutbound.hpp)).
  - The dial is still a blocking `connect`. The connected socket is then handed
    to the pool, and every send is a non-blocking `Pool::send`. What doesn't fit
    goes into that connection's tx buffer.
  - A **pump thread** drives `Pool::receive()`, which is what flushes the tx
    buffers and notices dead sockets. Bytes that arrive on an outbound socket
    are discarded.
  - A `{id, 0}` result (the pool dropped the connection) marks the handle dead,
    so the next send fails with `ConnectionNotFound` and the sender re-dials.
- **Measures:** the effect of making writes non-blocking while keeping
  thread-per-peer reads. A slow peer now costs a full buffer and dropped
  messages instead of stalling the sender.

### `poll` / `epoll`: one shared pool, one I/O thread, two connections

Both are `MultiplexedIo<Pool>` ([MultiplexedIo.hpp](../MultiplexedIo.hpp)) in the
`Dual` model. `Pool` is `TcpConnectionPool` for `poll` and `EpollConnectionPool`
for `epoll`.

- **One pool holds both directions.** The listener thread `accept()`s and adds
  each inbound socket to the pool. The sender's `open()` dials and adds each
  outbound socket to the same pool. Capacity is `2 × max_peers`.
- **One I/O thread** loops on `pool.receive()`. One call does two jobs:
  - returns bytes from any readable connection;
  - flushes queued writes on any writable one (POLLOUT/EPOLLOUT is armed only
    while a tx buffer has data).
- **Registry:** `registry_` maps pool id → `FrameChannel`. Accepted ids have a
  channel; dialed ids have none, and bytes arriving on them are ignored.
  - Whoever adds a connection holds `registry_mutex_` across both the pool insert
    and the registry insert, so the I/O thread can never receive bytes for an id
    it can't find yet.
- **Writes from the sender thread:** `send()` is `pool.send()`. It tries an
  immediate non-blocking `send()` and queues the remainder for the I/O thread.
- **poll vs epoll:**
  - `TcpConnectionPool` keeps a desired-state table. It rebuilds its `pollfd`
    array before each `poll()`, and producers wake it with an eventfd.
  - `EpollConnectionPool` calls `epoll_ctl` directly, even while `epoll_wait`
    is blocked.
  - Otherwise they behave identically, including the guarantee that every
    pool-initiated drop is reported once as `{id, 0}`.

### `poll-duplex` / `epoll-duplex`: one pool, one thread, one connection

The same `MultiplexedIo<Pool>`, but in the `Duplex` model.

- **Every connection gets a `FrameChannel`**, whether we accepted it or dialed
  it. Its `ChannelRef` uses the pool id as the handle, so the channel's own
  socket can be written to.
- **Acceptor side:** the Connect fills rx *and* tx with that one channel.
  `OutboundTransport::duplex()` is true, so the sender sends the ConnectAck on
  that same handle instead of dialing back.
  - If a reconnect replaced an older socket in the tx slot, its handle arrives
    as `EventPeerConnected.superseded` and the sender closes it.
- **Dialer side:** the dialed socket's channel is registered *before* the hello
  goes out, because the Ack comes back on the same socket within one RTT. The
  Ack re-attaches tx under the channel's own id, so one close releases both
  slots.
- **Closing:**
  - A pool-detected close (EOF, error, prune) goes through the usual
    `{id, 0}` → `on_closed()` path.
  - The sender's `close()` (a drop or reconnect) also calls `on_closed()`,
    because the pool doesn't report removals it was asked to make.
- **Capacity:** `max_peers + 16`. Per peer: half the sockets and fds of `poll`/`epoll`.

### `poll-split` / `epoll-split`: one connection, separate read and write pollers

`MultiplexedIo<Pool, TxPoller>` in the `Duplex` model. The link logic is the
same as the duplex modes. The difference is how the single socket is driven.

- **The connection is a `shared_ptr<TcpConnection>`** held by three owners:
  - the rx pool, which only reads it;
  - the `TxPoller` ([TcpSocket/TxPoller.hpp](../TcpSocket/TxPoller.hpp)), which
    only writes it;
  - the registry.
- **Two threads:** the I/O thread drives the rx pool. A separate **tx thread**
  runs `TxPoller::run()`, which waits for writability only on connections with
  queued bytes.
- **Writes** go through `TxPoller::send`, with the same fast path, queue, cap
  and `WouldBlock` behaviour as the pool.
- **Making sharing safe:**
  - Socket options are applied eagerly (`apply_options_now()`) before the
    connection is shared, so neither thread mutates them later.
  - Exactly one thread reads and one writes.
  - `O_NONBLOCK` is shared by both, since it belongs to the open file description.
  - Each poller removes the fd from its poll/epoll set before dropping its
    reference. The fd closes only when the last reference goes.
- **Failures:** a fatal write calls `shutdown(SHUT_RDWR)`. The rx pool then sees
  EOF and reports the disconnect, so disconnects always come from the read side.
  A drop reported by the rx pool also removes the connection from the `TxPoller`.
- **Measures:** whether giving writes their own thread helps compared with one
  thread doing both (`*-duplex`), at the cost of one extra thread.

## 3. Threads: how each mode builds them

Threads are created at three points. Two happen during startup in
`MainWorker::operator()` ([ThreadWorkers.cpp](../ThreadWorkers.cpp)). The third
happens at runtime, and only `threads` and `pool-sender` do it.

### The three creation points

1. **The transport's own threads, inside `make_io_stack()`.** The transport
   object is constructed before `MainWorker` starts any thread of its own.
   Background threads that belong to the transport start right there, in its
   constructor:
   - `PoolOutbound`: the **pump** thread ([PoolOutbound.hpp](../PoolOutbound.hpp)).
   - `MultiplexedIo`: the **I/O** thread, plus the **tx** thread in the split
     modes ([MultiplexedIo.cpp](../MultiplexedIo.cpp)).

   These threads already idle on their pools while the rest of the app is still
   being wired up.

2. **The fixed set, started by `MainWorker`**, in this order:
   - logger, store, sender;
   - keyboard (terminal mode only);
   - **listener**, whose body is `io.inbound->run()`;
   - metrics (only with `--metrics`).

   After that the **main thread** itself runs the render loop. In headless mode
   the render loop first starts a **stdin reader** thread
   ([Render.cpp](../Render.cpp)), which turns input lines into render events.

3. **Runtime threads, only in `threads` and `pool-sender`.** The listener
   thread's `ListenerWorker` binds the socket, then spawns:
   - one **watchdog** thread;
   - one **reader** thread for every connection it accepts. Each reader thread
     goes straight to the watchdog through `adopt()`, so the watchdog owns it,
     joins it when it finishes, and shuts down the least recently active readers
     when there are more than `--max-peers`.

### Per mode

| Mode | What runs on the listener thread | Transport threads (step 1) | Runtime threads (step 3) | Total, headless with `--metrics` |
|---|---|---|---|---|
| `threads` | `ListenerWorker`: blocking `accept()` loop | none | watchdog + 1 reader per inbound peer (at most `max_peers`) | 7 + 1 + N |
| `pool-sender` | same as `threads` | pump: drives `TcpConnectionPool::receive()` for outbound sockets | same as `threads` | 7 + 2 + N |
| `poll` / `epoll` | `MultiplexedIo::run()`: `accept()`, then hand the socket to the pool | I/O: `pool.receive()` loop, which reads every socket and flushes writes | none | 8 |
| `*-duplex` | same as `poll` / `epoll` | I/O: same, but every socket has a `FrameChannel` | none | 8 |
| `*-split` | same; each accepted socket goes into the rx pool and the `TxPoller` | I/O (reads) and tx (`TxPoller::run()`, writes only) | none | 9 |

The 7 base threads are main, stdin reader, logger, store, sender, listener and
metrics. These totals match what the quick benchmark run measured: 8 for
`epoll` and the duplex modes, 9 for split.

Who touches the sockets in each mode:

- **The sender thread** writes directly in every mode:
  - `threads`: a blocking `send`.
  - The pool-based modes: `pool.send()` or `TxPoller::send()`, which try an
    immediate non-blocking write and queue the rest.
- **Queued bytes are flushed** by a transport thread:
  - `pool-sender`: the pump thread.
  - poll/epoll/duplex: the I/O thread.
  - split: the tx thread.
- **Reading:** in the multiplexed modes, the listener thread never reads. It
  only accepts. All reading happens on the I/O thread.

### Shutdown: the same steps in reverse

1. The render loop returns, on `/quit`, EOF or Ctrl+C.
2. `io.inbound->stop()` wakes the blocking `accept()` by connecting to its own
   listen port:
   - **threads / pool-sender:** `ListenerWorker::stop()` also stops the
     watchdog. The listener loop then exits and joins the watchdog, which shuts
     down and joins every remaining reader.
   - **multiplexed modes:** `stop()` also closes the pool, so the I/O thread's
     `receive()` returns and its loop exits. On the way out it reports every
     peer still attached as disconnected.
3. The store, sender and render queues are shut down gracefully, then the
   keyboard, store, sender and listener threads are joined, then the metrics
   thread.
4. The logger is shut down and joined last, so no log record is lost.
5. When `MainWorker::operator()` returns, its local variables are destroyed in
   reverse order of creation. The `SenderWorker` goes before the `IoStack`, so
   the transport's destructor joins the I/O, tx or pump threads only after the
   sender has stopped using them.

## 4. PeerTable and FrameChannel, the simple version

Two objects keep track of peers. One knows **who** a peer is. The other reads
**one TCP connection**.

```
                 PeerTable (one per node)
   ┌──────────────────────────────────────────────────┐
   │ peer 7:  address 10.0.0.5:9000   Connected       │
   │          calls us #42                            │
   │          read from:  connection c1  ─────────┐   │
   │          write to:   connection c2  ──────┐  │   │
   └───────────────────────────────────────────│──│───┘
                                               │  │
                        FrameChannel c1 ◀──────│──┘   (one per TCP connection:
                        sender's socket c2 ◀───┘       decodes frames, knows it
                                                       belongs to peer 7)
```

### PeerTable: the address book

For every peer it remembers four things:

1. **Where the peer lives**: the address to dial.
2. **Whether it is up**: `Connected` or `Disconnected`.
3. **What the peer calls us**: its name for us. We send it back when we
   reconnect, so the peer knows it's us again.
4. **Which connections carry it**:
   - the **read slot** is where the peer's messages come from;
   - the **write slot** is where the sender writes to it.

A few rules make it work:

- **Peers are never forgotten.** When a peer goes away only its status changes,
  so its address is still there for a reconnect.
- **The newest connection wins a slot.** A reconnect simply replaces the old one.
- **Only the current connection can report the peer gone.** An old connection
  that was already replaced closes silently. That's why each connection has its
  own id (`connection_id`): the table checks "are you still the one in the slot?"

### FrameChannel: the reader of one connection

Every TCP connection that we read from has one `FrameChannel`. It doesn't own
the socket. Whoever owns the socket hands it bytes and tells it when the
connection ends.

What it does with each kind of frame:

| Frame arrives | Meaning | What the channel does |
|---|---|---|
| `Connect` | someone dialed us | names the peer (or recognises a returning one), puts itself in the peer's **read slot**, tells the store "peer connected" |
| `ConnectAck` | the answer to our dial | learns which peer it belongs to, puts itself in the peer's **read slot** |
| chat message | a message | tells the store "message from peer N" |
| (connection ends) | | asks the table to free its slots; if it was the current connection, tells the store "peer disconnected" |

### One connection or two

The only difference between the modes is **what goes in the write slot**:

- **Two connections** (`threads`, `pool-sender`, `poll`, `epoll`): the read slot
  and write slot are different connections. When someone dials us, our sender
  dials them back to get a connection to write on.
- **One connection** (`*-duplex`, `*-split`): the same connection is in both
  slots. Nobody dials back. The answer and all later messages go back on the
  connection that was dialed.

### A full example, one connection (A dials B)

1. A's sender writes B's address into A's table, dials B and sends `Connect`.
   That connection goes into A's write slot for B.
2. B's channel for that connection reads the `Connect`. It names A (say, peer 7)
   and puts itself in **both** of peer 7's slots.
3. B's sender sees that peer 7 already has a write slot, so it sends the
   `ConnectAck` on that same connection.
4. A's channel reads the Ack and puts itself in **both** of B's slots.
5. Now both sides read and write on one connection.

With two connections, step 3 instead dials a second connection back to A, and
on A's side the Ack arrives on that new connection.

### When things end

- **The peer hangs up:** the channel sees the end, the table frees its slots,
  and the store hears "peer disconnected" once. The sender then closes its
  write side.
- **The peer reconnects:** the new connection takes over the slots. The old
  connection's close is ignored. With one connection per peer, the sender also
  closes the old socket.
- **We close it ourselves while the handshake is still being read:** the channel
  marks itself as closing first. A handshake frame that arrives afterwards is
  ignored, so no slot is left pointing at a dead connection.

## 5. Ids used in the app

Every id is **local to the process that created it**. No id is global across
nodes, which is why a handshake has to exchange two of them.

### Summary

| Id | Type | Created by | Unique within | Lives until | Purpose |
|---|---|---|---|---|---|
| **peer id** | `uint64_t` | `next_local_peer_id()` (in `FrameChannel` on `Connect`, or the store on `/connect`) | this node | forever (the table keeps it) | our name for a remote peer |
| **assigned id** | `uint64_t` | the *remote* node | the remote node | forever (in `PeerTable::Entry`) | the remote peer's name for us |
| **connection id** (`ChannelRef::connection_id`) | `uint64_t` | `next_local_peer_id()` | this node | one TCP stream | says which stream currently holds a slot |
| **conversation id** | `uint64_t` | `StoreWorker` (`next_conversation_id_`) | this node | forever | one chat history; what the UI shows and `/switch` uses |
| **handle** (`OutboundTransport::Handle`, `ChannelRef::handle`) | `int` | the transport | one transport object | one TCP stream | what the sender passes to `send()` / `close()` |
| **pool id** | `int` | a pool's `add_connection()` (`next_id_`) | one pool | one TCP stream | key inside `TcpConnectionPool` / `EpollConnectionPool`; equals the handle in pool-based modes |
| **fd** | `int` | the kernel | the process, **reused after close** | until close | what is passed to `poll`/`epoll`/`send` |
| headless **seq** / **re** | number / string | `HeadlessRenderWorker` / the driver | one process run | one line or command | ordering and request matching in the JSONL protocol |
| stressgen **token** | `uint64_t` | stressgen | one stressgen run | one simulated peer | matches Acks and echoes to the right simulated peer |

### Identity: peer id and assigned id

**Peer id** is the name this node uses for a remote peer. The same machine has a
different peer id on each side of the link.

- **Who creates it:**
  - `FrameChannel` when someone dials us and sends a `Connect`
    ([FrameChannel.cpp](../FrameChannel.cpp)).
  - `StoreWorker::on_dial_peer` when the user runs `/connect`.
- **What uses it:**
  - It is the key in `PeerTable`.
  - It appears on every event (`EventMessage`, `EventPeerConnected` and so on).
  - It is how the store finds the conversation.

**Assigned id** is the name the *other* node uses for us. We learn it during the
handshake and store it in the peer's table entry. On a reconnect we send it back
as `Connect.assigned_peer_id`, which is what lets the other side recognise us and
reuse the old peer id instead of creating a new one.

Example: A dials B.

```
A's table:  peer 5  = B,  B calls us 9      (5 was created by A's /connect)
B's table:  peer 9  = A,  A calls us 5      (9 was created by B on the Connect)

Connect    A -> B : { peer_id = 5 (A's name for B), assigned_peer_id = 0 (first contact) }
ConnectAck B -> A : { peer_id = 9 (B's name for A), ack_peer_id = 5 (echoes A's name for B) }
Reconnect  A -> B : { peer_id = 5, assigned_peer_id = 9 }  -> B recognises 9 and keeps it
```

### Streams: connection id and ChannelRef

A **connection id** names one TCP byte stream. It answers "is this stream still
the current one?" Each `PeerTable` slot holds a `ChannelRef{connection_id, handle}`
([ChannelRef.hpp](../ChannelRef.hpp)), and `release(peer, connection_id)` only
acts if that id is still in the slot. That check is why an old, replaced stream
closes silently.

**Who creates them:**

- A `FrameChannel`, for its own stream.
- `MultiplexedIo`, for duplex channels.
- The sender, for each socket it opens.

**Peer ids and connection ids come from the same counter**,
`next_local_peer_id()` ([ThreadWorkers.cpp](../ThreadWorkers.cpp)): an atomic
that starts at 1. That has two consequences:

- Every peer id and every connection id on a node is distinct, so the two can
  never be confused, and 0 can safely mean "none".
- Both sequences have gaps. Peer ids are not 1, 2, 3; they are spread out among
  the connection ids.

### Conversation id

A **conversation id** is the store's name for one chat history
(`ConversationStore`). It is what the UI lists, what `/switch` takes, and what
subscriptions are keyed by.

- **Where it comes from:** `StoreWorker` allocates it from its own counter,
  `next_conversation_id_`, which starts at 1.
- **How it links to peers:**
  - Each conversation lists its peers, and the store keeps
    `peer_to_conversation_`, mapping peer id → conversation id.
  - That mapping survives a disconnect, so a reconnecting peer resumes the same
    conversation.
- **It is never sent over the network.** The wire `ChatMessage` has
  `conversation_id` and `sender_id` fields, but the sender fills them with 0, and
  the receiver routes by the peer id its channel learned during the handshake.
- **Event flow:** inbound `EventMessage`s arrive at the store with conversation 0
  plus a peer id, and the store fills in the conversation.

Messages inside a conversation have a `sender_id`: the author's peer id, or our
own id for messages we wrote.

### Transport: handle, pool id, fd

These three ids all name a socket, at increasing distance from the app logic.

**Handle** is what the sender holds and passes to `send()` / `close()`. Its
meaning depends on the transport:

| Transport | Handle is |
|---|---|
| `BlockingOutbound` (`threads`) | its own counter, `next_handle_`, starting at 1 ([BlockingOutbound.cpp](../BlockingOutbound.cpp)) |
| `PoolOutbound` (`pool-sender`) | the pool id |
| `MultiplexedIo` (poll/epoll and their derivatives) | the pool id |

In a `ChannelRef`, `handle == 0` means "this stream can't be written through a
transport". That is always the case for inbound channels in the two-connection
modes.

**Pool id** is the key a pool assigns in `add_connection()`, from `next_id_`,
starting at 1 ([TcpSocket.cpp](../TcpSocket/TcpSocket.cpp)). It comes back on
every `receive()` as `ReceiveResult::connection_id`, so the I/O thread can find
the stream's `FrameChannel` in `registry_`.

- **Two separate counters:** in `pool-sender` the reader threads have no pool,
  and the pump's pool numbers only outbound sockets. Each pool's ids are
  therefore only meaningful inside that pool.
- **Split modes:** the `TxPoller` reuses the rx pool's id (`tx_.add(id, conn)`),
  so a single number names the socket in both pollers.
- `epoll` keeps 0 for its wake-up eventfd (`kWakeToken`), which is another
  reason ids start at 1.
- The pool's `connection_id` is an `int` scoped to one pool, while
  `ChannelRef::connection_id` is a node-wide `uint64_t`. They are different
  things despite the same name.

**fd** is the kernel's descriptor, and **it is reused as soon as it is closed**.
That is why nothing above the pools uses fds as keys:

- The pools turn fds into ids that only increase and are never reused.
- `TcpConnection` is the only thing that holds the fd.
- `PollSet` maps its kernel array back to pool ids (`ids_`).
- In the split modes, shared ownership of the connection guarantees the fd stays
  open until both pollers have removed it.

### Protocol and test ids

- **Headless `seq`:** every JSONL output line carries a `seq` that increases by
  one per line, plus `t_ns`, so tests can check ordering.
- **Headless `re`:** a command written as `#<req-id> /cmd …` gets that req-id
  echoed back as `"re"` on its reply, even when the reply arrives later.
  `tools/node.py` uses this to match answers to requests.
- **stressgen token:** each simulated peer sends its token as `Connect.peer_id`.
  The node echoes it back as `ack_peer_id`, which is how stressgen knows which
  simulated peer an Ack belongs to (`by_token_`).

### How they link at runtime

```
UI/headless ──conversation id──▶ Store ──peer id──▶ Sender ──PeerTable.tx_of(peer)──▶ ChannelRef{conn id, handle}
                                   ▲                                                    │ handle
                                   │ peer id (learned in handshake)                      ▼
                          FrameChannel ◀──pool id (registry_)── I/O thread ◀── Pool ── fd (kernel)
```

The id that crosses the network is the peer id: `Connect` and `ConnectAck`
carry each node's name for the other. Each side's peer id for the other ends up
as the other side's assigned id. No other id leaves the process.

## 6. Measurements

There are three layers: the node measures itself, stressgen measures the node
from outside, and the matrix scripts turn both into comparable numbers.

### Inside the node: `Metrics`

Defined in [Metrics.hpp](../Metrics.hpp) and [Metrics.cpp](../Metrics.cpp).

#### Counters

The counters are relaxed atomics, each bumped with one `fetch_add`, so they take
no lock and don't allocate. Each component receives a `Metrics*` that may be
null, and a null pointer disables counting.

| Field | Kind | Counted by | Meaning |
|---|---|---|---|
| `accepts` | counter | listener / `MultiplexedIo::run()` | inbound connections accepted |
| `connections_live` | gauge | transports | connections currently open with a reader or channel |
| `bytes_in`, `frames_in` | counters | `FrameChannel` | bytes received, and complete frames decoded |
| `frames_out` | counter | `SenderWorker` | frames written successfully |
| `send_failures` | counter | `SenderWorker` | failed dials or sends; each retry counts |
| `send_backpressure_drops` | counter | `SenderWorker` | messages dropped because a tx buffer was full |
| `reconnects` | counter | `SenderWorker` | successful reconnects |
| `evictions` | counter | watchdog / pools | connections dropped for the peer cap, idleness or a stall |
| `echoed` | counter | `StoreWorker` | messages bounced back in `--echo` mode |

#### Measured when a snapshot is taken

| Field | Source | Meaning |
|---|---|---|
| `t_ns` | `steady_clock` | sample time (monotonic, not wall clock) |
| `utime_us`, `stime_us` | `getrusage` | user and kernel CPU used by the process so far |
| `nvcsw`, `nivcsw` | `getrusage` | voluntary (blocked) and involuntary (preempted) context switches so far |
| `maxrss_kb` | `getrusage` | peak resident memory so far |
| `threads` | `/proc/self/status` | current thread count |
| `fds` | `/proc/self/fd` | current open descriptors |
| `render_q_hwm`, `store_q_hwm`, `sender_q_hwm` | `ThreadQueue::high_water()` | the largest each queue has ever been, which shows where work backed up |

#### Where it is emitted

- **`--metrics=PATH`:** `MetricsWorker` writes one JSON line when it starts, one
  every second, and a final one at shutdown.
- **Headless `/stats`:** returns the same snapshot on demand, inside a
  `{"event":"stats","metrics":{…}}` line.

### Outside the node: stressgen

Defined in [stressgen.hpp](../stressgen.hpp).

#### How it works

stressgen opens `--peers` active peers. Each one sends messages at `--rate` per
second, or as fast as `--window` in-flight messages allow.

- **The message body:** `"<peer idx>:<seq>:<send time ns>:"`, padded to `--size`.
- **RTT:** the node echoes each message back, and stressgen computes the
  round-trip time as *receive time − embedded send time*.
- **What it counts:**
  - Only messages sent inside the measurement window count. The window is
    `--duration` seconds, starting after `--warmup`.
  - A `--grace` period after the window lets late echoes still be counted.
  - Idle peers send nothing.
  - Slow peers send but stop reading, and they are excluded from the main stats.
- **Histogram:** RTTs go into a log-linear histogram with 32 sub-buckets per
  power of two. Quantiles are reported as the bucket's upper edge, so they are
  accurate to about 3%. `max` and `mean` are exact.

#### What `--out` writes (one JSON object per run)

| Field | Meaning |
|---|---|
| `sent`, `received`, `lost`, `msgs_per_s` | window messages sent, echoes received (including the grace period), `sent − received`, and `received / duration` |
| `rtt_us{p50,p90,p99,p999,max,mean,samples}` | RTT distribution over the window, in µs |
| `total_sent`, `total_received` | everything, warmup included |
| `slow_sent` | messages sent by slow peers |
| `send_skipped` | messages stressgen itself skipped because its own output buffer was full; this is a load generator limit, not the node's |
| `peers_total`, `peers_ready` | peers configured, and peers that completed the handshake |
| `connect_failures`, `disconnects`, `churn_events`, `bad_frames` | connection health and protocol errors |
| `config{…}`, `start_unix_ns`, `end_unix_ns` | the parameters used, and wall-clock times for lining up with the node's metrics |

The one-line summary on stdout gives the same core numbers.

### The matrix: `run_matrix.py` and `analyze.py`

#### What one cell produces

Each cell is one scenario × parameter value × variant × repetition, and writes
five files under `results/<scenario>/<param>=<value>/<variant>/`:

- `rep<N>.stress.json`: stressgen's result.
- `rep<N>.metrics.jsonl`: the node's 1-second snapshots (the matrix starts the
  node with `--metrics`).
- `rep<N>.node.log` and `rep<N>.stressgen.txt`: logs and stdout.
- `rep<N>.meta.json`: parameters, timings, and a status of `ok`, `dirty` (the
  node didn't say `bye` and exit within `--quit-timeout`, default 30 s) or
  `failed`. It also records:
  - `shutdown_s`: time from `/quit` to exit (or to the kill), plus `bye` and
    `killed`;
  - `build`: git commit, branch, uncommitted files, a hash of the harness, and
    the sha256, size and mtime of `app` and `stressgen`, hashed for every cell.

  Cells written before these fields existed don't have them.

The node runs pinned to CPUs 0–1 and stressgen to 4–7, so they don't compete for
cores.

#### What analyze.py derives for each cell

| Metric | From | How |
|---|---|---|
| Throughput, RTT p50/p99/p99.9, lost | stress.json | taken directly |
| CPU % of one core | metrics.jsonl | Δ(utime + stime) / Δwall, over the measurement window |
| Voluntary and involuntary ctx switches per second | metrics.jsonl | Δnvcsw/Δt and Δnivcsw/Δt over the same window |
| Peak RSS, peak threads, peak fds | metrics.jsonl | maximum over all samples |
| Backpressure drops, evictions | metrics.jsonl | value in the last sample |

The **measurement window** for the node-side rates runs from `warmup` seconds
after `frames_in` first starts growing to the last sample where it grew. Startup
and shutdown are therefore left out without needing the two processes' clocks to
match.

Repetitions are reduced to the **median and IQR (Q1–Q3)**. The results go to
`summary.csv` (columns `<metric>_median`, `<metric>_q1`, `<metric>_q3`) and
`report.html`, which has charts and a "Problems" list for failed, dirty or
corrupt cells.

### Limitations

- **Measurement limits:**
  - **CPU and context switches cover the node process only.** stressgen's cost
    isn't included.
  - **RTT includes stressgen's own processing** and uses loopback on the same
    host. Treat it as a way to compare variants, not as network latency.
  - **Threads and fds are sampled once a second,** so short spikes can be missed.
  - **`maxrss_kb` is a high-water mark since process start**, not usage during
    the window.
- **Not collected:**
  - **No outbound byte counter:** only `bytes_in` exists, while `frames_out`
    counts frames.
  - **`connections_live` counts only connections that have a reader or
    channel.** In the two-connection modes the sender's outbound sockets are not
    counted. To compare connection cost across modes, use peak fds instead.

## 7. Terminal UI and headless interface

The terminal UI and the headless interface share everything except the two
ends: where input comes from and where output goes. The split is in
[Render.hpp](../Render.hpp) and [Render.cpp](../Render.cpp).

```
                 TERMINAL                                         HEADLESS
 keyboard thread (KeyboardListenerWorker)          stdin reader thread (read_lines)
   raw keys -> EventLocal                             each line -> EventCommandLine
            │                                                     │
            └──────────────────►  render queue  ◄─────────────────┘
                                  (store events arrive here too)
                                        │  popped on the main thread
            ┌───────────────────────────┴───────────────────────────┐
     TerminalRender::run()                                 HeadlessRenderWorker::Impl::run()
       keys -> line editor -> Enter ─┐                ┌─ line -> strip "#<req-id>" -> /stats?
                                     ▼                ▼
                             ChatController   (shared: commands, current conversation,
                                │   ▲          store events -> what to show)
                    store queue │   │ RenderSink calls
                                ▼   │
                             StoreWorker      TerminalRender : RenderSink   HeadlessRender : RenderSink
                                              (draws on TerminalUI)         (writes JSONL to stdout)
```

### The shared part: `ChatController`

`ChatController` is the front-end logic, with no input or output of its own.

- **`submit(line)`:** an empty line is ignored, a `/…` line is a command, and
  anything else is a message sent to the current conversation
  (`EventComposeMessage`).
- **`command(line)`** parses every command and turns it into store events:
  `/connect` becomes `EventDialPeer`, `/switch` becomes `EventSubscribe`, and so
  on. It returns a `CommandResult{Pending, arg}` that says what answer is still
  coming from the store (Connect, Switch, Conversations, Sync…).
- **`handle_store(event)`** interprets what the store sends back. Snapshots and
  deltas for the conversation being viewed become `sink.messages()`; a
  background conversation becomes `sink.notification()`. Peer events and errors
  map the same way. A stale snapshot for a conversation you've already switched
  away from is dropped.
- It also keeps the session state: the current conversation, the queue of
  outstanding list requests, and whether it's still running.

It never prints anything. Everything it wants shown goes through a
**`RenderSink`** reference, a small interface:

```cpp
info(text) · messages(conv, items, snapshot) · notification(conv) · conversations(list)
peer(what, conv, peer) · error(msg) · command_error(text) · sync() · quit()
```

So the commands, their validation, the conversation state and the meaning of
each store event are written once.

### Front end 1: the terminal (`TerminalRender`, run by `RenderWorker`)

- **Input:** `KeyboardListenerWorker` runs on its own thread. It reads raw bytes
  through `TerminalUI`, decodes key sequences, and pushes `EventLocal` key
  events into the render queue.
- **Loop** (on the main thread): it pops events from the render queue.
  - Key events go to a **line editor**: UTF-8 cursor movement, backspace, and
    history with the up and down arrows (`InputHistory`).
  - Pressing **Enter** calls `controller_.submit(line)`.
  - Everything else goes to `controller_.handle_store()`.
  - After each event it redraws the layout.
- **Output:** its `RenderSink` methods update a `ChatModel` that a `ChatWindow`
  draws, inside an `AbsoluteLayout` together with the `InputBox`. Info and
  errors appear as system messages.
- **Human-only behaviour:** the welcome line, `/help` text shown as chat lines,
  and `quit()` doing nothing because the window simply closes.

### Front end 2: headless (`HeadlessRenderWorker`)

- **Input:** a small **reader thread** polls the input fd every 50 ms, splits it
  into lines, and pushes each one into the **same render queue** as an
  `EventCommandLine`. EOF becomes `{eof: true}`, which ends the session. Because
  commands and store events come through one queue, they are handled in one
  total order, and tests rely on that.
- **Loop:** each line is passed to `handle_line()`:
  1. An optional `#<req-id>` prefix is removed and remembered.
  2. `/stats` is answered right here, from the metrics snapshot provider.
  3. Anything else goes to `controller_.submit()`.
  4. If the result is **pending**, the req-id is queued (`pending_`). When the
     matching sink call arrives later, for example `peer("dialed")` after a
     `/connect`, that reply carries `"re": <req-id>`.
  5. If nothing was emitted and nothing is pending, it emits `{"event":"ok"}`.
- **Output:** its `RenderSink` methods write **one JSON object per line**, each
  with `seq` (one per line, in order) and `t_ns`. A mutex protects the writes,
  so `announce_ready()` can be called from `MainWorker`'s thread. Fixed lines
  bracket the session: `{"event":"ready","listen":…}` once the listener is
  bound, and `{"event":"bye"}` at the end.
- **Machine-only commands:** the controller is built with `machine=true`, which
  enables:
  - `/send <conv> <text>`: no notion of a "current conversation" needed;
  - `/sync`: a barrier, whose reply proves every earlier command has been
    processed by the store;
  - `/echo on|off`.

  The terminal rejects these as unknown commands.

### How `MainWorker` picks between them

`--headless` decides it, in `MainWorker::operator()`
([ThreadWorkers.cpp](../ThreadWorkers.cpp)):

| | Terminal | Headless |
|---|---|---|
| `TerminalUI` | created (raw mode, alternate screen) | **never created**, so no tty is needed |
| input thread | `KeyboardListenerWorker` | headless stdin reader, started by the worker itself |
| render on main thread | `RenderWorker` → `TerminalRender` | `HeadlessRenderWorker` |
| readiness | none | the listener's ready callback calls `announce_ready()` |
| `/stats` | not available | wired to `Metrics::snapshot_json` |

Everything below the render queue is the same in both: the store, the sender,
the I/O mode and the metrics. Log lines go through `LoggerWorker` to the `--log`
file (stderr if it can't be opened), never to stdout, so headless stdout stays
pure JSONL. The e2e test `test_stdout_is_only_jsonl` checks this.

### What this buys

- **One implementation of the behaviour.** A bug fix in `ChatController`
  applies to both. The e2e suite, which drives headless nodes, therefore tests
  the same command and event logic a human uses. Only the line editor and the
  drawing aren't covered by it, and `test_render` covers those.
- **Adding a third front end** (a socket API, for example) means writing one
  more `RenderSink` and a loop that feeds lines or events into the controller.
- **The cost:** a few commands exist only in machine mode, and the request
  matching (`#req-id`, `Pending`) lives in the headless front end rather than in
  the controller. So a new command that answers asynchronously needs a matching
  `Pending` kind for headless clients to correlate its reply.

## 8. The test suite

Build and run everything:

```
cmake -B build -S . && cmake --build build -j -- -k   # -k: see "Known issues"
(cd build && ctest --output-on-failure)               # 16 unit targets + 8 e2e runs
```

### Unit tests (C++, plain assertions, one executable each)

| Target | What it covers | Modes exercised |
|---|---|---|
| `test_pool` | both pools: echo round trip, close reported as `{id,0}`, idle and LRU prunes reported once, `remove_connection()` silent, `shared_ptr` overload | poll, epoll and all derivatives |
| `test_tx_poller` | both `TxPoller` backends: fast path, queued flush, `WouldBlock` at cap, fatal write shuts the shared socket down, `remove()` leaves no dangling ref, rx pool + `TxPoller` on one shared socket | `*-split` |
| `test_pool_outbound` | `PoolOutbound`: hello, peer-close detection, `WouldBlock` → drop, `SenderWorker` over it | `pool-sender` |
| `test_multiplexed` | `MultiplexedIo` for both pools in all three models: inbound events, malformed frame closes, outbound send and failure. Duplex/split: one connection per pair (`accepts` sums to 1), Ack on the same socket, adopted reconnect closes the old channel, one disconnect per close, sender close releases both slots. Two in-process nodes per mode. | poll, epoll, `*-duplex`, `*-split` |
| `test_p2p` | `PeerTable` (rx/tx slots, supersede, simplex vs duplex release); `FrameChannel` closed before or during its bind (2,000-round race); listener/reader/watchdog: loopback, id reuse, cap pruning, stalled reader, superseded reader stays silent | `threads`, plus link logic for all |
| `test_sender` | `SenderWorker`: handshake, connection reuse, dial-back, retries, reconnect, unreachable; duplex fake transport answers on the existing handle and closes `superseded` | all (policy) |
| `test_store`, `test_render`, `test_headless`, `test_main`, `test_logger`, `test_queue`, `test_wire`, `test_workers`, `test_metrics` | store subscriptions and echo, command handling, headless JSONL protocol, whole-stack startup and shutdown, logger, queue, wire codec, keyboard, metrics | mode-independent |
| `test_stressgen` | stressgen against an in-process fake node, in both two-connection and `--duplex` shapes, including idle, slow and churn peers | harness |

### End-to-end tests (`tests/e2e/test_e2e.py`)

Real headless `app` processes driven through `tools/node.py`. CMake registers one
run per mode (`e2e_threads` … `e2e_epoll-split`), passing `E2E_IO=<mode>`.

The scenarios are: ready and clean shutdown; stdout is pure JSONL;
connect events; message round trip; conversation listing; disconnect and
reconnect; busy port; echo mode; `/stats` frame counters; `--metrics` file;
`/sync` barrier ordering.

Run one mode by hand:

```
E2E_IO=epoll-split APP_BIN=build/app python3 -m unittest discover -s tests/e2e -p test_e2e.py -v
```

### Load generator (`stressgen`)

An epoll-based client that speaks the wire protocol and measures echo RTT
against a node started with `--echo`.

- **Default shape:** it listens for the node's return connections.
- **`--duplex`:** it reads Acks and echoes on the socket it dialed, and is
  required for `*-duplex` and `*-split` nodes.

```
sleep 30 | build/app --headless --echo --io=epoll-duplex --port=9100 --max-peers=64 --backlog=4096 &
build/stressgen --target 127.0.0.1:9100 --peers 16 --rate 200 --duration 4 --duplex
```

Keep the node's stdin open (`sleep N |`): EOF on stdin means `/quit`.

Exit codes: 0 ok, 2 unreachable, 3 no peer completed the handshake.

### Benchmark matrix (`scripts/`)

`run_matrix.py` runs scenario × parameter × variant × repetition:

- **Pinning:** the node runs on CPUs 0–1 and stressgen on 4–7.
- **Resumable:** cells already marked `ok` or `dirty` are skipped on re-run.
- **Order:** variant order is shuffled each round.
- **Flags:** it passes `--duplex` to stressgen for `*-duplex`/`*-split` and
  starts every node with `--backlog=4096`.

| Scenario | Varies | Values |
|---|---|---|
| S1 fan-in | active peers | 1, 8, 64, 256, 1024 |
| S2 idle-heavy | idle peers on top of active | 0, 100, 300, 1000 |
| S3 slow consumers | peers that stop reading | 0, 1, 4 |
| S4 churn | peers that disconnect and redial | 0, 10, 50 |
| S5 throughput ceiling | message size (bytes) | 32, 4096, 65536 |

```
scripts/run_matrix.py --app build/app --stressgen build/stressgen --out results
scripts/run_matrix.py --quick --reps 1 --variants epoll,epoll-duplex,epoll-split
scripts/analyze.py results          # -> results/summary.csv, results/report.html
```

A quick run (`--quick --reps 1`) gave this shape:

| Variant | Threads | Max fds, 58 peers |
|---|---|---|
| `epoll` | 8 | 124 |
| `*-duplex` | 8 | ~66 |
| `*-split` | 9 | ~66 |

RTT was within noise across variants at those sizes. Treat these as sanity
checks, not results.

### Known issues

- `test_terminal.cpp` hasn't compiled since commit `0998ebb`
  ("TerminalUI header cleanup"): `Renderable::DrawCommand` no longer exists.
  It isn't registered with ctest, but it makes a plain build fail, hence `-- -k`.
- Under a parallel `ctest -j`, `test_pool_outbound`, `test_stressgen` and
  `e2e_epoll-split` have each failed once and then passed every rerun. These are
  timing-sensitive under load; root cause not yet found.
- Nothing tests `stressgen --duplex` against a two-connection node (expected
  exit 3). The fixture for it hung on teardown.
