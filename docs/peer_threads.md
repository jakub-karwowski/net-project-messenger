# `peer-threads`: one owner thread per peer, one connection

`--io=peer-threads` is the thread-per-connection mode with **one duplex TCP
connection per peer**. It sits between `threads` (two sockets per peer, a blocking
reader thread each, one blocking sender) and the multiplexed duplex modes
(`poll-duplex`/`epoll-duplex`: one socket per peer, one I/O thread for all of
them).

Sources:
- [PeerThreadIo.hpp](../PeerThreadIo.hpp) and [PeerThreadIo.cpp](../PeerThreadIo.cpp):
  the transport;
- [Transport.cpp:59](../Transport.cpp#L59): where `make_io_stack` builds it;
- [test_peer_threads.cpp](../test_peer_threads.cpp): its tests.

Everything else (`SenderWorker`, `PeerTable`, `FrameChannel`, `WatchdogWorker`,
the store) is shared with the other modes and unchanged.

## 1. The shape in one picture

```
             accept loop (listener thread, PeerThreadIo::run)
                      │ attach(conn, accepted=true)
                      ▼
 registry_: handle ─► Link { pool(1 connection), FrameChannel, ReaderActivity }
                      ▲                 ▲                      │
   SenderWorker ──────┘                 │                      │ adopt()
   send(handle) = link.pool.send()      │                      ▼
   close(handle) = link.pool.close_pool()     owner thread "pt-<handle>"   WatchdogWorker
   open(ip, port) = connect + attach    │   loops on link.pool.receive()   (cap, join)
                                        └── reads, flushes queued writes,
                                            sole caller of on_closed()
```

Each peer gets:

| Resource | Count | Why |
|---|---|---|
| Threads | 1 owner thread (`pt-<handle>`) | reads the socket, flushes queued writes, ends the link |
| TCP connections | 1 | duplex: the handshake and both directions share one socket |
| fds | 2 | the socket and the pool's wake eventfd |

Plus, for the whole node: the listener thread (blocking `accept()`), the watchdog
thread, and the single `SenderWorker` thread shared by every mode.

## 2. A link

A link is a **one-connection `TcpConnectionPool`**, the same `poll()` pool the
`poll` modes use, with capacity 1 ([PeerThreadIo.hpp:65-83](../PeerThreadIo.hpp#L65-L83)):

```cpp
struct Link {
    int handle;
    std::shared_ptr<ReaderActivity> activity; // shares the connection with the pool
    TcpConnectionPool pool;                   // exactly one connection: this peer's
    int pool_id{-1};
    FrameChannel channel;
    std::atomic<std::uint64_t> published_queued{0};
    std::atomic<std::uint64_t> published_flushes{0};
};
```

The pool already provides what a duplex link needs, so `PeerThreadIo` reimplements
none of it:

- `poll()` on the socket plus a wake descriptor;
- a thread-safe `send()`: an immediate non-blocking write, the rest queued up to a
  1 MiB cap, and `WouldBlock` once full (nothing partial is queued);
- flushing of queued bytes on `POLLOUT` inside `receive()`;
- exactly one `{id, 0}` report when the connection ends: EOF, an error, or a
  `shutdown()` from another thread.

The `TcpConnection` is held as a `shared_ptr` by both the pool and the link's
`ReaderActivity`. It is shared only because the reused `WatchdogWorker` ends a
reader by calling `ReaderActivity::connection->shutdown()`. Nothing else outside
the pool touches the socket.

The `FrameChannel` is built with a duplex `ChannelRef{connection_id, handle}`
([PeerThreadIo.cpp:31-42](../PeerThreadIo.cpp#L31-L42)):

```cpp
channel(store_queue, peers, std::move(logger), ChannelRef{next_local_peer_id(), handle_}, true)
```

- `connection_id` is a fresh, node-unique generation. It is what makes a
  superseded link silent (§6).
- `handle` is the `OutboundTransport::Handle` the sender writes on. It is also the
  registry key.

## 3. Creating a link: `attach()`

Both directions go through `attach()` ([PeerThreadIo.cpp:85-121](../PeerThreadIo.cpp#L85-L121)):
an accepted connection from `run()`, and a dialled one from `open()`.

```cpp
link = std::make_shared<Link>(..., std::make_shared<TcpConnection>(std::move(connection)),
                              next_handle_.fetch_add(1), ...);
{
    std::lock_guard lock(registry_mutex_);
    registry_.emplace(link->handle, link);          // 1. findable by handle
}
owner = std::thread{[this, link] {                  // 2. owner thread running
    set_thread_name("pt", link->handle);
    owner_loop(link);
}};
watchdog_.adopt(link->activity, std::move(owner));  // 3. counted against --max-peers
```

The order matters:

1. **Registry first.** The link must be findable by handle before anything can
   name that handle.
2. **Owner thread second.** It reads the socket, so the peer's frames are
   processed from the start.
3. **Watchdog last, outside `registry_mutex_`.** `adopt()` may evict and join
   other owner threads, and those threads take `registry_mutex_` on their way out.
   Holding it here would deadlock.

`open()` sends the hello only after `attach()` returns
([PeerThreadIo.cpp:214-226](../PeerThreadIo.cpp#L214-L226)). So the link is
registered and its owner is reading before the peer can answer, and a `ConnectAck`
can never arrive for an unknown link.

## 4. How the sender finds the right link

There is **no per-peer sender thread**. All writes come from the one
`SenderWorker` thread, as in every mode. The owner thread only flushes bytes that
the sender's write could not hand to the kernel immediately. So "finding the
sender for a peer" really means mapping a peer id to its link. That takes two
lookups, each under its own lock.

### 4.1 Peer id → handle: the `PeerTable` tx slot

The sender knows only the peer id from the `EventMessage`.
`SenderWorker::ensure_connection()` ([ThreadWorkers.cpp:632](../ThreadWorkers.cpp#L632))
reads the peer's tx slot:

```cpp
if (const auto tx = peers_.tx_of(peer_id)) {
    return tx->handle; // reuse the connection opened for an earlier message
}
const auto entry = peers_.lookup(peer_id);
if (!entry) { return std::nullopt; }
if (entry->address.port == 0) { return std::nullopt; } // never dial port 0
return open(peer_id, entry->address.ip, entry->address.port, wire::make_connect(...));
```

The link's own `FrameChannel` fills the slot during the handshake. It writes **its
own `ChannelRef` into both the rx and tx slots**, because the stream it reads is
also the one to write on ([FrameChannel.cpp:77-87](../FrameChannel.cpp#L77-L87)
when we were dialled, [FrameChannel.cpp:103](../FrameChannel.cpp#L103) when we
dialled):

```cpp
(void)peers_.attach_rx(peer_id_, ref_);
if (duplex_ && ref_.handle != 0) {
    if (const auto old = peers_.attach_tx(peer_id_, ref_);
        old && old->handle != 0 && old->handle != ref_.handle) {
        superseded = old->handle;   // an older link for this peer: the sender closes it
    }
}
```

This binding runs under the channel's `state_mutex_` and checks `closing()`. A
link that is already closing therefore never binds a slot, and the slot always
names a link that was alive when it was bound.

When **we** dialled, `SenderWorker::open()` writes the tx slot at once, with
`attach_tx(peer_id, ChannelRef{next_local_peer_id(), handle})`. That lets the next
message reuse the link before the `ConnectAck` arrives. The Ack later re-attaches
the slot under the channel's own `connection_id`, with the same handle. From then
on, the one `on_closed()` releases both slots.

### 4.2 Handle → link: the registry

`PeerThreadIo::send(handle, data)` ([PeerThreadIo.cpp:229-238](../PeerThreadIo.cpp#L229-L238))
looks the handle up and writes through that link's pool:

```cpp
const auto link = find(handle);                       // registry_mutex_
if (!link) { return std::unexpected(SocketError::ConnectionNotFound); }
auto sent = link->pool.send(link->pool_id, data);     // the pool's own mutex
publish_tx_stats(*link);
return sent;
```

- `find()` returns a `shared_ptr` copy. The link therefore stays alive for the
  whole `send()`, even if its owner exits and erases it from the registry
  meanwhile.
- Handles come from one atomic counter and are **never reused**. A stale handle
  (an old tx slot, or a superseded link) can only miss in the registry
  (`ConnectionNotFound`). It can never reach another peer's link.
- `pool.send()` serializes the sender's immediate write against the owner's flush
  under the pool's mutex, so frames never interleave on the socket.

### 4.3 What each send outcome means to the sender

| `send()` result | Cause | What `SenderWorker` does ([ThreadWorkers.cpp:852-883](../ThreadWorkers.cpp#L852-L883)) |
|---|---|---|
| ok | written, or queued under the cap | marks the peer Connected, counts `frames_out` |
| `WouldBlock` | 1 MiB tx cap full (slow reader) | drops the message as backpressure, with no retry or sleep (`backpressure_is_drop()` is true) |
| `ConnectionNotFound` | the handle is no longer in the registry: the link has ended | `drop_connection()` (clears the tx slot), sleeps 100 ms, retries; the next `ensure_connection()` re-dials or gives up |
| `ConnectionClosed` | `close_pool()` already ran: the owner is exiting | same as above |
| a socket error | the write failed: the peer reset, or the watchdog shut the socket down | same as above. The pool also drops the connection, so the owner's `receive()` returns `{id, 0}` |

## 5. The owner thread and how a link ends

`owner_loop()` ([PeerThreadIo.cpp:125-170](../PeerThreadIo.cpp#L125-L170)) is
the reader, the flusher and the **one and only caller of `on_closed()`** for its
link:

```cpp
for (;;) {
    auto received = link.pool.receive(std::span<std::byte>{scratch}); // poll(); flushes on POLLOUT
    publish_tx_stats(link);
    if (!received) { if (received.error() == SocketError::Interrupted) continue; break; }
    activity.tick.fetch_add(1);
    if (received->bytes_read == 0) { break; }                       // {id, 0}: the link ended
    activity.last_read_ns.store(steady_now_ns());                   // ranking key for the cap
    if (!link.channel.on_bytes(...)) { break; }                     // bad frame / consumer gone
}
link.pool.close_pool();                                   // 1. later sends fail, never queue
activity.connection->shutdown(TcpConnection::Shutdown::Both);
publish_tx_stats(link);
{ lock registry_mutex_; erase registry_[handle] if it is still this link; } // 2. unfindable
link.channel.on_closed();                                 // 3. release slots, report once
```

The `FinishGuard` at the top of the function sets `activity.finished` on every
exit path, which tells the watchdog it may join the thread.

Every way a link can end leads into this one exit path:

| Trigger | Who | How the owner wakes up |
|---|---|---|
| The peer closes, or the connection fails | kernel | `receive()` returns `{id, 0}` |
| Malformed frame | the owner itself | `on_bytes()` returns false |
| `close(handle)`: the sender dropped the link, or closed a superseded one | `SenderWorker` | `close_pool()` wakes `receive()` with `ConnectionClosed` |
| Cap eviction, or the shutdown drain | `WatchdogWorker` | `shutdown(Both)` on the socket, then `{id, 0}` |

Because only the owner calls `on_closed()`, the link is reported gone exactly
once. That holds whichever of the four triggers fires first, and even when several
race.

`on_closed()` ([FrameChannel.cpp:122](../FrameChannel.cpp#L122)) calls
`peers_.release(peer_id, connection_id)`
([ThreadWorkers.cpp:260](../ThreadWorkers.cpp#L260)):

- It clears **only** slots holding this link's `connection_id`.
- It returns true only if the rx slot was still this link's. Only then does it
  emit `EventPeerDisconnected`.
- The store forwards that event to the sender, which runs `drop_connection()` on
  whatever tx slot is left.

## 6. Superseded links

A peer can connect again while its old link still exists. For example, it
re-dials and re-presents its assigned id, so it is adopted under the same peer id.
What happens then:

1. The new link's `FrameChannel` binds rx and tx to the **new** `ChannelRef`.
   `attach_tx` returns the old ref, whose handle goes into
   `EventPeerConnected{..., superseded}`.
2. `SenderWorker::establish_ack()` ([ThreadWorkers.cpp:671](../ThreadWorkers.cpp#L671))
   calls `transport_->close(superseded)`, then sends the `ConnectAck` on the new
   handle.
3. The old owner exits. Its `release(peer_id, old_connection_id)` matches nothing,
   because both slots now hold the new id. So it returns false and **stays
   silent**: no spurious "peer disconnected" for a peer that is live on the new
   link.

## 7. The cap: eviction and what "restart" means

### 7.1 Eviction

`--max-peers` is enforced by the shared `WatchdogWorker`, exactly as in `threads`.
`attach()` hands every new owner thread to `watchdog_.adopt()`
([ThreadWorkers.cpp:392-438](../ThreadWorkers.cpp#L392-L438)). If more than
`max_peers` links are still live, adopt shuts down the ones with the **oldest
`last_read_ns`**:

```cpp
std::sort(watch_.begin(), watch_.end(), [](const Entry& a, const Entry& b) {
    return a.last_read_ns < b.last_read_ns;
});
for (Entry& existing : watch_) {
    if (need == 0) { break; }
    if (existing.activity->stopping) { continue; }
    shutdown_reader(existing);   // stopping = true; connection->shutdown(Both)
    bump(metrics_, &Metrics::evictions);
    --need;
}
```

- The new link is never its own victim. `Link`'s constructor stamps
  `last_read_ns = now`, which makes it the most recently active link.
- Inbound and dialled links count alike: both go through `attach()`.
- An evicted link is marked `stopping` at once. While its thread winds down, it
  is not counted again and cannot be picked a second time.

After `shutdown(Both)`, the evicted link ends through the normal path in §5:

- `receive()` returns `{id, 0}`, and the owner closes the pool and leaves the
  registry;
- `on_closed()` releases both slots and emits one `EventPeerDisconnected`, and the
  peer is marked `Disconnected` in the `PeerTable`;
- the watchdog joins the thread on its next scan or adopt, once `finished` is set.

**A send that races the eviction is safe.** Whatever the sender's `send()` meets
fails cleanly:

| How far the eviction has got | What `send()` sees |
|---|---|
| The socket is shut down but the link is still registered | `pool.send()` fails (socket error, or `ConnectionNotFound` once the pool dropped the connection) |
| `close_pool()` has run | `ConnectionClosed` |
| The link has left the registry | `ConnectionNotFound` from `find()` |

None of these can write into another peer's link (§4.2). The sender then clears
the tx slot and takes the retry path in §4.3. If `drop_connection()` clears the
tx slot before the owner's `release()`, the release still matches the rx slot, so
the disconnect is still reported exactly once.

### 7.2 Nothing is "restarted"; a new link is created

An evicted owner thread is **never restarted**. It is joined and gone. The peer
comes back only as a brand-new link, with a new handle, a new `connection_id`, a
new owner thread and a new `adopt()`, by one of two routes:

1. **The peer re-dials us.** `run()` accepts it, `attach()` creates the link, and
   the `Connect` re-presents its assigned id. `FrameChannel` adopts the old peer
   id (`peers_.lookup(returning)` finds the remembered entry), so the store
   resumes the same conversation. The old slots are already released, so nothing
   is superseded.
2. **We have something to send to the evicted peer.** The next `EventMessage`
   finds the peer `Disconnected`, so the sender takes the reconnect path
   ([ThreadWorkers.cpp:829-841](../ThreadWorkers.cpp#L829-L841)):

   ```cpp
   const bool reconnecting = peer->status == PeerTable::Status::Disconnected;
   if (reconnecting && drop_connection(message->peer_id)) { ... "dropped stale connection" }
   ...
   const auto connection = ensure_connection(message->peer_id); // tx slot empty -> open()
   ```

   `ensure_connection()` finds no tx slot and dials the remembered address:
   - `open()` connects and calls `attach()`, which **adopts the new link into the
     watchdog again**. The message goes out on the new link, and the peer is
     marked Connected and counted under `reconnects`.
   - If the peer advertised **port 0** (it accepts no connections, like duplex
     stressgen), there is nothing to dial. The sender stops at once, without
     sleeping through the retries, and reports the peer unreachable.

**Consequence for the cap.** Route 2 adds a live link, and `adopt()` may then
evict the next least-recently-read link to stay within `max_peers`. A node that
keeps writing to more peers than its cap can therefore cycle links. Each re-dial
evicts the stalest other peer, which may in turn be re-dialled later. Simplex
`threads` has the same property. The cap bounds the number of **concurrent**
owner threads; it does not decide which peers deserve service.

**In the benchmarks** duplex stressgen advertises port 0, so route 2 never fires.
Evicted peers are shed and stressgen does not redial them. S7 in
`results-0294f37` confirms this: at cap 256, `peer-threads` shows 1025 accepts
and 768 evictions (= 1024 − 256), with no re-dial churn.

## 8. Stall detection is off

`pool.receive()` blocks until there is data, a close or a wake-up. An idle peer's
owner therefore never advances `tick`. The watchdog's stall scan, which shuts down
readers whose tick stops moving, would kill every idle peer. It is disabled for
this transport ([PeerThreadIo.cpp:27](../PeerThreadIo.cpp#L27),
[PeerThreadIo.cpp:50](../PeerThreadIo.cpp#L50)):

```cpp
constexpr unsigned kNoStallDetection = std::numeric_limits<unsigned>::max();
watchdog_(net.max_peers, WatchdogWorker::kDefaultScanInterval, kNoStallDetection, logger)
```

Nothing is lost: `receive()` is always woken by `close_pool()` or a socket
shutdown, so an owner can't wedge the way a blocking `recv()` in `threads` can.
Cap eviction still works, because it ranks by `last_read_ns`, not by `tick`.

## 9. Shutdown

- `stop()` ([PeerThreadIo.cpp:199-211](../PeerThreadIo.cpp#L199-L211)) sets
  `stopping_`, calls `watchdog_.stop()`, then dials its own port to break the
  blocking `accept()`. It joins nothing itself.
- The watchdog thread then runs `drain()`
  ([ThreadWorkers.cpp:477](../ThreadWorkers.cpp#L477)): it shuts every remaining
  socket down and joins every owner thread. Each owner leaves through §5.
- A link attached while stopping is shut down and joined inside `adopt()`, so no
  thread is left unjoined.
- The destructor joins the watchdog thread.

`test_shutdown` checks that stopping and destroying the transport with 64 live
peers takes less than 1 s.

## 10. Counters

`tx_queued_sends` and `tx_flushes` live in each link's pool (`TxStats`).
`publish_tx_stats()` adds the delta since the last publish to the shared
`Metrics`. It runs in two places:

- on the sender after `send()`, where queueing happens;
- on the owner after `receive()`, where flushing happens.

Each side therefore reports its own counter promptly. Both use
`exchange()`-based deltas, so the totals are exact once the link goes quiet.

Other counters:

| Counter | Counted where |
|---|---|
| `accepts` | `attach(accepted=true)` |
| `evictions` | the watchdog |
| the live gauge | `attach()` and the owner's `FinishGuard` |

## 11. Tests

[test_peer_threads.cpp](../test_peer_threads.cpp):

| Test | Checks |
|---|---|
| `test_two_nodes` | exactly one TCP connection between two nodes; the Ack comes back on the dialled socket; messages flow both ways |
| `test_order` | 8 MiB in 64 KiB frames to a slow reader arrive in order; immediate and queued writes never interleave |
| `test_backpressure` | a peer that never reads eventually makes `send()` return `WouldBlock`, with nothing partial queued |
| `test_close` | `close(handle)` gives one `EventPeerDisconnected`, releases both slots, and a later `send()` gives `ConnectionNotFound` |
| `test_eviction` | with `max_peers = 2` and 3 dialers, one link is evicted: its owner exits, `on_closed` runs once, `evictions` is 1, its handle gives `ConnectionNotFound` |
| `test_shutdown` | stop plus destruction with 64 live peers takes less than 1 s |

`e2e_peer-threads` runs the end-to-end script in this mode.

## 12. How it performs (`results-0294f37`)

- **Below the thread ceiling it has the best tail latency or ties for it.**
  - S1 at 256 peers: p99 is 6.8 ms, against 7.2 ms for `threads` and 8.0–10.7 ms
    for the multiplexed one-connection modes.
  - S7 at a cap of 256: p99 is 6.8 ms at 58% CPU.
- **It shields other peers from a slow reader**, the way the pools do. S3 p99 is
  about 1.5 ms, against 2.2 s for `threads`, because no write ever blocks the
  sender.
- **It has the same ceiling as `threads`.** At 1024 peers on 2 node CPUs it
  collapses (20.8k of 102.4k msg/s), slightly ahead of `threads` (15.1k).
- **It handles large messages well.** S5 at 64 KiB: 16.2k msg/s, second only to
  `pool-sender`.
- **It costs 2 fds per peer** (the socket and the eventfd), the same total as the
  two-socket `threads` mode.
