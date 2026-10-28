# Report outline

**Proposed title:** Sockets on Linux, Hands On: Nine I/O Back Ends for a
Peer-to-Peer Chat Node

**Framing:** a report for a university course. The project is a hands-on study of
the Linux socket API; the report does not position itself against published work.

**Data:** one measured run of the full matrix, 864 cells (32 swept values × 9
variants × 3 repetitions) from one clean Release build. Every cell is `ok` and every
node exits with code 0. 840 cells pass the validity checks. The 24 that fail are all
simplex S6a cells at RTT 40 and 50 ms; they are reported as collapsed (§5.6). Figures
come from the run's `summary.csv` (median and IQR over 3 repetitions). Run and
commit names appear only in the Appendix; the body never names them.

Supporting documents: [docs/test_suite.md](../docs/test_suite.md) (modes, ids,
measurements), [docs/peer_threads.md](../docs/peer_threads.md),
[docs/experiments.md](../docs/experiments.md) (plan and hypotheses).

---

## Abstract

A high-concurrency peer-to-peer (P2P) messaging node has to keep application-level
sessions separate from the operating system's transport mechanics if it is to hold
low tail latency under fan-in, idle connections, slow consumers, connection churn
and WAN delay. This report describes a modular C++23 P2P messaging engine and
evaluates it empirically. The engine keeps its upper staged pipeline fixed (a shared
`ChatController`, a single-threaded `StoreWorker`, a policy-owning `SenderWorker` and
a socket-agnostic `FrameChannel`) and swaps in **nine** I/O backends with `--io`.
They span:

1. blocking thread-per-connection readers (`threads`);
2. threaded reads with pooled non-blocking writes (`pool-sender`);
3. two-socket (simplex) multiplexing over `poll(2)` and `epoll(7)` (`poll`, `epoll`);
4. single-socket (duplex) multiplexing (`poll-duplex`, `epoll-duplex`);
5. duplex multiplexing with separate read and write pollers (`poll-split`,
   `epoll-split`);
6. one duplex socket with one owner thread per peer (`peer-threads`).

A deterministic headless JSONL harness and an epoll-based load generator
(`stressgen`) run on separate pinned cores. Every backend is evaluated across seven
scenario families: fan-in up to 1,024 peers, idle-connection overhead, slow
consumers, churn, throughput ceiling, WAN churn and bottleneck over an unprivileged
netem lab, and capped fan-in. The report gives p50/p99/p99.9 RTT, throughput, loss,
CPU, context switches, threads, fds, handshake latency and tx-queue counters.

Headline findings:

- **Thread-per-connection collapses at 1,024 peers on 2 CPUs.** It delivers
  10–21k of 102k msg/s, with RTT around 18 s. Every multiplexed backend delivers
  everything.
- **Below that ceiling, threads are the cheapest and have the lowest tail.** At 256
  peers they need about 50% CPU, against 74–132% for the multiplexed modes.
- **`poll()` pays for idle sockets and `epoll` doesn't.** With 1,000 idle peers
  `poll` uses 100% CPU against 17% for `epoll`.
- **Only a blocking sender lets one slow peer stall everyone.** With one slow peer,
  `threads` p99 is 0.42 s; every other mode stays at about 1.5 ms.
- **The duplex handshake costs 2 RTT, simplex 3 RTT.** The simplex node's blocking
  dial-back saturates once churn × 2·RTT > 1.
- **A separate write thread pays off when the single I/O thread is the
  bottleneck:** a high message rate, large messages or heavy fan-in. The split
  modes reach 1.5–1.8× duplex throughput at 4 KiB, and gain nothing when the link
  is the limit.
- **Under churn, the tail latency comes from the sender, not the transport.** Every
  failed send sleeps 100 ms on the one shared sender thread. p99 follows the
  `send_failures` count across all nine modes.

---

## Section plan

### 1. Introduction

- **What the project is:** a university course project, a hands-on study of the
  socket API Linux offers to a program talking to many TCP peers: blocking and
  non-blocking sockets, `connect`/`accept`/`send`/`recv`, `SO_SNDTIMEO`, `poll`,
  `epoll`, `eventfd`, `shutdown`/`close` and fd reuse, `getrusage` and `/proc`,
  network namespaces, veth and netem. No comparison with other work and no C10K or
  SEDA citations.
- **The approach:** build one chat node whose application layer is fixed and swap
  only the transport (`--io=`), so a difference between runs traces back to how the
  sockets are used.
- **The three axes being ablated:**
  - concurrency model: a thread per connection, or one or two pollers;
  - readiness API: `poll` or `epoll`;
  - socket topology: two sockets per peer (simplex) or one (duplex).
- **The nine modes as a factorial-ish design.** A table maps each mode to its value
  on each axis. Name the pairs that isolate a single factor:

  | Pair | Factor isolated |
  |---|---|
  | `poll` ↔ `epoll` | readiness API |
  | `threads` ↔ `pool-sender` | blocking or pooled writes |
  | `epoll` ↔ `epoll-duplex` | two sockets or one |
  | `*-duplex` ↔ `*-split` | one I/O thread or a separate write thread |
  | `peer-threads` ↔ `threads` | one socket or two, with a thread per peer |
  | `peer-threads` ↔ `poll-duplex` | a thread per peer or a shared poller |

- **Research questions:** Q1–Q6 from [experiments.md §1](../docs/experiments.md).
- **Structure of the report:** one bullet per section, in place of a
  contributions list.

### 2. Mode-independent core architecture

- **The staged pipeline.** Render, store and sender queues, plus the
  `InboundTransport`/`OutboundTransport` seam built by `make_io_stack(IoMode)`.
  - `SenderWorker` owns all write policy: retries (3 × 100 ms), `SO_SNDTIMEO`
    500 ms, drop-on-`WouldBlock`, "never dial port 0", and no dials while quitting.
- **`ChatController` and `RenderSink`.** One controller serves two front ends:
  - the interactive terminal UI;
  - the headless JSONL worker, with `#req-id` → `"re"` correlation and `/sync`
    barriers.

  The headless path is what makes the e2e tests and the benchmark nodes
  deterministic.
- **The id domains:**
  - `peer_id` and `assigned_id` (identity);
  - `ChannelRef{connection_id, handle}` (streams);
  - the conversation id;
  - `Handle`, `pool_id` and the fd (transport).

  Kernel fds are never used as keys, which rules out ABA bugs from descriptor reuse
  across reconnects. Handles are never reused, so a stale handle can only miss.
- **Simplex and duplex links:**
  - simplex has two connections and a dial-back `ConnectAck`;
  - duplex has one connection and sends the Ack in-band on the dialled socket.

  The `PeerTable` rx/tx slots follow three rules: the newest channel wins, exactly
  one `EventPeerDisconnected` is emitted per peer, and the `FrameChannel::closing_`
  lock means a channel that closes mid-handshake binds nothing. The two shapes
  cannot interoperate in one run.
- **Problems met during implementation** (framed as what was learned, not as bugs
  fixed; no commit hashes):
  - fds are reused at once, so nothing above the pools is keyed by fd;
  - check-then-act across threads is a race: the port-0 dial, where a `connect()`
    to port 0 blocks for about 130 s of SYN retries (a duplex churn stall in about
    1 in 20 runs at churn 50);
  - a blocking `connect()` on a shared thread is expensive (it is also the S6a
    cliff);
  - shutdown is a state of the write policy: no re-dials after `/quit`;
  - a connection can close during its own handshake (the `closing_` flag);
  - tests need real readiness, not sleeps (port collisions, ready callbacks).
- **Figure:** the handshake in both shapes as two PlantUML sequence diagrams
  (`visualisation-scripts/handshake.puml`).

### 3. The nine I/O backends

- **3.1 `threads` and `pool-sender`:**
  - `ListenerWorker` and one `ReaderWorker` per connection;
  - `WatchdogWorker`: scans every 100 ms, treats 10 scans (1 s) without a heartbeat
    as a stall, and evicts the least recently active readers for `--max-peers`;
  - `BlockingOutbound` (500 ms `SO_SNDTIMEO`) against `PoolOutbound`, which has a
    pump thread, a 1 MiB tx cap and drop-on-`WouldBlock`.
- **3.2 `poll` and `epoll`, simplex and duplex:**
  - `MultiplexedIo<Pool>` in the `Dual` and `Duplex` models;
  - `TcpConnectionPool`: a desired-state table rebuilt into a `pollfd` array, an
    eventfd wakeup and a round-robin `cursor_`;
  - `EpollConnectionPool`: `epoll_ctl` called directly while another thread
    blocks, `kWakeToken`, and a fairness cursor over the ready events;
  - registry-mutex ordering, so bytes never arrive for an unknown id;
  - capacity: `2 × max_peers` for dual, `max_peers + 16` for duplex. This matters
    for S7 (§5.7). Duplex halves the fds and avoids the dial-back.
- **3.3 `poll-split` and `epoll-split`:**
  - each `shared_ptr<TcpConnection>` is shared by the rx pool, the `TxPoller` and
    the registry;
  - `apply_options_now()` runs before the connection is shared;
  - exactly one reader thread and one writer thread per socket;
  - a fatal write calls `shutdown(SHUT_RDWR)`, so the disconnect is always reported
    from the read side.
- **3.4 `peer-threads`** (new since the draft):
  - each link is a one-connection `TcpConnectionPool` driven by its own owner
    thread (`pt-<handle>`);
  - the owner is the sole caller of `on_closed()`, whichever of the four end
    triggers fires;
  - a superseded link stays silent;
  - cap eviction by `last_read_ns`, through the shared watchdog, with stall
    detection disabled;
  - an evicted link is never restarted; the peer returns only as a new link.

  Source: [peer_threads.md](../docs/peer_threads.md).
- **Summary table:** per mode, the connections per peer, threads (constant or
  per peer), fds per peer, write path, and backpressure behaviour.

### 4. Instrumentation and benchmark methodology

- **Node metrics:**
  - relaxed atomic counters;
  - a 1 Hz snapshot with `getrusage` (utime, stime, nvcsw, nivcsw, maxrss) and
    `/proc/self` (threads, fds);
  - **define CPU usage here, before its first use:**
    CPU = 100% × ((u_b + s_b) − (u_a + s_a)) / (t_b − t_a), from two `getrusage`
    snapshots bounding the window. CPU time over wall time is the average number of
    busy cores (100% = one core; the 2-CPU node tops out at 200%). Cite
    `getrusage(2)` and Stack Overflow question 1420426 (CPU usage of a process from
    `utime`/`stime`);
  - **context switches:** `ru_nvcsw` (voluntary: a thread blocked in `recv`,
    `poll`, `epoll_wait`, a mutex or a condition variable) and `ru_nivcsw`
    (involuntary: preempted while runnable), summed over all threads, turned into
    rates as Δcount / Δt over the same window;
  - queue high-water marks;
  - `tx_queued_sends` and `tx_flushes`;
  - names on every thread, for hang dumps.
- **`stressgen`:**
  - a single-threaded epoll client;
  - a log-bucketed RTT histogram: 32 sub-buckets per power of two, over-estimate
    below 3.125%;
  - warmup and grace windows;
  - open loop (`--rate`) or closed loop (`--window`);
  - idle and slow (`SO_RCVBUF` 4 KiB, stops reading) peers;
  - churn, open loop: at a fixed rate it closes a random *ready* peer and redials
    it at once, without waiting for earlier redials; the event is skipped if no
    peer is ready;
  - handshake latency, measured from connect to Ready;
  - `--duplex`.
- **The harness, as a working principle** (no audit section). Per cell,
  `run_matrix.py`:
  1. starts the node headless with `--echo`, `--io` and `--metrics` under `taskset`,
     and waits for its `ready` event;
  2. starts stressgen, which dials every peer in batches and waits for all
     handshakes or a ramp deadline;
  3. runs 5 s warm-up and a 20 s window, after which stressgen writes its results;
  4. sends `/quit`, waits up to 30 s for `bye` and exit, else records every
     thread's state from `/proc` and kills the node;
  5. writes the metadata: parameters, build stamp, exit status, validity.

  Other properties:
  - the node is pinned to CPUs 0–1 and stressgen to CPUs 4–7;
  - 5 s warmup, then a 20 s window;
  - variant order shuffled per (scenario, value, rep);
  - a Release-only guard, and a build stamp (commit, dirty flag, binary sha256) in
    every cell;
  - resume refuses to mix builds;
  - a hang dump on kill, and `node_rc` recorded.
- **WAN lab (`netlab.py`):**
  - an unprivileged `unshare` namespace with a veth pair, MTU 1500 and offloads off;
  - netem delay split per direction;
  - a rate limit on node egress only.
- **Analysis (`analyze.py`):**
  - the metrics window comes from stressgen's monotonic bounds;
  - per-cell CPU usage (the formula above) and context-switch rates;
  - the median and IQR over valid reps.
- **Validity gates.** A cell is `degraded`, and left out of the medians, if any of
  these fails:
  - peers ready: all of them, or at least 90% with churn, or the cap rule in S7;
  - offered load of at least 98% in open loop;
  - zero connect failures, bad frames and skipped sends;
  - no stall: `frames_in` still grows within 3 s of the window end.

  Statuses are `ok`, `dirty`, `degraded` and `failed`. Overload counts as a result,
  not a failure.
- **Limitations:**
  - one WSL2 host on a laptop CPU;
  - node-only CPU accounting, with no softirq share and no generator loop lag;
  - fds and threads sampled once a second;
  - no outbound byte counter;
  - the thread ceiling depends on the 2-CPU node allocation;
  - handshake statistics carry survivorship bias.

### 5. Results

One subsection per scenario. Each has a load table, the hypothesis from
experiments.md, the verdict, key numbers and a root cause where one is known. RTT
values are medians over 3 reps.

#### 5.1 S1 fan-in (1 → 1,024 active peers, 100 msg/s each, 64 B)

- **Confirmed: thread-per-connection collapses at 1,024 peers.**

  | Variant | Delivered (of 102.4k msg/s) | Median RTT | CPU | Threads |
  |---|---|---|---|---|
  | `threads` | 15.1k | ≈18 s | ≈190% | ≈1,030 |
  | `pool-sender` | 10.4k | ≈19 s | ≈190% | ≈1,030 |
  | `peer-threads` | 20.8k | ≈16 s | ≈190% | ≈1,030 |

  All six multiplexed modes deliver 102.4k msg/s with no loss.
- **Confirmed: duplex halves the fds.** At 1,024 peers it uses about 1,031 fds
  against 2,055. `peer-threads` uses 2 fds per peer: the socket and an eventfd.
- **The fan-in latency ordering holds with 3 reps.** p50 / p99 at 1,024 peers:

  | Variant | p50 | p99 |
  |---|---|---|
  | `poll-split` | 60 ms | 126 ms |
  | `poll-duplex` | 76 ms | 168 ms |
  | `epoll-split` | 147 ms | 654 ms |
  | `epoll-duplex` | 159 ms | 721 ms |
  | `epoll` | 176 ms | 772 ms |
  | `poll` | 210 ms | 445 ms |

  The poll duplex and split modes beat their epoll twins. **Unexplained by the
  data we have.** Candidate mechanism, stated as such: the order in which each pool
  serves ready connections (the `TcpConnectionPool` round-robin cursor against
  the order of epoll's ready list). Report it as an observation, not a finding.
- **New: below the ceiling, threads are cheaper.** At 256 peers:
  - `threads` and `peer-threads`: 49–54% CPU, p99 about 7 ms;
  - `poll` and `epoll`: 114–132% CPU, p99 about 20 ms;
  - duplex and split modes in between (74–100%, p99 8–11 ms).

  Discuss why: the work spreads over two cores, and a single I/O thread does a
  syscall per ready socket.
- **Figures:** throughput and p99 against peers (log x), CPU % against peers, and
  threads and fds against peers.

#### 5.2 S2 idle-heavy (8 active peers × 500 msg/s; 0 → 1,000 idle peers)

- **Confirmed: `poll` pays for idle sockets.** From 0 to 1,000 idle peers:

  | Variant | p50 | p99 | CPU |
  |---|---|---|---|
  | `poll` | 0.66 → 1.41 ms | 1.05 → 3.28 ms | 19 → 100% |
  | `poll-duplex`, `poll-split` | 0.95 ms at 1,000 | 2.2 ms at 1,000 | ≈65% at 1,000 |
  | every `epoll` variant | flat, ≈0.62–0.66 ms | ≈1.0–1.1 ms | 10–17% |

  The duplex poll modes pay about half, because they scan half as many sockets.
- **Confirmed: idle peers cost a thread each** in the thread-per-connection modes
  (1,016 threads). Latency stays flat, because blocked threads cost nothing on the
  CPU.
- **Figures:** p50 and p99 against idle peers, and CPU % against idle peers.

#### 5.3 S3 slow consumers (16 peers × 200 msg/s; 0, 1, 4 peers stop reading)

- **Confirmed: a blocking sender stalls everyone.** `threads` p99 goes from
  1.3 ms to 419 ms with one slow peer, and to 2.2 s with four. Every other mode
  stays at about 1.3–1.6 ms.
- **The pooled simplex modes queue writes** (`tx_queued_sends` ≈ 2,045 per slow
  peer), but there are **no backpressure drops**: the 1 MiB cap is never reached in
  20 s.
- **Open caveat (harness F3, still present):** the duplex, split and
  `peer-threads` modes show `tx_queued_sends = 0` even with the 4 KiB `SO_RCVBUF`
  on slow peers. So "pooled writes isolate a slow peer" is demonstrated only for
  the simplex pool modes. For the duplex shapes the result is "no stall observed",
  not "isolation under backpressure". Explain this or flag it.

#### 5.4 S4 churn (64 peers × 50 msg/s; 0, 10, 50 reconnects/s)

- **Churn mostly moves the tail, not the median.** At churn ≥ 10, p99 jumps to
  about 45–130 ms for most modes. `threads` stays at about 4.5 ms. `poll-duplex`
  stays at 4.7 ms at churn 10 only.
- **Simplex multiplexed modes also lose median latency at churn 50.** p50 is
  32 ms for `poll` and 13 ms for `epoll`, against about 2.4 ms for everything else.
  Losses are highest there too (2.3–2.6k, against 1.0–1.8k).
- **Verified from the run's counters: the ≈100 ms plateau is the sender's retry
  sleep.**
  - An echo addressed to a just-churned handle fails (`ConnectionNotFound`). The
    sender then sleeps 100 ms (`sleep_for(retry_delay_)`) on the one thread that
    writes for every peer, so every other peer waits too.
  - p99 follows the node's `send_failures` count (3 reps each):

    | Mode | Churn 10 | Churn 50 | p99 |
    |---|---|---|---|
    | `threads` | 0 0 0 | 0 0 0 | ≈4.5 ms |
    | `poll-duplex` | 4 1 1 | 14 14 20 | 4.7 ms at churn 10 |
    | `peer-threads` | 4 7 4 | 29 29 33 | 64–103 ms |
    | `epoll` | 39 43 41 | 125 128 125 | ≈105 ms |
    | `poll` | 57 62 62 | 151 165 166 | ≈128 ms |

  - **Why `threads` never fails (likely mechanism, not proven):** its blocking
    `send()` to a just-closed socket still succeeds into the kernel buffer, so the
    echo is lost silently instead of retried. It also has the lowest loss. The pool
    modes see the EOF first, so the next send fails and the sender sleeps.
  - **Consequence:** neither threaded reads nor isolating fds prevents the plateau.
    `pool-sender` and `peer-threads` read on their own threads and still reach a
    p99 of 64–105 ms. The ≈100 ms handshake p99 at RTT 0 (S4, S6a) has the same
    signature.
- **The split modes fare worse under churn:** 69–103 send failures at churn 50,
  against 14–25 for duplex.
- **Figures:** p99 against churn; p99 against `send_failures` per cell; and
  handshake p50/p99 against churn.

#### 5.5 S5 throughput ceiling (8 peers, closed loop, window 16)

Throughput by message size:

| Size | Best | Middle | Worst |
|---|---|---|---|
| 32 B | split modes, 206–215k msg/s | `peer-threads` 203k; duplex modes 188k; `threads` 186k | `poll` and `epoll`, 158–169k |
| 4 KiB | `threads`, `pool-sender`, `peer-threads`: 105–113k | split modes 82–90k | duplex and simplex multiplexed, 51–56k |
| 64 KiB | `pool-sender` 17.7k; `peer-threads` 16.2k | `threads` 12.4k; split modes 10.5k | single-thread multiplexed modes, 7.6–8.6k |

- **A separate write thread pays off once writes are large.** Against their duplex
  twins, the split modes gain +46% (`poll`) and +76% (`epoll`) at 4 KiB, +21% and
  +40% at 64 KiB, and +15% and +9% at 32 B.
- **With one I/O thread, CPU plateaus at ≈122–144% at 4 and 64 KiB.** The I/O
  thread is the bottleneck. The thread modes reach 170–180%.
- **Figures:** throughput against size (log y), and CPU % against size.

#### 5.6 S6 WAN (netem lab)

- **S6a, handshake cost confirmed.**
  - Simplex takes about 3 RTT: 62 ms at RTT 20.
  - Duplex takes about 2 RTT: 41 ms at RTT 20.
  - At RTT 50, the duplex modes take ≈102 ms.
- **S6a, the simplex collapse is a saturation cliff.** It is predicted by queueing
  on the sender's blocking dial-back, with ρ = churn × 2·RTT:
  - RTT 20, ρ = 0.64: stable. Handshake p99 is 654 ms, about 32 serialised
    connects.
  - RTT 30, ρ = 0.96: `poll` and `epoll` degrade (2.8k msg/s, p50 ≈ 100 ms);
    `threads` and `pool-sender` still hold.
  - RTT 40 and 50: all four simplex modes collapse. Only 10–13 of 32 peers are
    ready, and all 24 of these cells are flagged degraded.
  - Duplex modes stay healthy throughout, at ≈3.1k msg/s.
- **S6c (RTT 50, churn halved to 8/s, ρ = 0.8):** simplex recovers to 3.0k msg/s,
  with handshake p50 155 ms (3 RTT) and p99 1.7 s. This confirms a cliff, not a
  slope.
- **What causes the cliff, and what defines it.**
  - Not the retry sleep: the collapsed cells have only 1–4 send failures per run
    (≤0.4 s of sleep in 25 s). The cause is the serial blocking dial-back.
  - The *sharpness* is partly a property of the benchmark. Churn is open loop and
    always closes a ready peer, so once handshakes complete more slowly than churn
    removes ready peers, the ready set drains: only 278–341 of 400 scheduled churn
    events found a ready peer, the handshake p50 reached 2.6–3.2 s, and no echoes
    arrived in the window. A cell is then excluded by the 90%-ready check.
  - A closed-loop churn or a longer window would likely show a gradual slowdown
    instead. The limit belongs to the node; the label "collapsed" belongs to the
    benchmark.
- **S6b bottleneck** (4 ms RTT, 16 KiB frames, node egress at 1000, 200 and
  50 Mbit):
  - Every mode is link-limited: 7.2k, 1.45k and 363 msg/s, which is about 95% of
    the line rate.
  - Writes queue as expected. `tx_queued_sends` is 4–5k at 50 Mbit and below 60
    at 1 Gbit.
  - **Split does not beat duplex.** Their throughput and p99 are identical within
    noise. The extra write thread doesn't matter when the link is the bottleneck.
  - Simplex handshakes are slower behind the bottleneck: ≈78 ms against ≈9 ms. The
    dial-back Ack queues behind data on the node's egress.
- **Figures:** handshake p50 against RTT with 2·RTT and 3·RTT guide lines, and
  msg/s against RTT, marking the collapsed cells.

#### 5.7 S7 capped fan-in (1,024 peers offered; `--max-peers` 64, 128, 256, 1040)

- **Confirmed: a cap turns collapse into graceful shedding** for the
  thread-per-connection modes. `threads` at cap 256 serves 256 peers with no loss,
  p99 7.1 ms, at 50% CPU. Uncapped it serves 15.6k msg/s with 1.7 M lost.
  `peer-threads` at cap 256 has p99 6.8 ms at 58% CPU. Evictions are exactly
  1,024 − cap.
- **Resolved: capped `epoll` is not doing re-dial churn.** Its CPU at cap 256
  (115%) equals S1 `epoll` at 256 peers (114%). It is simply what a single I/O
  thread costs to serve 256 peers (see 5.1).
- **Caveat: "cap" means different things in different modes.**

  | Modes | Pool capacity | Peers served |
  |---|---|---|
  | duplex and split | `max_peers + 16` | 80, 144, 272 |
  | dual (`poll`, `epoll`) | `2 × max_peers`, shared with dial-backs | cap or cap − 1 |
  | thread modes | watchdog cap | exactly the cap |

  Multiplexed prunes don't count as `evictions`. Compare modes by peers served,
  not by the nominal cap.

### 6. Conclusions and final summary

No further experiments will be run. Every conclusion below rests on the measured
run and on the code it was built from. Only conclusions that
survived review against the data are included.

#### 6.1 Conclusions supported by the data

- **C1. A second, simplex channel per peer isn't worth it.**
  - **No throughput gain.** At 32 B simplex is slower (`poll` 158k against
    `poll-duplex` 188k msg/s; `epoll` 169k against 189k). At 4 and 64 KiB the two
    shapes are within noise.
  - **The handshake is longer.** It costs 3 RTT against 2: 62 ms against 41 ms
    at 20 ms RTT, and a p50 of ≈280 ms against ≈95 ms at 1,024 peers.
  - **The handshake is unreliable under churn.** Simplex collapses in S6a from
    40 ms RTT, and `poll`/`epoll` already degrade at 30 ms. Behind a rate limit
    the dial-back queues behind data (78 ms against 9 ms in S6b).
  - **It doubles the fds.**
  - **Attribution:** the collapse comes from the dial-back being a blocking
    `connect()` on the shared sender thread, not from the retry sleep. How abruptly
    it appears is partly set by the open-loop churn and the 90%-ready check (§5.6).
    A non-blocking dial-back would remove the cliff, but not the extra RTT or the
    extra connection.
- **C2. Split read/write pollers pay off when the single I/O thread is the
  bottleneck.** The trigger is a high message rate, large messages or heavy
  fan-in; any one of them is enough.
  - **Throughput:** at 4 KiB, +46% / +76% (`poll` / `epoll`) over duplex, with
    only 8 peers. +21% / +40% at 64 KiB, and +9–15% at 32 B.
  - **Fan-in:** at 256 peers, p99 is 8.0–8.4 ms against ≈10.6 ms, at lower CPU.
    At 1,024 peers, p50 is 60 against 76 ms (`poll`) and 147 against 159 ms
    (`epoll`).
  - **No gain** when the link is the limit (S6b) or the load is idle-heavy (S2).
  - **Worse under churn** (S4: more send failures).
- **C3. `peer-threads` is reliable and fast, but needs a thread cap, and its retry
  path is a limitation of the current implementation.**
  - **Reliable:** all 96 of its cells are valid. It stays healthy in S6a at
    40–50 ms, where simplex collapses. No hangs, and every exit code is 0.
  - **Fast:**
    - 203k msg/s at 32 B, 105k at 4 KiB, 16.2k at 64 KiB (second only to
      `pool-sender`);
    - the best p99 at 256 peers (6.8 ms);
    - it shields other peers from a slow reader (S3 p99 ≈1.5 ms).
  - **Needs a cap:** at 1,024 peers on 2 CPUs it delivers 20.8k of 102k msg/s.
    With a cap of 256 it serves those peers at p99 6.8 ms and 58% CPU.
  - **Limitation (code, measured in S4).** The write itself is already
    non-blocking. Two things block every peer:
    - retries are a 100 ms sleep on the shared `SenderWorker` thread: 29–33
      failures at churn 50, and p99 64–103 ms;
    - `open()` performs a blocking `connect()` on that same thread.

    The retry and the dial belong to the peer's owner thread, as a deadline in its
    poll loop rather than a sleep, with an asynchronous dial. The shared sender
    then becomes a dispatcher into per-peer queues.
  - **Caveats:**
    - It uses 2 fds per peer (the socket plus the pool's eventfd).
    - Its backpressure path was never exercised: S3 shows `tx_queued_sends = 0`
      for every duplex shape.
- **Also established by the data** (from §5):
  - **The fan-in collapse.** Thread-per-connection collapses at 1,024 peers on 2
    CPUs, and below that ceiling it is the cheapest mode.
  - **`poll` and idle sockets.** `poll` pays per idle socket; `epoll` stays flat.
  - **Slow readers.** Only a blocking sender lets one slow reader stall everyone.
  - **Churn.** Under churn, tail latency is driven by the shared sender's retry
    sleep (`send_failures`), not by the concurrency model.
- **Decision guide:**
  - **Up to a few hundred peers on few cores:** thread-per-peer with pooled writes
    (`peer-threads`, `pool-sender`), with `--max-peers` sized to the cores.
  - **Many or idle peers:** `epoll-duplex`, or `epoll-split` for large messages or
    high rates.
  - **WAN churn:** always duplex.

#### 6.2 Limitations and open observations

- **Measurement limits:**
  - one WSL2 host;
  - node-only CPU accounting;
  - the 2-CPU node allocation sets where the thread ceiling falls;
  - no outbound byte counter.
- **Unexplained:**
  - why the poll duplex and split modes beat `epoll` at 1,024 peers;
  - why the duplex shapes never queue writes in S3.
- **"Cap" isn't uniform across modes:** `max_peers + 16` for duplex pools,
  `2 × max_peers` for dual pools, an exact cap for the thread modes.

#### 6.3 Final summary: a hypothesis for a combined design (untested)

This comes last, after all the tested findings. It is presented explicitly as a
design hypothesis that the existing data motivates but does not test.

- **The proposal:** keep the `peer-threads` ownership model, but give each owner
  thread a **small pool of peers** instead of one, with a balancing strategy. That
  is an M:N reactor between the two measured extremes:
  - one peer per pool: `peer-threads`;
  - every peer in one pool: `*-duplex` and `*-split`.
- **Candidate balancing policies:**
  - uniform (least-loaded at attach);
  - separate pools for large-message peers and small-message peers;
  - separate pools for unreliable or churning peers.
- **Why the data makes it plausible:**
  - **No CPU starvation.** A bounded thread count avoids the 1,024-peer collapse:
    the single-poller modes served 1,024 peers at ≈140% CPU.
  - **Uses the cores.** One I/O thread plateaus at ≈122–144% CPU in S5, while the
    thread modes reach 170–180%. About one pool per core would use them.
  - **`poll` suits small pools.** Its cost grows with the number of fds (S2), and
    the poll duplex and split modes had the best fan-in latency.
  - **Fewer fds.** One eventfd per pool instead of per peer gives ≈ peers + pools.
- **Preconditions and risks:**
  - **The sender must move off the shared thread.** Retries and dials (C3) must
    move into the pool loops as timers. Otherwise the S4 plateau and the S6a
    cliff stay. A sleep inside a pool would stall every peer in it.
  - **Placement at attach, no migration at first.** Moving a live connection
    means handing over its fd and its `FrameChannel`.
  - **Some isolation policies lack evidence.** Message-size isolation is motivated
    by S5 but untested. Slow-reader isolation showed no clear need, since pooled
    writes didn't stall in S3.
- **How it would be evaluated:** sweep the pool size (1, 8, 32, all) with
  pools ≈ cores over S1, S4 and S5, under one capacity rule for all modes.
- **Other directions:**
  - `io_uring(7)` behind the same IoStack;
  - QUIC (RFC 9000), which is duplex and multiplexed by design.

### Appendix

- **A. Reproduction:** the measured run is `results-0294f37` (commit `0294f37`,
  branch `io-variants`), committed without node/generator logs; the commands from
  [experiments.md §4](../docs/experiments.md), the build stamp, and the CPU pinning.
- **B. Test suite:**
  - unit tests;
  - e2e tests per mode;
  - stressgen tests;
  - the analyzer tests;
  - test flakiness under parallel load.

  On flakiness: the port collisions are fixed, `ctest -j8` passes 26/26, and a
  ≈2% residual failure rate under artificial 6–8× load is accepted.
- **C. All results:** one landscape longtable per scenario (generated by
  `make_figures.py` from `summary.csv`): every swept value × mode, with valid reps,
  msg/s, lost, p50/p99/p99.9, CPU, voluntary/involuntary context switches, threads,
  fds, peers ready, handshake p50, queued sends and evictions.
