# Planned experiments

This is the plan for the next full benchmark run: what each scenario is meant to
show, what we already know from earlier partial runs, how the run is carried
out, and how its results are judged. How the modes, measurements and harness
work is in [test_suite.md](test_suite.md). This file only covers *what* we
measure and *why*.

## 1. Questions the experiments answer

1. **Threads vs multiplexing.** How do thread-per-connection modes (`threads`,
   `pool-sender`) compare with one I/O thread (`poll`, `epoll`) as peers,
   idle connections and slow consumers grow? Where does each one break?
2. **`poll` vs `epoll`.** Does `poll()`'s cost per wake-up, which grows with
   the number of connections, become visible?
3. **Blocking vs pooled writes.** Does a slow peer stall everybody when writes
   are blocking (`threads`)? Does the pool's tx buffer with drop-on-full
   (`pool-sender` and the multiplexed modes) isolate it?
4. **Two connections vs one per peer** (`poll`/`epoll` vs `*-duplex`). What
   does halving the sockets save: fds, handshake round trips, latency?
5. **One I/O thread vs a separate write thread** (`*-duplex` vs `*-split`).
   Does flushing writes on their own thread help once writes actually queue?
6. **Thread per connection, with one socket** (`peer-threads` vs `threads` and
   `poll-duplex`). Against `threads`: half the connections and fds, no blocking
   sender, but the same CPU-starvation ceiling (one runnable thread per peer, so
   expect the same collapse at 1024 peers on 2 CPUs). Against `poll-duplex`: the
   cost of a thread per peer instead of one shared poller.

## 2. The matrix

Every scenario runs **all 9 variants**: `threads`, `pool-sender`, `poll`,
`epoll`, `poll-duplex`, `epoll-duplex`, `poll-split`, `epoll-split`, `peer-threads`.

Each cell gets 20 s of measurement after a 5 s warmup. Variant order is
shuffled for every (scenario, value, rep), so slow drift on the machine (heat,
background load) spreads across variants instead of biasing one.

| Scenario | Swept value | Other load (stressgen) | Network |
|---|---|---|---|
| **S1** fan-in | active peers: 1, 8, 64, 256, 1024 | 100 msg/s per peer, 64 B | loopback |
| **S2** idle-heavy | idle peers: 0, 100, 300, 1000 | 8 active peers × 500 msg/s, 64 B | loopback |
| **S3** slow consumers | peers that stop reading: 0, 1, 4 | 16 active peers × 200 msg/s, 64 B | loopback |
| **S4** churn | reconnects per second: 0, 10, 50 | 64 peers × 50 msg/s, 64 B | loopback |
| **S5** throughput ceiling | message size: 32, 4096, 65536 B | 8 peers, closed loop (window 16) | loopback |
| **S6a** WAN churn | RTT: 0, 4, 20, 50 ms | 32 peers × 100 msg/s, 256 B, 16 reconnects/s | veth, MTU 1500, netem delay |
| **S6b** WAN bottleneck | node→generator rate: 1000, 200, 50 Mbit/s | 32 peers, closed loop (window 8), 16 KiB | veth, MTU 1500, 4 ms RTT, rate on the node side |
| **S7** capped fan-in | the node's `--max-peers`: 64, 128, 256, 1040 | 1024 peers × 100 msg/s, 64 B | loopback |

S6 is implemented as specified in [prompts/s6_wan.md](../prompts/s6_wan.md)
(merge `b632850`), and the values above match `SCENARIOS`. Its cells run in an
unprivileged `unshare` namespace started by `run_matrix.py`. When S1–S5 and S6
are selected together, S1–S5 run first, then S6. Handshakes count if they
complete during the ramp or inside the measurement window.

**Size of the run:** 29 swept values × 9 variants × 3 repetitions = **783
cells**, about 25–30 s each, so about **6 hours**.

Three repetitions, not the script's default of 5, keeps the run to one sitting.
The report gives the median and IQR, which are meaningful with 3, and the
earlier runs showed small spread between repetitions for everything except
overload cells.

## 3. Hypotheses and what to look at

"Seen" marks what the stopped 8-variant run (`results-8var/`, 208 of 432 cells,
S1 and most of S2) already showed. Everything else is a prediction.

### S1: fan-in

- **Thread-per-connection collapses at high peer counts on the node's 2 CPUs.**
  *Seen at 1024 peers:*
  - `threads` and `pool-sender` delivered about 11–13k of 102k msg/s, losing
    87–89%, with a median RTT of about 19 s.
  - They used both cores (about 193% CPU) and about 1,030 threads.
  - The multiplexed modes delivered everything.

  At 256 peers all modes still kept up.
- **One connection per peer halves the fds.** *Seen at 1024 peers:* about
  1,030 fds for `poll-split` against about 2,050 for `epoll`.
- **Latency under heavy fan-in is not yet explained.** *Seen at 1024 peers,*
  median RTT:

  | Variant | Median RTT |
  |---|---|
  | `poll-split` | 61 ms |
  | `poll-duplex` | 78 ms |
  | `epoll-split` | 155 ms |
  | `epoll-duplex` | 189 ms |
  | `epoll` | 197 ms |
  | `poll` | 218 ms |

  So halving the sockets helped `poll` a lot but `epoll` only slightly.
  Check whether this holds with 3 repetitions before reading anything into it.
  It may come from how each pool orders ready connections (see the fairness
  cursors in the pools), not from the connection model.
- **Look at:** throughput, lost, RTT p50/p99, CPU %, voluntary and involuntary
  context switches, peak threads, peak fds, and queue high-water marks.

### S2: idle-heavy

- **`poll()` pays for idle connections; `epoll` doesn't.** *Seen:*
  - As idle peers go from 0 to 1000, `poll`'s median RTT grows from 0.6 to
    1.5 ms and its p99 from 1.0 to 3.6 ms.
  - The `poll-duplex`/`poll-split` modes grow less (about 1.0 ms median), since
    they have half as many sockets to scan.
  - Every `epoll` variant stays flat at about 0.6 ms.
- **Idle peers cost a thread each in `threads`/`pool-sender`.** *Seen:* 1,016
  threads with 1,000 idle peers. Latency stays fine on 2 CPUs because idle
  threads are blocked, not running. Memory grows only a little (14 → 19 MB),
  because thread stacks are only committed when touched.
- **Look at:** RTT p50/p99 against idle peers, CPU %, peak threads, peak fds.

### S3: slow consumers

- **Blocking writes stall everyone.** In `threads`, a send to a peer that has
  stopped reading blocks the single sender thread for up to 500 ms
  (`SO_SNDTIMEO`), plus retries. Expect RTT p99 to jump for *all* peers as soon
  as one slow peer exists.
- **Pooled writes isolate the slow peer.** The pool fills that peer's 1 MiB tx
  buffer, then drops and counts its messages (`bp_drops > 0`). Other peers'
  latency should stay put.
- **Writes queue on loopback here too.** A peer that doesn't read fills its
  socket buffer even on loopback, so `tx_queued_sends` (new with S6) should be
  large in S3. This tests the premise of S6b: if queueing is common already on
  loopback, S6b's point becomes "the bottleneck makes queueing the normal case",
  not "loopback hides it".
- **Look at:** RTT p99/p99.9, backpressure drops, `tx_queued_sends`, lost.

### S4: churn

- **Reconnects cost more in simplex modes** because of the dial-back (an extra
  connection and handshake), but on loopback the difference should be small.
  S4 is the loopback baseline for S6a.
- **The thread-per-connection modes pay thread creation and teardown per
  reconnect.** Expect higher CPU and more context switches as churn grows.
- **Look at:** `handshake_p50/p99` (new with S6), churn events, connect
  failures, throughput, CPU %, `accepts`.

### S5: throughput ceiling

- **The maximum echo rate and the CPU cost per message**, with a closed loop
  (window 16) so the node is the bottleneck.
- **Large messages (64 KiB) are likely to queue writes on loopback too.**
  Check `tx_queued_sends`, as in S3.
- **Look at:** throughput, CPU %, context switches per second, RTT p50/p99,
  `tx_queued_sends`.

### S7: capped fan-in

- **The `--max-peers` cap exists to stop thread-per-connection modes from
  starving the node's CPUs.**
  - S1 at 1024 peers runs effectively uncapped (peers + 16 = 1040), and
    `threads`/`pool-sender` collapse there.
  - S7 offers the same load against caps of 64, 128 and 256.
  - **256 is the "reasonable" cap:** it is the largest S1 peer count every
    mode sustained on the 2 node CPUs.
  - 1040 repeats the uncapped S1 cell inside this scenario, as a reference.
- **Expected:** a capped node serves about `cap` peers with low latency and
  sheds the rest (they are evicted and stressgen doesn't redial), instead of
  serving everyone badly. *Seen in a single-rep Release check:*
  - `threads` at cap 256 served 256 peers with 0 lost, 4.1 ms median and
    6.9 ms p99 RTT, at 51% CPU.
  - Uncapped `threads` delivered 9.5k msg/s, lost 1.86 M messages and had a
    19.6 s RTT, at 190% CPU.
  - Uncapped `epoll` served all 1024 peers at 197 ms.
- **Unexplained:** capped `epoll` used about twice the CPU of capped `threads`
  (107% against 51% at cap 256), and its p99 was worse (22.5 ms against
  6.9 ms). A likely cause is that echoes for evicted peers make the sender
  re-dial them, because stressgen's listener is still up; that pushes the pool
  back over its cap and it prunes again. Check `send_failures`/`accepts` and
  the node log.
- **Measurement gap:** in the multiplexed modes a pool prune is counted as an
  ordinary close, not in `evictions`. For those modes, use stressgen's
  `disconnects` and `peers_ready` to see how many peers were shed.
- **Look at:** throughput, RTT p50/p99, peers served, disconnects, evictions,
  CPU %, peak threads.

### S6a: WAN churn

- **The time from connect to Ready is about 3 RTT in simplex and 2 RTT in
  duplex.** Simplex needs connect + Connect + dial-back + Ack; duplex sends the
  Ack on the dialed socket. At RTT 20 ms, expect `handshake_p50` ≈ 60 ms for
  `poll`/`epoll`/`threads`/`pool-sender` and ≈ 40 ms for `*-duplex`/`*-split`.
  The gap should grow linearly with RTT. *Seen in the quick S6 smoke run (a Debug build, so only the RTT-driven numbers are meaningful):*
  61.9 ms for `poll`/`epoll` against 40.9 ms for `epoll-duplex`/`epoll-split`
  at RTT 20 ms, and about 0.7 ms for all at RTT 0.
- **Unexplained:** the handshake p99 at RTT 0 was about 100 ms for `poll`,
  `epoll-duplex` and `epoll-split`, but not for `epoll`. Check whether this
  holds in the full run.
- **Look at:** `handshake_p50/p99` against RTT, connect failures (must be 0),
  throughput, RTT p99.

### S6b: WAN bottleneck

- **Behind a rate limit, writes queue in the node,** and the flush happens on
  POLLOUT/EPOLLOUT. Expect `tx_queued_sends` and `tx_flushes` to be well above
  zero at 200 and 50 Mbit. *Seen in the quick smoke run (Debug build: the counts are indicative, the throughput isn't):* 500–750 queued sends
  at 100 Mbit against 1–29 at 1000 Mbit. Throughput was link-limited at
  100 Mbit (about 720 msg/s, median RTT 125–168 ms).
- **Does a separate write thread (`*-split`) beat one I/O thread
  (`*-duplex`)?** Compare throughput and RTT p99/p99.9 at the same rate. The
  split modes also use an extra thread, so CPU and context switches matter
  too.
- **Look at:** throughput (should approach the link rate), RTT p50/p99/p99.9,
  `tx_queued_sends`, `tx_flushes`, CPU %, voluntary context switches per
  second, backpressure drops.

## 4. How the run is carried out

1. **Preconditions:**
   - S6 merged into `io-variants`.
   - `build/` rebuilt from that commit **as Release**
     (`cmake -B build -S . -DCMAKE_BUILD_TYPE=Release`).
     - `run_matrix.py` refuses a non-Release build unless given
       `--allow-non-release`, and records `build_type` in every cell.
     - A cached Debug `build/` silently halved throughput at 1024 peers once:
       102k against 54k msg/s.
   - The full `ctest` passes at least twice with `-j8`.
   - The working tree has no uncommitted changes, so every cell's `build` stamp
     names an exact commit. Check `git status`.
2. **Smoke run:**
   `scripts/run_matrix.py --quick --reps 1 --out results-smoke --base-port 23000`
   across all scenarios and variants, about 15 minutes. Every cell must be
   `ok`. Fix anything that isn't before the full run.
3. **Full run:**
   - Launch it in tmux so it survives the terminal, and log to
     `logs/stress_full.log`:
     ```
     scripts/run_matrix.py --app build/app --stressgen build/stressgen \
         --out results-<commit> --reps 3
     ```
   - The node is pinned to CPUs 0–1 and stressgen to 4–7, which are separate
     physical cores.
   - **Nothing else heavy should run on the machine meanwhile.** It's a laptop
     CPU, so builds, tests and other benchmarks would take power and turbo
     budget from the pinned cores.
4. **Analysis:** `scripts/analyze.py results-<commit>` writes `summary.csv` and
   `report.html`.
5. **Resuming:** an interrupted run continues when the same command is run
   again. Cells marked `ok` or `dirty` are kept, and `failed` ones are retried.
   Only resume with the **same build**: check that the `build` stamp in new
   cells matches the old ones.

## 5. When a result counts

- **`ok`:** valid.
- **`dirty`:** the node didn't exit within the 30 s `--quit-timeout`. The data
  from the measurement window is still valid, and `analyze.py` keeps it and
  lists the cell. Use `shutdown_s` in `meta.json` to see how long shutdown took.
  After the sender shutdown fix, a dirty cell is unusual and worth a look.
- **`failed`:** no data, for example when the node wasn't ready or stressgen
  exited with an error. Investigate it; never ignore it.
- **Overload is a result, not a failure.** A cell with high `lost`, a huge RTT
  and a full CPU is the "where does it break" answer. Report it as such.
- **Never mix builds within one results directory.** All cells must share one
  `build.commit` with `dirty: false`, and the same binary hashes.
- **Compare variants within one scenario value.** Numbers across scenarios use
  different loads and aren't comparable.

## 6. Known limitations

- **Single host:** the node and stressgen share one machine (WSL2). Loopback
  RTT includes stressgen's own processing. S6 adds a real MTU, delay and rate
  limit, but it is still one host.
- **CPU and context switches cover the node only.** stressgen's cost isn't
  included. Threads and fds are sampled once a second.
- **The thread-per-connection collapse depends on giving the node only 2
  CPUs.** With more cores the break point moves up. That's expected and should
  be stated alongside the S1 result.
- **No outbound byte counter:** the node counts frames out, not bytes out.

## 7. After this run

- **Optional sender change:** remember unreachable peers for a short back-off,
  instead of re-dialing on every message to them. It changes S4/S6a behaviour,
  so do it only together with a rerun.
- **Final run at code freeze:** one full rerun of everything with a single final
  build, used for the definitive report. Until then, each run is valid only
  within itself.
