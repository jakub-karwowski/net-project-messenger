# Coding Agent Prompt: S6 WAN scenarios (network namespace + veth + tc-netem)

## Role and objective

You are working on a **C++23** peer-to-peer TCP chat node and its Python
benchmark harness. Read `docs/test_suite.md` first: it describes the eight
`--io` modes, the peer link (simplex vs duplex), the measurements and the
existing matrix (S1–S5).

On loopback, two architectural differences between the modes are invisible:

1. **The cost of the handshake under churn.**
   - In simplex modes (`threads`, `pool-sender`, `poll`, `epoll`) the node's
     `SenderWorker` must dial a second TCP connection back to the peer before it
     can send `ConnectAck`.
   - Duplex modes (`*-duplex`, `*-split`) send the Ack on the dialed socket.
   - From the dialer's point of view, the time from connect to "Ready" is
     about **3 RTT in simplex** (connect + Connect + dial-back + Ack) and
     **2 RTT in duplex**. With loopback's ~20 µs RTT the difference disappears.
2. **Where writes get flushed.**
   - On loopback the socket buffers drain at once, so `Pool::send()` /
     `TxPoller::send()` almost always finish on the immediate write and never
     queue.
   - Behind a real bottleneck (MTU 1500, limited rate, delay) the node's socket
     buffers stay full. Writes then queue in `tx_buffer` and are flushed on
     POLLOUT/EPOLLOUT: by the shared I/O thread in `*-duplex`, by the dedicated
     `TxPoller` thread in `*-split`.

Add **S6a (WAN churn)** and **S6b (WAN bottleneck)**, which run the node and
stressgen across a veth link shaped with netem. S1–S5 must stay exactly as they
are.

## Constraints

- **No sudo.** This machine (WSL2, kernel 6.6) has no passwordless sudo, and
  none is needed. An unprivileged user + network + mount namespace was verified
  to support everything required:
  - `unshare -rnm`;
  - `ip netns add` after mounting a tmpfs on `/run`;
  - veth pairs, MTU 1500, and `ethtool -K … tso off gso off gro off`;
  - `tc qdisc … netem delay … rate …`; `sch_netem` loads on demand.

  Files created inside belong to the real user, and the namespaces disappear
  when the process exits. Do not add sudo, setuid or capability handling.
- **Do not disturb a running benchmark.** Before building, running tests or
  running any matrix, check that no other `run_matrix.py` is running
  (`pgrep -f run_matrix.py`). If one is, only edit files; do not run anything
  heavy.
  - Never rebuild a build directory that another run is using (for example
    `build-int/`). Use `build/`.
  - Always pass a `--base-port` and `--out` that differ from the running
    matrix.

## Step 1: Read the harness (no edits yet)

Read `stressgen.hpp`, `stressgen.cpp`, `scripts/run_matrix.py`,
`scripts/analyze.py`, `scripts/test_analyze.py` and `docs/test_suite.md`.

Facts you should confirm while reading. Each one changes what needs building:

- **stressgen already supports a non-loopback return address.**
  - `--listen-ip` sets both the address its return listener binds to and the
    address it advertises in `Connect.endpoints` (`Config::listen_ip`, default
    `127.0.0.1`).
  - For S6, pass `--listen-ip 10.200.1.2`. Do not add a new flag.
  - In `--duplex` mode it doesn't listen, and advertises that address with
    port 0.
- **The node already supports a bind address:** `--address=10.200.1.1`. It also
  advertises that address to peers.
- **`--churn C` is a total rate** of reconnects per second across all peers. It
  is not a peer count. Each reconnect uses a new token, so the node sees a new
  peer each time.
- **The node's `reconnects` counter is the wrong metric for churn.** It counts
  the node's own redials (`/reconnect`, or a silent redial of a Disconnected
  peer), not stressgen's reconnects.
- **Nothing measures handshake time today.** stressgen has no timer from
  connect to Ready. Without one, hypothesis 1 can't be tested directly.
- **Nothing shows whether writes queue.** There is no counter for "a send that
  had to queue bytes". Without one, hypothesis 2 is an assumption.
- **Scenario keys follow the `S<n>` convention** (`--scenarios S1,S5`). Each
  scenario has `title`, `x`, `values`, `base` and `quick_values`. `x` is
  currently always a stressgen option.
- **Repetitions are numbered from 1** (`rep1…`), not 0.

## Step 2: Add the missing measurements

1. **stressgen handshake latency:** record, for each peer, the time from
   starting `connect()` to reaching `Ready`. Do this for the first ramp *and*
   for churn redials. Store it in a second `Histogram`. Add
   `"handshake_us":{"p50","p90","p99","max","mean","samples"}` to `to_json`,
   using the same window rule as RTT: count handshakes that complete inside the
   measurement window. Keep every existing field unchanged, and extend
   `test_stressgen` with checks that the samples are present and positive in
   both shapes.
2. **Write-queueing counters on the node:**
   - Add, on `TcpConnectionPool`, `EpollConnectionPool` and `TxPoller`, relaxed
     atomic counters for sends that queued bytes (the slow path) and for flushes
     on writability. Expose them through a small `stats()` accessor; the
     TcpSocket library must not depend on `Metrics`.
   - Have `MultiplexedIo` and `PoolOutbound` copy them into two new `Metrics`
     fields, `tx_queued_sends` and `tx_flushes`, included in `snapshot_json`.
   - Update `test_pool`, `test_tx_poller` and `test_metrics`.

## Step 3: The network topology (no sudo)

Add `scripts/netlab.py` (stdlib only), used by `run_matrix.py` only for S6:

- **`run_matrix.py` is not run as root.** When any S6 scenario is selected:
  - S1–S5 cells run in the normal process, unchanged.
  - The S6 cells run in a child process that re-executes `run_matrix.py`
    (restricted to the S6 scenarios) under
    `unshare -rnm --propagation private`.
- **In the child, once, at startup:**
  1. `mount -t tmpfs none /run`, then `mkdir -p /run/netns`.
  2. `ip netns add ns_p2p_gen`.
  3. `ip link add veth-node mtu 1500 type veth peer name veth-gen mtu 1500 netns ns_p2p_gen`.
  4. Addresses: `veth-node` gets `10.200.1.1/24` in the child's own namespace,
     where the node runs. `veth-gen` gets `10.200.1.2/24` inside
     `ns_p2p_gen`. Bring up both interfaces, and `lo` on both sides.
  5. `ethtool -K <dev> tso off gso off gro off` on both ends. Treat a failure
     as a warning recorded in `meta.json`, not an error.
- **Cleanup:** the namespaces belong to the child process and vanish when it
  exits. There are no global interface names and no stale state, so no
  atexit or signal cleanup is needed.
- **`configure_netem(node_delay_ms, gen_delay_ms, node_rate_mbit=None, limit_pkts=…)`:**
  - Runs `tc qdisc replace dev veth-node root netem …`, and the same for
    `veth-gen` through `ip netns exec`.
  - A delay of 0 with no rate means `tc qdisc del` (back to the default
    qdisc).
  - The RTT is `node_delay_ms + gen_delay_ms`.
  - The **rate applies to `veth-node` egress only**, the node→generator
    direction (see S6b).
  - Record the `tc qdisc show` output of both ends in `meta.json`.
- **Starting the processes:**
  - Node: `taskset -c <node-cpus> app --address=10.200.1.1 …`.
  - stressgen: `ip netns exec ns_p2p_gen taskset -c <gen-cpus> stressgen --target 10.200.1.1:<P> --listen-ip 10.200.1.2 …`,
    plus `--duplex` for `*-duplex`/`*-split`, as today.

## Step 4: Scenarios S6a and S6b

Extend the scenario table with an optional `net` description. An `x` that is
not a stressgen option (`rtt_ms`, `rate_mbit`) must only feed `configure_netem`
and never be passed to stressgen.

| Key | Title | x and values | quick_values | netem | stressgen base |
|---|---|---|---|---|---|
| `S6a` | WAN churn | `rtt_ms`: 0, 4, 20, 50 | 0, 20 | delay `rtt_ms/2` on each end, no rate | `--peers 32 --rate 100 --size 256 --churn 16` (16 reconnects/s in total) |
| `S6b` | WAN bottleneck | `rate_mbit`: 1000, 200, 50 | 1000, 100 | delay 2 ms on each end (RTT 4 ms); rate on `veth-node` only; `limit` about 2×BDP in packets, at least 100 | `--peers 32 --rate 0 --window 8 --size 16384` |

Why S6b differs from the original draft:

- **Open-loop load was too high.** The draft used `--rate 200` with 32 peers ×
  16 KiB, which offers about 840 Mbit/s: 0.84 of a 1 Gbit link and 4–17 times
  over capacity at 200 and 50 Mbit. Open-loop overload measures netem's queue
  (a 10,000-packet limit is about 15 MB, or about 2.4 s at 50 Mbit) and
  stressgen's own drops (`send_skipped`), not the node.
- **Closed-loop instead.** A window (`--rate 0 --window 8`, like S5) limits the
  load to what the path can carry. It still keeps
  32 × 8 × 16 KiB ≈ 4 MiB in flight, well above the 4 ms BDP even at 1 Gbit,
  so the node's sockets stay full.
- **Rate on one side only.** With a symmetric rate the generator→node direction
  is limited as well. The node then never receives faster than it can send, and
  its tx buffers barely fill. Shaping only node→generator puts the bottleneck
  where hypothesis 2 needs it.

Everything else is the same as S1–S5:

- the 5-file cell layout under `<out>/<scenario>/<x>=<value>/<variant>/rep<N>.*`;
- `--max-peers` from `max_peers_for()`;
- `--backlog=4096`;
- CPU pinning;
- the resume rule.

`--quick` keeps its peer cap of 16 and its 2 s duration. Make sure the stressgen
timeout allows for the extra RTTs.

## Step 5: analyze.py and the docs

1. **`scripts/analyze.py`:** add these metrics to `METRICS` and `cell_metrics`.
   `summary.csv` and the report pick them up automatically.
   - From stress.json: `handshake_p50`, `handshake_p99`, `connect_failures`,
     `churn_events`, `send_skipped`.
   - From metrics.jsonl (last sample): `tx_queued_sends`, `tx_flushes`, and
     `accepts` as a sanity check.
   - Already present: `cpu_pct`, `nvcsw_rate`, `bp_drops`, RTT and throughput.

   `CHART_KEYS` applies to every scenario. Either add a per-scenario chart list
   in `SCENARIOS` (preferred: S6a charts `handshake_p50/p99`, `msgs_per_s`,
   `rtt_p99`; S6b charts `msgs_per_s`, `rtt_p50/p99/p999`, `tx_queued_sends`,
   `cpu_pct`, `nvcsw_rate`), or add them globally. Extend
   `scripts/test_analyze.py` to cover the new fields.
2. **`docs/test_suite.md`:**
   - Add S6a and S6b to the scenario table in section 8, "Benchmark matrix".
   - Add a paragraph on the namespace topology: unprivileged `unshare`,
     `10.200.1.1 ↔ 10.200.1.2`, MTU 1500, offloads off, netem with the rate on
     the node side only.
   - List the new fields in section 6, "Measurements".

## Step 6: Verify

Only run these once no other benchmark is running (see Constraints).

```bash
cmake -B build -S . && cmake --build build -j -- -k   # test_terminal.cpp is known broken
(cd build && ctest --output-on-failure)
python3 scripts/test_analyze.py
python3 scripts/run_matrix.py --app build/app --stressgen build/stressgen \
    --out results_s6_test --base-port 23000 \
    --scenarios S6a,S6b --variants poll,epoll,epoll-duplex,epoll-split --quick --reps 1
python3 scripts/analyze.py results_s6_test
```

Confirm all of the following:

- Every `results_s6_test/**/rep1.meta.json` has `"status": "ok"`.
- **Simplex variants work across the link:** `poll` and `epoll` complete
  handshakes and churn redials across `10.200.1.1 ↔ 10.200.1.2` with
  `connect_failures == 0`.
- **The two hypotheses show up:**
  - S6a at `rtt_ms=20`: `handshake_p50` is about 60 ms for simplex and about
    40 ms for duplex.
  - S6b: `tx_queued_sends > 0`.
- **netem was applied:** `meta.json` records the qdiscs.
- **Nothing is left behind:** after the run, `ip netns list` and `ip link`
  in the normal namespace show no `ns_p2p_gen` or `veth-*`.
- **No change to S1–S5:** a quick run, e.g.
  `--scenarios S1,S5 --quick --reps 1`, gives the same cell layout as before.

Report the commit(s), test results, the smoke-run summary, and every deviation
from this prompt.
