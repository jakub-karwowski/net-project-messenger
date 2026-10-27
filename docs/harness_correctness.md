
# Benchmark harness correctness report

> **Status of the findings at `23f4335` (2026-10-27).** The report below reviews
> `fca6b9e`, and its line links point at that version's code. Since then:
>
> | Finding | Status | Where |
> |---|---|---|
> | F1, collapsed or partial cells counted as `ok` | **fixed**: per-cell validity gate; failing cells are `degraded` and excluded from medians | `8756f4e` (`analyze.validity`) |
> | F1b, a hung node kept as `dirty` data | **fixed**: the stall check in the gate, plus a hang dump before kill. **The node bug itself is fixed too**: the port-0 race in the sender | `8756f4e`, `77f6be1` |
> | F2, S6a at 50 ms RTT measures the blocking sender | **addressed in the experiment**: RTT points 30 and 40 ms, S6c at half the churn, `sender_q_hwm` charted; collapsed cells are flagged by the gate | `8756f4e` |
> | F3, S3 slow peers not like-for-like | **fixed**: `--slow-rcvbuf` (4 KiB), `slow_unread_bytes`, a backpressure self-test | `2bb579b` |
> | F4, builds mixed silently | **fixed**: a resume guard (`--force-mixed`) and a build census and banner in the report | `8756f4e` |
> | F5, node exit code ignored | **fixed**: `node_rc`, a real `bye` required | `8756f4e` |
> | F6, window anchored to the first frame | **fixed**: stressgen exports its window on the node's clock; the analysis uses it | `2bb579b`, `8756f4e` |
> | F7, `lost` clamped | **fixed**: `received_raw`, `duplicates` | `2bb579b` |
> | F9, netem drops not measured | **fixed**: `netem_stats` per S6 cell; counters reset between cells | `8756f4e` |
> | F10, shuffle not reproducible | **fixed**: seeded per cell; the order is recorded | `8756f4e` |
> | F11, handshake survivorship and stale-Ack race | **fixed**: `handshake_failures`; stale Acks ignored | `2bb579b` |
> | F12, generator cost not recorded | **fixed**: `gen_cpu`, `sched_lag_max_us` | `2bb579b` |
> | F8, offered load depends on the system under test during churn | open (interpretation) | — |
> | F13, cosmetic | partly (churn no longer resets slow peers) | `2bb579b` |
>
> Missing tests added: a histogram quantile bound, a partial ramp, slow-peer
> backpressure and duplicates (`2bb579b`); `load()`, `aggregate()` and the
> report writers (`8756f4e`).

Commit under review: `fca6b9e` (branch `io-variants`). The system under test is the **measurement harness**, not the chat node. That means the load generator (`stressgen`), the WAN lab (`scripts/netlab.py` and the S6 parts of `run_matrix.py`), and the runner and analysis scripts (`run_matrix.py`, `analyze.py`).

## 0. Scope and method

**Read:** `stressgen.hpp`, `stressgen.cpp`, `test_stressgen.cpp`, `scripts/netlab.py`, `scripts/run_matrix.py`, `scripts/analyze.py`, `scripts/test_analyze.py`, `tests/e2e/test_e2e.py`, `tools/node.py`, `docs/test_suite.md` (sections 6 and 8), `docs/experiments.md`. Where a claim depends on node behaviour I also read `ThreadWorkers.cpp`, `BlockingOutbound.cpp`, `PoolOutbound.hpp`, `TcpSocket/TcpSocket.cpp` and `MultiplexedIo.cpp`.

**Constraint:** a benchmark (`results-fca6b9e/`) was running. Nothing was built and no binary or test was executed. `results-fca6b9e/`, `results-smoke-fca6b9e/`, `logs/` and the tmux session were not touched.

**Checks actually run** (all under `nice -n 19`, scratch dir `/tmp/claude-1000/-home-jakub-net-project/e5c096fa-542a-4bc6-bace-9bbc9c6a0868/scratchpad/`):
- `python3 scripts/test_analyze.py` (prints `ok`).
- `run_matrix.py --dry-run` and `--fake ... --out <scratch>`, then `analyze.py <scratch>`.
- `python3 -I` snippets that import the scripts to exercise pure functions.
- `run_cell` driven with a stubbed `Node` and `subprocess.run`, so no process was spawned.
- A Python transcription of the stressgen `Histogram`. This is the algorithm, not the binary.
- Read-only analysis of the finished run `results-9ba50b3/`: 696 cells, 8 variants, commit `9ba50b3`, one build stamp. Everything labelled "data" comes from there.

**Consequence:** stressgen runtime behaviour is established by code reading, by its self-test as written (not re-run), and by the stress and metrics files of the earlier run.

Status vocabulary: *proven by test*, *proven by argument*, *checked with data*, *not covered*, *BUG*.

## 1. Summary

| # | Component | Property | Status |
|---|---|---|---|
| A1 | stressgen | Body format; RTT = receive − embedded send time, same clock | proven by test + argument; checked with data |
| A2 | stressgen | Only window-sent messages count (warmup, slow, idle excluded; grace for late echoes) | proven by argument; partly by test (exclusion and grace untested) |
| A3 | stressgen | `lost = sent − received`; received ≤ sent; duplicates | checked with data; clamp masks duplicates (F7) |
| A4 | stressgen | `msgs_per_s = received / duration` | proven by argument; checked with data |
| A5 | stressgen | Histogram bucketing; over-estimate < 3.125 %; upper-edge rule | proven by argument + transcription; **no C++ test** |
| A6 | stressgen | `handshake_us` timing and window rule | proven by argument; rule not tested; survivorship bias (F11) |
| A7 | stressgen | Ready: return connection + Ack token (simplex) / Ack on dialed socket (duplex) | proven by test + data |
| A8 | stressgen | Slow peers stop reading in both shapes | code: proven by argument; behaviour: **not covered**; S3 duplex anomaly unexplained (F3) |
| A9 | stressgen | Churn implementation and rate | proven by argument; checked with data |
| A10 | stressgen | Idle peers send nothing | proven by argument; checked with data |
| A11 | stressgen | Open loop `--rate` vs closed loop `--window` | proven by argument; partly by test; closed-loop saturation not covered |
| A12 | stressgen | Exit codes | proven by argument; only rc 2 tested; **partial ramp exits 0 (F1)** |
| B1 | S6/netlab | Unprivileged namespace isolation, cleanup | proven by argument; checked with data |
| B2 | S6/netlab | Address plan, MTU, offload off | proven by argument; checked with data |
| B3 | S6/netlab | Delay split per direction, RTT = sum | proven by argument; checked with data |
| B4 | S6/netlab | Rate on node egress only; `limit` formula | proven by argument; checked with data; netem drops **not measured** (F9) |
| B5 | S6/netlab | qdisc recorded in `meta.json` | checked with data |
| B6 | S6/netlab | S1–S5/S7 unaffected by S6 machinery | proven by argument; checked by command capture |
| B7 | S6/netlab | S6a rtt=50 simplex anomaly | explained (about 85 % confidence): node head-of-line blocking; harness accepted cells as `ok` (F1, F2) |
| C1 | run_matrix | Scenario table, `cell_params`, `max_peers_for`, S7 override | checked (executed) |
| C2 | run_matrix | Correct flags for every cell | checked (command capture of all cells) |
| C3 | run_matrix | Base ports never collide within a run | proven by argument |
| C4 | run_matrix | Readiness and quit handling; ok/dirty/failed semantics | proven by argument; checked with data; **node exit code ignored (F5)** |
| C5 | run_matrix | Resume: never reruns ok/dirty; retries failed | checked (fake run) |
| C6 | run_matrix | Build stamp and Release guard; no silent build mixing | guard proven; **mixing neither prevented nor detected (F4)** |
| C7 | run_matrix | Variant shuffling | proven by argument; order not reproducible or recorded (F10) |
| C8 | run_matrix | Timeout formula, `--quit-timeout`, CPU pinning | proven by argument |
| D1 | analyze | `percentile`, `median_iqr` | proven by test; cross-checked |
| D2 | analyze | `metrics_window` and edge cases | proven by test (3 cases); checked with data; **window start bias (F6)** |
| D3 | analyze | CPU % and ctx-switch rate formulas | proven by test |
| D4 | analyze | Peaks and finals | proven by test; semantic caveats |
| D5 | analyze | Dirty kept, failed listed | checked (fake run); **dirty ≠ valid (F1b)** |
| D6 | analyze | Missing fields from older results | proven by test; failure path checked |
| D7 | analyze | Aggregation, charts | partly checked; charts and `load`/`aggregate` **not covered** |

---

## 2. Load generator: `stressgen`

### A1. Message body and RTT computation

**(a) Claim.** Each chat message carries its own send timestamp (`steady_clock` ns) in the body. RTT is the `steady_clock` reading at receive minus that timestamp. Both readings come from one process and one monotonic clock, so there is no clock-offset error and RTT cannot be negative.

**(b, c) Code.** [stressgen.hpp:450-463](../stressgen.hpp#L450-L463)

```cpp
inline void Generator::send_one(int i, std::int64_t now) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (p.out.size() - p.out_off > (1u << 20)) { ++res_->send_skipped; return; }
    char head[64];
    const int hl = std::snprintf(head, sizeof head, "%u:%llu:%lld:", static_cast<unsigned>(i),
                                 static_cast<unsigned long long>(p.seq++), static_cast<long long>(now));
    std::string body(head, static_cast<std::size_t>(hl));
    if (static_cast<int>(body.size()) < cfg_.size) { body.resize(static_cast<std::size_t>(cfg_.size), 'x'); }
    wire::encode_into(p.out, wire::make_chat_message(0, p.token, std::move(body)));
    ++p.inflight;
    ++res_->total_sent;
    if (p.kind == Kind::Slow) { ++res_->slow_sent; }
    else if (now >= ms_ && now < me_) { ++res_->sent; ++sent_meas_; }
}
```

[stressgen.hpp:360-371](../stressgen.hpp#L360-L371)

```cpp
inline void Generator::on_message(int peer, const wire::ChatMessage& m, std::int64_t now) {
    Peer& p = peers_[static_cast<std::size_t>(peer)];
    unsigned pid = 0; unsigned long long seq = 0; long long ts = 0;
    if (std::sscanf(m.body.c_str(), "%u:%llu:%lld:", &pid, &seq, &ts) != 3) { return; }
    ++res_->total_received;
    if (p.inflight > 0) { --p.inflight; }
    if (p.kind == Kind::Slow) { return; }
    if (ts >= ms_ && ts < me_) {
        ++res_->received;
        res_->rtt.record(now - ts);
    }
}
```

**(d) Why it holds.**
- `now` comes from `now_ns()` ([stressgen.hpp:42-45](../stressgen.hpp#L42-L45), `steady_clock`). The receive paths call the same function (`const std::int64_t now = now_ns();` in `on_ret_conn` and `on_duplex_read`).
- `sscanf` returns 3 only for a well-formed `idx:seq:ts:` prefix, so a malformed echo is ignored. The `'x'` padding cannot be mistaken for the header.
- `--size` is a lower bound: the body is never shorter than the header. The header is at most about 30 B in every scenario, below the smallest `--size` of 32.
- Caveat: `now` is the loop-iteration time handed to `tick_send`, not the instant `send()` runs. Waiting in the generator's own output buffer is therefore included in RTT. That is conservative and matches the documented "RTT includes stressgen's own processing".
- No coordinated-omission correction is applied (see A11).

**(e) Evidence.**
- Test: [test_stressgen.cpp:134-135](../test_stressgen.cpp#L134-L135): one RTT sample per echo, and p50 below 50 ms.
- Data: `rtt_us.samples == received` in all 696 cells.
- Wire size: S5 `size=32` frames are 60 B on the wire (`bytes_in/frames_in = 59.99997`) and S3 `size=64` frames are 92 B. The 28 B overhead is constant, so the body is as configured.
- Clock sanity on the real node: S6a echo RTT p50 for `epoll-duplex` is 688 / 4719 / 20972 / 51380 µs at netem RTT 0 / 4 / 20 / 50 ms. That is the netem RTT plus about 0.7 ms.

**Status:** proven by test + argument; checked with data.

### A2. Which messages count

**(a) Claim.** `sent`, `received` and the RTT histogram cover only messages whose send time lies in the half-open window `[ms_, me_)`. Warmup traffic, slow peers and idle peers are excluded. Echoes arriving up to `--grace` after the window still count.

**(b, c) Code.** [stressgen.hpp:581-583](../stressgen.hpp#L581-L583) and [:597-602](../stressgen.hpp#L597-L602) (the per-message tests are the last lines of the A1 snippets).

```cpp
                ms_ = now + static_cast<std::int64_t>(cfg_.warmup * 1e9);
                me_ = ms_ + static_cast<std::int64_t>(cfg_.duration * 1e9);
                send_end = me_;
```

```cpp
        } else if (phase == Phase::Grace) {
            for (std::size_t k = 0; k < peers_.size(); ++k) {
                if (peers_[k].state != State::Dead && !peers_[k].out.empty()) { flush(static_cast<int>(k)); }
            }
            if (res.received >= sent_meas_ || now >= grace_end) { phase = Phase::Done; }
        }
```

**(d) Why it holds.**
- Both sides test the same number (the `now` at send is the `ts` that returns) against the same bounds. A message is therefore in `sent` exactly when its echo is eligible for `received`.
- Before the ramp ends `ms_ == me_ == 0`, so `ts >= 0 && ts < 0` is false and nothing is counted.
- Slow peers return early in `on_message` and `send_one`. Idle peers are skipped in `tick_send` (A10).
- The grace phase ends when all window messages are back or `grace_end` passes. The matrix passes `--grace 1.0` ([run_matrix.py:154](../scripts/run_matrix.py#L154) `GRACE`; stressgen's default is 2). An echo later than that is counted in `lost`, so grace is a validity parameter (F9).

**(e) Evidence.**
- Test: [test_stressgen.cpp:132-134](../test_stressgen.cpp#L132-L134): `sent ≈ peers·rate·duration` and `received == sent`.
- **Not covered:** no test asserts that warmup sends are excluded or that grace captures a late echo.
- Data: see A3.

**Status:** proven by argument; partly by test.

### A3. `lost`, `received > sent`, duplicates

**(a) Claim.** `lost = sent − received`, and `received ≤ sent` provided the node echoes each message at most once.

**(b, c) Code.** [stressgen.hpp:604-609](../stressgen.hpp#L604-L609)

```cpp
    for (const Peer& p : peers_) { if (p.state == State::Ready) { ++res.peers_ready; } }
    res.lost = res.sent - std::min(res.sent, res.received);
    res.measured_seconds = cfg_.duration;
    res.msgs_per_s = cfg_.duration > 0 ? static_cast<double>(res.received) / cfg_.duration : 0;
    res.end_wall_ns = wall_ns();
    return 0;
```

**(d) Why it holds, and its limit.**
- `received` grows only in `on_message`, once per parsed echo. There is no deduplication; `seq` is parsed and then ignored.
- A node that echoed twice would push `received` above `sent`. The clamp would then report `lost = 0` and hide it (F7).
- The node's echo bounces each message once ([ThreadWorkers.cpp:1158-1169](../ThreadWorkers.cpp#L1158-L1169)).
- During churn an echo produced after a redial arrives once, on the new return connection.

**(e) Evidence.**
- Tests: [test_stressgen.cpp:133](../test_stressgen.cpp#L133), [:160](../test_stressgen.cpp#L160), [:185](../test_stressgen.cpp#L185).
- Data, my script over all 696 `*.stress.json`:

```
696 cells: received>sent: 0 | lost != sent-received: 0 | rtt.samples != received: 0
           send_skipped>0: 0 | bad_frames>0: 0 | connect_failures>0: 0
           lost>0: 157 (S1 1024 threads/pool-sender, all S4 churn, all S6a churn -- expected)
```

**Status:** checked with data; low masking risk (F7).

### A4. `msgs_per_s` definition

**(a) Claim.** `msgs_per_s = received / duration`. `duration` is the configured window length. `received` counts window-sent messages echoed by the end of the grace.

**(b, c) Code.** [stressgen.hpp:607](../stressgen.hpp#L607)

```cpp
    res.msgs_per_s = cfg_.duration > 0 ? static_cast<double>(res.received) / cfg_.duration : 0;
```

**(d) Why.** This is the rate of on-time echoes of window-sent messages, not the node's instantaneous goodput.
- In overload it under-reports the node's work. Echoes that never return within window + grace are not counted.
- For a closed loop it equals throughput, because in-flight at both window edges is the same.
- If peers cannot send (not Ready), the offered load itself drops (F8).

**(e) Evidence (data).**
- All multiplexed variants at S1 1024 peers report exactly `102400.0`, meaning the generator and node both kept up. The collapsed `threads` and `pool-sender` report 10,020–16,163.
- S6b at 1000 Mbit: 7222 msg/s × 16384 B × 8 = 947 Mbit, consistent with the 1 Gbit shaper.

**Status:** proven by argument; checked with data.

### A5. Histogram bucketing and quantile accuracy

**(a) Claim.**
- Values below 64 ns are stored exactly. Larger values go into 32 sub-buckets per power of two, so a bucket is at most 1/32 of its lower edge wide.
- `quantile(q)` returns the **upper edge** of the bucket containing rank ⌈q·n⌉, capped at the true maximum.
- A reported quantile is therefore never below the true value of that rank and is less than 3.125 % above it.
- `max` is exact. `mean` is exact (a running double sum).

**(b, c) Code.** [stressgen.hpp:86-110](../stressgen.hpp#L86-L110)

```cpp
    // Upper edge of the bucket holding the q-quantile, in ns.
    std::int64_t quantile(double q) const noexcept {
        if (count_ == 0) { return 0; }
        auto target = static_cast<std::uint64_t>(q * static_cast<double>(count_));
        if (static_cast<double>(target) < q * static_cast<double>(count_)) { ++target; }
        target = std::max<std::uint64_t>(target, 1);
        std::uint64_t seen = 0;
        for (std::size_t i = 0; i < buckets_.size(); ++i) {
            seen += buckets_[i];
            if (seen >= target) { return std::min<std::int64_t>(upper(i), max_); }
        }
        return max_;
    }
private:
    static std::size_t index(std::uint64_t v) noexcept {
        const int msb = 63 - std::countl_zero(v | 1);
        const int shift = msb > 5 ? msb - 5 : 0;
        return std::min<std::size_t>(static_cast<std::size_t>(shift) * 32 + static_cast<std::size_t>(v >> shift),
                                     64 * 32 - 1);
    }
    static std::int64_t upper(std::size_t idx) noexcept {
        const std::size_t shift = idx < 64 ? 0 : idx / 32 - 1;
        const std::uint64_t base = static_cast<std::uint64_t>(idx - shift * 32) << shift;
        return static_cast<std::int64_t>(base + ((1ull << shift) - 1));
    }
```

**(d) Why it holds.**
- *Index.* For `v < 64`, `shift = 0` and `index = v` (exact, 1 ns). For `v ≥ 64`, `shift = msb − 5` and `v >> shift ∈ [32,63]`, so `index ∈ [32(shift+1), 32(shift+1)+31]`. Consecutive shifts tile the index space with no gap (`v=63 → 63`, `v=64 → 64`).
- *Upper edge.* `upper(idx)` inverts it: `shift = idx/32 − 1`, `lower = (idx − 32·shift) << shift`, `upper = lower + 2^shift − 1`. This is the largest value mapping to the bucket, so `upper ≥ v` for every sample in it. Also `upper − v ≤ 2^shift − 1 < lower/32 ≤ v/32`, so the relative over-estimate is strictly below 1/32.
- *Rank.* `ceil(q·n)` is computed in `double` and floored at 1. For q ∈ {0.5, 0.9, 0.99, 0.999} and n ≤ 200,000 it equals the exact decimal ceiling.
- *Cap.* The 2048-bucket clamp corresponds to values above 2^63 ns, which is unreachable.

**(e) Evidence.** There is **no C++ test of accuracy**. [test_stressgen.cpp:136](../test_stressgen.cpp#L136) only checks monotonicity (`p99 ≥ p50`, `max ≥ p99.9`). I transcribed the three functions to Python line for line and ran this (transcription, not the binary):

```
$ nice -n 19 python3 -I scratchpad/hist.py
values outside own bucket: 0  worst (upper-v)/v = 0.03125 (1/32 = 0.03125)
index monotone on 0..2e6: True
1000 0.5 exact 164865 hist 167935 rel err 0.0186
1000 0.9 exact 727052 hist 737279 rel err 0.0141
1000 0.99 exact 2839828 hist 2883583 rel err 0.0154
1000 0.999 exact 4698068 hist 4718591 rel err 0.0044
100000 0.5 exact 161226 hist 163839 rel err 0.0162
100000 0.9 exact 755046 hist 770047 rel err 0.0199
100000 0.99 exact 2595204 hist 2621439 rel err 0.0101
100000 0.999 exact 6719459 hist 6815743 rel err 0.0143
single sample 1234567 -> p50 1234567 (capped by max)
rank != ceil(q*n), q in .5/.9/.99/.999, n<=200000: 0 mismatches
```

- The 0.03125 is the asymptotic bound for a sample at a bucket's lower edge. Typical errors are 0–2 %, always positive.
- Data cross-check: the S6a rtt=50 handshake percentiles print as `3221225.5 µs` = 3·2^30 ns, exactly a bucket upper edge.

**Status:** proven by argument + transcription; no C++ test. Suggested test: record 1..10⁶ and assert `0 ≤ (q − exact)/exact < 1/32`.

### A6. `handshake_us`: timing and window rule

**(a) Claim.** A sample is (Ready time − `dial_ns`). `dial_ns` is taken when `connect()` is started for the current attempt, either the initial ramp or a churn redial. It is recorded only if completion falls in the ramp (`ms_ == 0`) or in `[ms_, me_)`. Warmup completions, grace completions and slow peers are excluded.

**(b, c) Code.** [stressgen.hpp:263-269](../stressgen.hpp#L263-L269) (dial) and [:282-295](../stressgen.hpp#L282-L295)

```cpp
    const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof a);
    if (rc != 0 && errno != EINPROGRESS) { ::close(fd); ++res_->connect_failures; p.state = State::Dead; return; }
    p.wfd = fd;
    p.dial_ns = now_ns();
    p.state = State::Connecting;
    p.out.clear(); p.out_off = 0; p.want_out = true; p.no_read = false; p.inflight = 0;
```

```cpp
// A handshake counts when it completes during the ramp (no window yet, ms_ == 0)
// or inside the measurement window [ms_, me_); warmup and grace completions are
// excluded like for RTT.
inline void Generator::set_ready(Peer& p, std::int64_t now) {
    p.state = State::Ready;
    if (p.kind == Kind::Slow) { return; }
    if (ms_ == 0 || (now >= ms_ && now < me_)) { res_->handshake.record(now - p.dial_ns); }
}

inline void Generator::maybe_ready(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    if (cfg_.duplex) { return; }   // duplex: Ready is set when the Ack arrives
    if (p.state == State::Handshaking && p.rfd >= 0) { set_ready(p, now_ns()); }
}
```

**(d) Why it holds.**
- `dial_ns` is set right after the non-blocking `connect()` returns, so the sample covers the TCP round trip too.
- Simplex Ready needs the Ack to have been matched on a return connection (A7). Duplex Ready is set by the Ack on the dialed socket. `maybe_ready` is a no-op in duplex, so the paths cannot double-record.
- The rule matches [test_suite.md §6](test_suite.md) and [experiments.md §2](experiments.md).
- **Limits:**
  - Survivorship bias. Peers that become Ready after the ramp ended but before `ms_` are dropped. Peers still handshaking at the end give no sample. Read the histogram together with `peers_ready` (F11).
  - A stale Ack from a previous incarnation can attach to a new `Connecting` peer after a churn kill and make Ready fire early. This is a rare race.

**(e) Evidence.**
- Tests: [test_stressgen.cpp:137](../test_stressgen.cpp#L137), [:158](../test_stressgen.cpp#L158), [:172](../test_stressgen.cpp#L172), [:183](../test_stressgen.cpp#L183). They check that samples exist, are positive, and that churn adds samples. **The window-exclusion rule itself is not tested.**
- Data, S6a handshake p50:
  - duplex: 418 µs at RTT 0, 40.9 ms at 20, 102.8 ms at 50 (2·RTT).
  - simplex: 590 µs at 0, 61.9 ms at 20 (3·RTT).
  - Simplex at 50 ms deviates (B7).

**Status:** proven by argument; rule not covered; bias noted (F11).

### A7. Ready state: simplex vs `--duplex`

**(a) Claim.**
- Simplex: Ready when a return connection delivers a `ConnectInfoAck` with `ack_peer_id == token_i` for that peer.
- Duplex: Ready when the dialed socket delivers an Ack with `ack_peer_id == token`.

**(b, c) Code.** [stressgen.hpp:384-398](../stressgen.hpp#L384-L398) and [:429-430](../stressgen.hpp#L429-L430)

```cpp
                if (const auto* ack = wire::as_connect_ack(**item)) {
                    if (c.peer >= 0) { continue; }
                    auto it = by_token_.find(ack->ack_peer_id);
                    if (it == by_token_.end()) { continue; }
                    Peer& p = peers_[static_cast<std::size_t>(it->second)];
                    if (p.state == State::Dead) { close_conn(fd); return; }
                    if (p.rfd >= 0 && p.rfd != fd) { close_conn(p.rfd); }
                    c.peer = it->second;
                    p.rfd = fd;
                    if (p.kind == Kind::Slow) {   // accept, but never read again
                        ::epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
                        maybe_ready(it->second);
                        return;
                    }
                    maybe_ready(it->second);
```

```cpp
                if (const auto* ack = wire::as_connect_ack(**item)) {
                    if (ack->ack_peer_id == p.token && p.state == State::Handshaking) { set_ready(p, now); }
```

**(d) Why it holds.**
- `by_token_` maps `1,000,000 + i` to the peer ([stressgen.hpp:542-544](../stressgen.hpp#L542-L544)).
- The Ack is the first frame on an accepted connection and binds `c.peer`, so later echoes on it are attributed correctly.
- An Ack for a Dead peer closes the connection. A second return connection replaces the first.
- The argument order matches the real node. `establish_ack` sends `make_connect_ack(peer_id, assigned_peer_id)` ([ThreadWorkers.cpp:691](../ThreadWorkers.cpp#L691)), where `assigned_peer_id` is the name the peer gave, i.e. our token ([Wire.hpp:180-182](../Wire.hpp#L180-L182)). The self-test's fake node uses the same positions.
- In duplex there is no listener ([stressgen.hpp:229-233](../stressgen.hpp#L229-L233)). The advertised endpoint is ignored by the duplex node.

**(e) Evidence.**
- Tests: [test_stressgen.cpp:130](../test_stressgen.cpp#L130) (64/64 ready) and [:169](../test_stressgen.cpp#L169) (duplex, 32/32).
- Data: `peers_ready == peers_total` in every non-churn, non-S7 cell. The 72 exceptions are all S7 cap cells, where eviction is the point.
- Not covered: `--duplex` against a two-connection node. [test_suite.md](test_suite.md) "Known issues" says the fixture hung.

**Status:** proven by test + data.

### A8. Slow peers really stop reading (both shapes)

**(a) Claim.** A slow peer completes the handshake and keeps sending at the normal rate. It never reads application data again, so its receive buffer fills and the node's writes eventually back up.

**(b, c) Code, simplex.** [stressgen.hpp:393-397](../stressgen.hpp#L393-L397) (the return connection is removed from epoll right after its Ack):

```cpp
                    if (p.kind == Kind::Slow) {   // accept, but never read again
                        ::epoll_ctl(ep_, EPOLL_CTL_DEL, fd, nullptr);
                        maybe_ready(it->second);
                        return;
                    }
```

**Code, duplex.** [stressgen.hpp:297-301](../stressgen.hpp#L297-L301) and [:434-438](../stressgen.hpp#L434-L438) (EPOLLIN is masked on the dialed socket right after Ready, and `arm_write` never re-adds it):

```cpp
inline void Generator::arm_write(int i) {
    Peer& p = peers_[static_cast<std::size_t>(i)];
    const std::uint32_t in = p.no_read ? 0u : static_cast<std::uint32_t>(EPOLLIN);
    ctl(EPOLL_CTL_MOD, p.wfd, p.want_out ? (in | EPOLLOUT) : in);
}
```

```cpp
                if (p.kind == Kind::Slow && p.state == State::Ready && !p.no_read) {
                    p.no_read = true;   // accept the handshake, then never read again
                    arm_write(i);
                    return;
                }
```

**(d) Why it holds.** I enumerated every `recv()` call site:
- [:353](../stressgen.hpp#L353) is the simplex write socket, which only drains or detects EOF and is never the return socket.
- [:376](../stressgen.hpp#L376) is `on_ret_conn`, reachable only for fds still in epoll. The slow fd was deleted.
- [:421](../stressgen.hpp#L421) is `on_duplex_read`, entered only on `EPOLLIN`, which the slow socket's mask no longer has.

`flush()` goes through `arm_write`, which preserves `no_read`. `dial()` clears `no_read` only for a churn redial, and the next Ack re-arms it. So by code, **duplex slow peers stop reading exactly like simplex ones.** The unread echoes stay in the kernel.

**Does the data agree? No.** This is the S3 anomaly. Data: `results-9ba50b3/S3/slow_peers=4`, rep1. Each slow peer sent 5001 messages, so the node echoed about 5001 × 92 B ≈ 460 kB to each.

```
variant        tx_queued_sends  tx_flushes  send_failures  slow_sent
pool-sender          14092            0           0         20004
poll                 14092            0           0         20004
epoll                14051            2           0         20004
threads                  0            0           4         20004   (blocking send timed out, 500 ms)
poll-duplex              0            0           0         20004
epoll-duplex             0            0           0         20004
poll-split               0            0           0         20004
epoll-split              0            0           0         20004
```

- In the two-connection modes the node starts queueing about 3 s after the slow peers start sending. That is about 1500 echoes, or about 136 kB absorbed per socket.
- In every duplex/split cell the node's non-blocking writes **never** hit EAGAIN, for at least 460 kB per slow peer. `frames_out = frames_in − 20` (100,020), so all echoes were really written.
- stressgen has no code path that could read that data. **I conclude this is not a stressgen reading bug.** The difference is below stressgen: kernel socket buffering of the receiving socket, or the node's write path.
- I cannot name the mechanism without `ss -tmi` on a live slow socket. Hypotheses, in order of plausibility:
  1. The accepted return socket is limited by per-skb truesize. About 1.3 MB of rcvbuf over about 900 B per small skb gives about 1450 skbs, which matches the roughly 1500 echoes seen. The dialed duplex socket, which also sends, may coalesce small segments differently.
  2. Receive-window or ACK piggybacking differences on a socket that is also being written.
  3. A node-side duplex write-path difference (partly excluded by `frames_out`).
- The consequence does not depend on the cause. S3 duplex/split cells never exercise the slow-consumer path (`tx_queued_sends = 0`, `send_backpressure_drops = 0`). S3 therefore cannot compare the shapes (F3).

**(e) Evidence.**
- **Test: not covered.** The self-test checks only `slow_sent > 0` and `received > 0` ([test_stressgen.cpp:159](../test_stressgen.cpp#L159), [:184](../test_stressgen.cpp#L184)). Nothing checks that a slow socket's receive queue fills.
- `test_peer_threads.cpp:302` shows queueing with that test's own slow reader, not with stressgen.

**Status:** code proven by argument; duplex behaviour **not covered, anomaly open (F3)**.

### A9. Churn

**(a) Claim.** `--churn C` closes and redials one random Ready, non-idle peer every 1/C s, from the start of warmup until sending stops. The kill is not counted as a `disconnect`. The redial restarts a handshake with a new `dial_ns`.

**(b, c) Code.** [stressgen.hpp:491-508](../stressgen.hpp#L491-L508)

```cpp
inline void Generator::tick_churn(std::int64_t now) {
    if (cfg_.churn <= 0 || !sending_) { return; }
    const auto step = static_cast<std::int64_t>(1e9 / cfg_.churn);
    int budget = 16;
    while (churn_next_ <= now && budget-- > 0) {
        churn_next_ += step;
        std::vector<int> ready;
        for (std::size_t k = 0; k < peers_.size(); ++k) {
            if (peers_[k].state == State::Ready && peers_[k].kind != Kind::Idle) { ready.push_back(static_cast<int>(k)); }
        }
        if (ready.empty()) { continue; }
        const int i = ready[rng_() % ready.size()];
        kill_peer(i, false);
        ++res_->churn_events;
        dial(i);
    }
    if (churn_next_ < now - 1'000'000'000) { churn_next_ = now; }
}
```

**(d) Why it holds.**
- `churn_next_` advances by exactly `step` per event, so the long-run rate is C/s. Catch-up is capped at 16 per tick, and a stall over 1 s resets the clock.
- Events are skipped when no peer is Ready, so the effective rate drops when handshakes are slow. This is why S6a rtt=50 simplex shows 279 events instead of 400.
- `kill_peer(i, false)` does not touch `disconnects`, which counts node-initiated closes only.
- `kind != Idle` means slow peers can also be churned. That is cosmetic, since no scenario combines them.

**(e) Evidence.**
- Tests: [test_stressgen.cpp:157](../test_stressgen.cpp#L157), [:182](../test_stressgen.cpp#L182) (`churn_events > 10`).
- Data: 16/s over about 25 s gives **401** events in every healthy S6a cell (expected 400). S4 `churn=50` gives **1251** (expected 1250). S4 `churn=10` gives 251; the one hung node gave 161 (F1b).

**Status:** proven by argument; checked with data.

### A10. Idle peers

**(a) Claim.** Idle peers do the handshake and then stay silent. They count in `peers_total` and `peers_ready` and in the handshake samples, but not in sent or RTT.

**(b, c) Code.** [stressgen.hpp:470](../stressgen.hpp#L470) (in `tick_send`):

```cpp
        if (p.state != State::Ready || p.kind == Kind::Idle) { continue; }
```

**(e) Evidence.** Data: S2 `idle_peers=1000` has `total_sent = 100,008` against 8 · 500 · 25 = 100,000. The extra 8 are one boundary tick per active peer. `peers_ready = 1008/1008` and there are 1008 handshake samples. Idle peers cannot be churned. **No dedicated test**: the mixed test uses 4 idle peers but asserts nothing about them.

**Status:** proven by argument; checked with data.

### A11. Open loop (`--rate`) vs closed loop (`--window`)

**(a) Claim.**
- `--rate R` with no window is open loop. Each Ready active or slow peer is offered one message every 1/R s regardless of echoes, with a catch-up budget of 256 and a guard that forgets a stall longer than 100 ms.
- `--rate 0 --window W` is closed loop. Each peer keeps at most W messages in flight and sends as soon as one returns.
- With both set, the window is an additional cap.

**(b, c) Code.** [stressgen.hpp:465-489](../stressgen.hpp#L465-L489)

```cpp
inline void Generator::tick_send(std::int64_t now) {
    const std::int64_t interval = cfg_.rate > 0 ? static_cast<std::int64_t>(1e9 / cfg_.rate) : 0;
    const std::uint64_t win = static_cast<std::uint64_t>(cfg_.window);
    for (std::size_t k = 0; k < peers_.size(); ++k) {
        Peer& p = peers_[k];
        if (p.state != State::Ready || p.kind == Kind::Idle) { continue; }
        const int i = static_cast<int>(k);
        if (interval > 0) {
            if (now - p.next_send > 100'000'000) { p.next_send = now; }   // don't replay a long stall
            int budget = 256;
            while (p.next_send <= now && budget-- > 0) {
                if (win && p.inflight >= win) { break; }
                send_one(i, now);
                p.next_send += interval;
            }
            if (win && p.inflight >= win && p.next_send < now) { p.next_send = now; }
        } else {
            int budget = 64;
            while (budget-- > 0 && (!win || p.inflight < win) && p.out.size() - p.out_off < (64u << 10)) {
                send_one(i, now);
            }
        }
        if (!p.out.empty()) { flush(i); }
    }
}
```

**(d) Why it holds.**
- `inflight` is incremented in `send_one`, decremented on every parsed echo, and reset on `dial` and `kill_peer`. With `win` set, the `inflight < win` test is inside the send loop, so the cap is exact.
- Limits:
  1. If the node drops an echo, the slot is never freed and the peer's window shrinks permanently (no timeout). No S5 or S6b cell showed this: `lost = 0` in `results-9ba50b3`.
  2. The 100 ms replay guard silently lowers offered load after a stall. The only trace is `sent` below `peers·rate·duration`, which analysis does not check (F8).
  3. stressgen is one thread. Data shows it sustains the S1 1024-peer open loop exactly (`sent = 2,048,000`). For the closed-loop S5 ceilings (up to about 215 k msg/s) there is no evidence that the node, rather than the generator, is the limit. The generator's own CPU and loop lag are not recorded (F12).

**(e) Evidence.**
- Tests: [test_stressgen.cpp:146-147](../test_stressgen.cpp#L146-L147) (window run completes, traffic flows, nothing lost). It does not assert `inflight ≤ window`.
- Data: `sent` equals `peers·rate·duration` to within 0.1 % in healthy S1–S4 cells.

**Status:** proven by argument; partly by test; closed-loop saturation not covered.

### A12. Exit codes

**(a) Claim.** `0` ok. `2` target unreachable (probe fails, or every connect fails). `3` no peer completed the handshake. `1` other (bad arguments, listen failure, cannot write `--out`). `run_matrix.py` treats any non-zero code as a failed cell.

**(b, c) Code.** [stressgen.hpp:574-580](../stressgen.hpp#L574-L580)

```cpp
            if (dial_cursor_ >= total && (pending == 0 || now >= ramp_end)) {
                if (ready == 0) {
                    err = res.connect_failures ? "target unreachable (all connects failed)"
                                               : cfg_.duplex ? "no peer completed the handshake (no ConnectInfoAck on the dialed socket)"
                                                         : "no peer completed the handshake (no ConnectInfoAck on return connection)";
                    return res.connect_failures ? 2 : 3;
                }
```

[stressgen.cpp:56-57](../stressgen.cpp#L56-L57) returns `rc` when non-zero.

**(d) Why, and the gap.** The failure test is `ready == 0`. **Any run with at least one Ready peer returns 0.** That includes S6a rtt=50, where 22 of 32 peers never completed (F1).

**(e) Evidence.** Test: [test_stressgen.cpp:194](../test_stressgen.cpp#L194) (rc 2). rc 3 and rc 1 are untested.

**Status:** proven by argument; rc 2 by test; partial-ramp gap (F1).

---

## 3. S6: `netlab.py` and the S6 parts of `run_matrix.py`

### B1. Unprivileged isolation and cleanup

**(a) Claim.** S6 cells run in a private user + network + mount namespace created without sudo, where the process is "root". Everything (namespace registry, veth pair, qdiscs) disappears when the child exits. No host interface or global state is touched.

**(b, c) Code.** [run_matrix.py:378-383](../scripts/run_matrix.py#L378-L383) and [netlab.py:43-46](../scripts/netlab.py#L43-L46)

```python
    cmd = ["unshare", "-rnm", "--propagation", "private", sys.executable, os.path.abspath(__file__),
           "--app", args.app, "--stressgen", args.stressgen, "--out", args.out,
           "--variants", ",".join(variants), "--scenarios", ",".join(wan_scs), "--reps", str(reps),
           "--node-cpus", args.node_cpus, "--gen-cpus", args.gen_cpus, "--base-port", str(args.base_port),
           "--ready-timeout", str(args.ready_timeout), "--quit-timeout", str(args.quit_timeout),
           "--seed", str(args.seed), "--_in_ns", "--_source", json.dumps(args.source)]
```

```python
    def setup(self):
        _run(["mount", "-t", "tmpfs", "none", "/run"])
        _run(["mkdir", "-p", "/run/netns"])
        _run(["ip", "netns", "add", NS])
```

**(d) Why it holds.**
- `unshare -rnm --propagation private` creates a new user namespace (uid mapped to 0, giving capabilities only over the new namespaces), a new network namespace, and a mount namespace with private propagation. The tmpfs on `/run` and the `ip netns add` bind mount therefore never reach the host.
- `ns_p2p_gen` is pinned only by a bind mount inside that mount namespace. The veths belong to the netns. When the child exits the last references drop and the kernel destroys all three, so there is no cleanup code to forget.
- Every cell does `qdisc replace` or `qdisc del … root`, so shaping state cannot leak between cells.
- Failure modes: a Ctrl-C reaches the child (same process group). A `kill -9` of the parent orphans a child that still cleans up on its own exit.

**(e) Evidence.** Data: all 168 S6 cells of `results-9ba50b3` ran this path with `netlab.warnings == []`. Not checked: the live host's `ip netns list`, which I avoided to keep the benchmark undisturbed.

**Status:** proven by argument; checked with data.

### B2. Address plan, MTU, offload

**(a) Claim.** Node and generator sit on a /24 across one veth pair, with MTU 1500 on both ends. TSO, GSO and GRO are off, so netem sees real MTU-sized packets. Otherwise a 64 kB GSO "packet" would be delayed and counted against `limit` as one packet.

**(b, c) Code.** [netlab.py:16-19](../scripts/netlab.py#L16-L19) and [:47-58](../scripts/netlab.py#L47-L58)

```python
NS = "ns_p2p_gen"
NODE_DEV, GEN_DEV = "veth-node", "veth-gen"
NODE_IP, GEN_IP = "10.200.1.1", "10.200.1.2"
MTU = 1500
```

```python
        _run(["ip", "link", "add", NODE_DEV, "mtu", str(MTU), "type", "veth",
              "peer", "name", GEN_DEV, "mtu", str(MTU), "netns", NS])
        _run(["ip", "addr", "add", f"{NODE_IP}/24", "dev", NODE_DEV])
        _run(in_gen(["ip", "addr", "add", f"{GEN_IP}/24", "dev", GEN_DEV]))
        _run(["ip", "link", "set", "lo", "up"])
        _run(["ip", "link", "set", NODE_DEV, "up"])
        _run(in_gen(["ip", "link", "set", "lo", "up"]))
        _run(in_gen(["ip", "link", "set", GEN_DEV, "up"]))
        for dev, pre in ((NODE_DEV, []), (GEN_DEV, in_gen([]))):
            r = _run(pre + ["ethtool", "-K", dev, "tso", "off", "gso", "off", "gro", "off"], check=False)
            if r.returncode != 0:
                self.warnings.append(f"ethtool -K {dev}: {(r.stderr or r.stdout).strip()}")
```

**(d) Why it holds.**
- Both addresses are on the same /24, so no routes are needed. The node binds `10.200.1.1` (`--address`) and stressgen listens on `10.200.1.2` (`--listen-ip`).
- `lo` is up in both namespaces.
- Offload is switched off on both ends. A failure is recorded in `meta["netlab"]["warnings"]`, not ignored.
- The `limit` formula in B4 assumes 1500 B packets, which is only true with offload off.

**(e) Evidence.** Data: S6 `meta.json` carries `"mtu": 1500` and `"warnings": []` in 168/168 cells. Not covered: no test of `NetLab`. Other offloads (checksum) are not examined.

**Status:** proven by argument; checked with data.

### B3. Delay split per direction; RTT = sum

**(a) Claim.** `veth-node` egress gets `node_delay_ms` and `veth-gen` egress gets `gen_delay_ms`. For S6a each is rtt/2, so a request and its reply experience rtt in total. A zero delay with no rate leaves the default qdisc.

**(b, c) Code.** [netlab.py:67-82](../scripts/netlab.py#L67-L82) and [run_matrix.py:141-145](../scripts/run_matrix.py#L141-L145)

```python
    def configure_netem(self, node_delay_ms, gen_delay_ms, node_rate_mbit=None, limit_pkts=None):
        """One-way delays per end (RTT = sum); the rate applies to veth-node egress
        (node -> generator) only. Delay 0 and no rate: default qdisc."""
        def args(delay, rate):
            if not delay and not rate:
                return None
            a = ["delay", f"{delay:g}ms"] if delay else []
            if rate:
                a += ["rate", f"{rate:g}mbit"]
            if limit_pkts:
                a += ["limit", str(int(limit_pkts))]
            return a
        self._tc(NODE_DEV, [], args(node_delay_ms, node_rate_mbit))
        self._tc(GEN_DEV, in_gen([]), args(gen_delay_ms, None))
        self.qdiscs = self.show_qdiscs()
        return self.qdiscs
```

```python
    "S6a": dict(title="WAN churn", x="rtt_ms", values=[0, 4, 20, 50],
                base=dict(peers=32, rate=100, size=256, churn=16, duration=20, warmup=5),
                quick_values=[0, 20],
                net=lambda rtt: dict(node_delay_ms=rtt / 2, gen_delay_ms=rtt / 2)),
```

**(d) Why it holds.**
- A root netem delays every packet leaving that device. A request and its reply each cross exactly one shaped device, so RTT = node + gen delay.
- `f"{delay:g}ms"` renders 25.0 as `25ms`.

**(e) Evidence.** Data: echo RTT p50 for `epoll-duplex` at S6a is 688 / 4719 / 20972 / 51380 µs for netem RTT 0 / 4 / 20 / 50 ms, i.e. the netem RTT plus the 0.7 ms loopback baseline. The handshake times (A6) show 2·RTT and 3·RTT, which is the same sum. The recorded qdisc strings show `delay 25ms` on both devices (B5).

**Status:** proven by argument; checked with data.

### B4. Rate on node egress only; `limit` formula

**(a) Claim.** In S6b the bottleneck applies to node → generator only. There is 2 ms delay each way (RTT 4 ms). The netem queue `limit` is about 2 × BDP, with a floor of 100 packets.

**(b, c) Code.** [run_matrix.py:133-137](../scripts/run_matrix.py#L133-L137) and [netlab.py:79-80](../scripts/netlab.py#L79-L80)

```python
def _s6b_net(rate_mbit):
    """Rate on the node->generator direction only; RTT 4 ms; netem limit ~2x BDP (>= 100 pkts)."""
    bdp_bytes = rate_mbit * 1e6 / 8 * 0.004
    return dict(node_delay_ms=2, gen_delay_ms=2, node_rate_mbit=rate_mbit,
                limit_pkts=max(100, math.ceil(2 * bdp_bytes / netlab.MTU)))
```

```python
        self._tc(NODE_DEV, [], args(node_delay_ms, node_rate_mbit))
        self._tc(GEN_DEV, in_gen([]), args(gen_delay_ms, None))
```

**(d) Why it holds.**
- `NODE_DEV` gets the rate and `GEN_DEV` gets `None`, so generator → node carries only the 2 ms delay.
- `limit_pkts = max(100, ceil(2·BDP/1500))` with BDP = rate/8 · 4 ms. Executed: `{1000: 667, 200: 134, 100: 100 (floor), 50: 100 (floor; formula gives 34)}`.

**(e) Evidence.**
- Data, S6b rep1 `epoll`: throughput is link-limited. `msgs_per_s·16384·8` is 947 / 190 / 47 Mbit at 1000 / 200 / 50 Mbit.
- The recorded qdiscs read `limit 667 delay 2ms rate 1Gbit`, `limit 134 … rate 200Mbit`, `limit 100 … rate 50Mbit`.

**Limits (F9).**
- The closed loop (32 peers × window 8 × 16 KiB = 4 MiB) exceeds BDP + `limit` at every rate, so netem may tail-drop and TCP recovers. The harness records no `tc -s qdisc` counters, so this is unmeasured.
- In a closed loop, RTT equals window ÷ throughput. At 50 Mbit the p50 RTT of 0.53–0.69 s is queue depth in the node's buffers, not per-message latency.

**Status:** proven by argument; checked with data; netem drops unmeasured.

### B5. qdisc recorded in `meta.json`

**(b, c) Code.** [run_matrix.py:248-254](../scripts/run_matrix.py#L248-L254)

```python
    wan = "net" in SCENARIOS[sc]
    if wan:
        if LAB is None:
            raise RuntimeError("S6 cells need the netlab namespace (run via run_matrix.py without --_in_ns)")
        meta["netem"] = dict(SCENARIOS[sc]["net"](x))
        LAB.configure_netem(**meta["netem"])
        meta["qdiscs"] = LAB.qdiscs
        meta["netlab"] = LAB.info()
```

**(e) Evidence.** Data, S6a rtt=50 `epoll` rep1: `"veth-node": "qdisc netem 8008: root refcnt 23 limit 1000 delay 25ms"` and `"veth-gen": "qdisc netem 8009: root refcnt 23 limit 1000 delay 25ms"`. All 168 S6 cells carry it. A `configure_netem` failure raises before the `try:`, so the whole child run aborts loudly with no half-written cell.

**Status:** checked with data.

### B6. S1–S5 and S7 unaffected by the S6 machinery

**(a) Claim.** Only scenarios with a `net` entry use the namespace, `--address`, `--listen-ip`, `ip netns exec` and netem. All others run in the parent on loopback, before the S6 child starts.

**(b, c) Code.** [run_matrix.py:371-377](../scripts/run_matrix.py#L371-L377) and [:450-459](../scripts/run_matrix.py#L450-L459)

```python
def run_with_wan(args, scs, wan_scs, variants, reps):
    """S1-S5 here, in this process; the S6 scenarios in a re-executed child under
    `unshare -rnm` (own network + mount namespace, uid mapped to root, gone on exit)."""
    rc = 0
    rest = [sc for sc in scs if sc not in wan_scs]
    if rest:
        rc = main_args(args, rest)
```

```python
    wan_scs = [sc for sc in scs if "net" in SCENARIOS[sc]]
    if wan_scs and not args.fake and not args.dry_run:
        if args._in_ns:
            global LAB
            LAB = netlab.NetLab()
            LAB.setup()
            print(f"netlab: {netlab.NS}, {netlab.NODE_DEV} {netlab.NODE_IP} <-> {netlab.GEN_DEV} {netlab.GEN_IP}"
                  + (f" (warnings: {LAB.warnings})" if LAB.warnings else ""), flush=True)
        else:
            return run_with_wan(args, scs, wan_scs, variants, reps)
```

**(d) Why it holds.** `wan = "net" in SCENARIOS[sc]` ([run_matrix.py:247](../scripts/run_matrix.py#L247)) gates every S6-specific argument. `LAB` stays `None` in the parent. The child is created only after `main_args(args, rest)` returns, so no namespace exists while S1–S5 and S7 run. The default order is `S1,S2,S3,S4,S5,S7,S6a,S6b`.

**(e) Evidence.** Command capture (`run_cell` with stubbed launch, my `flags_check.py`):
- No S1–S5 or S7 command contains `--address`, `--listen-ip` or `ip netns exec`. Every S6 command contains all three.
- S6a x=50 `epoll-duplex`:
  ```
  node: build/app --headless --echo --address=10.200.1.1 --io=epoll-duplex --max-peers=48 --backlog=4096 --port=21001 ...
  sg  : ip netns exec ns_p2p_gen taskset -c 4-7 build/stressgen --target 10.200.1.1:21001 --listen-ip 10.200.1.2 --peers 32 --rate 100 --size 256 --duration 20 --warmup 5 --idle-peers 0 --slow-peers 0 --churn 16 --grace 1.0 --out ... --duplex
  ```
- S1 x=1024 `threads`: `taskset -c 4-7 build/stressgen --target 127.0.0.1:21001 --peers 1024 --rate 100 ...` (no `--duplex`).

**Status:** proven by argument; checked by command capture.

### B7. The S6a rtt=50 simplex anomaly

**What the data says (`results-9ba50b3`).** For the four simplex variants (`threads`, `pool-sender`, `poll`, `epoll`) at `rtt_ms=50`, all 12 cells (4 variants × 3 reps) look like this:

```
stressgen: peers=10/32 sent=821 recv=0 lost=821 0 msg/s rtt_us p50=0 ...
handshake p50 = p99 = 3.22 s, 233 samples
```

- `peers_ready = 10/32`, `msgs_per_s = 0`, `sent ≈ 800` (about 1 % of the 64,000 expected), `churn_events = 279` (not 401), status `ok`.
- The four duplex variants at the same RTT are healthy (handshake p50 about 100 ms, about 3,100 msg/s). Simplex at RTT 0/4/20 is healthy (handshake p50 62 ms at RTT 20).
- The node log of such a cell has 21 `handshake answer failed` lines. All are timestamped `12:29:24`, which is the cell's `t_end`. They occur at shutdown, not during the run.

**Most likely cause (about 85 % confidence).** The node's single `SenderWorker` is head-of-line blocked by blocking `connect()` calls on the dial-back path. The 16/s churn saturates it at this RTT.

1. In all four simplex variants the dial-back runs on the sender thread through `TcpConnection::connect`, a plain **blocking** `::connect()`. [TcpSocket.cpp:270-273](../TcpSocket/TcpSocket.cpp#L270-L273):

```cpp
    if (::connect(client_fd, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
        close(client_fd);
        return std::unexpected(get_last_socket_error());
    }
```

   - It is called from `BlockingOutbound::open` ([BlockingOutbound.cpp:27](../BlockingOutbound.cpp#L27)) and `PoolOutbound::open` ([PoolOutbound.hpp:60](../PoolOutbound.hpp#L60)).
   - It is reached through `SenderWorker::establish_ack` ([ThreadWorkers.cpp:686-692](../ThreadWorkers.cpp#L686-L692)).
   - Every echo to every peer shares that one thread, and each blocking connect costs one network RTT, 50 ms here.
   - The duplex variants answer on the existing socket ([ThreadWorkers.cpp:668-684](../ThreadWorkers.cpp#L668-L684)) and never dial. That is why they are immune.
2. A churn event costs the sender about two blocking connects: the handshake answer, and a reconnect when the next echo to the re-appearing peer finds it `Disconnected` ([ThreadWorkers.cpp:814-857](../ThreadWorkers.cpp#L814-L857), logged `reconnected`). Metrics support this: `reconnects = 257` against `churn_events = 279`. Service time is therefore about 2·RTT: 100 ms at RTT 50 and 40 ms at RTT 20.
3. Offered demand is 16 churns/s.
   - At RTT 20, ρ = 16 × 0.040 = 0.64: stable, but the initial burst of 32 serialised connects gives handshake p99 = **654 ms = 32 × 20 ms**.
   - At RTT 50, ρ = 16 × 0.100 = **1.6 > 1**: unstable. In a closed population of N = 32 peers a saturated server gives W = N × S = 32 × 0.1 s = **3.2 s**. That is the observed 3.22 s (the 3·2^30 ns bucket edge) for essentially all 233 samples.
4. The node's queue confirms it. `sender_q_hwm` is as follows:

   | variant | RTT 0 | RTT 4 | RTT 20 | RTT 50 |
   |---|---|---|---|---|
   | `threads` | 33 | 33 | 499 | 2195 |
   | `pool-sender` | 34 | 338 | 483 | 2172 |
   | `epoll` | 332 | 344 | 509 | 2361 |
   | `epoll-duplex` | about 340 | about 340 | about 340 | about 340 |

   Echoes queue behind the connects for seconds, so no window-sent message is echoed by the end of grace and `received = 0`. The 21 shutdown warnings are the still-queued dial-backs failing once stressgen closed its listener.
5. Only about 10 of 32 peers are Ready at any instant. Churn draws from the Ready set, hence 279 events. Peers send only while Ready, hence `sent` collapses to 821.

**Ruled out from code and logs.**
- netem `limit`: default 1000, nowhere near full.
- Listener backlog: `listen(lfd_, 4096)` ([stressgen.hpp:242](../stressgen.hpp#L242)), and the accept loop drains until EAGAIN.
- SYN-retransmit stalls: the node log shows dial-backs *answered* within the first 2 s, and the 3.22 s is explained quantitatively above.
- The generator ramp: 32 peers, 10 s timeout.

**Uncertainty.** Nothing was run. "Two connects per churn" is inferred from code plus the `reconnects` counter. The saturation argument only needs service time above 62.5 ms. Direct confirmation would be `strace -T -e connect` on the sender thread, or sub-second node log timestamps.

**Is it a harness bug?** No, the harness did what was asked. But:
- The experimental point (16/s at 50 ms) lies beyond the node's capacity for these four variants, so the number measures overload, not WAN churn cost.
- The cells were reported `ok` with 10/32 peers, 1 % offered load and 0 msg/s (F1, F2).

**Status:** explained (high confidence); validity gate missing.

---

## 4. Runner: `run_matrix.py`

### C1. Scenario table, `cell_params`, `max_peers_for`, S7

**(a) Claim.** Each cell's parameters are the scenario `base` overlaid with the swept value, with idle, slow and churn defaulting to 0. `--quick` shrinks duration and peers. The node's `--max-peers` is `peers + idle + slow + 16`, unless the scenario sweeps the cap itself (S7).

**(b, c) Code.** [run_matrix.py:157-174](../scripts/run_matrix.py#L157-L174)

```python
def cell_params(sc, x, quick):
    p = dict(SCENARIOS[sc]["base"])
    if quick:
        p.update(QUICK_BASE)
        p["peers"] = min(p.get("peers", 1), QUICK_PEERS_CAP)
    p[SCENARIOS[sc]["x"]] = x
    p.setdefault("idle_peers", 0)
    p.setdefault("slow_peers", 0)
    p.setdefault("churn", 0)
    return p


def max_peers_for(p):
    """The node's --max-peers: an explicit cap (S7), else room for every peer so
    caps never evict."""
    if p.get("max_peers"):
        return int(p["max_peers"])
    return int(p["peers"] + p["idle_peers"] + p["slow_peers"] + 16)
```

**(d) Why it holds.** `p[x_name] = x` writes the swept value into the dict that feeds stressgen. For `rtt_ms`, `rate_mbit` and `max_peers` these keys are never passed to stressgen, because `run_cell` reads only named keys. For S7 `max_peers` is non-zero, so the cap is the swept value. Elsewhere the cap leaves 16 slots of slack, so it never evicts.

**(e) Evidence (executed).**

```
(value, --max-peers):
S1 peers [(1, 17), (8, 24), (64, 80), (256, 272), (1024, 1040)]
S2 idle_peers [(0, 24), (100, 124), (300, 324), (1000, 1024)]
S3 slow_peers [(0, 32), (1, 33), (4, 36)]
S4 churn [(0, 80), (10, 80), (50, 80)]
S5 size [(32, 24), (4096, 24), (65536, 24)]
S7 max_peers [(64, 64), (128, 128), (256, 256), (1040, 1040)]
S6a rtt_ms [(0, 48), (4, 48), (20, 48), (50, 48)]
S6b rate_mbit [(1000, 48), (200, 48), (50, 48)]
S7 quick x=8: {'peers': 16, ..., 'max_peers': 8, ...} -> 8
--dry-run: 261 cells (--reps 1; 29 values x 9 variants); 783 cells (--reps 3)
```

- The quick S7 value (8 with 16 peers) does force evictions, as intended. S7's 1040 equals S1's 1024+16, so it repeats the S1 1024 cell.
- Not covered: no unit test of these functions. Only the fake run exercises them.

**Status:** checked by execution.

### C2. Every cell gets the correct flags

**(a) Claim.** `--duplex` goes to stressgen exactly for `*-duplex`, `*-split` and `peer-threads`. `--window` is passed only when the scenario sets it. `--target` and `--listen-ip` use the veth addresses only for S6. The node gets `--echo --io=<variant> --max-peers --backlog=4096 --port --log --metrics`.

**(b, c) Code.** [run_matrix.py:28-30](../scripts/run_matrix.py#L28-L30) and [:255-278](../scripts/run_matrix.py#L255-L278)

```python
def is_duplex(variant):
    """duplex/split nodes answer on the dialed socket: stressgen --duplex, ~1 fd per peer."""
    return variant.endswith(("-duplex", "-split")) or variant == "peer-threads"
```

```python
    node = Node(["taskset", "-c", args.node_cpus, args.app, "--headless", "--echo",
                 *([f"--address={netlab.NODE_IP}"] if wan else []),
                 f"--io={variant}", f"--max-peers={max_peers_for(p)}", "--backlog=4096", f"--port={port}",
                 f"--log={pre}.node.log", f"--metrics={pre}.metrics.jsonl"])
    try:
        if not node.ready.wait(args.ready_timeout) or node.ready_err or node.p.poll() is not None:
            raise RuntimeError("node not ready: " + (str(node.ready_err) if node.ready_err
                               else "timeout/exited; " + " | ".join(node.tail)))
        sg = ["taskset", "-c", args.gen_cpus, args.stressgen,
              "--target", f"{netlab.NODE_IP if wan else '127.0.0.1'}:{port}",
              *(["--listen-ip", netlab.GEN_IP] if wan else []),
              "--peers", str(p["peers"]), "--rate", str(p["rate"]), "--size", str(p["size"]),
              "--duration", str(p["duration"]), "--warmup", str(p["warmup"]),
              "--idle-peers", str(p["idle_peers"]), "--slow-peers", str(p["slow_peers"]),
              "--churn", str(p["churn"]), "--grace", str(0.5 if args.quick else GRACE),
              "--out", f"{pre}.stress.json"]
        if wan:
            sg = netlab.in_gen(sg)
        if is_duplex(variant):
            sg += ["--duplex"]
        if p.get("window"):
            sg += ["--window", str(p["window"])]
```

**(e) Evidence (executed, launch stubbed).** I enumerated every (scenario, value) for `threads`, `pool-sender`, `epoll`, `epoll-duplex`, `poll-split` and `peer-threads`. Samples:

```
S5 65536 epoll-duplex  sg: ... --rate 0 --size 65536 ... --duplex --window 16   node: --io=epoll-duplex --max-peers=24 --backlog=4096
S5 65536 threads       sg: ... --rate 0 --size 65536 ... --window 16            (no --duplex)
S1 1024  peer-threads  sg: ... --peers 1024 --rate 100 --size 64 ... --duplex   node: --io=peer-threads --max-peers=1040
S3 4     epoll-duplex  sg: ... --peers 16 --rate 200 --slow-peers 4 ... --duplex   node: --max-peers=36
S4 50    threads       sg: ... --peers 64 --rate 50 ... --churn 50              node: --max-peers=80
S2 1000  threads       sg: ... --peers 8 --rate 500 ... --idle-peers 1000       node: --max-peers=1024
S7 1040  epoll-duplex  --peers 1024 ... --duplex                                node: --max-peers=1040
```

- `is_duplex` over all variants gives True only for `poll-duplex`, `epoll-duplex`, `poll-split`, `epoll-split`, `peer-threads`.
- `--window` appears only in S5 and S6b. `--duplex` is appended after `in_gen(sg)` but is still the tail of the stressgen arguments, so the namespace wrapper is unaffected.
- [test_suite.md §8](test_suite.md) lists "`*-duplex`/`*-split`" and omits `peer-threads` (cosmetic doc gap; the code is right).

**Status:** checked (command capture).

### C3. Base ports never collide within a run

**(b, c) Code.** [run_matrix.py:470-484](../scripts/run_matrix.py#L470-L484)

```python
    port = args.base_port
    counts = {}
    for i, (sc, x, v, rep) in enumerate(cells, 1):
        d = cell_dir(args.out, sc, x, v)
        tag = f"[{i}/{len(cells)}] {sc} {SCENARIOS[sc]['x']}={x} {v} rep{rep}"
        if is_done(d, rep):
            print(tag, "skip (done)")
            counts["skip"] = counts.get("skip", 0) + 1
            continue
        if args.dry_run:
            print(tag, "would run")
            continue
        d.mkdir(parents=True, exist_ok=True)
        port = port + 1 if port < 60000 else args.base_port + 1
```

**(d) Why it holds.**
- A run has at most 783 cells, so ports are 21001…21783. The wrap at 60000 is unreachable. All are below this host's ephemeral range (`/proc/sys/net/ipv4/ip_local_port_range` = `44620 48715`), so a client connection cannot take a node port.
- stressgen's own listener uses an ephemeral port (`--listen-port 0`).
- The S6 child uses the same base but runs after the parent finished, in a fresh netns.
- A resumed run does not consume ports for skipped cells, so it restarts the numbering and may reuse a port within a minute of the interrupted one. A busy port yields a node `ready` error and a `failed` cell that is retried next time. It does not produce a silently wrong measurement (`test_busy_port` shows the node reports `error`).

**Status:** proven by argument.

### C4. Readiness, quit handling, status semantics

**(a) Claim.** A cell is `ok` only if the node reported `ready` without error, stressgen exited 0, and the node answered `/quit` with `bye` and exited within `--quit-timeout`. If only the last condition fails it is `dirty` (data kept). Otherwise it is `failed`.

**(b, c) Code.** [run_matrix.py:216-238](../scripts/run_matrix.py#L216-L238) and [:283-296](../scripts/run_matrix.py#L283-L296)

```python
    def quit(self, timeout=30):
        """Sends /quit and waits (one deadline) for "bye" and the process exit.
        Returns dict(clean, bye, shutdown_s, killed): clean means both happened in
        time; shutdown_s is /quit -> exit (or -> kill), so a slow drain shows up as
        a number rather than only as a dirty flag."""
        t0 = time.monotonic()
        deadline = t0 + timeout
        sent = True
        try:
            self.p.stdin.write("/quit\n")
            self.p.stdin.flush()
        except Exception:
            sent = False
        bye = self.bye.wait(max(0.0, deadline - time.monotonic()))
        killed = False
        try:
            self.p.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
            killed = True
        return dict(clean=sent and bye and not killed, bye=bye,
                    shutdown_s=round(time.monotonic() - t0, 3), killed=killed)
```

```python
        meta["status"] = "ok"
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
    finally:
        q = node.quit(args.quit_timeout)
        meta.update(shutdown_s=q["shutdown_s"], bye=q["bye"], killed=q["killed"])
        if not q["clean"]:
            meta["node_unclean"] = True
            if meta["status"] == "ok":
                meta["status"] = "dirty"
                meta["error"] = (f"node did not exit cleanly after /quit "
                                 f"(bye={q['bye']}, killed={q['killed']}, {q['shutdown_s']}s)")
        meta["t_end"] = time.time()
        (d / f"rep{rep}.meta.json").write_text(json.dumps(meta, indent=1))
```

**(d) Why it holds.**
- One deadline covers both the `bye` wait and the process exit. On expiry the node is killed, so a hung node cannot stall the matrix. `shutdown_s` is always recorded.
- A readiness failure raises before stressgen starts, and `finally` still quits the node.
- **Gaps:**
  1. The reader thread sets `bye` at EOF too ([run_matrix.py:213-214](../scripts/run_matrix.py#L213-L214)), and the node's exit status is never read. A node that crashes during shutdown (EOF, no real `bye`) satisfies `clean`, and the cell stays `ok` (F5).
  2. `dirty` means "data kept", but a node that hung during measurement is also `dirty` (F1b).

**(e) Evidence.**
- Data: `results-9ba50b3` has 695 `ok` and 1 `dirty`. The dirty cell is `S4/churn=10/poll-split/rep3`, with `shutdown_s = 30.002` and `killed = true`.
- The e2e suite proves the node-side contract: [test_e2e.py:64-72](../tests/e2e/test_e2e.py#L64-L72) checks the `ready` event, `bye` last, and exit code 0. It goes through `tools/node.py`, a *different* driver from `run_matrix.Node`, which has no test.

**Status:** proven by argument; checked with data; gap F5.

### C5. Resume: never reruns `ok`/`dirty`, retries `failed`

**(b, c) Code.** [run_matrix.py:181-185](../scripts/run_matrix.py#L181-L185) and [:475-478](../scripts/run_matrix.py#L475-L478)

```python
def is_done(d, rep):
    try:
        return json.loads((d / f"rep{rep}.meta.json").read_text()).get("status") in ("ok", "dirty")
    except Exception:
        return False
```

```python
        if is_done(d, rep):
            print(tag, "skip (done)")
            counts["skip"] = counts.get("skip", 0) + 1
            continue
```

**(e) Evidence (executed on fake data).** After a full fake run (128 cells) I changed one meta to `failed`, one to `dirty`, emptied one metrics file and deleted one stress file, then reran the identical command:

```
$ nice -n 19 python3 scripts/run_matrix.py --fake --quick --reps 2 --variants threads,epoll,epoll-duplex,peer-threads --out $S/fake
... summary: {'ok': 128}
(after mutation, same command) -> only the 'failed' cell re-executed, everything else "skip (done)"
S1/peers=1/threads/rep1  ok None       (was failed)
S1/peers=8/epoll/rep1    dirty slow    (kept)
S1/peers=8/threads/rep2  ok None       (ok-but-missing-stress.json: NOT rerun)
```

- Resume behaves as documented. One consequence: an `ok` cell whose files were later damaged is never repaired. `analyze.py` lists it as `bad data`.
- A retried cell's files are overwritten: node log (`std::ofstream` truncates), metrics (`std::ios::trunc` at [Metrics.cpp:71](../Metrics.cpp#L71)) and `--out` JSON.

**Status:** checked (fake run).

### C6. Build stamp, Release guard, mixing builds

**(b, c) Code.** [run_matrix.py:73-100](../scripts/run_matrix.py#L73-L100) and [:432-444](../scripts/run_matrix.py#L432-L444)

```python
def build_type(path):
    """CMAKE_BUILD_TYPE of the build directory holding `path` (from its
    CMakeCache.txt), or None when it can't be found. A hash doesn't reveal an
    unoptimised binary; this does."""
    for d in Path(path).resolve().parents:
        cache = d / "CMakeCache.txt"
        if cache.exists():
            for line in cache.read_text(errors="replace").splitlines():
                if line.startswith("CMAKE_BUILD_TYPE:"):
                    return line.split("=", 1)[1].strip() or None
            return None
    return None


def binary_stamp(path):
    """Identity of one binary at the moment a cell starts (per cell, so a rebuild
    in the middle of a run shows up as a different hash)."""
    try:
        st = os.stat(path)
    except OSError:
        return dict(path=str(path), sha256=None)
    return dict(path=str(Path(path).resolve()), sha256=_sha256(path), size=st.st_size,
                mtime=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(st.st_mtime)),
                build_type=build_type(path))


def build_stamp(args):
    return dict(args.source, app=binary_stamp(args.app), stressgen=binary_stamp(args.stressgen))
```

```python
        for b in (args.app, args.stressgen):
            if not os.access(b, os.X_OK):
                sys.exit(f"not executable: {b}")
            bt = build_type(b)
            if bt != "Release" and not args.allow_non_release:
                sys.exit(f"{b} comes from a {bt or 'unknown'} build; benchmark numbers need "
                         f"Release (cmake -DCMAKE_BUILD_TYPE=Release), or pass --allow-non-release")
```

**(d) Why it holds.**
- `build_type` walks up from the binary to the nearest `CMakeCache.txt`. Anything but `Release`, including unknown, aborts unless `--allow-non-release`. The guard runs in the parent and again in the S6 child, because the flag is forwarded.
- `CMakeLists.txt` defaults to Release when unset ([CMakeLists.txt:6-7](../CMakeLists.txt#L6-L7)).
- Binary hashes are per cell. The git stamp (`commit`, `branch`, `dirty`, `dirty_files`, `harness_sha256`) is taken once at start with `--untracked-files=no`, so a new untracked source file would not mark the tree dirty.

**(e) Evidence.** Data: `build_type(build/app) = Release`, `build-int/app = Release`. `results-9ba50b3` has 696 cells with ONE distinct stamp: `('9ba50b3', dirty False, harness 3e7a0e0a, app adc070f0, stressgen e6430af8, Release)`.

**The gap.** Nothing uses the stamp. `is_done` ignores it, and `analyze.py` never reads `build`, so cells of different builds in one directory are not flagged. "Never mix builds" ([experiments.md §5](experiments.md)) is manual discipline (F4). The guard checks only `CMAKE_BUILD_TYPE`, not flags such as sanitizers.

**Status:** guard proven; mixing neither prevented nor detected (F4).

### C7. Variant shuffling

**(b, c) Code.** [run_matrix.py:460-469](../scripts/run_matrix.py#L460-L469)

```python
    rnd = random.Random(args.seed if args.fake else None)
    # Shuffle variant order within each (scenario, param, rep) to spread drift.
    cells = []
    for sc in scs:
        vals = SCENARIOS[sc]["quick_values"] if args.quick else SCENARIOS[sc]["values"]
        for x in vals:
            for rep in range(1, reps + 1):
                vs = variants[:]
                rnd.shuffle(vs)
                cells += [(sc, x, v, rep) for v in vs]
```

**(d) Why it holds.** The permutation is independent per (scenario, value, rep), so each variant's cells are interleaved with the others' over the roughly 13 minutes it takes to run 3 reps × 9 variants of one value. That spreads slow drift. In real runs the RNG is seeded from the OS, so the order **cannot be reproduced and is not recorded**. Only per-cell `t_start` reveals it (F10).

**Status:** proven by argument.

### C8. Timeout formula, `--quit-timeout`, CPU pinning

**(b, c) Code.** [run_matrix.py:277-279](../scripts/run_matrix.py#L277-L279)

```python
        limit = (p["duration"] + p["warmup"] + 60 + (p["peers"] + p["idle_peers"]) * 0.02
                 + (S6_EXTRA_TIMEOUT if wan else 0))
        r = subprocess.run(sg, capture_output=True, text=True, timeout=limit)
```

**(d) Why it holds.**
- The slowest legitimate stressgen run is probe (≤ 3 s) + ramp (≤ 10 s) + warmup + duration + grace (1 s). For S1 1024 that is about 39 s against a limit of 105 s. The timeout is a hang guard, not a truncation.
- `--quit-timeout` defaults to 30 s and is the `dirty` threshold.
- Pinning: the node runs as `taskset -c 0-1 app …` and stressgen as `taskset -c 4-7 …` (after `ip netns exec` in S6). Threads inherit the masks. The Python runner is unpinned but idle.
- Not inspected: the affinity of the running benchmark's processes, which I deliberately left alone.

**Status:** proven by argument. Not covered: softirq and netem work executing on node CPUs in S6 (section 7).

---

## 5. Analysis: `analyze.py`

### D1. `percentile`, `median_iqr`

**(b, c) Code.** [analyze.py:62-77](../scripts/analyze.py#L62-L77)

```python
def percentile(xs, q):
    """Linear-interpolated percentile, q in [0,100]. NaN for empty input."""
    xs = sorted(xs)
    if not xs:
        return float("nan")
    if len(xs) == 1:
        return float(xs[0])
    k = (len(xs) - 1) * q / 100.0
    lo = math.floor(k)
    hi = math.ceil(k)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def median_iqr(xs):
    xs = [x for x in xs if x is not None and not math.isnan(x)]
    return percentile(xs, 50), percentile(xs, 25), percentile(xs, 75)
```

**(d) Why.** This is linear interpolation between order statistics with `k = (n−1)q/100`, the standard "type 7" estimator. `None` and NaN are dropped first. An empty set gives NaN, rendered as `-`.

**(e) Evidence.** Tests: [test_analyze.py:13-21](../scripts/test_analyze.py#L13-L21). Executed cross-check: `median_iqr` against `statistics.quantiles(method="inclusive")` on random lists of length 2–39 gave **0 mismatches**. Edge cases: `percentile([3],50)=3.0`, `percentile([1,2],0)=1.0`, `percentile([1,2],100)=2.0`, `median_iqr([None]) = (nan, nan, nan)`.

**Status:** proven by test + cross-check.

### D2. `metrics_window`

**(a) Claim.** The node-side window is `[first_growth_sample − 1 + ceil(warmup), last_growth_sample]` in 1 Hz sample indices, where "growth" means `frames_in` increased.

**(b, c) Code.** [analyze.py:93-103](../scripts/analyze.py#L93-L103)

```python
def metrics_window(rows, warmup):
    """(start, end) sample indices of the measurement window: from `warmup`
    seconds after the first frames_in growth to the last growth."""
    grow = [i for i in range(1, len(rows)) if rows[i].get("frames_in", 0) > rows[i - 1].get("frames_in", 0)]
    if not grow:
        return None
    s = grow[0] - 1 + int(math.ceil(warmup))
    e = grow[-1]
    if e <= s:
        s = grow[0] - 1
    return (s, e) if e > s else None
```

**(d) Edge cases (executed).**

```
one growth sample, warmup 5:           (0, 1)    # falls back to the span of growth
one growth sample, warmup 0:           (0, 1)
growth right at sample 1, warmup 0.5:  (1, 3)
no frames_in key:                       None     # cpu/ctx fields become None; cell still loads
late growth (idle first), warmup 2:    (5, 9)    # follows traffic, not t=0
```

When warmup exceeds the traffic span the window falls back to the whole span, and with no growth CPU and ctx fields are `None`, not 0.

**Systematic bias (F6).**
- `frames_in` counts every frame, including the `ConnectInfo` handshake frames ([test_e2e.py:165-184](../tests/e2e/test_e2e.py#L165-L184): "ConnectInfo + 5 messages"). The first growth is therefore the ramp, not the first chat message.
- stressgen starts its warmup only when the ramp ends. The analyzer's start is `ramp start + 5 s`. The true window starts at `ramp start + ramp duration + 5 s`. Both ends are quantised to 1 s samples.
- Measured on `results-9ba50b3` (rep1, ok cells; seconds from node start; true window is 20 s):

```
S1 peers=1      window length 20.1   start 5.0      first growth sample 1.0
S1 peers=1024   window length 20.3-20.8   start 5.0-5.2
S4 churn=10/50  window length 21.0   (frames_in keeps growing with redials)
S6a rtt=50      window length 21.0-22.0   start 5.0   (simplex ramp lasts seconds; start falls inside it)
S7 max_peers=1040  window length 20.2-20.9
```

- For healthy cells the window is 20–21 s, starts 0.1–1 s early and ends up to 1 s late. The effect on CPU and ctx rates is a few percent at most. For long ramps it can include seconds of start-up.

**(e) Evidence.** Tests: [test_analyze.py:23-27](../scripts/test_analyze.py#L23-L27) cover `(2, 6)`, `None` and `(0, 6)`. The ramp bias is not tested.

**Status:** proven by test for the algorithm; bias quantified (F6).

### D3. CPU % and ctx-switch rates

**(b, c) Code.** [analyze.py:125-131](../scripts/analyze.py#L125-L131)

```python
    w = metrics_window(rows, meta.get("params", {}).get("warmup", 0))
    if w:
        a, b = rows[w[0]], rows[w[1]]
        wall = (b["t_ns"] - a["t_ns"]) / 1e9
        out["cpu_pct"] = 100.0 * ((b["utime_us"] + b["stime_us"]) - (a["utime_us"] + a["stime_us"])) / 1e6 / wall
        out["nvcsw_rate"] = (b["nvcsw"] - a["nvcsw"]) / wall
        out["nivcsw_rate"] = (b["nivcsw"] - a["nivcsw"]) / wall
```

**(d) Why.** The `getrusage` counters are cumulative, so the difference between two samples is the work in between. `wall` is the difference of the node's own monotonic `t_ns`, so the denominator is the real interval, not an assumed 1 s. `cpu_pct` is "% of one core" and can reach about 200 on the 2 pinned CPUs. The thread-per-connection collapse in the data shows about 190–193 %.

**(e) Evidence.** Test: [test_analyze.py:36-43](../scripts/test_analyze.py#L36-L43). 0.5 CPU-s per second gives exactly 50 %, and the `nvcsw` slope is 7/s. Limits as documented: node process only, not stressgen. For S6 the veth/netem softirq work is not necessarily charged to the node's rusage.

**Status:** proven by test.

### D4. Peaks and finals

**(b, c) Code.** [analyze.py:134-141](../scripts/analyze.py#L134-L141)

```python
    out["maxrss_kb"] = max(r.get("maxrss_kb", 0) for r in rows)
    out["threads"] = max(r.get("threads", 0) for r in rows)
    out["fds"] = max(r.get("fds", 0) for r in rows)
    out["bp_drops"] = rows[-1].get("send_backpressure_drops", 0)
    out["evictions"] = rows[-1].get("evictions", 0)
    out["tx_queued_sends"] = rows[-1].get("tx_queued_sends")   # absent before S6
    out["tx_flushes"] = rows[-1].get("tx_flushes")
    out["accepts"] = rows[-1].get("accepts")
```

**(d) Why, and semantics.**
- The peaks are maxima over 1 Hz samples, so a spike shorter than 1 s can be missed. `maxrss_kb` is already a process high-water mark.
- The finals are the **last sample's cumulative counters**, so they include ramp and warmup. That is fine for comparing variants but is not "during the window".
- A clean node writes a final line on stop. A killed node leaves a stale last sample, and a truncated last line is skipped by `read_jsonl` ([analyze.py:81-90](../scripts/analyze.py#L81-L90)).

**(e) Evidence.** Test: [test_analyze.py:44-48](../scripts/test_analyze.py#L44-L48) (`threads == 9`, `fds == 24`, `maxrss_kb == 1004`, `bp_drops == 4`, `tx_queued_sends == 20`, `accepts == 4`).

**Status:** proven by test.

### D5. Dirty kept, failed listed

**(b, c) Code.** [analyze.py:156-166](../scripts/analyze.py#L156-L166)

```python
        if meta.get("status") not in ("ok", "dirty"):
            problems.append((label, f"status={meta.get('status')}: {meta.get('error')}"))
            continue
        try:
            m = cell_metrics(d, meta["rep"], meta)
        except (ValueError, KeyError, ZeroDivisionError) as e:
            problems.append((label, f"bad data: {e}"))
            continue
        if meta["status"] == "dirty":
            problems.append((label, f"dirty (data kept): {meta.get('error')}"))
        data.setdefault((meta["scenario"], meta["x_name"], meta["x"], meta["variant"]), []).append(m)
```

**(e) Evidence (executed on fake data).** I mutated four cells (failed, dirty, empty metrics, missing stress file) and ran `analyze.py`:

```
wrote .../fake/summary.csv and .../fake/report.html: 64 groups, 4 problem cells
S1 peers=1 threads rep1       status=failed: x
S1 peers=8 epoll rep1         dirty (data kept): slow
S1 peers=8 epoll-duplex rep2  bad data: metrics.jsonl has <2 samples
S1 peers=8 threads rep2       bad data: missing stress.json
```

Failed and bad cells are excluded from aggregation and listed under "Problems". The dirty cell is kept and listed.

**The semantic gap (F1b).** [experiments.md §5](experiments.md) says dirty data from the window "is still valid". For the one real dirty cell, `S4/churn=10/poll-split/rep3`, that is false:
- `frames_in` is frozen at 41,409 from about t = 18 s while stressgen kept sending.
- `connections_live` drops 64 → 0 only at `/quit`, and the process had to be killed after 30.0 s.
- stressgen shows `peers=0/64`, `msgs_per_s = 748` (the other reps have 3,200) and `lost = 10,221`.
- The median over 3 reps hides it, and the row says only "dirty (data kept)".

**Status:** checked (fake run); validity caveat F1b.

### D6. Missing fields from older results

**(d) Why.** `st.get(...)` is used at [analyze.py:115-119](../scripts/analyze.py#L115-L119) and `rows[-1].get(...)` at [:139-141](../scripts/analyze.py#L139-L141). Required fields (`rtt_us`, `msgs_per_s`, `t_ns`, `utime_us`) raise `KeyError`, which `load` catches ([analyze.py:161-163](../scripts/analyze.py#L161-L163)) and reports as `bad data`.

**(e) Evidence.** Test: [test_analyze.py:49-54](../scripts/test_analyze.py#L49-L54). A pre-S6 stress file and metrics without tx counters load, giving `None` for the new fields. Executed: the mutation above shows the failure path reports instead of crashing.

**Status:** proven by test; failure path checked.

### D7. Aggregation and charts

**(d) Why.**
- [analyze.py:170-181](../scripts/analyze.py#L170-L181) groups by `(scenario, x_name, x, variant)` and reduces each metric with `median_iqr`. `n` is the number of reps present (CSV `reps`, table column).
- Charts use a log axis only when all values are positive and span more than 8× (`min(vals) > 0`, [analyze.py:274](../scripts/analyze.py#L274)). A zero (S6a `msgs_per_s = 0`) therefore gives a linear axis rather than a wrong log plot.

**(e) Evidence.** The pipeline ran on 128 fake cells (64 groups) without error. Not covered by tests: `load`, `aggregate`, `write_csv`, `chart`, `write_html`. There is no check that all reps of a group share a build (F4), or that `peers_ready` or `sent` match the offered load (F1, F8).

**Status:** partly checked; not covered by tests.

---

## 6. Findings (highest severity first)

### F1. Collapsed or partially ramped cells are accepted as valid `ok` data (affects results)
- **Where:** stressgen fails only when `ready == 0` ([stressgen.hpp:574-580](../stressgen.hpp#L574-L580)). [run_matrix.py:283](../scripts/run_matrix.py#L283) sets `meta["status"] = "ok"` on rc 0 and nothing else. `analyze.py` has no validity gate.
- **Impact:** the 12 simplex cells of S6a rtt=50 (`peers_ready 10/32`, `sent ≈ 821` of 64,000, `msgs_per_s = 0`) are `ok`. They enter the medians and charts as zeros, and `report.html`'s Problems list does not flag them. The same hole would hide any cell where a fraction of peers silently fail to handshake.
- **Fix:** in `run_cell`, read the stress JSON and write a `validity` block into `meta.json`:
  - `peers_ready == peers_total` for non-churn, non-S7 cells; at least 0.9 for churn cells.
  - `sent ≥ 0.98 · peers·rate·duration` for open-loop cells.
  - `connect_failures == bad_frames == send_skipped == 0`.
  - If a check fails, set `status = "degraded"`. Keep the cell, list it in `analyze.py` and exclude it from medians (or draw it hatched).
  - Optionally make stressgen exit 3 when `ready < 0.9·peers`.

### F1b. `dirty` is treated as valid data, but a hung node is also `dirty` (affects results)
- **Where:** [analyze.py:164-166](../scripts/analyze.py#L164-L166) and [experiments.md §5](experiments.md).
- **Impact:** `S4/churn=10/poll-split/rep3` (node stalled mid-run, see D5) is aggregated like a normal rep. The median of 3 hides it. With 2 surviving reps it would not. It is also a node bug worth investigating: `frames_in` froze for about 10 s, then the process failed to exit.
- **Fix:** add a stall check (last `frames_in` growth earlier than stressgen's end by more than 3 s) to the validity block above. Or exclude dirty cells from aggregation by default and show `shutdown_s` in the report.

### F2. S6a at RTT 50 ms measures overload of the node's single blocking sender, not WAN churn cost (interpretation, medium)
- See B7.
- **When it happens:** 16 reconnects/s × (2·RTT) > 1, i.e. RTT > about 31 ms. The same mechanism is already visible at 20 ms as a handshake p99 of 654 ms.
- **Fix (experiment):**
  - Report the RTT=50 simplex cells as "collapsed".
  - Add an RTT point near 35 ms and/or run RTT 50 with churn of about 8/s to measure the degradation curve.
  - Add `sender_q_hwm` to the S6a chart set so the cause shows in the report.
- **Fix (node, out of scope here):** make the dial-back non-blocking, or run it per peer, so one RTT does not stall every echo.

### F3. S3 slow-consumer comparison is not like-for-like across shapes (affects results, medium)
- **Where:** simplex [stressgen.hpp:393-397](../stressgen.hpp#L393-L397) vs duplex [:297-301](../stressgen.hpp#L297-L301) and [:434-438](../stressgen.hpp#L434-L438); see A8.
- **Impact:** duplex and split cells show `tx_queued_sends = 0` and `send_backpressure_drops = 0`. S3's premise that pooled writes isolate a slow peer is untested for 4 of the 8 variants in the old run. `peer-threads` has not been checked against this data.
- **Cause:** stressgen stops reading in both shapes, so this is not a generator reading bug. Its only lever is "don't call recv", and kernel buffering then decides whether the node ever blocks.
- **Fix:**
  - Give slow peers an identical, tiny receive window. Set `SO_RCVBUF` to about 4 KiB on the dialed socket before `connect()` (duplex) and on the listener before `listen()` (simplex; accepted sockets inherit it).
  - Record unread bytes in the stress JSON (`ioctl(FIONREAD)` at the end).
  - Add a self-test asserting the slow socket's queue grows.
  - Then rerun S3.

### F4. Builds can be mixed silently on resume and in analysis (medium)
- **Where:** [run_matrix.py:181-185](../scripts/run_matrix.py#L181-L185) and [analyze.py:145-167](../scripts/analyze.py#L145-L167). The stamp is recorded per cell and never compared.
- **Fix:**
  - When resuming into a non-empty `--out`, read an existing `meta.json` and refuse (or require `--force-mixed`) if `build.commit`, `build.app.sha256` or `build.stressgen.sha256` differ.
  - In `analyze.py`, print a "Build" section listing the distinct `(commit, dirty, app sha, stressgen sha, build_type)` with cell counts, and show a red banner if there is more than one.

### F5. Node exit status is never checked, and EOF counts as `bye` (low-medium)
- **Where:** [run_matrix.py:213-214](../scripts/run_matrix.py#L213-L214) and [:232-238](../scripts/run_matrix.py#L232-L238). A crash during shutdown is recorded `ok`.
- **Fix:** store `self.p.returncode`. Require `rc == 0` and a `bye` event actually parsed (not set at EOF) for `clean`. Otherwise mark the cell `dirty` and record `node_rc` in `meta.json`.

### F6. `metrics_window` is anchored to the first handshake frame, not stressgen's window (low)
- See D2. The window starts 0.1–1 s early (more with long ramps) and ends up to 1 s late.
- **Fix:**
  - Have stressgen write `window_start_unix_ns` and `window_end_unix_ns` (it already has `start_wall_ns`).
  - Put wall-clock time in the node's metrics lines.
  - Alternatively, start the window at the sample where chat frames start (`frames_in − accepts` growth).
  - At minimum, add the measured window length to `summary.csv`.

### F7. `lost` is clamped, hiding `received > sent` (low)
- [stressgen.hpp:605](../stressgen.hpp#L605). Currently 0 occurrences in 696 cells.
- **Fix:** add `duplicates = received > sent ? received − sent : 0` to the JSON. Have `analyze.py` list cells with `duplicates > 0`.

### F8. Offered load depends on the system under test during churn (low, interpretation)
- Peers send only while Ready, so slower handshakes lower `sent`. S6a rtt=20 gives simplex 63,377, duplex 63,956 and nominal 64,000. Simplex rtt=50 gives 821. `msgs_per_s` and `lost` are not comparable across variants without normalising.
- **Fix:** put `offered = peers·rate·duration` in the JSON. Have `analyze.py` plot `received / offered` next to `msgs_per_s`.

### F9. S6b: netem drops unmeasured, RTT is a queue depth, grace is tight (low)
- See B4 and A2.
- **Fix:**
  - Save `tc -s qdisc show dev …` before and after each S6 cell into `meta.json` (drops, overlimits, backlog).
  - Raise `GRACE` to at least 3 s for S6b. One `rate_mbit=50` cell already lost 2 messages at the 1 s grace, with p99 RTT of 1.6–2.8 s.

### F10. Shuffle order is neither reproducible nor recorded (low)
- [run_matrix.py:460](../scripts/run_matrix.py#L460) uses `random.Random(args.seed if args.fake else None)`.
- **Fix:** seed it in real runs too and write the seed to `meta.json`.

### F11. Handshake statistics have survivorship bias and a stale-Ack race (low)
- See A6.
- **Fix:**
  - Report the number of handshakes that did not finish (started minus samples).
  - Ignore an Ack whose peer is not in `Handshaking`.

### F12. Generator CPU and loop lag, and node softirq cost, are not recorded (low)
- stressgen records neither its own `rusage` nor its schedule lateness. The node's CPU excludes veth/netem softirq work executed on the node CPUs.
- **Fix:** add `getrusage` and "max (now − next_send)" to the stress JSON, and flag cells where lateness exceeds about 5 ms.

### F13. Cosmetic
- [test_suite.md §8](test_suite.md) lists only `*-duplex`/`*-split` for `--duplex`; `peer-threads` also gets it.
- `tick_churn` can pick slow peers.
- The `rep<N>.stressgen.txt` of a retried failed cell may be stale until the retry writes it.
- `peer-threads` is not in the old results, so statements about its data are by code only.

---

## 7. Not covered / limits of this report

- **No real binary was executed.**
  - stressgen behaviour comes from code reading, the self-test as written (not re-run), and the files of `results-9ba50b3`.
  - That run is build `9ba50b3`, which predates `peer-threads` and might differ from `fca6b9e` in the node.
  - The currently running `results-fca6b9e` was deliberately not read.
- **The histogram was verified on a Python transcription**, not on the C++ code.
- **No proven mechanism** for the S3 duplex asymmetry (A8/F3). That needs `ss -tmi` on a live slow socket, or a test with a deliberately slow reader against the real generator.
- **The S6a anomaly (B7) is explained from code and counters, not by tracing.**
- **No test covers `run_matrix.py` or `netlab.py`.**
  - Sections 3 and 4 rest on code reading, the stubbed command capture, and the fake run.
  - Actual netem behaviour (drops, softirq placement, namespace teardown) was inferred from recorded qdisc strings and results, not from `tc -s`.
- **Not examined:**
  - CPU affinity of the running processes.
  - Kernel settings other than `ip_local_port_range`, `tcp_rmem`/`tcp_wmem` and `somaxconn`, which were read from `/proc/sys`.
  - Thermal and turbo drift, and the WSL2 scheduler.
  - stressgen headroom in closed-loop S5/S6b.
  - Long-run memory of stressgen with 1000+ peers.
  - Node correctness beyond what the harness depends on.
- **Ephemeral ports:** this host's range is only 4,096 wide (44620–48715). Per-destination uniqueness makes exhaustion unlikely at the rates used (S4 churn=50 is about 1,250 connects per cell, S1 1024 is 1,024 per cell). It was not observed: there were no `connect_failures` in 696 cells. Much higher churn or tighter back-to-back cells could hit TIME_WAIT limits.
- **Scratch files** (`flags_check.py`, `hist.py`, `checks.py`, `win.py`, the fake results) live under the session scratchpad and are not part of the repository.

