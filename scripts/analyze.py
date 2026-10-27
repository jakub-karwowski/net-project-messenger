#!/usr/bin/env python3
"""Aggregate run_matrix.py results into summary.csv and a self-contained report.html.

usage: analyze.py [results_dir]        (default: results)
Cells are grouped by (scenario, param, variant); each metric is reduced to
median and IQR (Q1..Q3) across reps. Missing/failed/dirty cells are listed in
the report instead of being silently dropped.

Validity: every cell is checked post hoc (validity()); a cell that was ok/dirty
but fails the checks is `degraded`: listed under Problems with its reasons and
excluded from the medians/IQR (summary.csv keeps `reps` and adds `reps_valid`).
"""
import csv, html, json, math, sys
from pathlib import Path

VARIANTS = ["threads", "pool-sender", "poll", "epoll",
            "poll-duplex", "epoll-duplex", "poll-split", "epoll-split", "peer-threads"]
# Colour-blind-safe-ish categorical colours (light, dark) + distinct marker shapes.
COLORS = {"threads": ("#0072B2", "#56B4E9"), "pool-sender": ("#D55E00", "#F0A060"),
          "poll": ("#009E73", "#4FD1A5"), "epoll": ("#CC79A7", "#E8A3C8"),
          # duplex/split: shifted shades of their base poll (green) / epoll (pink) hue
          "poll-duplex": ("#005C42", "#8FE3C2"), "poll-split": ("#8BC34A", "#B5E27A"),
          "epoll-duplex": ("#7A2E58", "#F5C4DE"), "epoll-split": ("#E040A0", "#FF7FC8"),
          # one thread per peer on one socket: a darker shade of the `threads` blue
          "peer-threads": ("#003E73", "#9CC3FF")}
MARKERS = {"threads": "circle", "pool-sender": "square", "poll": "triangle", "epoll": "diamond",
           "poll-duplex": "cross", "poll-split": "star", "epoll-duplex": "hexagon", "epoll-split": "plus", "peer-threads": "triangle-down"}

# key, label, unit, prefer-log
METRICS = [
    ("msgs_per_s", "Throughput", "msg/s", True),
    ("rtt_p50", "RTT p50", "us", True),
    ("rtt_p99", "RTT p99", "us", True),
    ("rtt_p999", "RTT p99.9", "us", True),
    ("lost", "Lost messages", "msgs", False),
    ("cpu_pct", "CPU (utime+stime)", "% of one core", False),
    ("nvcsw_rate", "Voluntary ctx switches", "/s", True),
    ("nivcsw_rate", "Involuntary ctx switches", "/s", True),
    ("maxrss_kb", "Peak RSS", "kB", False),
    ("threads", "Peak threads", "threads", False),
    ("fds", "Peak fds", "fds", False),
    ("bp_drops", "Backpressure drops", "msgs", False),
    ("evictions", "Evictions", "conns", False),
    ("handshake_p50", "Handshake p50 (connect to Ready)", "us", True),
    ("handshake_p99", "Handshake p99 (connect to Ready)", "us", True),
    ("connect_failures", "Connect failures", "conns", False),
    ("churn_events", "Churn reconnects", "events", False),
    ("peers_ready", "Peers served at the end", "peers", False),
    ("disconnects", "Peers disconnected by the node", "peers", False),
    ("send_skipped", "Generator send drops", "msgs", False),
    ("tx_queued_sends", "Sends that queued bytes", "sends", True),
    ("tx_flushes", "Flushes on writability", "flushes", True),
    ("accepts", "Accepts", "conns", False),
    ("duplicates", "Duplicate echoes (received > sent)", "msgs", False),
    ("handshake_failures", "Handshake failures", "conns", False),
    ("slow_unread_bytes", "Unread bytes at slow peers", "bytes", True),
    ("gen_cpu_pct", "Generator CPU (utime+stime)", "% of one core", False),
    ("sender_q_hwm", "Sender queue high-water mark", "items", False),
    ("netem_drops", "netem drops (both ends)", "pkts", False),
]
CHART_KEYS = ["msgs_per_s", "rtt_p50", "rtt_p99", "rtt_p999", "cpu_pct", "nvcsw_rate",
              "maxrss_kb", "threads", "fds"]
# Per-scenario chart lists override CHART_KEYS (S6 looks at different things).
SCENARIO_CHARTS = {
    "S7": ["msgs_per_s", "rtt_p50", "rtt_p99", "peers_ready", "evictions", "cpu_pct", "threads"],
    "S6a": ["handshake_p50", "handshake_p99", "msgs_per_s", "rtt_p99", "sender_q_hwm"],
    "S3": CHART_KEYS + ["slow_unread_bytes", "tx_queued_sends"],
    "S6b": ["msgs_per_s", "rtt_p50", "rtt_p99", "rtt_p999", "tx_queued_sends", "cpu_pct", "nvcsw_rate", "netem_drops"],
}
SCENARIO_CHARTS["S6c"] = SCENARIO_CHARTS["S6a"]

# --- Validity thresholds (see validity()) -------------------------------------
READY_FRAC_CHURN = 0.9     # churn cells: peers_ready >= this * peers_total (peers are mid-redial at the end)
READY_FRAC_S7 = 0.9        # S7: peers_ready >= this * min(peers, max_peers) (the cap evicts the rest by design)
SENT_FRAC_OPEN_LOOP = 0.98 # open-loop cells: sent >= this * peers * rate * duration
STALL_MARGIN_S = 3.0       # node frames_in must still grow within this of the generator window end (same clock)
STALL_MARGIN_WALL_S = 5.0  # looser margin when the end had to be mapped through wall clock (~1 s error)


# --- Stats helpers ----------------------------------------------------------
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


# --- Loading ----------------------------------------------------------------
def read_jsonl(path):
    rows = []
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if line:
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass   # truncated last line of a killed node
    return rows


def metrics_window_m(rows, warmup, st=None):
    """-> ((start, end) sample indices or None, method). With stressgen's monotonic
    window bounds (same clock as the node's t_ns): first sample at/after the start
    and last sample at/before the end. Otherwise (old results) the frames_in
    heuristic of metrics_window()."""
    if st:
        ws, we = st.get("window_start_mono_ns"), st.get("window_end_mono_ns")
        if isinstance(ws, int) and isinstance(we, int) and we > ws:
            idx = [i for i, r in enumerate(rows) if ws <= r.get("t_ns", -1) <= we]
            if len(idx) >= 2:
                return (idx[0], idx[-1]), "mono"
    return metrics_window(rows, warmup), "frames_in"


def metrics_window(rows, warmup):
    """(start, end) sample indices of the measurement window: from `warmup`
    seconds after the first frames_in growth to the last growth (fallback for
    results without stressgen's window bounds)."""
    grow = [i for i in range(1, len(rows)) if rows[i].get("frames_in", 0) > rows[i - 1].get("frames_in", 0)]
    if not grow:
        return None
    s = grow[0] - 1 + int(math.ceil(warmup))
    e = grow[-1]
    if e <= s:
        s = grow[0] - 1
    return (s, e) if e > s else None


def validity(meta, stress, rows):
    """-> (ok, reasons). Post-hoc validity of one cell; thresholds are the module
    constants above. A failed cell is `degraded` (excluded from medians).
    Rules:
      1. ready: peers_ready == peers_total. Exceptions: churn>0 needs >=
         READY_FRAC_CHURN*total; S7 (explicit max_peers) needs >= READY_FRAC_S7 *
         min(peers, max_peers). (Both given: the lower bar.)
      2. open loop (rate>0, not S7, churn==0): sent >= SENT_FRAC_OPEN_LOOP *
         peers*rate*duration (a collapsed or partly ramped generator).
      3. connect_failures == 0 (not for S7: the cap refuses connections by
         design), bad_frames == 0, send_skipped == 0.
      4. no stall: the node's frames_in grew within STALL_MARGIN_S of the
         generator's window end. With window_end_mono_ns the clocks match. Without
         it (old results) end_unix_ns is mapped onto the metrics clock through
         meta.t_start (wall) ~= first metrics sample, an error of about 1 s, so
         STALL_MARGIN_WALL_S is used.
    Missing fields are skipped, so old results are judged on what they have."""
    if not stress:
        return True, []
    reasons = []
    p = meta.get("params", {}) or {}
    s7 = meta.get("scenario") == "S7" or bool(p.get("max_peers"))
    churn = p.get("churn", 0) or 0
    total, ready = stress.get("peers_total"), stress.get("peers_ready")
    if total is not None and ready is not None:
        need, why = total, "peers_ready"
        if churn > 0:
            need, why = READY_FRAC_CHURN * total, f"peers_ready (churn, >= {READY_FRAC_CHURN:g})"
        if s7:
            cap = min(total, p.get("max_peers") or total)
            # The two-connection pool modes serve cap - 1 (the dial-back shares the
            # pool), so one peer short always passes; this only matters at the
            # tiny quick-mode caps, where 0.9 * cap rounds above cap - 1.
            need = min(READY_FRAC_S7 * cap, cap - 1)
            why = f"peers_ready (S7 cap {cap}, >= min({READY_FRAC_S7:g}*cap, cap-1))"
        if ready < need:
            reasons.append(f"{why} {ready}/{total}")
    rate, dur, peers = p.get("rate") or 0, p.get("duration"), p.get("peers")
    if rate > 0 and not s7 and not churn and dur and peers and "sent" in stress:
        want = peers * rate * dur
        if stress["sent"] < SENT_FRAC_OPEN_LOOP * want:
            reasons.append(f"sent {stress['sent']} < {SENT_FRAC_OPEN_LOOP:g} * offered {want:g}")
    if not s7 and (stress.get("connect_failures") or 0) > 0:
        reasons.append(f"connect_failures={stress['connect_failures']}")
    for k in ("bad_frames", "send_skipped"):
        if (stress.get(k) or 0) > 0:
            reasons.append(f"{k}={stress[k]}")
    if rows and len(rows) >= 2:
        end_ns, margin = stress.get("window_end_mono_ns"), STALL_MARGIN_S
        if not isinstance(end_ns, int):
            eu = stress.get("end_unix_ns")
            if isinstance(eu, int) and eu > 0 and meta.get("t_start") and "t_ns" in rows[0]:
                end_ns = rows[0]["t_ns"] + (eu / 1e9 - meta["t_start"]) * 1e9
                margin = STALL_MARGIN_WALL_S
            else:
                end_ns = None
        if end_ns is not None:
            grow = [rows[i]["t_ns"] for i in range(1, len(rows))
                    if rows[i].get("frames_in", 0) > rows[i - 1].get("frames_in", 0) and "t_ns" in rows[i]]
            if not grow:
                reasons.append("node frames_in never grew")
            elif (end_ns - grow[-1]) / 1e9 > margin:
                reasons.append(f"node stalled: frames_in stopped growing {(end_ns - grow[-1]) / 1e9:.1f}s "
                               f"before the generator window end (margin {margin:g}s)")
    return not reasons, reasons


def cell_metrics(cell_dir, rep, meta):
    """Derive per-rep scalar metrics. Raises ValueError with a clear message.
    Also sets out["valid"] / out["reasons"] (validity()) and out["window_method"]."""
    sp = cell_dir / f"rep{rep}.stress.json"
    mp = cell_dir / f"rep{rep}.metrics.jsonl"
    if not sp.exists():
        raise ValueError("missing stress.json")
    st = json.loads(sp.read_text())
    out = dict(msgs_per_s=st["msgs_per_s"], lost=st["lost"], rtt_p50=st["rtt_us"]["p50"],
               rtt_p99=st["rtt_us"]["p99"], rtt_p999=st["rtt_us"]["p999"])
    hs = st.get("handshake_us")   # absent in results from before S6
    out.update(handshake_p50=hs["p50"] if hs else None, handshake_p99=hs["p99"] if hs else None,
               connect_failures=st.get("connect_failures"), churn_events=st.get("churn_events"),
               peers_ready=st.get("peers_ready"), disconnects=st.get("disconnects"),
               send_skipped=st.get("send_skipped"), duplicates=st.get("duplicates"),
               handshake_failures=st.get("handshake_failures"),
               slow_unread_bytes=st.get("slow_unread_bytes"))
    if not mp.exists():
        raise ValueError("missing metrics.jsonl")
    rows = read_jsonl(mp)
    if len(rows) < 2:
        raise ValueError("metrics.jsonl has <2 samples")
    w, method = metrics_window_m(rows, meta.get("params", {}).get("warmup", 0), st)
    out["window_method"] = method
    if w:
        a, b = rows[w[0]], rows[w[1]]
        wall = (b["t_ns"] - a["t_ns"]) / 1e9
        out["cpu_pct"] = 100.0 * ((b["utime_us"] + b["stime_us"]) - (a["utime_us"] + a["stime_us"])) / 1e6 / wall
        out["nvcsw_rate"] = (b["nvcsw"] - a["nvcsw"]) / wall
        out["nivcsw_rate"] = (b["nivcsw"] - a["nivcsw"]) / wall
    else:
        out.update(cpu_pct=None, nvcsw_rate=None, nivcsw_rate=None)
    gc = st.get("gen_cpu")
    out["gen_cpu_pct"] = None
    if gc:
        ws, we = st.get("window_start_mono_ns"), st.get("window_end_mono_ns")
        secs = (we - ws) / 1e9 if isinstance(ws, int) and isinstance(we, int) and we > ws \
            else st.get("config", {}).get("duration")
        if secs:
            out["gen_cpu_pct"] = 100.0 * (gc.get("utime_us", 0) + gc.get("stime_us", 0)) / 1e6 / secs
    out["maxrss_kb"] = max(r.get("maxrss_kb", 0) for r in rows)
    out["threads"] = max(r.get("threads", 0) for r in rows)
    out["fds"] = max(r.get("fds", 0) for r in rows)
    out["bp_drops"] = rows[-1].get("send_backpressure_drops", 0)
    out["evictions"] = rows[-1].get("evictions", 0)
    out["tx_queued_sends"] = rows[-1].get("tx_queued_sends")   # absent before S6
    out["tx_flushes"] = rows[-1].get("tx_flushes")
    out["accepts"] = rows[-1].get("accepts")
    out["sender_q_hwm"] = rows[-1].get("sender_q_hwm")
    ns = meta.get("netem_stats")
    out["netem_drops"] = sum(v.get("dropped", 0) for v in ns.values() if isinstance(v, dict)) if ns else None
    out["valid"], out["reasons"] = validity(meta, st, rows)
    return out


def build_key(build):
    """The identity of a build stamp: (commit, dirty, app sha256, stressgen sha256,
    build_type) or None when the cell has no stamp ("unknown")."""
    if not build:
        return None
    app, sg = build.get("app") or {}, build.get("stressgen") or {}
    return (build.get("commit"), build.get("dirty"), app.get("sha256"), sg.get("sha256"), app.get("build_type"))


def fmt_build_key(k):
    if k is None:
        return "unknown (no build stamp)"
    c, dirty, app, sg, bt = k
    sh = lambda x: (x or "?")[:10]
    return f"commit {sh(c)}{' (dirty)' if dirty else ''}, app {sh(app)}, stressgen {sh(sg)}, {bt or 'unknown'} build"


def load(root, builds=None):
    """-> (data, problems). data[(sc, x_name, x, variant)] = list of per-rep dicts,
    each with "valid" (False for degraded cells). `builds`, when a dict, is filled
    with {build_key: cell count} over every cell that has a meta.json."""
    data, problems = {}, []
    for mp in sorted(Path(root).glob("*/*/*/rep*.meta.json")):
        d = mp.parent
        try:
            meta = json.loads(mp.read_text())
        except ValueError:
            problems.append((str(mp.relative_to(root)), "unreadable meta.json"))
            continue
        if builds is not None:
            k = build_key(meta.get("build"))
            builds[k] = builds.get(k, 0) + 1
        label = f"{meta.get('scenario')} {meta.get('x_name')}={meta.get('x')} {meta.get('variant')} rep{meta.get('rep')}"
        if meta.get("status") not in ("ok", "dirty"):
            problems.append((label, f"status={meta.get('status')}: {meta.get('error')}"))
            continue
        try:
            m = cell_metrics(d, meta["rep"], meta)
        except (ValueError, KeyError, ZeroDivisionError) as e:
            problems.append((label, f"bad data: {e}"))
            continue
        if not m["valid"]:
            problems.append((label, f"degraded (excluded from medians; was {meta['status']}): " + "; ".join(m["reasons"])))
        elif meta["status"] == "dirty":
            problems.append((label, f"dirty (data kept): {meta.get('error')}"))
        data.setdefault((meta["scenario"], meta["x_name"], meta["x"], meta["variant"]), []).append(m)
    return data, problems


def aggregate(data):
    """Medians/IQR over the valid reps only. n = reps loaded, n_valid = reps used."""
    agg = {}
    for key, reps in data.items():
        good = [r for r in reps if r.get("valid", True)]
        a = {"n": len(reps), "n_valid": len(good)}
        for mk, _, _, _ in METRICS:
            vals = [r.get(mk) for r in good]
            a[mk] = median_iqr(vals)
            a[mk + "_peak"] = max([v for v in vals if v is not None], default=float("nan"))
        methods = {r.get("window_method") for r in good if r.get("window_method")}
        a["window_method"] = methods.pop() if len(methods) == 1 else ("mixed" if methods else "")
        agg[key] = a
    return agg


# --- CSV --------------------------------------------------------------------
def write_csv(path, agg):
    cols = ["scenario", "param", "value", "variant", "reps", "reps_valid", "window_method"]
    for mk, _, _, _ in METRICS:
        cols += [mk + "_median", mk + "_q1", mk + "_q3"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for (sc, xn, x, v) in sorted(agg, key=lambda k: (k[0], k[2], VARIANTS.index(k[3]) if k[3] in VARIANTS else 9)):
            a = agg[(sc, xn, x, v)]
            row = [sc, xn, x, v, a["n"], a["n_valid"], a["window_method"]]
            for mk, _, _, _ in METRICS:
                row += [f"{t:.6g}" for t in a[mk]]
            w.writerow(row)


# --- SVG charts -------------------------------------------------------------
def fmt(v):
    if v != v:
        return "-"
    a = abs(v)
    if a >= 1e6:
        return f"{v/1e6:.3g}M"
    if a >= 1e4:
        return f"{v/1e3:.3g}k"
    if a >= 100:
        return f"{v:.0f}"
    return f"{v:.3g}"


def nice_ticks(lo, hi, log):
    if log:
        a, b = math.floor(math.log10(lo)), math.ceil(math.log10(hi))
        if a == b:
            b += 1
        return [10.0 ** e for e in range(a, b + 1)]
    if hi <= lo:
        hi = lo + 1
    step = 10 ** math.floor(math.log10((hi - lo) / 4))
    for m in (1, 2, 5, 10):
        if (hi - lo) / (step * m) <= 5:
            step *= m
            break
    t0 = math.floor(lo / step) * step
    ticks = []
    t = t0
    while t <= hi + step * 0.999:
        ticks.append(t)
        t += step
    return ticks


def marker(shape, x, y, cls):
    r = 4
    if shape == "circle":
        return f'<circle class="{cls}" cx="{x:.1f}" cy="{y:.1f}" r="{r}"/>'
    if shape == "square":
        return f'<rect class="{cls}" x="{x-r:.1f}" y="{y-r:.1f}" width="{2*r}" height="{2*r}"/>'
    if shape == "triangle":
        return f'<polygon class="{cls}" points="{x:.1f},{y-r-1:.1f} {x+r+1:.1f},{y+r:.1f} {x-r-1:.1f},{y+r:.1f}"/>'
    if shape == "triangle-down":
        return f'<polygon class="{cls}" points="{x:.1f},{y+r+1:.1f} {x+r+1:.1f},{y-r:.1f} {x-r-1:.1f},{y-r:.1f}"/>'
    def poly(pts):
        return f'<polygon class="{cls}" points="' + " ".join(f"{x+dx:.1f},{y+dy:.1f}" for dx, dy in pts) + '"/>'
    if shape == "plus":
        t = 1.6
        return poly([(-t, -r-1), (t, -r-1), (t, -t), (r+1, -t), (r+1, t), (t, t), (t, r+1), (-t, r+1), (-t, t), (-r-1, t), (-r-1, -t), (-t, -t)])
    if shape == "cross":   # plus rotated 45 degrees
        t = 1.6
        pts = [(-t, -r-1), (t, -r-1), (t, -t), (r+1, -t), (r+1, t), (t, t), (t, r+1), (-t, r+1), (-t, t), (-r-1, t), (-r-1, -t), (-t, -t)]
        return poly([((px - py) * 0.7071, (px + py) * 0.7071) for px, py in pts])
    if shape == "hexagon":
        return poly([(r+1, 0), (r/2, (r+1)*0.866), (-r/2, (r+1)*0.866), (-r-1, 0), (-r/2, -(r+1)*0.866), (r/2, -(r+1)*0.866)])
    if shape == "star":
        pts = []
        for k in range(10):
            rad = (r + 2) if k % 2 == 0 else (r + 2) * 0.45
            ang = -math.pi / 2 + k * math.pi / 5
            pts.append((rad * math.cos(ang), rad * math.sin(ang)))
        return poly(pts)
    return f'<polygon class="{cls}" points="{x:.1f},{y-r-1:.1f} {x+r+1:.1f},{y:.1f} {x:.1f},{y+r+1:.1f} {x-r-1:.1f},{y:.1f}"/>'


def chart(title, unit, xname, xs, series, prefer_log):
    """series: {variant: [(median,q1,q3) or None per x]}. Returns SVG string."""
    W, H, L, R, T, B = 460, 280, 62, 14, 30, 46
    vals = [t for s in series.values() for p in s if p for t in p if t == t]
    if not vals:
        return f'<div class="nodata">{html.escape(title)}: no data</div>'
    pos = [v for v in vals if v > 0]
    log = prefer_log and bool(pos) and min(pos) > 0 and max(pos) / min(pos) > 8 and min(vals) > 0
    if log:
        lo, hi = min(pos), max(pos)
        ticks = nice_ticks(lo, hi, True)
        lo, hi = ticks[0], ticks[-1]
        ty = lambda v: math.log10(max(v, lo))
        y0, y1 = math.log10(lo), math.log10(hi)
    else:
        lo, hi = 0.0, max(vals)
        if hi <= 0:
            hi = 1.0
        ticks = nice_ticks(lo, hi, False)
        hi = ticks[-1]
        ty = lambda v: v
        y0, y1 = lo, hi
    pw, ph = W - L - R, H - T - B
    X = lambda i: L + (pw * (i + 0.5) / len(xs))
    Y = lambda v: T + ph * (1 - (ty(v) - y0) / (y1 - y0))
    o = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{html.escape(title)} by {html.escape(xname)}">']
    o.append(f'<text class="ct" x="{L}" y="16">{html.escape(title)} ({html.escape(unit)}){" - log y" if log else ""}</text>')
    for t in ticks:
        y = Y(t)
        o.append(f'<line class="grid" x1="{L}" x2="{W-R}" y1="{y:.1f}" y2="{y:.1f}"/>')
        o.append(f'<text class="tk" x="{L-6}" y="{y+3:.1f}" text-anchor="end">{fmt(t)}</text>')
    for i, x in enumerate(xs):
        o.append(f'<text class="tk" x="{X(i):.1f}" y="{H-B+16}" text-anchor="middle">{html.escape(str(x))}</text>')
    o.append(f'<text class="tk" x="{L+pw/2}" y="{H-8}" text-anchor="middle">{html.escape(xname)}</text>')
    o.append(f'<line class="axis" x1="{L}" x2="{L}" y1="{T}" y2="{T+ph}"/><line class="axis" x1="{L}" x2="{W-R}" y1="{T+ph}" y2="{T+ph}"/>')
    nv = len(series)
    for vi, (v, pts) in enumerate(series.items()):
        cls = "s-" + v
        dx = (vi - (nv - 1) / 2) * 5   # small horizontal dodge so IQR bars don't overlap
        path = [(X(i) + dx, Y(p[0])) for i, p in enumerate(pts) if p and p[0] == p[0]]
        if len(path) > 1:
            o.append(f'<polyline class="ln {cls}" points="' + " ".join(f"{a:.1f},{b:.1f}" for a, b in path) + '"/>')
        for i, p in enumerate(pts):
            if not p or p[0] != p[0]:
                continue
            x = X(i) + dx
            if p[1] == p[1] and p[2] == p[2] and p[2] > p[1]:
                o.append(f'<line class="iqr {cls}" x1="{x:.1f}" x2="{x:.1f}" y1="{Y(p[1]):.1f}" y2="{Y(p[2]):.1f}"/>')
            o.append(marker(MARKERS.get(v, "circle"), x, Y(p[0]), "mk " + cls) +
                     f'<title>{html.escape(v)} {xname}={xs[i]}: median {fmt(p[0])} (IQR {fmt(p[1])}-{fmt(p[2])}) {html.escape(unit)}</title>')
    o.append("</svg>")
    return "".join(o)


# --- HTML -------------------------------------------------------------------
CSS = """
:root{--bg:#fff;--fg:#1a1a1a;--mut:#666;--grid:#e3e3e3;--card:#fafafa;--bd:#d8d8d8;
%s}
@media (prefers-color-scheme:dark){:root{--bg:#16181c;--fg:#e8e8e8;--mut:#9aa0a6;--grid:#33373d;--card:#1e2126;--bd:#3a3e45;
%s}}
body{background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,sans-serif;margin:0 auto;max-width:1100px;padding:16px}
h1{font-size:22px}h2{margin-top:36px;border-bottom:1px solid var(--bd);padding-bottom:4px}h3{font-size:15px;margin:18px 0 6px}
.legend{display:flex;gap:16px;flex-wrap:wrap;margin:8px 0}.legend span{display:inline-flex;align-items:center;gap:6px}
.grid3{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:10px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:6px;padding:4px}
svg{width:100%%;height:auto;display:block}
.ct{font-size:12px;font-weight:600;fill:var(--fg)}.tk{font-size:10px;fill:var(--mut)}
.grid{stroke:var(--grid);stroke-width:1}.axis{stroke:var(--mut);stroke-width:1}
.ln{fill:none;stroke-width:2}.iqr{stroke-width:2;opacity:.55}
table{border-collapse:collapse;font-size:12px;margin:6px 0;display:block;overflow-x:auto}
th,td{border:1px solid var(--bd);padding:3px 7px;text-align:right;white-space:nowrap}th{background:var(--card)}
td:first-child,th:first-child,td:nth-child(2){text-align:left}
.err{color:#b00020}@media (prefers-color-scheme:dark){.err{color:#ff6b81}}
.nodata{color:var(--mut);padding:20px}.mut{color:var(--mut)}
"""


def css():
    light = "".join(f"--c-{v}:{c[0]};" for v, c in COLORS.items())
    dark = "".join(f"--c-{v}:{c[1]};" for v, c in COLORS.items())
    body = CSS % (light, dark)
    for v in COLORS:
        body += f".s-{v}.ln,.s-{v}.iqr{{stroke:var(--c-{v})}}.s-{v}.mk{{fill:var(--c-{v})}}.sw-{v}{{background:var(--c-{v})}}\n"
    return body


def table(sc, xn, xs, variants, agg):
    cols = ["variant", xn, "reps"] + [m[1] for m in METRICS]
    h = ["<table><tr>" + "".join(f"<th>{html.escape(c)}</th>" for c in cols) + "</tr>"]
    for x in xs:
        for v in variants:
            a = agg.get((sc, xn, x, v))
            if not a:
                h.append(f"<tr><td>{v}</td><td>{x}</td><td colspan='{len(cols)-2}' class='err'>no data</td></tr>")
                continue
            cells = [v, str(x), str(a["n"]) if a["n"] == a["n_valid"] else f"{a['n_valid']}/{a['n']} valid"]
            for mk, _, _, _ in METRICS:
                m, q1, q3 = a[mk]
                cells.append("-" if m != m else f"{fmt(m)} <span class='mut'>[{fmt(q1)}-{fmt(q3)}]</span>")
            h.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    return "".join(h) + "</table>"


def build_html(builds):
    o = ["<h2>Build</h2>"]
    if len(builds) > 1:
        o.append("<p class=err><b>WARNING: this directory mixes %d different builds.</b> "
                 "Medians combine cells from different binaries; do not compare variants across them.</p>" % len(builds))
    o.append("<table><tr><th>build</th><th>cells</th></tr>")
    for k, n in sorted(builds.items(), key=lambda kv: -kv[1]):
        o.append(f"<tr><td style='text-align:left'>{html.escape(fmt_build_key(k))}</td><td>{n}</td></tr>")
    o.append("</table>")
    return "".join(o)


def write_html(path, agg, problems, root, titles, builds=None):
    scs = sorted({k[0] for k in agg})
    o = [f"<!doctype html><html lang=en><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
         f"<title>Stress report</title><style>{css()}</style><body><h1>I/O variant stress report</h1>",
         f"<p class=mut>Source: {html.escape(str(root))}. Points are medians across reps; vertical bars are IQR (Q1-Q3). "
         "CPU, ctx-switch rates are computed over the measurement window (after warmup) from the node's metrics.jsonl "
         "(bounds from stressgen's monotonic window when present, else a frames_in heuristic; see window_method in summary.csv); "
         "RSS and threads are peaks. Degraded cells (failed validity checks) are listed under Problems and excluded.</p>",
         "<div class=legend>" + "".join(f"<span><i class='sw-{v}' style='width:14px;height:14px;display:inline-block;border-radius:3px'></i>{v}</span>" for v in VARIANTS) + "</div>"]
    if builds:
        o.append(build_html(builds))
    if problems:
        o.append(f"<h2>Problems ({len(problems)})</h2><table><tr><th>cell</th><th>issue</th></tr>")
        o += [f"<tr><td class=err>{html.escape(c)}</td><td style='text-align:left'>{html.escape(m)}</td></tr>" for c, m in problems]
        o.append("</table>")
    else:
        o.append("<p>No missing or dirty cells.</p>")
    for sc in scs:
        keys = [k for k in agg if k[0] == sc]
        xn = keys[0][1]
        xs = sorted({k[2] for k in keys})
        variants = [v for v in VARIANTS if any(k[3] == v for k in keys)]
        o.append(f"<h2>{sc}: {html.escape(titles.get(sc, ''))}</h2><div class=grid3>")
        for mk in SCENARIO_CHARTS.get(sc, CHART_KEYS):
            _, label, unit, lg = next(m for m in METRICS if m[0] == mk)
            series = {v: [agg[(sc, xn, x, v)][mk] if (sc, xn, x, v) in agg else None for x in xs] for v in variants}
            o.append(f"<div class=card>{chart(label, unit, xn, xs, series, lg)}</div>")
        for mk in ("lost", "bp_drops", "evictions", "connect_failures", "send_skipped",
                   "duplicates", "handshake_failures", "gen_cpu_pct"):   # only interesting when nonzero
            if any((agg[k][mk][0] or 0) > 0 for k in keys):
                _, label, unit, lg = next(m for m in METRICS if m[0] == mk)
                series = {v: [agg[(sc, xn, x, v)][mk] if (sc, xn, x, v) in agg else None for x in xs] for v in variants}
                o.append(f"<div class=card>{chart(label, unit, xn, xs, series, False)}</div>")
        o.append("</div><h3>Table (median [Q1-Q3])</h3>" + table(sc, xn, xs, variants, agg))
    o.append("</body></html>")
    Path(path).write_text("".join(o))


def main():
    root = Path(sys.argv[1] if len(sys.argv) > 1 else "results")
    if not root.is_dir():
        sys.exit(f"no such results dir: {root}")
    builds = {}
    data, problems = load(root, builds)
    if not data:
        print("warning: no usable cells found", file=sys.stderr)
    agg = aggregate(data)
    titles = {}
    try:
        sys.path.insert(0, str(Path(__file__).parent))
        import run_matrix
        titles = {k: v["title"] for k, v in run_matrix.SCENARIOS.items()}
    except Exception:
        pass
    write_csv(root / "summary.csv", agg)
    write_html(root / "report.html", agg, problems, root, titles, builds)
    print("builds:")
    for k, n in sorted(builds.items(), key=lambda kv: -kv[1]):
        print(f"  {n:5d} cells  {fmt_build_key(k)}")
    if len(builds) > 1:
        print("WARNING: mixed builds in one results directory", file=sys.stderr)
    deg = sum(1 for _, m in problems if m.startswith("degraded"))
    if deg:
        print(f"{deg} degraded cells excluded from medians")
    print(f"wrote {root/'summary.csv'} and {root/'report.html'}: {len(agg)} groups, {len(problems)} problem cells")
    return 0


if __name__ == "__main__":
    sys.exit(main())
