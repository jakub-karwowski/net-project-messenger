#!/usr/bin/env python3
"""Aggregate run_matrix.py results into summary.csv and a self-contained report.html.

usage: analyze.py [results_dir]        (default: results)
Cells are grouped by (scenario, param, variant); each metric is reduced to
median and IQR (Q1..Q3) across reps. Missing/failed/dirty cells are listed in
the report instead of being silently dropped.
"""
import csv, html, json, math, sys
from pathlib import Path

VARIANTS = ["threads", "pool-sender", "poll", "epoll",
            "poll-duplex", "epoll-duplex", "poll-split", "epoll-split"]
# Colour-blind-safe-ish categorical colours (light, dark) + distinct marker shapes.
COLORS = {"threads": ("#0072B2", "#56B4E9"), "pool-sender": ("#D55E00", "#F0A060"),
          "poll": ("#009E73", "#4FD1A5"), "epoll": ("#CC79A7", "#E8A3C8"),
          # duplex/split: shifted shades of their base poll (green) / epoll (pink) hue
          "poll-duplex": ("#005C42", "#8FE3C2"), "poll-split": ("#8BC34A", "#B5E27A"),
          "epoll-duplex": ("#7A2E58", "#F5C4DE"), "epoll-split": ("#E040A0", "#FF7FC8")}
MARKERS = {"threads": "circle", "pool-sender": "square", "poll": "triangle", "epoll": "diamond",
           "poll-duplex": "cross", "poll-split": "star", "epoll-duplex": "hexagon", "epoll-split": "plus"}

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
]
CHART_KEYS = ["msgs_per_s", "rtt_p50", "rtt_p99", "rtt_p999", "cpu_pct", "nvcsw_rate",
              "maxrss_kb", "threads", "fds"]


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


def cell_metrics(cell_dir, rep, meta):
    """Derive per-rep scalar metrics. Raises ValueError with a clear message."""
    sp = cell_dir / f"rep{rep}.stress.json"
    mp = cell_dir / f"rep{rep}.metrics.jsonl"
    if not sp.exists():
        raise ValueError("missing stress.json")
    st = json.loads(sp.read_text())
    out = dict(msgs_per_s=st["msgs_per_s"], lost=st["lost"], rtt_p50=st["rtt_us"]["p50"],
               rtt_p99=st["rtt_us"]["p99"], rtt_p999=st["rtt_us"]["p999"])
    if not mp.exists():
        raise ValueError("missing metrics.jsonl")
    rows = read_jsonl(mp)
    if len(rows) < 2:
        raise ValueError("metrics.jsonl has <2 samples")
    w = metrics_window(rows, meta.get("params", {}).get("warmup", 0))
    if w:
        a, b = rows[w[0]], rows[w[1]]
        wall = (b["t_ns"] - a["t_ns"]) / 1e9
        out["cpu_pct"] = 100.0 * ((b["utime_us"] + b["stime_us"]) - (a["utime_us"] + a["stime_us"])) / 1e6 / wall
        out["nvcsw_rate"] = (b["nvcsw"] - a["nvcsw"]) / wall
        out["nivcsw_rate"] = (b["nivcsw"] - a["nivcsw"]) / wall
    else:
        out.update(cpu_pct=None, nvcsw_rate=None, nivcsw_rate=None)
    out["maxrss_kb"] = max(r.get("maxrss_kb", 0) for r in rows)
    out["threads"] = max(r.get("threads", 0) for r in rows)
    out["fds"] = max(r.get("fds", 0) for r in rows)
    out["bp_drops"] = rows[-1].get("send_backpressure_drops", 0)
    out["evictions"] = rows[-1].get("evictions", 0)
    return out


def load(root):
    """-> (data, problems). data[(sc, x_name, x, variant)] = list of per-rep dicts."""
    data, problems, titles = {}, [], {}
    for mp in sorted(Path(root).glob("*/*/*/rep*.meta.json")):
        d = mp.parent
        try:
            meta = json.loads(mp.read_text())
        except ValueError:
            problems.append((str(mp.relative_to(root)), "unreadable meta.json"))
            continue
        label = f"{meta.get('scenario')} {meta.get('x_name')}={meta.get('x')} {meta.get('variant')} rep{meta.get('rep')}"
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
    return data, problems


def aggregate(data):
    agg = {}
    for key, reps in data.items():
        a = {"n": len(reps)}
        for mk, _, _, _ in METRICS:
            vals = [r.get(mk) for r in reps]
            if mk in ("maxrss_kb", "threads"):   # peak across reps' peaks: report median of peaks too
                pass
            a[mk] = median_iqr(vals)
            a[mk + "_peak"] = max([v for v in vals if v is not None], default=float("nan"))
        agg[key] = a
    return agg


# --- CSV --------------------------------------------------------------------
def write_csv(path, agg):
    cols = ["scenario", "param", "value", "variant", "reps"]
    for mk, _, _, _ in METRICS:
        cols += [mk + "_median", mk + "_q1", mk + "_q3"]
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for (sc, xn, x, v) in sorted(agg, key=lambda k: (k[0], k[2], VARIANTS.index(k[3]) if k[3] in VARIANTS else 9)):
            a = agg[(sc, xn, x, v)]
            row = [sc, xn, x, v, a["n"]]
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
            cells = [v, str(x), str(a["n"])]
            for mk, _, _, _ in METRICS:
                m, q1, q3 = a[mk]
                cells.append("-" if m != m else f"{fmt(m)} <span class='mut'>[{fmt(q1)}-{fmt(q3)}]</span>")
            h.append("<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>")
    return "".join(h) + "</table>"


def write_html(path, agg, problems, root, titles):
    scs = sorted({k[0] for k in agg})
    o = [f"<!doctype html><html lang=en><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'>"
         f"<title>Stress report</title><style>{css()}</style><body><h1>I/O variant stress report</h1>",
         f"<p class=mut>Source: {html.escape(str(root))}. Points are medians across reps; vertical bars are IQR (Q1-Q3). "
         "CPU, ctx-switch rates are computed over the measurement window (after warmup) from the node's metrics.jsonl; "
         "RSS and threads are peaks.</p>",
         "<div class=legend>" + "".join(f"<span><i class='sw-{v}' style='width:14px;height:14px;display:inline-block;border-radius:3px'></i>{v}</span>" for v in VARIANTS) + "</div>"]
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
        for mk in CHART_KEYS:
            _, label, unit, lg = next(m for m in METRICS if m[0] == mk)
            series = {v: [agg[(sc, xn, x, v)][mk] if (sc, xn, x, v) in agg else None for x in xs] for v in variants}
            o.append(f"<div class=card>{chart(label, unit, xn, xs, series, lg)}</div>")
        for mk in ("lost", "bp_drops", "evictions"):   # only interesting when nonzero
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
    data, problems = load(root)
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
    write_html(root / "report.html", agg, problems, root, titles)
    print(f"wrote {root/'summary.csv'} and {root/'report.html'}: {len(agg)} groups, {len(problems)} problem cells")
    return 0


if __name__ == "__main__":
    sys.exit(main())
