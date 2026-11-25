#!/usr/bin/env python3
"""Summarise a run_matrix.py results directory.

    analyze.py RESULTS            # -> RESULTS/summary.csv and RESULTS/report.html

Every cell (scenario, value, variant) is summarised over its valid repetitions
as median and interquartile range. A completed cell is invalid when one of
validity()'s reasons applies; invalid and failed cells are left out of the
medians and listed under "Problems" in the report.

The report has, per scenario, four charts (throughput, round-trip p99, node
CPU, node memory; one measure each, never two axes) and the full table. It is one
self-contained HTML file: no network, light and dark mode.
"""

import csv
import html
import json
import math
import statistics
import sys
from pathlib import Path

# Fixed order: a variant keeps its colour whichever subset is shown. A run's
# own variant set (run.json) replaces it; see use_variant_order().
VARIANT_ORDER = ["fixed-1", "fixed-4", "balancer", "balancer-fast"]


def use_variant_order(run_info):
    """Colours follow the order of the variants the run compared."""
    variants = list((run_info or {}).get("variants") or [])
    if variants:
        VARIANT_ORDER[:] = variants
# Categorical slots 1-4 of the reference palette (adjacent pairs validated in
# both modes); dark steps are the same hues stepped for the dark surface.
LIGHT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
DARK = ["#3987e5", "#d95926", "#199e70", "#c98500"]

X_LABELS = {"peers": "active peers", "idle": "idle peers", "slow": "slow peers", "churn": "reconnects per second",
            "size": "message size (bytes)", "max_links": "node --max-links"}


# ------------------------------------------------------------------ validity

def validity(meta, result):
    """Reasons a completed cell must not count; empty when it is valid."""
    reasons = []
    gen, node = result.get("loadgen"), result.get("node", {})
    if gen is None:
        return ["no loadgen result"]
    if node.get("exit_code") != 0 or node.get("forced"):
        reasons.append(f"node exit {node.get('exit_code')}{' (forced)' if node.get('forced') else ''}")
    peers = gen["config"]["peers"]
    expected = peers if meta.get("x") != "max_links" else min(peers, int(meta["value"]))
    if gen["active_ready"] < math.floor(0.95 * expected):
        reasons.append(f"only {gen['active_ready']} of {expected} peers connected")
    if gen.get("behind", 0) > 0:
        reasons.append(f"loadgen fell behind {gen['behind']} times: the generator, not the node, set the pace")
    if gen.get("protocol_errors", 0) > 0:
        reasons.append(f"{gen['protocol_errors']} protocol errors")
    duration = gen["config"]["duration"]
    if node.get("cpu_window_s", 0) < 0.5 * duration:
        reasons.append(f"CPU sampled over {node.get('cpu_window_s', 0)} s of a {duration} s measurement")
    return reasons


# ------------------------------------------------------------------ loading

def load_cells(root):
    cells = []
    for meta_path in sorted(Path(root).glob("*/*=*/*/rep*/meta.json")):
        meta = json.loads(meta_path.read_text())
        result_path = meta_path.parent / "result.json"
        result = json.loads(result_path.read_text()) if result_path.exists() else None
        cells.append((meta, result))
    return cells


def metrics(result):
    """The numbers one repetition contributes."""
    gen, node = result["loadgen"], result["node"]
    cfg = gen["config"]
    cpu = node.get("cpu_percent", {})
    bal = node.get("balancer") or {}
    offered = cfg["peers"] * cfg["rate"] if cfg["rate"] > 0 else None
    return {
        "active_ready": gen["active_ready"],
        "offered_per_s": offered,
        "echoed_per_s": float(gen["echoed_per_s"]),
        "delivered_pct": 100.0 * gen["echoed"] / gen["sent"] if gen["sent"] else None,
        "rtt_p50_ms": gen["rtt"]["p50_ms"],
        "rtt_p99_ms": gen["rtt"]["p99_ms"],
        "lost": gen["lost"],
        "send_failed": gen["send_failed"],
        "handshake_p99_ms": gen["handshake"]["p99_ms"],
        "reconnect_p50_ms": gen["reconnect"]["p50_ms"] if gen["reconnects"] else None,
        "node_cpu_pct": node.get("cpu_percent_total"),
        "busiest_thread": next(iter(cpu), None),
        "busiest_cpu_pct": next(iter(cpu.values()), None),
        "threads": node.get("max_threads"),
        "rss_mb": round(node.get("max_rss_kb", 0) / 1024, 1),
        "fds": node.get("max_fds"),
        "pts_max": node.get("max_pt_threads", bal.get("max_pts")),
        "moves": bal.get("moves"),
        # Backpressure: the deepest Store or Router inbox, pauses and holds.
        "inbox_max": max((bal.get("inbox_max") or {}).get(q, 0) for q in ("store", "router")) if bal.get("inbox_max") else None,
        "pauses": bal.get("pauses"),
        "holds": bal.get("holds"),
    }


NUMERIC = ["active_ready", "offered_per_s", "echoed_per_s", "delivered_pct", "rtt_p50_ms", "rtt_p99_ms", "lost",
           "send_failed", "handshake_p99_ms", "reconnect_p50_ms", "node_cpu_pct", "busiest_cpu_pct", "threads",
           "rss_mb", "fds", "pts_max", "moves", "inbox_max", "pauses", "holds"]


def quartiles(values):
    """(Q1, median, Q3) with linear interpolation; one value gives three equal ones."""
    v = sorted(values)
    if len(v) == 1:
        return v[0], v[0], v[0]
    q1, med, q3 = statistics.quantiles(v, n=4, method="inclusive")
    return q1, med, q3


def value_key(v):
    try:
        return (0, float(v))
    except (TypeError, ValueError):
        return (1, str(v))


def aggregate(cells):
    """{(scenario, x, value, variant): summary} over valid repetitions, plus the problems."""
    groups, problems, titles = {}, [], {}
    for meta, result in cells:
        key = (meta["scenario"], meta["x"], meta["value"], meta["variant"])
        g = groups.setdefault(key, dict(valid=[], total=0))
        g["total"] += 1
        if meta.get("status") == "failed" or result is None or result.get("loadgen") is None:
            problems.append((meta, [meta.get("error", "no result")]))
            continue
        reasons = validity(meta, result)
        if reasons:
            problems.append((meta, reasons))
            continue
        g["valid"].append(metrics(result))
    summary = {}
    for key, g in groups.items():
        row = dict(n_valid=len(g["valid"]), n_total=g["total"])
        for m in NUMERIC:
            vals = [r[m] for r in g["valid"] if r.get(m) is not None]
            row[m] = quartiles(vals) if vals else None
        threads = [r["busiest_thread"] for r in g["valid"] if r.get("busiest_thread")]
        row["busiest_thread"] = max(set(threads), key=threads.count) if threads else None
        summary[key] = row
    return summary, problems


# ------------------------------------------------------------------ CSV

def write_csv(summary, path):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        header = ["scenario", "x", "value", "variant", "n_valid", "n_total", "busiest_thread"]
        for m in NUMERIC:
            header += [f"{m}_median", f"{m}_q1", f"{m}_q3"]
        w.writerow(header)
        for key in sorted(summary, key=lambda k: (k[0], value_key(k[2]), variant_rank(k[3]))):
            row = summary[key]
            out = [*key, row["n_valid"], row["n_total"], row["busiest_thread"] or ""]
            for m in NUMERIC:
                q = row[m]
                out += [f"{q[1]:.4g}", f"{q[0]:.4g}", f"{q[2]:.4g}"] if q else ["", "", ""]
            w.writerow(out)


def variant_rank(v):
    return VARIANT_ORDER.index(v) if v in VARIANT_ORDER else len(VARIANT_ORDER)


# ------------------------------------------------------------------ charts

def nice_ticks(lo, hi, count=4):
    if hi <= lo:
        hi = lo + 1
    raw = (hi - lo) / count
    mag = 10 ** math.floor(math.log10(raw))
    step = min((s * mag for s in (1, 2, 2.5, 5, 10) if s * mag >= raw), default=raw)
    start = math.floor(lo / step) * step
    ticks = []
    t = start
    while t <= hi + step * 0.5:
        ticks.append(round(t, 10))
        t += step
    return ticks


def fmt(v):
    if v is None:
        return "-"
    if abs(v) >= 1000:
        return f"{v / 1000:.1f}k" if abs(v) < 1e6 else f"{v / 1e6:.2f}M"
    if abs(v) >= 10 or v == int(v):
        return f"{v:.0f}"
    return f"{v:.2g}" if abs(v) < 1 else f"{v:.1f}"


def line_chart(scenario, x, values, variants, summary, metric, title, unit, log=False):
    """One SVG line chart: x = the swept values (evenly spaced), y = `metric`,
    one line per variant with its IQR as whiskers. Hover shows the numbers."""
    width, height = 560, 260
    left, right, top, bottom = 56, 96, 16, 40
    pw, ph = width - left - right, height - top - bottom
    pts = {}
    for v in variants:
        series = []
        for i, val in enumerate(values):
            row = summary.get((scenario, x, val, v))
            q = row.get(metric) if row else None
            if q is not None:
                series.append((i, q))
        pts[v] = series
    all_y = [y for s in pts.values() for _, q in s for y in q]
    if not all_y:
        return f'<p class="empty">{html.escape(title)}: no valid data.</p>'
    positive = [y for y in all_y if y > 0]
    log = log and positive and max(positive) / min(positive) > 50
    if log:
        lo = 10 ** math.floor(math.log10(min(positive)))
        hi = 10 ** math.ceil(math.log10(max(positive)))
        ticks = [10 ** e for e in range(int(math.log10(lo)), int(math.log10(hi)) + 1)]
        ymap = lambda y: top + ph - (math.log10(max(y, lo)) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * ph
    else:
        ticks = nice_ticks(0, max(all_y))
        hi = ticks[-1]
        ymap = lambda y: top + ph - (y / hi) * ph
    n = len(values)
    xmap = lambda i: left + (pw * (i + 0.5) / n)

    out = [f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{html.escape(title)}" class="chart">']
    for t in ticks:
        y = ymap(t)
        out.append(f'<line class="grid" x1="{left}" x2="{left + pw}" y1="{y:.1f}" y2="{y:.1f}"/>')
        out.append(f'<text class="tick" x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{fmt(t)}</text>')
    for i, val in enumerate(values):
        out.append(f'<text class="tick" x="{xmap(i):.1f}" y="{top + ph + 16}" text-anchor="middle">{html.escape(str(val))}</text>')
    out.append(f'<text class="axis" x="{left + pw / 2:.1f}" y="{height - 6}" text-anchor="middle">'
               f'{html.escape(X_LABELS.get(x, x))}</text>')
    out.append(f'<text class="axis" x="12" y="{top + ph / 2:.1f}" text-anchor="middle" '
               f'transform="rotate(-90 12 {top + ph / 2:.1f})">{html.escape(unit)}{" (log)" if log else ""}</text>')

    ends = []
    for v in variants:
        series = pts[v]
        if not series:
            continue
        slot = variant_rank(v) % 4 + 1
        d = " ".join(f"{'M' if k == 0 else 'L'}{xmap(i):.1f},{ymap(q[1]):.1f}" for k, (i, q) in enumerate(series))
        out.append(f'<path class="line s{slot}" d="{d}"/>')
        for i, q in series:
            cx = xmap(i)
            out.append(f'<line class="whisker s{slot}" x1="{cx:.1f}" x2="{cx:.1f}" y1="{ymap(q[0]):.1f}" y2="{ymap(q[2]):.1f}"/>')
        for i, q in series:
            cx, cy = xmap(i), ymap(q[1])
            tip = (f"{v} · {X_LABELS.get(x, x)} {values[i]}: {fmt(q[1])} {unit} "
                   f"(IQR {fmt(q[0])}–{fmt(q[2])})")
            out.append(f'<circle class="dot s{slot}" cx="{cx:.1f}" cy="{cy:.1f}" r="4"/>')
            out.append(f'<circle class="hit" cx="{cx:.1f}" cy="{cy:.1f}" r="11" data-tip="{html.escape(tip)}"/>')
        last_i, last_q = series[-1]
        ends.append([ymap(last_q[1]), v, slot, xmap(last_i)])
    # Direct labels at the line ends, pushed apart so they never overlap.
    ends.sort()
    for k in range(1, len(ends)):
        ends[k][0] = max(ends[k][0], ends[k - 1][0] + 13)
    for y, v, slot, xend in ends:
        out.append(f'<text class="label" x="{xend + 10:.1f}" y="{y + 4:.1f}">{html.escape(v)}</text>')
    out.append("</svg>")
    return f'<figure><figcaption>{html.escape(title)}</figcaption>{"".join(out)}</figure>'


# ------------------------------------------------------------------ report

CSS = """
.viz-root { color-scheme: light; --surface-1:#fcfcfb; --surface-2:#f3f2ef; --text-primary:#0b0b0b;
  --text-secondary:#52514e; --text-muted:#7a7974; --grid:#e4e3df; --border:#d8d7d2;
  --s1:%s; --s2:%s; --s3:%s; --s4:%s; }
@media (prefers-color-scheme: dark) { :root:where(:not([data-theme="light"])) .viz-root { color-scheme: dark;
  --surface-1:#1a1a19; --surface-2:#242422; --text-primary:#f0efec; --text-secondary:#c3c2b7; --text-muted:#8f8e86;
  --grid:#30302d; --border:#3a3a37; --s1:%s; --s2:%s; --s3:%s; --s4:%s; } }
:root[data-theme="dark"] .viz-root { color-scheme: dark; --surface-1:#1a1a19; --surface-2:#242422;
  --text-primary:#f0efec; --text-secondary:#c3c2b7; --text-muted:#8f8e86; --grid:#30302d; --border:#3a3a37;
  --s1:%s; --s2:%s; --s3:%s; --s4:%s; }
html, body { margin:0; }
body.viz-root { background:var(--surface-1); color:var(--text-primary);
  font:14px/1.45 system-ui,-apple-system,"Segoe UI",sans-serif; padding:24px 16px; max-width:1200px; margin:auto; }
h1 { font-size:22px; margin:0 0 4px; } h2 { font-size:18px; margin:32px 0 4px; } p { color:var(--text-secondary); margin:4px 0 12px; }
.legend { display:flex; gap:16px; flex-wrap:wrap; margin:8px 0 12px; color:var(--text-secondary); }
.legend span::before { content:""; display:inline-block; width:14px; height:3px; border-radius:2px; margin-right:6px;
  vertical-align:middle; background:var(--c); }
.charts { display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:12px; }
figure { margin:0; background:var(--surface-2); border-radius:8px; padding:8px 8px 0; }
figcaption { font-weight:600; font-size:13px; padding:4px 4px 0; }
.chart { width:100%%; height:auto; display:block; }
.grid { stroke:var(--grid); stroke-width:1; } .tick { fill:var(--text-muted); font-size:11px; }
.axis { fill:var(--text-secondary); font-size:11px; } .label { fill:var(--text-secondary); font-size:11px; }
.line { fill:none; stroke-width:2; stroke-linejoin:round; stroke-linecap:round; }
.whisker { stroke-width:1.5; opacity:.55; }
.dot { stroke:var(--surface-2); stroke-width:2; }
.s1 { stroke:var(--s1); } .dot.s1 { fill:var(--s1); } .s2 { stroke:var(--s2); } .dot.s2 { fill:var(--s2); }
.s3 { stroke:var(--s3); } .dot.s3 { fill:var(--s3); } .s4 { stroke:var(--s4); } .dot.s4 { fill:var(--s4); }
.hit { fill:transparent; cursor:default; }
#tip { position:fixed; pointer-events:none; background:var(--surface-1); color:var(--text-primary);
  border:1px solid var(--border); border-radius:6px; padding:6px 8px; font-size:12px; display:none; z-index:2;
  box-shadow:0 2px 8px rgba(0,0,0,.15); max-width:320px; }
.table-wrap { overflow-x:auto; margin-top:12px; }
table { border-collapse:collapse; font-size:12px; font-variant-numeric:tabular-nums; }
th, td { padding:4px 8px; border-bottom:1px solid var(--grid); text-align:right; white-space:nowrap; }
th { color:var(--text-secondary); font-weight:600; position:sticky; top:0; background:var(--surface-1); }
td:nth-child(-n+2), th:nth-child(-n+2) { text-align:left; }
.iqr { color:var(--text-muted); } .empty { color:var(--text-muted); }
ul.problems li { color:var(--text-secondary); }
""" % (*LIGHT, *DARK, *DARK)

JS = """
const tip = document.getElementById('tip');
document.addEventListener('pointerover', e => {
  const t = e.target.closest('[data-tip]'); if (!t) return;
  tip.textContent = t.dataset.tip; tip.style.display = 'block';
});
document.addEventListener('pointermove', e => {
  if (tip.style.display !== 'block') return;
  tip.style.left = Math.min(e.clientX + 14, innerWidth - tip.offsetWidth - 8) + 'px';
  tip.style.top = (e.clientY + 14) + 'px';
});
document.addEventListener('pointerout', e => { if (e.target.closest('[data-tip]')) tip.style.display = 'none'; });
"""

TABLE_COLUMNS = [("echoed_per_s", "echoed/s"), ("delivered_pct", "delivered %"), ("rtt_p50_ms", "RTT p50 ms"),
                 ("rtt_p99_ms", "RTT p99 ms"), ("send_failed", "refused"), ("node_cpu_pct", "node CPU %"),
                 ("busiest_cpu_pct", "busiest %"), ("threads", "threads"), ("rss_mb", "RSS MB"), ("fds", "fds"),
                 ("pts_max", "pts"), ("moves", "moves"), ("inbox_max", "inbox max"), ("pauses", "pauses"),
                 ("holds", "holds"), ("handshake_p99_ms", "handshake p99 ms"),
                 ("reconnect_p50_ms", "reconnect p50 ms"), ("active_ready", "peers up")]


def cell_text(q):
    if q is None:
        return "-"
    if q[0] == q[2]:
        return fmt(q[1])
    return f'{fmt(q[1])} <span class="iqr">[{fmt(q[0])}–{fmt(q[2])}]</span>'


def write_report(summary, problems, run_info, path):
    scenarios = run_info.get("scenarios", {})
    by_scenario = {}
    for (s, x, value, variant) in summary:
        by_scenario.setdefault(s, dict(x=x, values=set(), variants=set()))
        by_scenario[s]["values"].add(value)
        by_scenario[s]["variants"].add(variant)
    used = sorted({v for d in by_scenario.values() for v in d["variants"]}, key=variant_rank)
    legend = "".join(f'<span style="--c:var(--s{variant_rank(v) % 4 + 1})">{html.escape(v)}</span>' for v in used)
    build = run_info.get("build", {})
    parts = [f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Load matrix report</title>
<style>{CSS}</style></head><body class="viz-root"><div id="tip"></div>
<h1>Load matrix report</h1>
<p>Commit {html.escape(str(build.get('commit', '?'))[:10])}{' with uncommitted changes' if build.get('dirty_files') else ''},
{html.escape(str(build.get('build_type', '?')))} build{', quick run' if run_info.get('quick') else ''}.
Points are medians over valid repetitions; whiskers and brackets are the interquartile range.</p>
<div class="legend">{legend}</div>"""]
    for s in sorted(by_scenario):
        d = by_scenario[s]
        values = sorted(d["values"], key=value_key)
        variants = sorted(d["variants"], key=variant_rank)
        title = scenarios.get(s, {}).get("title", s)
        parts.append(f"<h2>{html.escape(s)}: {html.escape(title)}</h2>")
        base = scenarios.get(s, {}).get("base")
        if base:
            parts.append(f"<p>Swept: {html.escape(X_LABELS.get(d['x'], d['x']))}. Other load: "
                         f"{html.escape(', '.join(f'{k} {v}' for k, v in base.items()))}.</p>")
        parts.append('<div class="charts">')
        parts.append(line_chart(s, d["x"], values, variants, summary, "echoed_per_s", "Throughput", "msg/s"))
        parts.append(line_chart(s, d["x"], values, variants, summary, "rtt_p99_ms", "Round trip, p99", "ms", log=True))
        parts.append(line_chart(s, d["x"], values, variants, summary, "node_cpu_pct", "Node CPU", "% of one core"))
        parts.append(line_chart(s, d["x"], values, variants, summary, "rss_mb", "Node memory", "MB", log=True))
        parts.append("</div>")
        rows = []
        for value in values:
            for v in variants:
                row = summary.get((s, d["x"], value, v))
                if not row:
                    continue
                cells = "".join(f"<td>{cell_text(row[m])}</td>" for m, _ in TABLE_COLUMNS)
                rows.append(f"<tr><td>{html.escape(str(value))}</td><td>{html.escape(v)}</td>{cells}"
                            f"<td>{html.escape(row['busiest_thread'] or '-')}</td><td>{row['n_valid']}/{row['n_total']}</td></tr>")
        head = "".join(f"<th>{html.escape(label)}</th>" for _, label in TABLE_COLUMNS)
        parts.append(f'<div class="table-wrap"><table><thead><tr><th>{html.escape(d["x"])}</th><th>variant</th>{head}'
                     f'<th>busiest thread</th><th>valid</th></tr></thead><tbody>{"".join(rows)}</tbody></table></div>')
    parts.append("<h2>Problems</h2>")
    if problems:
        items = "".join(f"<li>{html.escape(m['scenario'])} {html.escape(str(m['x']))}={html.escape(str(m['value']))} "
                        f"{html.escape(m['variant'])} rep{m['rep']}: {html.escape('; '.join(r))}</li>" for m, r in problems)
        parts.append(f'<ul class="problems">{items}</ul>')
    else:
        parts.append("<p>None: every cell completed and is valid.</p>")
    parts.append(f"<script>{JS}</script></body></html>")
    Path(path).write_text("\n".join(parts))


def main(argv):
    if len(argv) != 1:
        sys.exit(__doc__.split("\n\n")[1])
    root = Path(argv[0])
    cells = load_cells(root)
    if not cells:
        sys.exit(f"analyze.py: no cells under {root}")
    try:
        run_info = json.loads((root / "run.json").read_text())
    except (OSError, json.JSONDecodeError):
        run_info = {}
    use_variant_order(run_info)
    summary, problems = aggregate(cells)
    write_csv(summary, root / "summary.csv")
    write_report(summary, problems, run_info, root / "report.html")
    valid = sum(r["n_valid"] for r in summary.values())
    print(f"{len(cells)} cells, {valid} valid, {len(problems)} with problems -> "
          f"{root / 'summary.csv'}, {root / 'report.html'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
