#!/usr/bin/env python3
"""Build every figure and table of the report from one results directory.

    python make_figures.py [--results ../../results-0294f37] [--out ../tex]

Reads summary.csv (medians and IQR over valid repetitions, written by
scripts/analyze.py) and, for the churn scatter, the per-repetition files.
Writes PDFs to <out>/figures and LaTeX table bodies to <out>/tables.
"""
import argparse
import csv
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import style
from style import VARIANTS, line_kw, color

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]


def num(text):
    try:
        v = float(text)
    except (TypeError, ValueError):
        return math.nan
    return v


def load_summary(results):
    rows = {}
    with open(results / "summary.csv", newline="") as f:
        for r in csv.DictReader(f):
            rows[(r["scenario"], int(float(r["value"])), r["variant"])] = r
    return rows


class Data:
    def __init__(self, results):
        self.results = results
        self.rows = load_summary(results)

    def values(self, scenario):
        return sorted({v for (s, v, _) in self.rows if s == scenario})

    def get(self, scenario, value, variant, column, stat="median"):
        r = self.rows.get((scenario, value, variant))
        return num(r[f"{column}_{stat}"]) if r else math.nan

    def series(self, scenario, variant, column, scale=1.0):
        xs = self.values(scenario)
        return xs, [self.get(scenario, x, variant, column) * scale for x in xs]


def save(fig, out, name):
    fig.savefig(out / "figures" / f"{name}.pdf")
    plt.close(fig)
    print("wrote", f"figures/{name}.pdf")


def lines(ax, d, scenario, column, scale=1.0, xpos=None, iqr=True):
    """One line per variant; the shaded band is the IQR over repetitions."""
    for v in VARIANTS:
        xs, ys = d.series(scenario, v, column, scale)
        px = xpos(xs) if xpos else xs
        ax.plot(px, ys, **line_kw(v))
        if iqr:
            lo = [d.get(scenario, x, v, column, "q1") * scale for x in xs]
            hi = [d.get(scenario, x, v, column, "q3") * scale for x in xs]
            ax.fill_between(px, lo, hi, color=color(v), alpha=0.10, linewidth=0)


def categorical_x(ax, xs, label):
    ax.set_xticks(range(len(xs)))
    ax.set_xticklabels([str(x) for x in xs])
    ax.set_xlabel(label)
    return lambda vals: [xs.index(v) for v in vals]


# --- figures -------------------------------------------------------------------

def fig_s1(d, out):
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.6))
    peers = d.values("S1")
    for v in VARIANTS:
        xs, mps = d.series("S1", v, "msgs_per_s")
        axes[0].plot(xs, [100 * m / (x * 100) for m, x in zip(mps, xs)], **line_kw(v))
    axes[0].set(xscale="log", xlabel="active peers", ylabel="delivered (% of offered)",
                title="Delivered load", ylim=(0, 105))
    lines(axes[1], d, "S1", "rtt_p99", 1e-3)
    axes[1].set(xscale="log", yscale="log", xlabel="active peers", ylabel="RTT p99 (ms)",
                title="Tail latency")
    lines(axes[2], d, "S1", "cpu_pct", iqr=False)
    axes[2].set(xscale="log", xlabel="active peers", ylabel="node CPU (%)",
                title="CPU (2 cores = 200%)", ylim=(0, 210))
    for ax in axes:
        ax.set_xticks(peers)
        ax.set_xticklabels([str(p) for p in peers])
        ax.minorticks_off() if ax is not axes[1] else None
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    style.shared_legend(fig, axes[0], ncol=9)
    save(fig, out, "s1_fanin")


def fig_s2(d, out):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6))
    xs = d.values("S2")
    pos = categorical_x(axes[0], xs, "idle peers (plus 8 active)")
    lines(axes[0], d, "S2", "rtt_p99", 1e-3, xpos=pos)
    axes[0].set(ylabel="RTT p99 (ms)", title="Tail latency")
    pos = categorical_x(axes[1], xs, "idle peers (plus 8 active)")
    lines(axes[1], d, "S2", "cpu_pct", xpos=pos, iqr=False)
    axes[1].set(ylabel="node CPU (%)", title="CPU")
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    style.shared_legend(fig, axes[0], ncol=9)
    save(fig, out, "s2_idle")


def fig_s3(d, out):
    fig, ax = plt.subplots(figsize=(3.5, 2.6))
    xs = d.values("S3")
    pos = categorical_x(ax, xs, "slow peers (of 16 active)")
    lines(ax, d, "S3", "rtt_p99", 1e-3, xpos=pos)
    ax.set(yscale="log", ylabel="RTT p99 of all peers (ms)")
    ax.legend(ncol=2, loc="upper left", fontsize=6.5)
    fig.tight_layout()
    save(fig, out, "s3_slow")


def s4_cells(results):
    """Per-repetition (variant, churn, send_failures, p99 ms) for S4."""
    cells = []
    for meta in sorted((results / "S4").glob("churn=*/*/rep*.meta.json")):
        stem = str(meta)[: -len(".meta.json")]
        m = json.loads(meta.read_text())
        if not m.get("validity", {}).get("ok", True):
            continue
        stress = json.loads(Path(stem + ".stress.json").read_text())
        last = Path(stem + ".metrics.jsonl").read_text().strip().splitlines()[-1]
        cells.append((m["variant"], int(m["x"]), json.loads(last)["send_failures"],
                      stress["rtt_us"]["p99"] / 1e3))
    return cells


def fig_s4(d, out):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.7))
    xs = d.values("S4")
    pos = categorical_x(axes[0], xs, "reconnects per second")
    lines(axes[0], d, "S4", "rtt_p99", 1e-3, xpos=pos)
    axes[0].set(yscale="log", ylabel="RTT p99 (ms)", title="Tail latency under churn")
    for v, churn, fails, p99 in s4_cells(d.results):
        if churn == 0:
            continue
        kw = line_kw(v)
        axes[1].scatter(fails, p99, color=kw["color"], marker=kw["marker"], s=22,
                        edgecolors="white", linewidths=0.6, zorder=3)
    axes[1].axhline(100, color=style.TEXT_2, linewidth=0.8, linestyle="--")
    axes[1].annotate("100 ms retry sleep", (1, 100), xytext=(1.2, 120),
                     fontsize=7, color=style.TEXT_2)
    axes[1].set(xscale="symlog", yscale="log", xlabel="node send failures (whole run)",
                ylabel="RTT p99 (ms)", title="p99 follows the sender's failures")
    axes[1].set_xlim(-0.3, 300)
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    style.shared_legend(fig, axes[0], ncol=9)
    save(fig, out, "s4_churn")


def fig_s5(d, out):
    sizes = d.values("S5")
    fig, axes = plt.subplots(1, len(sizes), figsize=(7.2, 2.7), sharey=True)
    labels = {32: "32 B", 4096: "4 KiB", 65536: "64 KiB"}
    for ax, size in zip(axes, sizes):
        for i, v in enumerate(VARIANTS):
            med = d.get("S5", size, v, "msgs_per_s") / 1e3
            lo = d.get("S5", size, v, "msgs_per_s", "q1") / 1e3
            hi = d.get("S5", size, v, "msgs_per_s", "q3") / 1e3
            kw = line_kw(v)
            ax.hlines(i, 0, med, color=kw["color"], linewidth=1.2, linestyle=kw["linestyle"])
            ax.plot(med, i, marker=kw["marker"], color=kw["color"], markersize=6,
                    markeredgecolor="white", markeredgewidth=0.8)
            ax.hlines(i, lo, hi, color=style.TEXT, linewidth=0.8)
        ax.set(title=labels.get(size, f"{size} B"), xlabel="thousand msg/s")
        ax.set_xlim(left=0)
        ax.grid(axis="y", visible=False)
    axes[0].set_yticks(range(len(VARIANTS)))
    axes[0].set_yticklabels(VARIANTS)
    axes[0].invert_yaxis()
    fig.tight_layout()
    save(fig, out, "s5_throughput")


def fig_s6a(d, out):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.7))
    rtts = d.values("S6a")
    lines(axes[0], d, "S6a", "handshake_p50", 1e-3)
    axes[0].plot(rtts, [2 * r for r in rtts], color=style.TEXT_2, linewidth=0.8, linestyle="-.")
    axes[0].plot(rtts, [3 * r for r in rtts], color=style.TEXT_2, linewidth=0.8, linestyle="-.")
    axes[0].text(rtts[-1] + 1, 3 * rtts[-1], "3·RTT", fontsize=7, color=style.TEXT_2, va="center")
    axes[0].text(rtts[-1] + 1, 2 * rtts[-1], "2·RTT", fontsize=7, color=style.TEXT_2, va="center")
    axes[0].set_xlim(right=rtts[-1] + 7)
    axes[0].set(xlabel="RTT (ms)", ylabel="handshake p50 (ms)",
                title="Connect to Ready", xticks=rtts)
    lines(axes[1], d, "S6a", "msgs_per_s")
    for v in ["threads", "pool-sender", "poll", "epoll"]:
        for r in rtts:
            if math.isnan(d.get("S6a", r, v, "msgs_per_s")):
                axes[1].plot(r, 1150, marker="x", color=color(v), markersize=6)
    axes[1].annotate("simplex modes collapsed\n(10–13 of 32 peers ready)", (44, 1150),
                     xytext=(18, 1500), fontsize=7, color=style.TEXT_2,
                     arrowprops=dict(arrowstyle="-", color=style.TEXT_2, linewidth=0.6))
    axes[1].set(xlabel="RTT (ms)", ylabel="msg/s (offered 3,200)", title="Delivered rate",
                xticks=rtts, ylim=(1000, 3300))
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    style.shared_legend(fig, axes[0], ncol=9)
    save(fig, out, "s6a_wan_churn")


def fig_s7(d, out):
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6))
    caps = d.values("S7")
    pos = categorical_x(axes[0], caps, "--max-peers (1,024 peers offered)")
    lines(axes[0], d, "S7", "rtt_p99", 1e-3, xpos=pos)
    axes[0].set(yscale="log", ylabel="RTT p99 (ms)", title="Tail latency")
    pos = categorical_x(axes[1], caps, "--max-peers (1,024 peers offered)")
    lines(axes[1], d, "S7", "cpu_pct", xpos=pos, iqr=False)
    axes[1].set(ylabel="node CPU (%)", title="CPU", ylim=(0, 210))
    fig.tight_layout(rect=(0, 0.1, 1, 1))
    style.shared_legend(fig, axes[0], ncol=9)
    save(fig, out, "s7_capped")


# --- tables --------------------------------------------------------------------

def tex_variant(v):
    return r"\texttt{" + v + "}"


def fmt(x, digits=0):
    if math.isnan(x):
        return "--"
    return f"{x:,.{digits}f}".replace(",", "{,}")


def write_table(out, name, header, rows, groups=()):
    # The rules live in the file: \input inside a tabular cannot be followed by \bottomrule.
    # `groups` are extra header lines (e.g. \multicolumn and \cmidrule) above `header`.
    lines_ = [r"\toprule", *groups, " & ".join(header) + r" \\", r"\midrule"]
    lines_ += [" & ".join(r) + r" \\" for r in rows]
    lines_.append(r"\bottomrule")
    (out / "tables" / f"{name}.tex").write_text("\n".join(lines_) + "\n")
    print("wrote", f"tables/{name}.tex")


def tables(d, out):
    g = d.get
    write_table(out, "s1_1024",
                ["Variant", "msg/s", "lost", "p50 (ms)", "p99 (ms)", "CPU \\%", "threads", "fds"],
                [[tex_variant(v), fmt(g("S1", 1024, v, "msgs_per_s")), fmt(g("S1", 1024, v, "lost")),
                  fmt(g("S1", 1024, v, "rtt_p50") / 1e3), fmt(g("S1", 1024, v, "rtt_p99") / 1e3),
                  fmt(g("S1", 1024, v, "cpu_pct")), fmt(g("S1", 1024, v, "threads")),
                  fmt(g("S1", 1024, v, "fds"))] for v in VARIANTS])
    write_table(out, "s1_256",
                ["Variant", "p50 (ms)", "p99 (ms)", "CPU \\%", "vol.\\ ctx/s"],
                [[tex_variant(v), fmt(g("S1", 256, v, "rtt_p50") / 1e3, 1),
                  fmt(g("S1", 256, v, "rtt_p99") / 1e3, 1), fmt(g("S1", 256, v, "cpu_pct")),
                  fmt(g("S1", 256, v, "nvcsw_rate"))] for v in VARIANTS])
    write_table(out, "s5",
                ["Variant", "32 B", "4 KiB", "64 KiB", "CPU \\% (4 KiB)"],
                [[tex_variant(v)] + [fmt(g("S5", s, v, "msgs_per_s") / 1e3, 1) for s in (32, 4096, 65536)]
                 + [fmt(g("S5", 4096, v, "cpu_pct"))] for v in VARIANTS])
    write_table(out, "s6b",
                ["Variant"] + [f"{r} Mbit/s" for r in (1000, 200, 50)] + ["queued (50)", "handshake p50 (ms)"],
                [[tex_variant(v)] + [fmt(g("S6b", r, v, "msgs_per_s")) for r in (1000, 200, 50)]
                 + [fmt(g("S6b", 50, v, "tx_queued_sends")), fmt(g("S6b", 1000, v, "handshake_p50") / 1e3, 1)]
                 for v in VARIANTS])
    write_table(out, "s7_256",
                ["Variant", "peers served", "msg/s", "p99 (ms)", "CPU \\%", "threads", "evictions"],
                [[tex_variant(v), fmt(g("S7", 256, v, "peers_ready")), fmt(g("S7", 256, v, "msgs_per_s")),
                  fmt(g("S7", 256, v, "rtt_p99") / 1e3, 1), fmt(g("S7", 256, v, "cpu_pct")),
                  fmt(g("S7", 256, v, "threads")), fmt(g("S7", 256, v, "evictions"))] for v in VARIANTS])
    fails = {}
    for v, churn, f, _ in s4_cells(d.results):
        fails.setdefault((v, churn), []).append(f)
    write_table(out, "s4",
                ["Variant", "fails, churn 10", "fails, churn 50", "p99 churn 10 (ms)", "p99 churn 50 (ms)",
                 "lost, churn 50"],
                [[tex_variant(v), " ".join(map(str, sorted(fails.get((v, 10), [])))),
                  " ".join(map(str, sorted(fails.get((v, 50), [])))),
                  fmt(g("S4", 10, v, "rtt_p99") / 1e3, 1), fmt(g("S4", 50, v, "rtt_p99") / 1e3, 1),
                  fmt(g("S4", 50, v, "lost"))] for v in VARIANTS])


# Section "blocking, poll and epoll": the same points seen through two comparisons.
CMP_POINTS = [   # (scenario, value, label)
    ("S1", 8, "S1, 8 peers"), ("S1", 256, "S1, 256 peers"), ("S1", 1024, "S1, 1,024 peers"),
    ("S2", 1000, "S2, 1,000 idle"), ("S3", 4, "S3, 4 slow"), ("S4", 50, "S4, churn 50"),
    ("S5", 32, "S5, 32 B"), ("S5", 4096, "S5, 4 KiB"), ("S6b", 1000, "S6b, 1 Gbit/s"),
]


def comparison_tables(d, out):
    g = d.get
    rows = []
    for sc, x, label in CMP_POINTS:
        row = [label]
        for shape in ("", "-duplex", "-split"):
            row += [fmt(g(sc, x, "poll" + shape, "cpu_pct")) + " / " + fmt(g(sc, x, "epoll" + shape, "cpu_pct"))]
        for shape in ("", "-duplex", "-split"):
            row += [fmt(g(sc, x, "poll" + shape, "rtt_p99") / 1e3, 1) + " / "
                    + fmt(g(sc, x, "epoll" + shape, "rtt_p99") / 1e3, 1)]
        rows.append(row)
    write_table(out, "cmp_poll_epoll",
                ["Point", "simplex", "duplex", "split", "simplex", "duplex", "split"], rows,
                groups=[r" & \multicolumn{3}{c}{CPU \% (\texttt{poll} / \texttt{epoll})}"
                        r" & \multicolumn{3}{c}{p99 in ms (\texttt{poll} / \texttt{epoll})} \\",
                        r"\cmidrule(lr){2-4}\cmidrule(l){5-7}"])
    rows = []
    for sc, x, label in CMP_POINTS:
        row = [label]
        for v in ("threads", "pool-sender", "epoll-duplex"):
            row += [fmt(g(sc, x, v, "msgs_per_s") / 1e3, 1), fmt(g(sc, x, v, "rtt_p99") / 1e3, 1),
                    fmt(g(sc, x, v, "cpu_pct"))]
        rows.append(row)
    write_table(out, "cmp_blocking",
                ["Point"] + ["k msg/s", "p99 (ms)", "CPU \\%"] * 3, rows,
                groups=[r" & \multicolumn{3}{c}{\texttt{threads}} & \multicolumn{3}{c}{\texttt{pool-sender}}"
                        r" & \multicolumn{3}{c}{\texttt{epoll-duplex}} \\",
                        r"\cmidrule(lr){2-4}\cmidrule(lr){5-7}\cmidrule(l){8-10}"])


# Appendix: every swept value x variant of every scenario, as a longtable per scenario.
FULL_COLUMNS = [   # (header, summary column, scale, digits)
    ("msg/s", "msgs_per_s", 1, 0), ("lost", "lost", 1, 0),
    ("p50", "rtt_p50", 1e-3, 1), ("p99", "rtt_p99", 1e-3, 1), ("p99.9", "rtt_p999", 1e-3, 1),
    ("CPU \\%", "cpu_pct", 1, 0), ("vol./s", "nvcsw_rate", 1, 0), ("invol./s", "nivcsw_rate", 1, 0),
    ("thr.", "threads", 1, 0), ("fds", "fds", 1, 0), ("ready", "peers_ready", 1, 0),
    ("hs p50", "handshake_p50", 1e-3, 1), ("queued", "tx_queued_sends", 1, 0),
    ("evict.", "evictions", 1, 0),
]
FULL_SCENARIOS = {"S1": "fan-in", "S2": "idle connections", "S3": "slow consumers", "S4": "churn",
                  "S5": "throughput", "S6a": "WAN churn", "S6c": "WAN churn, halved",
                  "S6b": "WAN bottleneck", "S7": "capped fan-in"}


def full_tables(d, out):
    param = {}
    for (sc, _, _), r in d.rows.items():
        param[sc] = r["param"]
    for sc, title in FULL_SCENARIOS.items():
        cols = "ll" + "r" * (1 + len(FULL_COLUMNS))
        head = (" & ".join([param[sc].replace("_", r"\_"), "Variant", "valid"]
                           + [h for h, *_ in FULL_COLUMNS]) + r" \\")
        lines_ = [r"\begin{longtable}{@{}" + cols + "@{}}",
                  r"\caption{" + sc + " (" + title + r"): all results.}\label{tab:full-" + sc + r"}\\",
                  r"\toprule", head, r"\midrule", r"\endfirsthead",
                  r"\toprule", head, r"\midrule", r"\endhead",
                  r"\bottomrule", r"\endfoot"]
        for i, x in enumerate(d.values(sc)):
            if i:
                lines_.append(r"\midrule")
            for j, v in enumerate(VARIANTS):
                r = d.rows.get((sc, x, v))
                if r is None:
                    continue
                cells = [fmt(x) if j == 0 else "", tex_variant(v),
                         f"{int(num(r['reps_valid']))}/{int(num(r['reps']))}"]
                cells += [fmt(d.get(sc, x, v, c) * k, dg) for _, c, k, dg in FULL_COLUMNS]
                lines_.append(" & ".join(cells) + r" \\")
        lines_.append(r"\end{longtable}")
        (out / "tables" / f"full_{sc}.tex").write_text("\n".join(lines_) + "\n")
        print("wrote", f"tables/full_{sc}.tex")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--results", type=Path, default=REPO / "results-0294f37")
    ap.add_argument("--out", type=Path, default=REPO / "report" / "tex")
    args = ap.parse_args()
    (args.out / "figures").mkdir(parents=True, exist_ok=True)
    (args.out / "tables").mkdir(parents=True, exist_ok=True)
    style.setup()
    d = Data(args.results)
    for build in (fig_s1, fig_s2, fig_s3, fig_s4, fig_s5, fig_s6a, fig_s7, tables, comparison_tables, full_tables):
        build(d, args.out)


if __name__ == "__main__":
    main()
