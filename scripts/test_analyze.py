#!/usr/bin/env python3
"""Tests for analyze.py and the matrix plan: statistics, validity rules,
aggregation over a synthetic results directory, and the report's charts
(well-formed SVG, labels inside the chart and apart, colours fixed per
variant). Plain assertions, stdlib only."""

import json
import re
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze      # noqa: E402
import run_matrix   # noqa: E402

failures = 0
checks = 0


def check(condition, what):
    global failures, checks
    checks += 1
    if not condition:
        failures += 1
        print(f"  FAIL: {what}")


def fake_result(peers=8, rate=100, echoed=800, sent=800, behind=0, exit_code=0, forced=False, ready=None, rtt=1.0,
                cpu_window=4.0, duration=5):
    hist = dict(count=echoed, mean_ms=rtt, p50_ms=rtt, p90_ms=rtt, p99_ms=rtt * 2, p999_ms=rtt * 3, max_ms=rtt * 4)
    gen = dict(config=dict(peers=peers, rate=rate, duration=duration), active_ready=peers if ready is None else ready,
               sent=sent, echoed=echoed, lost=sent - echoed, send_failed=0, behind=behind, protocol_errors=0,
               echoed_per_s=echoed / duration, rtt=hist, handshake=hist, reconnect=hist, reconnects=0)
    node = dict(exit_code=exit_code, forced=forced, cpu_percent={"router": 30.0, "store": 20.0}, cpu_percent_total=50.0,
                cpu_window_s=cpu_window, max_threads=10, max_rss_kb=10240, max_fds=40, max_pt_threads=2,
                balancer=dict(max_pts=2, moves=3))
    return dict(loadgen=gen, node=node)


def test_quartiles():
    check(analyze.quartiles([5]) == (5, 5, 5), "one value: Q1 = median = Q3")
    q1, med, q3 = analyze.quartiles([1, 2, 3])
    check((q1, med, q3) == (1.5, 2, 2.5), f"three values, inclusive method: {(q1, med, q3)}")
    q1, med, q3 = analyze.quartiles([4, 1, 3, 2])
    check(med == 2.5 and q1 == 1.75 and q3 == 3.25, "unsorted input is sorted first")


def test_validity():
    meta = dict(x="peers", value=8)
    check(analyze.validity(meta, fake_result()) == [], "a clean cell is valid")
    check(any("exit 3" in r for r in analyze.validity(meta, fake_result(exit_code=3))), "a node that exited 3")
    check(any("forced" in r for r in analyze.validity(meta, fake_result(forced=True))), "a forced shutdown")
    check(any("only 6 of 8" in r for r in analyze.validity(meta, fake_result(ready=6))), "peers that did not connect")
    check(any("fell behind" in r for r in analyze.validity(meta, fake_result(behind=2))), "a generator that fell behind")
    check(any("CPU sampled" in r for r in analyze.validity(meta, fake_result(cpu_window=1.0))), "a too short CPU window")
    s7 = dict(x="max_links", value=64)
    check(analyze.validity(s7, fake_result(peers=1024, ready=64)) == [], "S7: the cap decides how many may connect")
    check(analyze.validity(s7, fake_result(peers=1024, ready=40)) != [], "S7: fewer than the cap allows is not valid")
    check(analyze.validity(meta, dict(loadgen=None, node={})) == ["no loadgen result"], "no loadgen result")


def write_cell(root, scenario, x, value, variant, rep, result, status="ok"):
    d = Path(root) / scenario / f"{x}={value}" / variant / f"rep{rep}"
    d.mkdir(parents=True)
    meta = dict(scenario=scenario, x=x, value=value, variant=variant, rep=rep, status=status)
    (d / "meta.json").write_text(json.dumps(meta))
    if result is not None:
        (d / "result.json").write_text(json.dumps(result))


def test_aggregate_and_report():
    with tempfile.TemporaryDirectory() as root:
        for rep, echoed in enumerate([700, 800, 900], 1):
            for value in (8, 64):
                write_cell(root, "S1", "peers", value, "balancer", rep, fake_result(echoed=echoed * value // 8, sent=900 * value // 8, peers=value))
                write_cell(root, "S1", "peers", value, "fixed-1", rep, fake_result(echoed=echoed * value // 8, sent=900 * value // 8, peers=value, rtt=50.0))
        write_cell(root, "S1", "peers", 64, "fixed-4", 1, fake_result(peers=64, behind=5))   # degraded
        write_cell(root, "S1", "peers", 64, "fixed-4", 2, None, status="failed")
        cells = analyze.load_cells(root)
        check(len(cells) == 14, f"every cell is found ({len(cells)})")
        summary, problems = analyze.aggregate(cells)
        row = summary[("S1", "peers", 8, "balancer")]
        check(row["n_valid"] == 3 and row["echoed_per_s"][1] == 160.0, "median over three valid repetitions")
        check(row["echoed_per_s"][0] == 150.0 and row["echoed_per_s"][2] == 170.0, "and its interquartile range")
        check(row["busiest_thread"] == "router", "the busiest thread")
        bad = summary[("S1", "peers", 64, "fixed-4")]
        check(bad["n_valid"] == 0 and bad["n_total"] == 2 and bad["echoed_per_s"] is None,
              "degraded and failed cells are left out of the medians")
        check(len(problems) == 2, "and listed as problems")

        analyze.write_csv(summary, Path(root) / "summary.csv")
        lines = (Path(root) / "summary.csv").read_text().splitlines()
        check(len(lines) == 1 + 5 and lines[0].startswith("scenario,x,value,variant,n_valid"), "one CSV row per cell")
        check(lines[1].split(",")[3] == "fixed-1" and lines[2].split(",")[3] == "balancer",
              "rows in variant order within a value")

        run_info = dict(scenarios=run_matrix.SCENARIOS, build=dict(commit="abc123"))
        analyze.write_report(summary, problems, run_info, Path(root) / "report.html")
        text = (Path(root) / "report.html").read_text()
        check_report(text)


def check_report(text):
    svgs = re.findall(r"<svg .*?</svg>", text, flags=re.S)
    check(len(svgs) == 3, f"three charts for one scenario ({len(svgs)})")
    for svg in svgs:
        try:
            root = ET.fromstring(svg)
        except ET.ParseError as e:
            check(False, f"a chart is not well-formed SVG: {e}")
            continue
        width = float(root.get("viewBox").split()[2])
        height = float(root.get("viewBox").split()[3])
        labels = [(float(t.get("x")), float(t.get("y")), t.text) for t in root.iter("text") if t.get("class") == "label"]
        check(len(labels) >= 2, "direct labels on the lines")
        for x, y, name in labels:
            # About 6.5 px per character at 11 px.
            check(0 <= x and x + 6.5 * len(name) <= width and 8 <= y <= height, f"label {name} inside the chart")
        ys = sorted(y for _, y, _ in labels)
        check(all(b - a >= 12 for a, b in zip(ys, ys[1:])), "labels never overlap")
        classes = {p.get("class") for p in root.iter("path")}
        check("line s1" in classes and "line s3" in classes, "fixed-1 is slot 1 and balancer slot 3, whatever is shown")
    check("Problems" in text and "fell behind" in text and "no result" in text, "the problems are listed")
    check("<table>" in text, "a table view")
    check("prefers-color-scheme: dark" in text and 'data-theme="dark"' in text, "dark mode, by setting and by toggle")


def test_plan():
    class A:
        scenarios = ["S1"]
        variants = list(run_matrix.VARIANTS)
        reps = 2
        quick = True
        seed = 1
    cells = run_matrix.plan(A)
    check(len(cells) == 2 * 2 * 4, "values x reps x variants")
    orders = {tuple(c["variant_order"]) for c in cells}
    check(len(orders) > 1, "the variant order is shuffled per (value, rep)")
    check(run_matrix.plan(A) == cells, "and the shuffle is reproducible")
    node, load = run_matrix.cell_args("S7", 64, quick=False)
    check(node == ["--max-links", "64"] and "--peers" in load and "1024" in load, "S7 sweeps the node's link cap")
    node, load = run_matrix.cell_args("S2", 100, quick=False)
    check(node == ["--max-links", str(8 + 100 + 16)] and load[load.index("--idle") + 1] == "100",
          "elsewhere the cap never refuses anybody")


def main():
    print("test_analyze")
    test_quartiles()
    test_validity()
    test_aggregate_and_report()
    test_plan()
    print(f"{checks - failures}/{checks} checks passed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
