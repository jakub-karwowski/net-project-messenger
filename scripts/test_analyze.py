#!/usr/bin/env python3
"""Unit tests for analyze.py helpers. Run: python3 scripts/test_analyze.py"""
import math, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import analyze as A


def close(a, b):
    assert math.isclose(a, b, rel_tol=1e-9, abs_tol=1e-9), (a, b)


close(A.percentile([1, 2, 3, 4, 5], 50), 3)
close(A.percentile([1, 2, 3, 4], 50), 2.5)
close(A.percentile([1, 2, 3, 4, 5], 25), 2)
close(A.percentile([1, 2, 3, 4, 5], 75), 4)
close(A.percentile([7], 99), 7)
close(A.percentile([10, 0], 100), 10)
assert math.isnan(A.percentile([], 50))
m, q1, q3 = A.median_iqr([5, 1, 3, None, float("nan"), 2, 4])
close(m, 3); close(q1, 2); close(q3, 4)

# measurement window: growth in samples 1..6, warmup 2 -> start at sample 0+2
rows = [dict(frames_in=min(i, 6) * 10) for i in range(9)]
assert A.metrics_window(rows, 2) == (2, 6)
assert A.metrics_window([dict(frames_in=0)] * 3, 1) is None
assert A.metrics_window(rows, 100) == (0, 6)   # warmup longer than run: fall back to full

# cell_metrics: window rates from the deltas, peaks over every sample, finals from the last
import json, tempfile
with tempfile.TemporaryDirectory() as tmp:
    d = Path(tmp)
    (d / "rep0.stress.json").write_text(json.dumps(dict(
        msgs_per_s=100.0, lost=2, rtt_us=dict(p50=10.0, p99=20.0, p999=30.0))))
    samples = [dict(t_ns=i * 1_000_000_000, frames_in=min(i, 3) * 10,
                    utime_us=i * 250_000, stime_us=i * 250_000, nvcsw=i * 7, nivcsw=i,
                    maxrss_kb=1000 + i, threads=8 + (i == 1), fds=20 + 2 * min(i, 2),
                    send_backpressure_drops=i, evictions=0) for i in range(5)]
    (d / "rep0.metrics.jsonl").write_text("\n".join(json.dumps(s) for s in samples) + "\n")
    m = A.cell_metrics(d, 0, dict(params=dict(warmup=0)))
    close(m["cpu_pct"], 50.0)          # 0.5 s of CPU per 1 s wall over the window 0..3
    close(m["nvcsw_rate"], 7.0)
    assert m["threads"] == 9 and m["fds"] == 24 and m["maxrss_kb"] == 1004
    assert m["bp_drops"] == 4 and m["lost"] == 2
    assert {"fds"} <= {k for k, *_ in A.METRICS} and "fds" in A.CHART_KEYS
print("ok")
