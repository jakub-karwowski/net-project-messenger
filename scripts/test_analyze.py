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
print("ok")
