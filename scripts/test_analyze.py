#!/usr/bin/env python3
"""Unit tests for analyze.py, the netlab parser and the run_matrix runner pieces (pure python). Run: python3 scripts/test_analyze.py"""
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
        msgs_per_s=100.0, lost=2, rtt_us=dict(p50=10.0, p99=20.0, p999=30.0),
        handshake_us=dict(p50=60000.0, p99=90000.0), connect_failures=0, churn_events=12, send_skipped=3)))
    samples = [dict(t_ns=i * 1_000_000_000, frames_in=min(i, 3) * 10,
                    utime_us=i * 250_000, stime_us=i * 250_000, nvcsw=i * 7, nivcsw=i,
                    maxrss_kb=1000 + i, threads=8 + (i == 1), fds=20 + 2 * min(i, 2),
                    send_backpressure_drops=i, evictions=0, accepts=i, tx_queued_sends=i * 5, tx_flushes=i * 2) for i in range(5)]
    (d / "rep0.metrics.jsonl").write_text("\n".join(json.dumps(s) for s in samples) + "\n")
    m = A.cell_metrics(d, 0, dict(params=dict(warmup=0)))
    close(m["cpu_pct"], 50.0)          # 0.5 s of CPU per 1 s wall over the window 0..3
    close(m["nvcsw_rate"], 7.0)
    assert m["threads"] == 9 and m["fds"] == 24 and m["maxrss_kb"] == 1004
    assert m["bp_drops"] == 4 and m["lost"] == 2
    assert m["handshake_p50"] == 60000.0 and m["handshake_p99"] == 90000.0
    assert m["connect_failures"] == 0 and m["churn_events"] == 12 and m["send_skipped"] == 3
    assert m["tx_queued_sends"] == 20 and m["tx_flushes"] == 8 and m["accepts"] == 4
    # results from before S6 (no new fields) still load
    (d / "rep1.stress.json").write_text(json.dumps(dict(
        msgs_per_s=1.0, lost=0, rtt_us=dict(p50=1.0, p99=2.0, p999=3.0))))
    (d / "rep1.metrics.jsonl").write_text((d / "rep0.metrics.jsonl").read_text().replace('"tx_queued_sends"', '"x1"'))
    old = A.cell_metrics(d, 1, dict(params=dict(warmup=0)))
    assert old["handshake_p50"] is None and old["churn_events"] is None
    keys = {k for k, *_ in A.METRICS}
    assert {"handshake_p50", "handshake_p99", "tx_queued_sends", "tx_flushes", "connect_failures",
            "churn_events", "send_skipped", "accepts"} <= keys
    assert all(k in keys for cs in A.SCENARIO_CHARTS.values() for k in cs)
    assert {"fds"} <= {k for k, *_ in A.METRICS} and "fds" in A.CHART_KEYS

# --- validity() ---------------------------------------------------------------
T0 = 1_000_000_000_000


def mrows(last_growth, n=30, t0=T0):
    """1 Hz node samples; frames_in grows until sample index `last_growth`."""
    return [dict(t_ns=t0 + i * 10**9, frames_in=min(i, last_growth) * 10) for i in range(n)]


def vmeta(**p):
    base = dict(peers=10, rate=100, duration=20, warmup=5, churn=0, idle_peers=0, slow_peers=0)
    base.update(p)
    return dict(scenario="S1", params=base, t_start=1000.0)


def vstress(**kw):
    st = dict(sent=20000, received=20000, peers_total=10, peers_ready=10, connect_failures=0,
              bad_frames=0, send_skipped=0, window_start_mono_ns=T0 + 5 * 10**9,
              window_end_mono_ns=T0 + 25 * 10**9)
    st.update(kw)
    return st


ROWS_OK = mrows(25)
ok, why = A.validity(vmeta(), vstress(), ROWS_OK)
assert ok and why == [], why
assert A.validity(vmeta(), None, ROWS_OK) == (True, [])
# 1. ready
ok, why = A.validity(vmeta(), vstress(peers_ready=9), ROWS_OK)
assert not ok and "peers_ready" in why[0]
ok, why = A.validity(vmeta(churn=5), vstress(peers_ready=9, peers_total=10), ROWS_OK)   # churn: >= 0.9
assert ok, why
ok, why = A.validity(vmeta(churn=5), vstress(peers_ready=8), ROWS_OK)
assert not ok and "churn" in why[0]
m7 = vmeta(max_peers=64, peers=1024)
m7["scenario"] = "S7"
ok, why = A.validity(m7, vstress(peers_total=1024, peers_ready=58, sent=1, connect_failures=500), ROWS_OK)  # 58 >= .9*64; no sent/connect rule
assert ok, why
ok, why = A.validity(m7, vstress(peers_total=1024, peers_ready=57), ROWS_OK)
assert not ok and "S7" in why[0]
m7b = dict(m7, params=dict(m7["params"], max_peers=2000))     # cap above peers: min(peers, cap)
assert not A.validity(m7b, vstress(peers_total=1024, peers_ready=900), ROWS_OK)[0]
# 2. open loop
ok, why = A.validity(vmeta(), vstress(sent=19000), ROWS_OK)           # 0.95 < 0.98
assert not ok and "sent" in why[0]
assert A.validity(vmeta(), vstress(sent=19700), ROWS_OK)[0]
assert A.validity(vmeta(rate=0), vstress(sent=5), ROWS_OK)[0]          # closed loop
assert A.validity(vmeta(churn=4), vstress(sent=5), ROWS_OK)[0]         # churn: no sent rule
# 3. counters
for k in ("connect_failures", "bad_frames", "send_skipped"):
    ok, why = A.validity(vmeta(), vstress(**{k: 2}), ROWS_OK)
    assert not ok and k in why[0], (k, why)
# 4. stall, monotonic bounds: growth stops 12 s before the window end
ok, why = A.validity(vmeta(), vstress(), mrows(13))
assert not ok and "stalled" in why[0] and "margin 3" in why[0], why
assert A.validity(vmeta(), vstress(), mrows(23))[0]                    # 2 s before the end: fine
assert not A.validity(vmeta(), vstress(), mrows(0))[0]                 # never grew
# 4b. old results (no mono bounds): wall mapping, 5 s margin. First sample ~ t_start.
old = vstress()
del old["window_start_mono_ns"], old["window_end_mono_ns"]
old["end_unix_ns"] = int((1000.0 + 25) * 1e9)
assert A.validity(vmeta(), old, mrows(22))[0]          # 3 s before the end, within the looser margin
ok, why = A.validity(vmeta(), old, mrows(15))
assert not ok and "margin 5" in why[0], why
no_end = dict(old)
del no_end["end_unix_ns"]
assert A.validity(vmeta(), no_end, mrows(3))[0]        # nothing to compare against: skipped

# --- metrics_window with monotonic bounds ---------------------------------------
rows = mrows(25)
w, method = A.metrics_window_m(rows, 5, vstress())
assert w == (5, 25) and method == "mono", (w, method)
w, method = A.metrics_window_m(rows, 5, dict(sent=1))
assert method == "frames_in" and w == A.metrics_window(rows, 5)
w, method = A.metrics_window_m(rows, 5, vstress(window_start_mono_ns=T0 + 100 * 10**9, window_end_mono_ns=T0 + 101 * 10**9))
assert method == "frames_in"           # bounds outside the samples: fall back

# --- load() / aggregate() / csv / html over a synthetic results tree --------------
import csv, tempfile


def write_cell(root, sc, xn, x, var, rep, status="ok", error=None, stress_extra=None, build=None, rows=None, params=None,
               drop_new=False, **meta_extra):
    d = Path(root) / sc / f"{xn}={x}" / var
    d.mkdir(parents=True, exist_ok=True)
    p = dict(peers=10, rate=100, duration=20, warmup=5, churn=0, idle_peers=0, slow_peers=0)
    p.update(params or {})
    st = vstress(msgs_per_s=1000.0 + rep, lost=0, rtt_us=dict(p50=10.0 * rep, p99=20.0, p999=30.0))
    st.update(stress_extra or {})
    if drop_new:
        for k in list(st):
            if k in ("window_start_mono_ns", "window_end_mono_ns"):
                del st[k]
        st["end_unix_ns"] = int((1000.0 + 25) * 1e9)
    (d / f"rep{rep}.stress.json").write_text(json.dumps(st))
    rs = rows or [dict(r, utime_us=i * 100_000, stime_us=0, nvcsw=i, nivcsw=i, maxrss_kb=5, threads=3, fds=4)
                  for i, r in enumerate(mrows(25))]
    (d / f"rep{rep}.metrics.jsonl").write_text("\n".join(json.dumps(r) for r in rs) + "\n")
    meta = dict(scenario=sc, x_name=xn, x=x, variant=var, rep=rep, params=p, status=status, error=error, t_start=1000.0, **meta_extra)
    if build is not None:
        meta["build"] = build
    (d / f"rep{rep}.meta.json").write_text(json.dumps(meta))


def bstamp(commit="aaa", app="a1", sg="s1", bt="Release", dirty=False):
    return dict(commit=commit, dirty=dirty, app=dict(sha256=app, build_type=bt), stressgen=dict(sha256=sg))


with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    B1, B2 = bstamp(), bstamp(app="a2")
    write_cell(root, "S1", "peers", 10, "epoll", 1, build=B1)
    write_cell(root, "S1", "peers", 10, "epoll", 2, build=B1, stress_extra=dict(peers_ready=3))        # degraded (ok)
    write_cell(root, "S1", "peers", 10, "epoll", 3, build=B1, status="dirty", error="slow exit")        # dirty, valid
    write_cell(root, "S1", "peers", 10, "threads", 1, build=B2, status="dirty", error="hung",
               rows=[dict(r, utime_us=0, stime_us=0, nvcsw=0, nivcsw=0) for r in mrows(8)])             # dirty + stalled
    write_cell(root, "S1", "peers", 10, "threads", 2, build=B2, status="failed", error="boom")
    write_cell(root, "S1", "peers", 10, "poll", 1, drop_new=True)                                        # old format, no stamp
    write_cell(root, "S1", "peers", 10, "poll", 2, stress_extra=dict(duplicates=3, handshake_failures=1,
               slow_unread_bytes=4096, gen_cpu=dict(utime_us=4_000_000, stime_us=2_000_000)), build=B1,
               netem_stats={"veth-node": dict(dropped=5), "veth-gen": dict(dropped=2)})
    builds = {}
    data, problems = A.load(root, builds)
    agg = A.aggregate(data)
    e = agg[("S1", "peers", 10, "epoll")]
    assert e["n"] == 3 and e["n_valid"] == 2
    close(e["msgs_per_s"][0], 1002.0)             # reps 1 and 3 only (rep 2 degraded): median of 1001, 1003
    assert e["window_method"] == "mono"
    th = agg[("S1", "peers", 10, "threads")]
    assert th["n"] == 1 and th["n_valid"] == 0 and math.isnan(th["msgs_per_s"][0])
    po = agg[("S1", "peers", 10, "poll")]
    assert po["n_valid"] == 2 and po["window_method"] == "mixed"
    msgs = {c: m for c, m in problems}
    assert msgs["S1 peers=10 epoll rep2"].startswith("degraded") and "peers_ready" in msgs["S1 peers=10 epoll rep2"]
    assert msgs["S1 peers=10 epoll rep3"].startswith("dirty (data kept)")
    assert msgs["S1 peers=10 threads rep1"].startswith("degraded") and "stalled" in msgs["S1 peers=10 threads rep1"]
    assert msgs["S1 peers=10 threads rep2"].startswith("status=failed")
    assert len(problems) == 4, problems
    # new fields, and old results without them
    p2 = [r for r in data[("S1", "peers", 10, "poll")] if r["duplicates"] is not None][0]
    assert p2["duplicates"] == 3 and p2["handshake_failures"] == 1 and p2["slow_unread_bytes"] == 4096
    close(p2["gen_cpu_pct"], 100 * 6.0 / 20)       # 6 CPU-s over the 20 s mono window
    assert p2["netem_drops"] == 7
    p1 = [r for r in data[("S1", "peers", 10, "poll")] if r["duplicates"] is None][0]
    assert p1["gen_cpu_pct"] is None and p1["netem_drops"] is None and p1["sender_q_hwm"] is None
    # build census: 3 x B1 (epoll x3) + poll rep2 = 4, B2 x2, unknown x1
    assert builds[A.build_key(B1)] == 4 and builds[A.build_key(B2)] == 2 and builds[None] == 1, builds
    assert "unknown" in A.fmt_build_key(None)
    # csv + html
    A.write_csv(root / "summary.csv", agg)
    with open(root / "summary.csv") as f:
        rowsc = list(csv.DictReader(f))
    r_e = [r for r in rowsc if r["variant"] == "epoll"][0]
    assert r_e["reps"] == "3" and r_e["reps_valid"] == "2" and r_e["window_method"] == "mono"
    A.write_html(root / "report.html", agg, problems, root, {}, builds)
    h = (root / "report.html").read_text()
    assert "mixes 3 different builds" in h and "degraded (excluded" in h and "2/3 valid" in h
    # one build only: no banner
    for_one = {A.build_key(B1): 7}
    A.write_html(root / "report1.html", agg, [], root, {}, for_one)
    assert "mixes" not in (root / "report1.html").read_text()

# --- netlab: tc -s parsing (real output captured under `unshare -rnm`) ---------------
import netlab
TC_NETEM = """qdisc netem 8010: root refcnt 23 limit 100 delay 2ms rate 50Mbit
 Sent 90 bytes 1 pkt (dropped 0, overlimits 0 requeues 0) 
 backlog 0b 0p requeues 0
"""
TC_NOQUEUE = """qdisc noqueue 0: root refcnt 2 
 Sent 0 bytes 0 pkt (dropped 0, overlimits 0 requeues 0) 
 backlog 0b 0p requeues 0
"""
t = netlab.parse_tc_stats(TC_NETEM)
assert t == dict(kind="netem", sent_bytes=90, sent_pkts=1, dropped=0, overlimits=0, requeues=0,
                 backlog_bytes=0, backlog_pkts=0), t
assert netlab.parse_tc_stats(TC_NOQUEUE)["kind"] == "noqueue"
t = netlab.parse_tc_stats(TC_NETEM.replace("dropped 0, overlimits 0 requeues 0", "dropped 12, overlimits 3 requeues 1")
                          .replace("backlog 0b 0p", "backlog 4500b 3p"))
assert (t["dropped"], t["overlimits"], t["requeues"], t["backlog_bytes"], t["backlog_pkts"]) == (12, 3, 1, 4500, 3)
assert netlab.parse_tc_stats("garbage")["dropped"] is None

# --- run_matrix: shuffle, build guard, Node exit handling, hang dump --------------------
import argparse, time
import run_matrix as R

vs = ["a", "b", "c", "d", "e", "f"]
assert R.shuffled_variants(vs, 1, "S1", 8, 2) == R.shuffled_variants(vs, 1, "S1", 8, 2)
assert any(R.shuffled_variants(vs, 1, "S1", 8, r) != R.shuffled_variants(vs, 1, "S1", 8, 1) for r in range(2, 8))
assert any(R.shuffled_variants(vs, sd, "S1", 8, 1) != R.shuffled_variants(vs, 1, "S1", 8, 1) for sd in range(2, 8))
assert sorted(R.shuffled_variants(vs, 1, "S3", 1, 1)) == vs and vs == ["a", "b", "c", "d", "e", "f"]
assert R.order_meta((2, ["x", "y", "z"])) == dict(order_index=2, variant_order=["x", "y", "z"]) and R.order_meta(None) == {}

with tempfile.TemporaryDirectory() as tmp:
    src = dict(commit="c1", dirty=False)
    a = argparse.Namespace(source=src, fake=True)
    assert R.check_build_guard(tmp, a) is None                       # empty dir
    write_cell(tmp, "S1", "peers", 1, "epoll", 1, build=dict(src))
    assert R.check_build_guard(tmp, a) is None                       # same stamp
    err = R.check_build_guard(tmp, argparse.Namespace(source=dict(commit="c2", dirty=False), fake=True))
    assert err and "--force-mixed" in err and "c1" in err
    assert R.check_build_guard(tmp, argparse.Namespace(source=dict(commit="c1", dirty=True), fake=True))   # dirty differs
    write_cell(tmp, "S1", "peers", 1, "poll", 1)                      # cell without a stamp counts as different
    assert "unknown" in R.check_build_guard(tmp, a)
    # real stamps: app sha256 / build_type differences are caught
    real = argparse.Namespace(source=src, fake=False, app="/nonexistent/app", stressgen="/nonexistent/sg")
    cur = R.build_key(R.build_stamp(real))
    assert cur[2] is None and cur[4] is None

PY = sys.executable


def fake_node(body):
    return [PY, "-u", "-c", "import sys,time,json\nprint(json.dumps(dict(event='ready')))\n" + body]


def run_node(body, timeout=5, hang=None):
    n = R.Node(fake_node(body), hang_path=hang)
    assert n.ready.wait(5)
    return n.quit(timeout)


q = run_node("sys.stdin.readline()\nprint(json.dumps(dict(event='bye')))\n")
assert q["clean"] and q["bye"] and q["rc"] == 0 and not q["killed"] and not q["hang_dump"], q
q = run_node("sys.stdin.readline()\nprint(json.dumps(dict(event='bye')))\nsys.exit(3)\n")
assert not q["clean"] and q["bye"] and q["rc"] == 3, q                   # rc != 0 is unclean
q = run_node("sys.stdin.readline()\nsys.exit(0)\n")
assert not q["clean"] and not q["bye"] and q["rc"] == 0, q               # EOF is not a bye
q = run_node("sys.exit(1)\n")
assert not q["clean"] and not q["bye"] and q["rc"] == 1, q               # crashed before /quit

with tempfile.TemporaryDirectory() as tmp:
    hp = Path(tmp) / "rep1.hang.txt"
    t0 = time.time()
    q = run_node("time.sleep(60)\n", timeout=1, hang=hp)               # ignores /quit
    assert q["killed"] and not q["clean"] and q["hang_dump"] and q["rc"] == -9 and time.time() - t0 < 10, q
    txt = hp.read_text()
    assert "--- tid " in txt and " comm: " in txt and " wchan: " in txt and " syscall: " in txt and " state: " in txt, txt
    assert not (Path(tmp) / "none.txt").exists()
    # no hang_path: no dump
    q = run_node("time.sleep(60)\n", timeout=1)
    assert q["killed"] and not q["hang_dump"]
    # a pid that does not exist: dump_hang must not raise
    R.dump_hang(2**22 + 12345, Path(tmp) / "gone.txt")

# --- scenarios -----------------------------------------------------------------------
assert R.SCENARIOS["S6a"]["values"] == [0, 4, 20, 30, 40, 50]
c = R.SCENARIOS["S6c"]
assert c["x"] == "rtt_ms" and c["values"] == [50] and c["quick_values"] == [50] and c["title"] == "WAN churn, half rate"
assert c["base"]["churn"] == 8 and dict(c["base"], churn=16) == R.SCENARIOS["S6a"]["base"] and "net" in c
assert c["net"](50) == R.SCENARIOS["S6a"]["net"](50)
print("ok")
