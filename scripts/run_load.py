#!/usr/bin/env python3
"""Run one load test against a node and collect everything into one JSON file.

    run_load.py --out DIR [node options] -- [loadgen options]

Starts `node --cli --echo` (a load-test target that sends every message
straight back), runs `loadgen` against it, and while the load runs samples the
node from /proc: CPU per thread, memory, threads, open descriptors. Afterwards
it shuts the node down (end of input) and reads the node's metric log: the
Balancer's view of every IO worker per tick, and its moves, spawns and
retirements.

Writes DIR/node.log, DIR/loadgen.json and DIR/result.json, and prints a
one-line summary. CPU figures cover loadgen's measurement window only (both
sides use the monotonic clock). --node-cpus / --loadgen-cpus pin the two
processes, e.g. --node-cpus 0-3 --loadgen-cpus 8-9. Exit status: 0 when the run completed and the node exited
normally, 1 otherwise.

Node options (anything else before `--` is an error):
    --min-pts N, --max-pts N, --no-balancer, --balance-period MS, --balance-max-moves N,
    --shutdown-grace MS, --max-links N, --max-links-per-pt N
Everything after `--` goes to loadgen as it is; --target is added.

Example: the Balancer's effect at 512 peers x 100 msg/s

    scripts/run_load.py --out runs/bal-on  --max-pts 4 -- --peers 512 --rate 100 --duration 20
    scripts/run_load.py --out runs/bal-off --no-balancer -- --peers 512 --rate 100 --duration 20
"""

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CLK_TCK = os.sysconf("SC_CLK_TCK")

NODE_FLAGS = {"--no-balancer"}
NODE_VALUES = {"--min-pts", "--max-pts", "--balance-period", "--balance-max-moves", "--shutdown-grace", "--max-links",
               "--max-links-per-pt", "--inbox-high", "--inbox-low", "--rcvbuf"}

METRIC_RE = re.compile(r"^\S+\s+(?P<t>\d+\.\d+) METRIC +\[balancer\] balancer: "
                       r"pt=(?P<pt>\d+) links=(?P<links>\d+) load=(?P<load>[\d.]+) stalled=(?P<stalled>\w+) backlog=(?P<backlog>\d+)$")
# Inbox depths (QueueMonitor) and the IO workers' backpressure pauses.
QUEUE_RE = re.compile(r"METRIC +\[queues\] queues: queue_depth (?P<rest>.*)$")
PAUSE_RE = re.compile(r"METRIC +\[pt-\d+\] pt-\d+: reads=paused")
# Decisions and topology changes: key=value Metric lines.
EVENT_RE = re.compile(r"^\S+\s+(?P<t>\d+\.\d+) METRIC +\[balancer\] balancer: (?P<kind>decision|topology)=(?P<what>\w+)(?P<rest>.*)$")


def parse_args(argv):
    if "--" in argv:
        split = argv.index("--")
        ours, loadgen_args = argv[:split], argv[split + 1:]
    else:
        ours, loadgen_args = argv, []
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", required=True, type=Path, help="directory for the results")
    p.add_argument("--build", type=Path, default=REPO / "build-release", help="build directory (default build-release)")
    p.add_argument("--sample-interval", type=float, default=0.5, help="seconds between /proc samples")
    p.add_argument("--node-cpus", help="pin the node to these CPUs, e.g. 0-3")
    p.add_argument("--loadgen-cpus", help="pin loadgen to these CPUs, e.g. 8-9")
    args, node_args = p.parse_known_args(ours)
    i = 0
    while i < len(node_args):
        a = node_args[i]
        if a in NODE_FLAGS:
            i += 1
        elif a in NODE_VALUES and i + 1 < len(node_args):
            i += 2
        else:
            p.error(f"not a node option this script passes on: {a}")
    if any(a == "--target" for a in loadgen_args):
        p.error("--target is set by this script")
    return args, node_args, loadgen_args


def read_task_times(pid):
    """{thread name: CPU seconds} for every thread of `pid`."""
    out = {}
    for task in Path(f"/proc/{pid}/task").iterdir():
        try:
            name = (task / "comm").read_text().strip()
            fields = (task / "stat").read_text().rsplit(")", 1)[1].split()
            out[name] = out.get(name, 0.0) + (int(fields[11]) + int(fields[12])) / CLK_TCK   # utime + stime
        except (FileNotFoundError, ProcessLookupError, IndexError):
            pass
    return out


def read_status(pid):
    status = {}
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            key, _, value = line.partition(":")
            status[key] = value.strip()
        fds = len(os.listdir(f"/proc/{pid}/fd"))
    except (FileNotFoundError, ProcessLookupError):
        return None
    return {"rss_kb": int(status.get("VmRSS", "0 kB").split()[0]), "threads": int(status.get("Threads", "0")), "fds": fds}


class Sampler(threading.Thread):
    """Samples the node every `interval` seconds until stopped."""

    def __init__(self, pid, interval):
        super().__init__(daemon=True)
        self.pid, self.interval = pid, interval
        self.samples = []
        self.stop = threading.Event()

    def run(self):
        start = time.monotonic()
        while not self.stop.is_set():
            times = read_task_times(self.pid)
            status = read_status(self.pid)
            if status is None:
                return
            now = time.monotonic()
            self.samples.append({"t": round(now - start, 3), "mono": now, "cpu_s": times, **status})
            self.stop.wait(self.interval)


def cpu_percent(samples, first, last):
    """Mean CPU % per thread between two samples (100 = one core)."""
    a, b = samples[first], samples[last]
    dt = b["t"] - a["t"]
    if dt <= 0:
        return {}
    names = set(a["cpu_s"]) | set(b["cpu_s"])
    per = {n: round(100.0 * (b["cpu_s"].get(n, 0.0) - a["cpu_s"].get(n, 0.0)) / dt, 1) for n in names}
    return dict(sorted(per.items(), key=lambda kv: -kv[1]))


def parse_node_log(path):
    ticks = {}
    events = []
    inbox_max = {}
    pauses = 0
    for line in path.read_text(errors="replace").splitlines():
        q = QUEUE_RE.search(line)
        if q:
            for kv in q["rest"].split():
                key, _, value = kv.partition("=")
                if key.endswith("_max"):
                    inbox_max[key[:-4]] = max(inbox_max.get(key[:-4], 0), int(value))
            continue
        if PAUSE_RE.search(line):
            pauses += 1
            continue
        m = METRIC_RE.match(line)
        if m:
            t = round(float(m["t"]), 2)
            ticks.setdefault(t, []).append({"pt": int(m["pt"]), "links": int(m["links"]), "load": float(m["load"]),
                                            "stalled": m["stalled"] == "true", "backlog": int(m["backlog"])})
            continue
        e = EVENT_RE.match(line)
        if e:
            fields = dict(kv.split("=", 1) for kv in e["rest"].split())
            events.append({"t": float(e["t"]), e["kind"]: e["what"], **{k: int(v) for k, v in fields.items()}})
    series = [{"t": t, "pts": len(pts), "workers": pts} for t, pts in sorted(ticks.items())]
    count = lambda what: sum(1 for e in events if e.get("decision") == what)
    return {"ticks": series, "events": events, "moves": count("move"), "spawns": count("spawn"),
            "retires": count("retire"), "evictions": count("evict"), "holds": count("hold"),
            "max_pts": max((s["pts"] for s in series), default=0), "inbox_max": inbox_max, "pauses": pauses}


def parse_cpus(text):
    """'0-3,8' -> {0, 1, 2, 3, 8}; None or '' -> None (no pinning)."""
    if not text:
        return None
    cpus = set()
    for part in text.split(","):
        lo, _, hi = part.partition("-")
        cpus.update(range(int(lo), int(hi or lo) + 1))
    return cpus


def pinned(cpus):
    """A preexec_fn that pins the child to `cpus` (None: no pinning)."""
    return (lambda: os.sched_setaffinity(0, cpus)) if cpus else None


def run_cell(build, out, node_args, loadgen_args, sample_interval=0.5, node_cpus=None, loadgen_cpus=None):
    """Runs one load test; writes out/{node.log,loadgen.json,result.json} and returns the result."""
    node_bin = Path(build) / "App" / "node"
    loadgen_bin = Path(build) / "Tools" / "loadgen" / "loadgen"
    for b in (node_bin, loadgen_bin):
        if not b.exists():
            raise FileNotFoundError(f"{b} not found (build it: cmake --build {build} --target node loadgen)")
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    log_path = out / "node.log"
    log_path.unlink(missing_ok=True)

    node_cmd = [str(node_bin), "--cli", "--echo", "--listen", "127.0.0.1:0", "--log", str(log_path),
                "--log-level", "metric", *node_args]
    node = subprocess.Popen(node_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                            preexec_fn=pinned(node_cpus))
    ready = json.loads(node.stdout.readline() or "{}")
    if ready.get("ev") != "ready":
        node.kill()
        node.wait()
        raise RuntimeError(f"the node did not start: {ready}")
    target = ready["listen"]

    sampler = Sampler(node.pid, sample_interval)
    sampler.start()
    started = time.monotonic()
    lg = subprocess.run([str(loadgen_bin), "--target", target, *loadgen_args], capture_output=True, text=True,
                        preexec_fn=pinned(loadgen_cpus))
    load_seconds = time.monotonic() - started
    sampler.stop.set()
    sampler.join()

    quit_at = time.monotonic()
    node.stdin.close()   # end of input: the normal shutdown
    try:
        node_out = node.stdout.read()
        node_exit = node.wait(timeout=60)
    except subprocess.TimeoutExpired:
        node.kill()
        node.wait()
        node_out, node_exit = "", None
    shutdown_ms = round(1000 * (time.monotonic() - quit_at))

    (out / "loadgen.json").write_text(lg.stdout)
    try:
        gen = json.loads(lg.stdout)
    except json.JSONDecodeError:
        gen = None
    samples = sampler.samples
    window = measurement_samples(samples, gen)
    cpu = cpu_percent(samples, window[0], window[1]) if window else {}
    node_report = {
        "command": node_cmd,
        "exit_code": node_exit,
        "shutdown_ms": shutdown_ms,
        "forced": '"shutdown_forced"' in node_out,
        "cpu_percent": cpu,
        "cpu_percent_total": round(sum(cpu.values()), 1),
        "cpu_window_s": round(samples[window[1]]["mono"] - samples[window[0]]["mono"], 2) if window else 0,
        "max_rss_kb": max((s["rss_kb"] for s in samples), default=0),
        "max_threads": max((s["threads"] for s in samples), default=0),
        "max_fds": max((s["fds"] for s in samples), default=0),
        # IO worker threads seen (named pt-N); the Balancer's log has none without a Balancer.
        "max_pt_threads": max((sum(1 for n in s["cpu_s"] if n.startswith("pt-")) for s in samples), default=0),
        "balancer": parse_node_log(log_path) if log_path.exists() else None,
        "samples": samples,
    }
    result = {"loadgen_args": loadgen_args, "loadgen_exit": lg.returncode, "loadgen_stderr": lg.stderr.strip(),
              "load_seconds": round(load_seconds, 2), "node_cpus": sorted(node_cpus) if node_cpus else None,
              "loadgen_cpus": sorted(loadgen_cpus) if loadgen_cpus else None, "loadgen": gen, "node": node_report}
    (out / "result.json").write_text(json.dumps(result, indent=1))
    return result


def measurement_samples(samples, gen):
    """Indexes of the first and last sample inside loadgen's measurement window
    (both on the monotonic clock), or None when there are not two of them."""
    if not gen or len(samples) < 2:
        return None
    begin, end = gen["measure_begin_ns"] / 1e9, gen["measure_end_ns"] / 1e9
    inside = [i for i, s in enumerate(samples) if begin <= s["mono"] <= end]
    if len(inside) < 2:
        return None
    return inside[0], inside[-1]


def summary_line(out, result):
    gen, node = result["loadgen"], result["node"]
    if not gen:
        return f"{out}: loadgen failed (exit {result['loadgen_exit']}): {result['loadgen_stderr']}"
    top = ", ".join(f"{k} {v:.0f}%" for k, v in list(node["cpu_percent"].items())[:3])
    bal = node["balancer"] or {}
    return (f"{out}: {gen['active_ready']} peers, {gen['echoed_per_s']} msg/s echoed, "
            f"rtt p50 {gen['rtt']['p50_ms']:.2f} ms p99 {gen['rtt']['p99_ms']:.2f} ms, lost {gen['lost']}, "
            f"node {node['cpu_percent_total']:.0f}% CPU ({top}), pts max {node.get('max_pt_threads', '?')}, "
            f"moves {bal.get('moves', '?')}, inbox max {max((bal.get('inbox_max') or {0: 0}).values())}, "
            f"pauses {bal.get('pauses', '?')}, rss {node['max_rss_kb'] // 1024} MB, refused {gen['send_failed']}, "
            f"exit {node['exit_code']} after {node['shutdown_ms']} ms")


def main(argv):
    args, node_args, loadgen_args = parse_args(argv)
    try:
        result = run_cell(args.build, args.out, node_args, loadgen_args, args.sample_interval,
                          parse_cpus(args.node_cpus), parse_cpus(args.loadgen_cpus))
    except (FileNotFoundError, RuntimeError) as e:
        sys.exit(f"run_load.py: {e}")
    print(summary_line(args.out, result), file=sys.stdout if result["loadgen"] else sys.stderr)
    return 0 if result["loadgen"] is not None and result["node"]["exit_code"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
