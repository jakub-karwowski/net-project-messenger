#!/usr/bin/env python3
"""Run the stress experiment matrix: scenario x param x variant x rep.

Per cell: start the node headless with --echo, wait for the "ready" JSONL
event, run stressgen against it, send /quit, wait for "bye" + exit.
Output layout:
  <out>/<scenario>/<param>/<variant>/rep<N>.{stress.json,metrics.jsonl,node.log,meta.json}
Cells whose meta.json has status ok|dirty are skipped on re-run (resume).
`--fake` writes synthetic results (no binaries) to exercise analyze.py.
"""
import argparse, json, math, os, random, resource, subprocess, sys, threading, time, zlib
from pathlib import Path

VARIANTS = ["threads", "pool-sender", "poll", "epoll"]

# --- Scenario table ---------------------------------------------------------
# x = the swept parameter name (a stressgen option), values = its values,
# base = the remaining stressgen settings. quick_values shrink the sweep.
SCENARIOS = {
    "S1": dict(title="Fan-in scaling", x="peers", values=[1, 8, 64, 256, 1024],
               base=dict(peers=64, rate=100, size=64, duration=20, warmup=5),
               quick_values=[1, 8]),
    "S2": dict(title="Idle-heavy", x="idle_peers", values=[0, 100, 300, 1000],
               base=dict(peers=8, rate=500, size=64, duration=20, warmup=5),
               quick_values=[0, 50]),
    "S3": dict(title="Slow consumers", x="slow_peers", values=[0, 1, 4],
               base=dict(peers=16, rate=200, size=64, duration=20, warmup=5),
               quick_values=[0, 1]),
    "S4": dict(title="Connection churn", x="churn", values=[0, 10, 50],
               base=dict(peers=64, rate=50, size=64, duration=20, warmup=5),
               quick_values=[0, 5]),
    "S5": dict(title="Throughput ceiling", x="size", values=[32, 4096, 65536],
               base=dict(peers=8, rate=0, window=16, size=64, duration=20, warmup=5),
               quick_values=[32, 4096]),
}
QUICK_BASE = dict(duration=2, warmup=0.5)
QUICK_PEERS_CAP = 16
GRACE = 1.0


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
    return int(p["peers"] + p["idle_peers"] + p["slow_peers"] + 16)


def cell_dir(out, sc, x, variant):
    return Path(out) / sc / f"{SCENARIOS[sc]['x']}={x}" / variant


def is_done(d, rep):
    try:
        return json.loads((d / f"rep{rep}.meta.json").read_text()).get("status") in ("ok", "dirty")
    except Exception:
        return False


# --- Real run ---------------------------------------------------------------
class Node:
    """Node subprocess; a reader thread watches stdout JSONL for ready/bye."""
    def __init__(self, cmd):
        self.p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.ready = threading.Event()
        self.bye = threading.Event()
        self.ready_err = None
        self.tail = []
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self):
        for line in self.p.stdout:
            self.tail = (self.tail + [line.rstrip()])[-5:]
            try:
                ev = json.loads(line)
            except ValueError:
                continue
            k = ev.get("event")
            if k == "ready":
                self.ready_err = ev.get("error")
                self.ready.set()
            elif k == "bye":
                self.bye.set()
        self.ready.set()   # EOF: unblock waiters
        self.bye.set()

    def quit(self, timeout=10):
        """True if the node said bye and exited within the timeout."""
        clean = True
        try:
            self.p.stdin.write("/quit\n")
            self.p.stdin.flush()
        except Exception:
            clean = False
        if not self.bye.wait(timeout):
            clean = False
        try:
            self.p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
            clean = False
        return clean


def run_cell(args, sc, x, variant, rep, port, d):
    p = cell_params(sc, x, args.quick)
    pre = d / f"rep{rep}"
    meta = dict(scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep,
                port=port, params=p, status="failed", error=None, t_start=time.time())
    node = Node(["taskset", "-c", args.node_cpus, args.app, "--headless", "--echo",
                 f"--io={variant}", f"--max-peers={max_peers_for(p)}", f"--port={port}",
                 f"--log={pre}.node.log", f"--metrics={pre}.metrics.jsonl"])
    try:
        if not node.ready.wait(args.ready_timeout) or node.ready_err or node.p.poll() is not None:
            raise RuntimeError("node not ready: " + (str(node.ready_err) if node.ready_err
                               else "timeout/exited; " + " | ".join(node.tail)))
        sg = ["taskset", "-c", args.gen_cpus, args.stressgen, "--target", f"127.0.0.1:{port}",
              "--peers", str(p["peers"]), "--rate", str(p["rate"]), "--size", str(p["size"]),
              "--duration", str(p["duration"]), "--warmup", str(p["warmup"]),
              "--idle-peers", str(p["idle_peers"]), "--slow-peers", str(p["slow_peers"]),
              "--churn", str(p["churn"]), "--grace", str(0.5 if args.quick else GRACE),
              "--out", f"{pre}.stress.json"]
        if p.get("window"):
            sg += ["--window", str(p["window"])]
        limit = p["duration"] + p["warmup"] + 60 + (p["peers"] + p["idle_peers"]) * 0.02
        r = subprocess.run(sg, capture_output=True, text=True, timeout=limit)
        (d / f"rep{rep}.stressgen.txt").write_text(r.stdout + r.stderr)
        if r.returncode != 0:
            raise RuntimeError(f"stressgen exit {r.returncode}: {r.stderr.strip()[-200:]}")
        meta["status"] = "ok"
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
    finally:
        if not node.quit():
            meta["node_unclean"] = True
            if meta["status"] == "ok":
                meta["status"] = "dirty"
                meta["error"] = "node did not exit cleanly after /quit"
        meta["t_end"] = time.time()
        (d / f"rep{rep}.meta.json").write_text(json.dumps(meta, indent=1))
    return meta["status"]


# --- Fake run ---------------------------------------------------------------
# Plausible synthetic model. (cpu us/msg, rtt base us, rtt us per peer, threads per conn)
FAKE = {"threads": (14, 90, 0.9, 1.0), "pool-sender": (12, 80, 0.7, 1.0),
        "poll": (7, 110, 1.4, 0.0), "epoll": (6, 70, 0.2, 0.0)}


def fake_cell(args, sc, x, variant, rep, port, d):
    rnd = random.Random(zlib.crc32(f"{sc}{x}{variant}{rep}{args.seed}".encode()))
    p = cell_params(sc, x, args.quick)
    cpu_us, rtt0, rttk, thr = FAKE[variant]
    n = p["peers"]

    def noise(s=0.05):
        return math.exp(rnd.gauss(0, s))
    offered = n * p["rate"] if p["rate"] else 60000
    cap = 1e6 / (cpu_us * (1 + 0.3 * p["size"] / 4096)) * 0.5
    mps = min(offered, cap) * noise() * (1 - 0.02 * p["slow_peers"]) * (1 - 0.003 * p["churn"])
    rtt50 = (rtt0 + rttk * (n + p["idle_peers"] * 0.1)) * noise(0.1) * (1 + p["size"] / 40000)
    dur = p["duration"]
    st = dict(sent=int(mps * dur), received=int(mps * dur),
              lost=int(rnd.random() < 0.1) * rnd.randint(1, 20), msgs_per_s=round(mps, 1),
              rtt_us=dict(p50=rtt50, p90=rtt50 * 2, p99=rtt50 * 6 * noise(0.2),
                          p999=rtt50 * 30 * noise(0.3), max=rtt50 * 90, mean=rtt50 * 1.3,
                          samples=int(mps * dur)),
              config=dict(p, target=f"127.0.0.1:{port}"), start_unix_ns=0, end_unix_ns=int(dur * 1e9))
    (d / f"rep{rep}.stress.json").write_text(json.dumps(st))
    live = n + p["idle_peers"] + p["slow_peers"]
    total = p["warmup"] + dur + 1
    nsamp = int(math.ceil(total)) + 2
    ut = stt = 0.0
    fin = vcsw = ivcsw = 0
    rows = []
    t0 = 10_000_000_000
    for i in range(nsamp):
        rate = mps if 0 < i <= int(math.ceil(total)) else 0
        cpu = rate * cpu_us * noise(0.03)
        ut += cpu * 0.6
        stt += cpu * 0.4 + 2000
        fin += int(rate)
        vcsw += int(rate * (0.4 if variant in ("poll", "epoll") else 2.0))
        ivcsw += int(rate * 0.02 + 50)
        rows.append(dict(
            t_ns=t0 + i * 10**9, accepts=live, connections_live=live,
            bytes_in=fin * (p["size"] + 20), frames_in=fin, frames_out=fin, send_failures=0,
            send_backpressure_drops=int(fin * 0.001) if p["slow_peers"] and variant != "threads" else 0,
            reconnects=int(p["churn"] * i),
            evictions=int(p["churn"] * i * 0.1) if variant in ("poll", "epoll") else 0,
            echoed=fin, render_q_hwm=0, store_q_hwm=int(rnd.random() * 50),
            sender_q_hwm=int(rnd.random() * 50), utime_us=int(ut), stime_us=int(stt),
            nvcsw=vcsw, nivcsw=ivcsw, maxrss_kb=20000 + int(live * (60 if thr else 8)) + i * 10,
            threads=int(8 + live * thr + (4 if variant != "threads" else 0)), fds=live * 2 + 10))
    (d / f"rep{rep}.metrics.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    (d / f"rep{rep}.node.log").write_text("fake\n")
    (d / f"rep{rep}.meta.json").write_text(json.dumps(dict(
        scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep, port=port,
        params=p, status="ok", fake=True), indent=1))
    return "ok"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--app", default="build/app")
    ap.add_argument("--stressgen", default="build/stressgen")
    ap.add_argument("--out", default="results")
    ap.add_argument("--variants", default=",".join(VARIANTS))
    ap.add_argument("--scenarios", default=",".join(SCENARIOS))
    ap.add_argument("--reps", type=int, default=None, help="default 5 (1 with --quick)")
    ap.add_argument("--quick", action="store_true", help="tiny durations/sizes for smoke runs")
    ap.add_argument("--node-cpus", default="0-1")
    ap.add_argument("--gen-cpus", default="4-7")
    ap.add_argument("--base-port", type=int, default=21000)
    ap.add_argument("--ready-timeout", type=float, default=15)
    ap.add_argument("--fake", action="store_true", help="synthetic results, no binaries")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    reps = args.reps or (1 if args.quick else 5)
    variants = [v for v in args.variants.split(",") if v]
    scs = [s for s in args.scenarios.split(",") if s]
    for v in variants:
        if v not in VARIANTS:
            sys.exit(f"unknown variant {v}")
    for s in scs:
        if s not in SCENARIOS:
            sys.exit(f"unknown scenario {s}")
    if not args.fake and not args.dry_run:
        for b in (args.app, args.stressgen):
            if not os.access(b, os.X_OK):
                sys.exit(f"not executable: {b}")
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
        except (ValueError, OSError) as e:
            print(f"warning: cannot raise RLIMIT_NOFILE ({e}); soft={soft}", file=sys.stderr)
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
        t = time.time()
        st = (fake_cell if args.fake else run_cell)(args, sc, x, v, rep, port, d)
        print(f"{tag} -> {st} ({time.time() - t:.1f}s)", flush=True)
        counts[st] = counts.get(st, 0) + 1
        if not args.fake:
            time.sleep(0.3)   # let sockets settle
    print("summary:", counts)
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())
