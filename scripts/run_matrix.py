#!/usr/bin/env python3
"""Run the stress experiment matrix: scenario x param x variant x rep.

Per cell: start the node headless with --echo, wait for the "ready" JSONL
event, run stressgen against it, send /quit, wait for "bye" + exit.
Output layout:
  <out>/<scenario>/<param>/<variant>/rep<N>.{stress.json,metrics.jsonl,node.log,meta.json}
S6 (WAN) scenarios run node and stressgen on both ends of a netem-shaped veth link
(scripts/netlab.py). That needs no sudo: this script re-executes itself, restricted
to the S6 scenarios, under `unshare -rnm`; S1-S5 cells run in the normal process.
Cells whose meta.json has status ok|dirty are skipped on re-run (resume).
`--fake` writes synthetic results (no binaries) to exercise analyze.py.
Every meta.json carries a "build" stamp (git commit, uncommitted changes, binary
hashes) so cells produced by different builds can be told apart.
"""
import argparse, hashlib, json, math, os, random, resource, subprocess, sys, threading, time, zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import netlab

REPO = Path(__file__).resolve().parent.parent

VARIANTS = ["threads", "pool-sender", "poll", "epoll",
            "poll-duplex", "epoll-duplex", "poll-split", "epoll-split"]


def is_duplex(variant):
    """duplex/split nodes answer on the dialed socket: stressgen --duplex, ~1 fd per peer."""
    return variant.endswith(("-duplex", "-split"))


def base_variant(variant):
    return variant.split("-")[0] if is_duplex(variant) else variant


# --- Build stamp -------------------------------------------------------------

def _git(*argv, strip=True):
    try:
        r = subprocess.run(["git", "-C", str(REPO), *argv], capture_output=True, text=True, timeout=10)
        if r.returncode != 0:
            return None
        return r.stdout.strip() if strip else r.stdout
    except (OSError, subprocess.TimeoutExpired):
        return None


def _sha256(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def source_stamp():
    """Git state of the tree the run was started from (once per run). `dirty`
    means tracked files differ from `commit`; their names are listed so a cell
    built from uncommitted code says so."""
    # Porcelain lines are "XY <path>": keep the raw output, the leading status
    # column may be a space.
    changed = _git("status", "--porcelain", "--untracked-files=no", strip=False)
    files = [line[3:] for line in changed.splitlines() if line.strip()] if changed else []
    return dict(commit=_git("rev-parse", "HEAD"), branch=_git("rev-parse", "--abbrev-ref", "HEAD"),
                dirty=bool(files) if changed is not None else None, dirty_files=files,
                harness_sha256=_sha256(__file__))


def binary_stamp(path):
    """Identity of one binary at the moment a cell starts (per cell, so a rebuild
    in the middle of a run shows up as a different hash)."""
    try:
        st = os.stat(path)
    except OSError:
        return dict(path=str(path), sha256=None)
    return dict(path=str(Path(path).resolve()), sha256=_sha256(path), size=st.st_size,
                mtime=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(st.st_mtime)))


def build_stamp(args):
    return dict(args.source, app=binary_stamp(args.app), stressgen=binary_stamp(args.stressgen))

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


def _s6b_net(rate_mbit):
    """Rate on the node->generator direction only; RTT 4 ms; netem limit ~2x BDP (>= 100 pkts)."""
    bdp_bytes = rate_mbit * 1e6 / 8 * 0.004
    return dict(node_delay_ms=2, gen_delay_ms=2, node_rate_mbit=rate_mbit,
                limit_pkts=max(100, math.ceil(2 * bdp_bytes / netlab.MTU)))


# S6: x is NOT a stressgen option, it only feeds netlab.configure_netem via `net`.
SCENARIOS.update({
    "S6a": dict(title="WAN churn", x="rtt_ms", values=[0, 4, 20, 50],
                base=dict(peers=32, rate=100, size=256, churn=16, duration=20, warmup=5),
                quick_values=[0, 20],
                net=lambda rtt: dict(node_delay_ms=rtt / 2, gen_delay_ms=rtt / 2)),
    "S6b": dict(title="WAN bottleneck", x="rate_mbit", values=[1000, 200, 50],
                base=dict(peers=32, rate=0, window=8, size=16384, duration=20, warmup=5),
                quick_values=[1000, 100], net=_s6b_net),
})
LAB = None   # netlab.NetLab, set up once in the unshare child when an S6 scenario runs
S6_EXTRA_TIMEOUT = 15   # seconds of slack on the stressgen timeout for WAN round trips
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

    def quit(self, timeout=30):
        """Sends /quit and waits (one deadline) for "bye" and the process exit.
        Returns dict(clean, bye, shutdown_s, killed): clean means both happened in
        time; shutdown_s is /quit -> exit (or -> kill), so a slow drain shows up as
        a number rather than only as a dirty flag."""
        t0 = time.monotonic()
        deadline = t0 + timeout
        sent = True
        try:
            self.p.stdin.write("/quit\n")
            self.p.stdin.flush()
        except Exception:
            sent = False
        bye = self.bye.wait(max(0.0, deadline - time.monotonic()))
        killed = False
        try:
            self.p.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            self.p.kill()
            self.p.wait()
            killed = True
        return dict(clean=sent and bye and not killed, bye=bye,
                    shutdown_s=round(time.monotonic() - t0, 3), killed=killed)


def run_cell(args, sc, x, variant, rep, port, d):
    p = cell_params(sc, x, args.quick)
    pre = d / f"rep{rep}"
    meta = dict(scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep,
                port=port, params=p, status="failed", error=None, t_start=time.time(),
                build=build_stamp(args))
    wan = "net" in SCENARIOS[sc]
    if wan:
        if LAB is None:
            raise RuntimeError("S6 cells need the netlab namespace (run via run_matrix.py without --_in_ns)")
        meta["netem"] = dict(SCENARIOS[sc]["net"](x))
        LAB.configure_netem(**meta["netem"])
        meta["qdiscs"] = LAB.qdiscs
        meta["netlab"] = LAB.info()
    node = Node(["taskset", "-c", args.node_cpus, args.app, "--headless", "--echo",
                 *([f"--address={netlab.NODE_IP}"] if wan else []),
                 f"--io={variant}", f"--max-peers={max_peers_for(p)}", "--backlog=4096", f"--port={port}",
                 f"--log={pre}.node.log", f"--metrics={pre}.metrics.jsonl"])
    try:
        if not node.ready.wait(args.ready_timeout) or node.ready_err or node.p.poll() is not None:
            raise RuntimeError("node not ready: " + (str(node.ready_err) if node.ready_err
                               else "timeout/exited; " + " | ".join(node.tail)))
        sg = ["taskset", "-c", args.gen_cpus, args.stressgen,
              "--target", f"{netlab.NODE_IP if wan else '127.0.0.1'}:{port}",
              *(["--listen-ip", netlab.GEN_IP] if wan else []),
              "--peers", str(p["peers"]), "--rate", str(p["rate"]), "--size", str(p["size"]),
              "--duration", str(p["duration"]), "--warmup", str(p["warmup"]),
              "--idle-peers", str(p["idle_peers"]), "--slow-peers", str(p["slow_peers"]),
              "--churn", str(p["churn"]), "--grace", str(0.5 if args.quick else GRACE),
              "--out", f"{pre}.stress.json"]
        if wan:
            sg = netlab.in_gen(sg)
        if is_duplex(variant):
            sg += ["--duplex"]
        if p.get("window"):
            sg += ["--window", str(p["window"])]
        limit = (p["duration"] + p["warmup"] + 60 + (p["peers"] + p["idle_peers"]) * 0.02
                 + (S6_EXTRA_TIMEOUT if wan else 0))
        r = subprocess.run(sg, capture_output=True, text=True, timeout=limit)
        (d / f"rep{rep}.stressgen.txt").write_text(r.stdout + r.stderr)
        if r.returncode != 0:
            raise RuntimeError(f"stressgen exit {r.returncode}: {r.stderr.strip()[-200:]}")
        meta["status"] = "ok"
    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
    finally:
        q = node.quit(args.quit_timeout)
        meta.update(shutdown_s=q["shutdown_s"], bye=q["bye"], killed=q["killed"])
        if not q["clean"]:
            meta["node_unclean"] = True
            if meta["status"] == "ok":
                meta["status"] = "dirty"
                meta["error"] = (f"node did not exit cleanly after /quit "
                                 f"(bye={q['bye']}, killed={q['killed']}, {q['shutdown_s']}s)")
        meta["t_end"] = time.time()
        (d / f"rep{rep}.meta.json").write_text(json.dumps(meta, indent=1))
    return meta["status"]


# --- Fake run ---------------------------------------------------------------
# Plausible synthetic model. (cpu us/msg, rtt base us, rtt us per peer, threads per conn)
FAKE = {"threads": (14, 90, 0.9, 1.0), "pool-sender": (12, 80, 0.7, 1.0),
        "poll": (7, 110, 1.4, 0.0), "epoll": (6, 70, 0.2, 0.0),
        # duplex: one shared socket per peer, slightly less syscall work; split: +1 thread, a bit more latency
        "poll-duplex": (6.5, 105, 1.3, 0.0), "epoll-duplex": (5.5, 65, 0.2, 0.0),
        "poll-split": (7.5, 120, 1.3, 0.0), "epoll-split": (6.5, 80, 0.2, 0.0)}


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
    rtt_net = p["rtt_ms"] * 1000 if sc == "S6a" else 4000 if sc == "S6b" else 0
    hs = (rtt_net * (2 if is_duplex(variant) else 3) + 300) * noise(0.05)
    st = dict(sent=int(mps * dur), received=int(mps * dur),
              lost=int(rnd.random() < 0.1) * rnd.randint(1, 20), msgs_per_s=round(mps, 1),
              rtt_us=dict(p50=rtt50, p90=rtt50 * 2, p99=rtt50 * 6 * noise(0.2),
                          p999=rtt50 * 30 * noise(0.3), max=rtt50 * 90, mean=rtt50 * 1.3,
                          samples=int(mps * dur)),
              handshake_us=dict(p50=hs, p90=hs * 1.3, p99=hs * 2, max=hs * 3, mean=hs, samples=n + int(p["churn"] * dur)),
              send_skipped=0, connect_failures=0, churn_events=int(p["churn"] * dur),
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
        vcsw += int(rate * (0.4 if base_variant(variant) in ("poll", "epoll") else 2.0))
        ivcsw += int(rate * 0.02 + 50)
        rows.append(dict(
            t_ns=t0 + i * 10**9, accepts=live, connections_live=live,
            bytes_in=fin * (p["size"] + 20), frames_in=fin, frames_out=fin, send_failures=0,
            send_backpressure_drops=int(fin * 0.001) if p["slow_peers"] and variant != "threads" else 0,
            reconnects=int(p["churn"] * i),
            evictions=int(p["churn"] * i * 0.1) if base_variant(variant) in ("poll", "epoll") else 0,
            echoed=fin, render_q_hwm=0, store_q_hwm=int(rnd.random() * 50),
            sender_q_hwm=int(rnd.random() * 50), utime_us=int(ut), stime_us=int(stt),
            nvcsw=vcsw, nivcsw=ivcsw, maxrss_kb=20000 + int(live * (60 if thr else 8)) + i * 10,
            threads=int(8 + live * thr + (4 if variant != "threads" else 0) + (1 if variant.endswith("-split") else 0)),
            fds=live * (1 if is_duplex(variant) else 2) + 10,
            tx_queued_sends=int(fin * 0.3) if sc == "S6b" and base_variant(variant) in ("poll", "epoll") and is_duplex(variant) else 0,
            tx_flushes=int(fin * 0.2) if sc == "S6b" and is_duplex(variant) else 0))
    (d / f"rep{rep}.metrics.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    (d / f"rep{rep}.node.log").write_text("fake\n")
    (d / f"rep{rep}.meta.json").write_text(json.dumps(dict(
        scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep, port=port,
        params=p, status="ok", fake=True, build=dict(args.source)), indent=1))
    return "ok"


def run_with_wan(args, scs, wan_scs, variants, reps):
    """S1-S5 here, in this process; the S6 scenarios in a re-executed child under
    `unshare -rnm` (own network + mount namespace, uid mapped to root, gone on exit)."""
    rc = 0
    rest = [sc for sc in scs if sc not in wan_scs]
    if rest:
        rc = main_args(args, rest)
    cmd = ["unshare", "-rnm", "--propagation", "private", sys.executable, os.path.abspath(__file__),
           "--app", args.app, "--stressgen", args.stressgen, "--out", args.out,
           "--variants", ",".join(variants), "--scenarios", ",".join(wan_scs), "--reps", str(reps),
           "--node-cpus", args.node_cpus, "--gen-cpus", args.gen_cpus, "--base-port", str(args.base_port),
           "--ready-timeout", str(args.ready_timeout), "--quit-timeout", str(args.quit_timeout),
           "--seed", str(args.seed), "--_in_ns", "--_source", json.dumps(args.source)]
    if args.quick:
        cmd.append("--quick")
    print("S6 cells: re-executing under unshare -rnm", flush=True)
    return max(rc, subprocess.run(cmd).returncode)


def main_args(args, scs):
    """Runs `scs` (already validated) in this process."""
    sub = argparse.Namespace(**vars(args))
    sub.scenarios = ",".join(scs)
    return main(sub)


def main(parsed=None):
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
    ap.add_argument("--quit-timeout", type=float, default=30,
                    help="seconds from /quit to exit before a cell is marked dirty")
    ap.add_argument("--fake", action="store_true", help="synthetic results, no binaries")
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--_in_ns", action="store_true", help=argparse.SUPPRESS)   # set by the unshare re-exec
    ap.add_argument("--_source", default=None, help=argparse.SUPPRESS)
    args = parsed or ap.parse_args()
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
    # Git is run once in the parent; the unshare child receives the stamp (no git inside the namespace).
    args.source = json.loads(args._source) if args._source else source_stamp()
    if args.source["dirty"] and not args.dry_run and not args._in_ns:
        print(f"warning: uncommitted changes in {', '.join(args.source['dirty_files'])}; "
              f"cells will record commit {args.source['commit']} as dirty", file=sys.stderr)
    wan_scs = [sc for sc in scs if "net" in SCENARIOS[sc]]
    if wan_scs and not args.fake and not args.dry_run:
        if args._in_ns:
            global LAB
            LAB = netlab.NetLab()
            LAB.setup()
            print(f"netlab: {netlab.NS}, {netlab.NODE_DEV} {netlab.NODE_IP} <-> {netlab.GEN_DEV} {netlab.GEN_IP}"
                  + (f" (warnings: {LAB.warnings})" if LAB.warnings else ""), flush=True)
        else:
            return run_with_wan(args, scs, wan_scs, variants, reps)
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
