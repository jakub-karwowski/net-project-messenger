#!/usr/bin/env python3
"""Run the stress experiment matrix: scenario x param x variant x rep.

Per cell: start the node headless with --echo, wait for the "ready" JSONL
event, run stressgen against it, send /quit, wait for "bye" + exit.
Output layout:
  <out>/<scenario>/<param>/<variant>/rep<N>.{stress.json,metrics.jsonl,node.log,meta.json}
S6 (WAN) scenarios run node and stressgen on both ends of a netem-shaped veth link
(scripts/netlab.py). That needs no sudo: this script re-executes itself, restricted
to the S6 scenarios, under `unshare -rnm`; S1-S5 cells run in the normal process.
Cells whose meta.json has status ok|dirty are skipped on re-run (resume); resuming into
a directory whose cells carry a different build stamp is refused unless --force-mixed.
meta.json also records `validity` {ok, reasons} (analyze.validity; analyze.py lists
failing cells as "degraded" and excludes them from medians), `node_rc`, `order_index` /
`variant_order` (the seeded shuffle) and, for S6, `netem_stats`; a node that has to be
killed leaves rep<N>.hang.txt (per-thread wchan/syscall/state).
`--fake` writes synthetic results (no binaries) to exercise analyze.py.
Every meta.json carries a "build" stamp (git commit, uncommitted changes, binary
hashes) so cells produced by different builds can be told apart.
"""
import argparse, hashlib, json, math, os, random, resource, subprocess, sys, threading, time, zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import netlab
import analyze

REPO = Path(__file__).resolve().parent.parent

VARIANTS = ["threads", "pool-sender", "poll", "epoll",
            "poll-duplex", "epoll-duplex", "poll-split", "epoll-split", "peer-threads"]


def is_duplex(variant):
    """duplex/split nodes answer on the dialed socket: stressgen --duplex, ~1 fd per peer."""
    return variant.endswith(("-duplex", "-split")) or variant == "peer-threads"


def base_variant(variant):
    return variant.split("-")[0] if is_duplex(variant) and variant != "peer-threads" else variant


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


def build_type(path):
    """CMAKE_BUILD_TYPE of the build directory holding `path` (from its
    CMakeCache.txt), or None when it can't be found. A hash doesn't reveal an
    unoptimised binary; this does."""
    for d in Path(path).resolve().parents:
        cache = d / "CMakeCache.txt"
        if cache.exists():
            for line in cache.read_text(errors="replace").splitlines():
                if line.startswith("CMAKE_BUILD_TYPE:"):
                    return line.split("=", 1)[1].strip() or None
            return None
    return None


def binary_stamp(path):
    """Identity of one binary at the moment a cell starts (per cell, so a rebuild
    in the middle of a run shows up as a different hash)."""
    try:
        st = os.stat(path)
    except OSError:
        return dict(path=str(path), sha256=None)
    return dict(path=str(Path(path).resolve()), sha256=_sha256(path), size=st.st_size,
                mtime=time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime(st.st_mtime)),
                build_type=build_type(path))


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
    # The --max-peers cap exists to keep thread-per-connection modes from starving
    # the node's CPUs. S1 at 1024 peers runs effectively uncapped (peers + 16) and
    # threads/pool-sender collapse; here the same load meets a cap. 256 is the
    # largest S1 peer count every mode sustained on the 2 node CPUs; 1040 repeats
    # the uncapped S1 cell inside this scenario. x is NOT a stressgen option: it
    # only sets the node's --max-peers (see max_peers_for).
    "S7": dict(title="Capped fan-in", x="max_peers", values=[64, 128, 256, 1040],
               base=dict(peers=1024, rate=100, size=64, duration=20, warmup=5),
               quick_values=[8, 64]),
}


def _s6b_net(rate_mbit):
    """Rate on the node->generator direction only; RTT 4 ms; netem limit ~2x BDP (>= 100 pkts)."""
    bdp_bytes = rate_mbit * 1e6 / 8 * 0.004
    return dict(node_delay_ms=2, gen_delay_ms=2, node_rate_mbit=rate_mbit,
                limit_pkts=max(100, math.ceil(2 * bdp_bytes / netlab.MTU)))


# S6: x is NOT a stressgen option, it only feeds netlab.configure_netem via `net`.
SCENARIOS.update({
    "S6a": dict(title="WAN churn", x="rtt_ms", values=[0, 4, 20, 30, 40, 50],
                base=dict(peers=32, rate=100, size=256, churn=16, duration=20, warmup=5),
                quick_values=[0, 20],
                net=lambda rtt: dict(node_delay_ms=rtt / 2, gen_delay_ms=rtt / 2)),
    "S6c": dict(title="WAN churn, half rate", x="rtt_ms", values=[50],
                base=dict(peers=32, rate=100, size=256, churn=8, duration=20, warmup=5),
                quick_values=[50],
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


def shuffled_variants(variants, seed, sc, x, rep):
    """Variant order for one (scenario, value, rep): deterministic in all four plus the seed."""
    vs = list(variants)
    random.Random(zlib.crc32(f"{seed}|{sc}|{x}|{rep}".encode())).shuffle(vs)
    return vs


def build_key(build):
    """What must match between cells of one results directory (None = no stamp)."""
    return analyze.build_key(build)


def check_build_guard(out, args):
    """Refuse to add cells to a non-empty `out` whose existing meta.json build stamps
    (commit, dirty, app sha256, stressgen sha256, app build_type) differ from the
    current ones. Returns an error message or None."""
    current = build_key(dict(args.source) if args.fake else build_stamp(args))   # fake cells carry no binary stamps
    seen = {}
    for mp in Path(out).glob("*/*/*/rep*.meta.json"):
        try:
            k = build_key(json.loads(mp.read_text()).get("build"))
        except (ValueError, OSError):
            continue
        seen[k] = seen.get(k, 0) + 1
    other = {k: n for k, n in seen.items() if k != current}
    if not other:
        return None
    lines = [f"  {n:5d} cells: {analyze.fmt_build_key(k)}" for k, n in sorted(other.items(), key=lambda kv: -kv[1])]
    return (f"{out} already holds cells from a different build than the current one "
            f"({analyze.fmt_build_key(current)}):\n" + "\n".join(lines) +
            "\nUse a fresh --out, or pass --force-mixed if mixing is intended.")


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
    """The node's --max-peers: an explicit cap (S7), else room for every peer so
    caps never evict."""
    if p.get("max_peers"):
        return int(p["max_peers"])
    return int(p["peers"] + p["idle_peers"] + p["slow_peers"] + 16)


def cell_dir(out, sc, x, variant):
    return Path(out) / sc / f"{SCENARIOS[sc]['x']}={x}" / variant


def is_done(d, rep):
    try:
        return json.loads((d / f"rep{rep}.meta.json").read_text()).get("status") in ("ok", "dirty")
    except Exception:
        return False


# --- Real run ---------------------------------------------------------------
def dump_hang(pid, path):
    """Write what every thread of `pid` is doing (comm, wchan, syscall, state, and
    /proc/<pid>/stack when readable) to `path`; called just before a hung node is
    killed. Never raises."""
    def rd(f):
        try:
            return Path(f).read_text(errors="replace").strip()
        except OSError as e:
            return f"<unreadable: {e.strerror}>"
    out = [f"pid {pid}  {time.strftime('%Y-%m-%dT%H:%M:%S%z')}", f"cmdline: {rd(f'/proc/{pid}/cmdline').replace(chr(0), ' ')}"]
    try:
        tids = sorted(int(t) for t in os.listdir(f"/proc/{pid}/task"))
    except OSError as e:
        tids = []
        out.append(f"<no task list: {e.strerror}>")
    for tid in tids:
        t = f"/proc/{pid}/task/{tid}"
        stat = rd(f"{t}/stat")
        state = stat.rsplit(")", 1)[-1].split()[0] if ")" in stat else stat   # field 3, after the (comm)
        out.append(f"--- tid {tid}\n comm: {rd(t + '/comm')}\n state: {state}\n wchan: {rd(t + '/wchan')}\n"
                   f" syscall: {rd(t + '/syscall')}\n stat: {stat}")
        stack = rd(f"{t}/stack")
        if not stack.startswith("<unreadable"):
            out.append(f" stack:\n{stack}")
    out.append(f"--- process stack\n{rd(f'/proc/{pid}/stack')}")
    try:
        Path(path).write_text("\n".join(out) + "\n")
    except OSError:
        pass


class Node:
    """Node subprocess; a reader thread watches stdout JSONL for ready/bye.
    `bye_seen` is True only when a bye event was actually parsed (`bye` is also set at
    EOF to unblock waiters). `hang_path`: where quit() dumps thread states before a kill."""
    def __init__(self, cmd, hang_path=None):
        self.p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, text=True, bufsize=1)
        self.ready = threading.Event()
        self.bye = threading.Event()
        self.bye_seen = False
        self.hang_path = hang_path
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
            k = ev.get("event") if isinstance(ev, dict) else None
            if k == "ready":
                self.ready_err = ev.get("error")
                self.ready.set()
            elif k == "bye":
                self.bye_seen = True
                self.bye.set()
        self.ready.set()   # EOF: unblock waiters
        self.bye.set()

    def quit(self, timeout=30):
        """Sends /quit and waits (one deadline) for "bye" and the process exit.
        Returns dict(clean, bye, rc, shutdown_s, killed, hang_dump): clean means a bye
        event was parsed, the process exited in time and rc == 0; shutdown_s is
        /quit -> exit (or -> kill). On timeout the threads are dumped to hang_path
        (hang_dump=True) before the kill."""
        t0 = time.monotonic()
        deadline = t0 + timeout
        sent = True
        try:
            self.p.stdin.write("/quit\n")
            self.p.stdin.flush()
        except Exception:
            sent = False
        self.bye.wait(max(0.0, deadline - time.monotonic()))
        killed = dumped = False
        try:
            self.p.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            if self.hang_path:
                dump_hang(self.p.pid, self.hang_path)
                dumped = True
            self.p.kill()
            self.p.wait()
            killed = True
        rc = self.p.returncode
        return dict(clean=sent and self.bye_seen and not killed and rc == 0, bye=self.bye_seen, rc=rc,
                    shutdown_s=round(time.monotonic() - t0, 3), killed=killed, hang_dump=dumped)


def run_cell(args, sc, x, variant, rep, port, d, order=None):
    p = cell_params(sc, x, args.quick)
    pre = d / f"rep{rep}"
    meta = dict(scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep,
                port=port, params=p, status="failed", error=None, t_start=time.time(),
                build=build_stamp(args), **order_meta(order))
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
                 f"--log={pre}.node.log", f"--metrics={pre}.metrics.jsonl"],
                hang_path=Path(f"{pre}.hang.txt"))
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
        meta.update(shutdown_s=q["shutdown_s"], bye=q["bye"], killed=q["killed"], node_rc=q["rc"])
        if q["hang_dump"]:
            meta["hang_dump"] = True
        if not q["clean"]:
            meta["node_unclean"] = True
            if meta["status"] == "ok":
                meta["status"] = "dirty"
                meta["error"] = (f"node did not exit cleanly after /quit "
                                 f"(bye={q['bye']}, rc={q['rc']}, killed={q['killed']}, {q['shutdown_s']}s)")
        if wan:   # before the next cell's configure_netem resets the counters
            try:
                meta["netem_stats"] = LAB.qdisc_stats()
            except Exception as e:
                meta["netem_stats"] = dict(error=f"{type(e).__name__}: {e}")
        meta["t_end"] = time.time()
        if meta["status"] in ("ok", "dirty"):
            meta["validity"] = cell_validity(d, rep, meta)
        (d / f"rep{rep}.meta.json").write_text(json.dumps(meta, indent=1))
    if meta["status"] in ("ok", "dirty") and not meta["validity"]["ok"]:
        print("  INVALID (degraded): " + "; ".join(meta["validity"]["reasons"]), flush=True)
        return "degraded"
    return meta["status"]


def order_meta(order):
    """meta.json fields for the seeded variant shuffle: order = (index, full order)."""
    return dict(order_index=order[0], variant_order=list(order[1])) if order else {}


def cell_validity(d, rep, meta):
    """analyze.validity() on the files this cell just wrote -> {ok, reasons}."""
    try:
        st = json.loads((d / f"rep{rep}.stress.json").read_text())
        rows = analyze.read_jsonl(d / f"rep{rep}.metrics.jsonl")
        ok, reasons = analyze.validity(meta, st, rows)
        return dict(ok=ok, reasons=reasons)
    except Exception as e:
        return dict(ok=False, reasons=[f"validity check failed: {type(e).__name__}: {e}"])


# --- Fake run ---------------------------------------------------------------
# Plausible synthetic model. (cpu us/msg, rtt base us, rtt us per peer, threads per conn)
FAKE = {"threads": (14, 90, 0.9, 1.0), "pool-sender": (12, 80, 0.7, 1.0),
        "poll": (7, 110, 1.4, 0.0), "epoll": (6, 70, 0.2, 0.0),
        # duplex: one shared socket per peer, slightly less syscall work; split: +1 thread, a bit more latency
        "poll-duplex": (6.5, 105, 1.3, 0.0), "epoll-duplex": (5.5, 65, 0.2, 0.0),
        "poll-split": (7.5, 120, 1.3, 0.0), "epoll-split": (6.5, 80, 0.2, 0.0),
        # one thread per peer like threads, one duplex socket: cheaper than threads, thread-bound at scale
        "peer-threads": (12, 85, 0.8, 1.0)}


def fake_cell(args, sc, x, variant, rep, port, d, order=None):
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
    rtt_net = p["rtt_ms"] * 1000 if sc in ("S6a", "S6c") else 4000 if sc == "S6b" else 0
    hs = (rtt_net * (2 if is_duplex(variant) else 3) + 300) * noise(0.05)
    t0 = 10_000_000_000
    st = dict(sent=int(max(mps, offered if p["rate"] else 0) * dur), received=int(mps * dur),
              lost=int(rnd.random() < 0.1) * rnd.randint(1, 20), msgs_per_s=round(mps, 1),
              rtt_us=dict(p50=rtt50, p90=rtt50 * 2, p99=rtt50 * 6 * noise(0.2),
                          p999=rtt50 * 30 * noise(0.3), max=rtt50 * 90, mean=rtt50 * 1.3,
                          samples=int(mps * dur)),
              handshake_us=dict(p50=hs, p90=hs * 1.3, p99=hs * 2, max=hs * 3, mean=hs, samples=n + int(p["churn"] * dur)),
              send_skipped=0, connect_failures=0, churn_events=int(p["churn"] * dur),
              peers_total=n + p["idle_peers"] + p["slow_peers"], peers_ready=n + p["idle_peers"] + p["slow_peers"],
              bad_frames=0, duplicates=0, handshake_failures=0,
              slow_unread_bytes=int(p["slow_peers"] * 262144 * noise(0.1)),
              gen_cpu=dict(utime_us=int(mps * 4 * dur), stime_us=int(mps * 3 * dur), nvcsw=int(mps * dur), nivcsw=int(dur * 50)),
              window_start_mono_ns=t0 + int((1 + p["warmup"]) * 1e9), window_end_mono_ns=t0 + int((1 + p["warmup"] + dur) * 1e9),
              config=dict(p, target=f"127.0.0.1:{port}"), start_unix_ns=0, end_unix_ns=int(dur * 1e9))
    (d / f"rep{rep}.stress.json").write_text(json.dumps(st))
    live = n + p["idle_peers"] + p["slow_peers"]
    total = p["warmup"] + dur + 1
    nsamp = int(math.ceil(total)) + 2
    ut = stt = 0.0
    fin = vcsw = ivcsw = 0
    rows = []
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
            fds=live * (2 if variant == "peer-threads" or not is_duplex(variant) else 1) + 10,
            tx_queued_sends=int(fin * 0.3) if sc == "S6b" and base_variant(variant) in ("poll", "epoll") and is_duplex(variant) else 0,
            tx_flushes=int(fin * 0.2) if sc == "S6b" and is_duplex(variant) else 0))
    (d / f"rep{rep}.metrics.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
    (d / f"rep{rep}.node.log").write_text("fake\n")
    (d / f"rep{rep}.meta.json").write_text(json.dumps(dict(
        scenario=sc, x_name=SCENARIOS[sc]["x"], x=x, variant=variant, rep=rep, port=port,
        params=p, status="ok", fake=True, build=dict(args.source), t_start=0, node_rc=0, bye=True,
        validity=dict(ok=True, reasons=[]), **order_meta(order),
        **(dict(netem_stats={dev: dict(dropped=int(sc == "S6b" and x < 1000) * rnd.randint(0, 40))
                             for dev in (netlab.NODE_DEV, netlab.GEN_DEV)} if sc.startswith("S6") else {}))), indent=1))
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
    if args.force_mixed:
        cmd.append("--force-mixed")
    if args.allow_non_release:
        cmd.append("--allow-non-release")
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
    ap.add_argument("--allow-non-release", action="store_true",
                    help="run with Debug/unknown builds (numbers are not comparable)")
    ap.add_argument("--seed", type=int, default=1,
                    help="seeds the variant shuffle per (seed, scenario, value, rep) (default 1)")
    ap.add_argument("--force-mixed", action="store_true",
                    help="allow resuming into a --out whose cells have a different build stamp")
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
            bt = build_type(b)
            if bt != "Release" and not args.allow_non_release:
                sys.exit(f"{b} comes from a {bt or 'unknown'} build; benchmark numbers need "
                         f"Release (cmake -DCMAKE_BUILD_TYPE=Release), or pass --allow-non-release")
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
    if not args.dry_run and not args.force_mixed and Path(args.out).is_dir():
        err = check_build_guard(args.out, args)
        if err:
            sys.exit(err)
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
    # Shuffle variant order within each (scenario, param, rep) to spread drift; the
    # shuffle is seeded per (seed, scenario, value, rep) so a run is reproducible and
    # a resumed run reproduces the same order.
    cells = []
    for sc in scs:
        vals = SCENARIOS[sc]["quick_values"] if args.quick else SCENARIOS[sc]["values"]
        for x in vals:
            for rep in range(1, reps + 1):
                vs = shuffled_variants(variants, args.seed, sc, x, rep)
                cells += [(sc, x, v, rep, (i, vs)) for i, v in enumerate(vs)]
    port = args.base_port
    counts = {}
    for i, (sc, x, v, rep, order) in enumerate(cells, 1):
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
        st = (fake_cell if args.fake else run_cell)(args, sc, x, v, rep, port, d, order)
        print(f"{tag} -> {st} ({time.time() - t:.1f}s)", flush=True)
        counts[st] = counts.get(st, 0) + 1
        if not args.fake:
            time.sleep(0.3)   # let sockets settle
    print("summary:", counts)
    return 1 if counts.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())
