#!/usr/bin/env python3
"""Run the experiment matrix: scenario x swept value x node variant x repetition.

    run_matrix.py --out results                 # the full matrix (hours; resumable)
    run_matrix.py --out /tmp/smoke --quick      # a few minutes: short runs, small sweeps
    run_matrix.py --out results --scenarios S1,S4 --variants balancer,fixed-4 --reps 3
    run_matrix.py --out results --dry-run       # list the cells and estimate the time

Every cell is one run_load.run_cell(): an echo node with the variant's options,
loadgen with the scenario's load, the node pinned to --node-cpus and loadgen
to --loadgen-cpus. Layout:

    <out>/<scenario>/<x>=<value>/<variant>/rep<N>/{result.json,loadgen.json,node.log,meta.json}

meta.json has the cell's status (ok: the run completed and the node exited 0;
dirty: it completed but the node did not exit cleanly; failed: no result), its
validity (analyze.validity: reasons a completed cell must not be counted), the
build stamp (git commit, uncommitted files, binary hashes, build type) and
where it fell in the shuffled order. A re-run skips cells that are ok or dirty
and retries failed ones; it refuses to add cells from a different build to
an existing directory unless --force-mixed.

Variant order is shuffled per (scenario, value, rep) with a seed, so slow drift
on the machine spreads over the variants instead of favouring one.

S6 (WAN, netem in a namespace) is not ported yet.
"""

import argparse
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import time
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import analyze      # noqa: E402  (validity rules live with the analysis)
import run_load     # noqa: E402

REPO = Path(__file__).resolve().parent.parent

# Node configurations compared in every scenario.
VARIANTS = {
    "fixed-1": ["--no-balancer", "--min-pts", "1"],
    "fixed-4": ["--no-balancer", "--min-pts", "4"],
    "balancer": ["--max-pts", "4"],
    "balancer-fast": ["--max-pts", "4", "--balance-max-moves", "32"],
}

# x: the swept parameter; a loadgen option, except max_links (a node option).
# base: the other loadgen settings. quick: a shorter sweep for --quick.
SCENARIOS = {
    "S1": dict(title="Fan-in", x="peers", values=[1, 8, 64, 256, 1024],
               base=dict(peers=64, rate=100, size=64), quick=[8, 64]),
    "S2": dict(title="Idle-heavy", x="idle", values=[0, 100, 300, 1000],
               base=dict(peers=8, rate=500, size=64), quick=[0, 50]),
    "S3": dict(title="Slow consumers", x="slow", values=[0, 1, 4],
               base=dict(peers=16, rate=200, size=64), quick=[0, 1]),
    "S4": dict(title="Connection churn", x="churn", values=[0, 10, 50],
               base=dict(peers=64, rate=50, size=64), quick=[0, 5]),
    "S5": dict(title="Throughput ceiling", x="size", values=[32, 4096, 65536],
               base=dict(peers=8, rate=0, window=16, size=64), quick=[32, 4096]),
    # The node's --max-links meets more peers than it accepts; 1040 is uncapped.
    "S7": dict(title="Capped fan-in", x="max_links", values=[64, 128, 256, 1040],
               base=dict(peers=1024, rate=100, size=64), quick=[16, 80], quick_base=dict(peers=64)),
}

FULL = dict(duration=20, warmup=5, grace=2)
QUICK = dict(duration=2, warmup=1, grace=1)


# ------------------------------------------------------------------ build stamp

def git(*argv):
    try:
        r = subprocess.run(["git", "-C", str(REPO), *argv], capture_output=True, text=True, timeout=10)
        return r.stdout if r.returncode == 0 else None
    except (OSError, subprocess.TimeoutExpired):
        return None


def sha256(path):
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except OSError:
        return None


def build_type(binary):
    for d in Path(binary).resolve().parents:
        cache = d / "CMakeCache.txt"
        if cache.exists():
            for line in cache.read_text(errors="replace").splitlines():
                if line.startswith("CMAKE_BUILD_TYPE:"):
                    return line.split("=", 1)[1].strip() or None
            return None
    return None


def build_stamp(build):
    status = git("status", "--porcelain", "--untracked-files=no") or ""
    node = Path(build) / "App" / "node"
    loadgen = Path(build) / "Tools" / "loadgen" / "loadgen"
    return dict(commit=(git("rev-parse", "HEAD") or "").strip() or None,
                dirty_files=[line[3:] for line in status.splitlines() if line.strip()],
                node_sha256=sha256(node), loadgen_sha256=sha256(loadgen), build_type=build_type(node),
                harness_sha256=sha256(__file__))


def same_build(a, b):
    keys = ("commit", "node_sha256", "loadgen_sha256")
    return all(a.get(k) == b.get(k) for k in keys)


# ------------------------------------------------------------------ cells

def cell_args(scenario, value, quick):
    """(node args, loadgen args) for one cell, without the variant's options."""
    s = SCENARIOS[scenario]
    load = dict(s["base"], **(s.get("quick_base", {}) if quick else {}))
    node = []
    if s["x"] == "max_links":
        node += ["--max-links", str(value)]
    else:
        load[s["x"]] = value
    if s["x"] != "max_links":
        # Never let the link cap refuse anybody outside S7.
        total = load.get("peers", 0) + load.get("idle", 0) + load.get("slow", 0)
        node += ["--max-links", str(total + 16)]
    timing = QUICK if quick else FULL
    loadgen = []
    for k, v in {**load, **timing}.items():
        loadgen += [f"--{k}", str(v)]
    return node, loadgen


def plan(args):
    cells = []
    for scenario in args.scenarios:
        s = SCENARIOS[scenario]
        values = s["quick"] if args.quick else s["values"]
        for value in values:
            for rep in range(1, args.reps + 1):
                order = list(args.variants)
                random.Random(zlib.crc32(f"{args.seed}/{scenario}/{value}/{rep}".encode())).shuffle(order)
                for index, variant in enumerate(order):
                    cells.append(dict(scenario=scenario, x=s["x"], value=value, variant=variant, rep=rep,
                                      order_index=index, variant_order=order))
    return cells


def cell_dir(out, c):
    return Path(out) / c["scenario"] / f"{c['x']}={c['value']}" / c["variant"] / f"rep{c['rep']}"


def run_one(args, c, stamp):
    d = cell_dir(args.out, c)
    node_args, loadgen_args = cell_args(c["scenario"], c["value"], args.quick)
    node_args = VARIANTS[c["variant"]] + node_args
    meta = dict(c, node_args=node_args, loadgen_args=loadgen_args, build=stamp, quick=args.quick,
                started=time.strftime("%Y-%m-%dT%H:%M:%S%z"))
    t0 = time.monotonic()
    try:
        result = run_load.run_cell(args.build, d, node_args, loadgen_args, node_cpus=args.node_cpus,
                                   loadgen_cpus=args.loadgen_cpus)
    except (RuntimeError, FileNotFoundError, OSError) as e:
        meta.update(status="failed", error=str(e), seconds=round(time.monotonic() - t0, 1))
        (d / "meta.json").write_text(json.dumps(meta, indent=1))
        return meta, None
    clean = result["node"]["exit_code"] == 0 and not result["node"]["forced"]
    status = "failed" if result["loadgen"] is None else ("ok" if clean else "dirty")
    reasons = analyze.validity(meta, result) if result["loadgen"] else ["no loadgen result"]
    meta.update(status=status, validity=dict(ok=not reasons, reasons=reasons), seconds=round(time.monotonic() - t0, 1))
    (d / "meta.json").write_text(json.dumps(meta, indent=1))
    return meta, result


def main(argv):
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)   # quiet when piped into head
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--out", required=True, type=Path)
    p.add_argument("--build", type=Path, default=REPO / "build-release")
    p.add_argument("--scenarios", default=",".join(SCENARIOS))
    p.add_argument("--variants", default=",".join(VARIANTS))
    p.add_argument("--reps", type=int, default=3)
    p.add_argument("--quick", action="store_true", help="2 s runs and short sweeps (a smoke test)")
    p.add_argument("--node-cpus", default="0-3", help="CPUs for the node (default 0-3)")
    p.add_argument("--loadgen-cpus", default="8-9", help="CPUs for loadgen (default 8-9)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--force-mixed", action="store_true", help="add cells from another build to an existing run")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)
    args.scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    args.variants = [v.strip() for v in args.variants.split(",") if v.strip()]
    for s in args.scenarios:
        if s not in SCENARIOS:
            p.error(f"unknown scenario {s} (known: {', '.join(SCENARIOS)})")
    for v in args.variants:
        if v not in VARIANTS:
            p.error(f"unknown variant {v} (known: {', '.join(VARIANTS)})")
    available = os.sched_getaffinity(0)
    args.node_cpus = run_load.parse_cpus(args.node_cpus)
    args.loadgen_cpus = run_load.parse_cpus(args.loadgen_cpus)
    for name, cpus in (("--node-cpus", args.node_cpus), ("--loadgen-cpus", args.loadgen_cpus)):
        if cpus and not cpus <= available:
            p.error(f"{name}: CPUs {sorted(cpus - available)} are not available here")
    if args.node_cpus and args.loadgen_cpus and args.node_cpus & args.loadgen_cpus:
        p.error("--node-cpus and --loadgen-cpus overlap")

    cells = plan(args)
    timing = QUICK if args.quick else FULL
    per_cell = timing["duration"] + timing["warmup"] + timing["grace"] + 3
    todo = [c for c in cells if read_meta(cell_dir(args.out, c)).get("status") not in ("ok", "dirty")]
    print(f"{len(cells)} cells, {len(todo)} to run, about {len(todo) * per_cell / 60:.0f} min")
    if args.dry_run:
        for c in todo:
            print(" ", cell_dir(args.out, c))
        return 0

    stamp = build_stamp(args.build)
    if stamp["build_type"] != "Release":
        print(f"warning: {args.build} is a {stamp['build_type']} build; measurements want Release", file=sys.stderr)
    for c in cells:
        old = read_meta(cell_dir(args.out, c))
        if old.get("status") in ("ok", "dirty") and not same_build(old.get("build", {}), stamp) and not args.force_mixed:
            sys.exit(f"run_matrix.py: {args.out} already has cells from another build "
                     f"(commit {old['build'].get('commit')}); use a new --out or --force-mixed")
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "run.json").write_text(json.dumps(dict(argv=argv, build=stamp, variants=VARIANTS, scenarios=SCENARIOS,
                                                       quick=args.quick, seed=args.seed), indent=1))

    started = time.monotonic()
    for n, c in enumerate(todo, 1):
        meta, result = run_one(args, c, stamp)
        left = (time.monotonic() - started) / n * (len(todo) - n)
        tag = meta["status"] if meta.get("validity", {}).get("ok", meta["status"] != "failed") else "degraded"
        line = run_load.summary_line(cell_dir(args.out, c).relative_to(args.out), result) if result else meta.get("error")
        print(f"[{n}/{len(todo)} {tag}, {left / 60:.0f} min left] {line}", flush=True)
    print(f"done; analyse with: scripts/analyze.py {args.out}")
    return 0


def read_meta(d):
    try:
        return json.loads((d / "meta.json").read_text())
    except (OSError, json.JSONDecodeError):
        return {}


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
