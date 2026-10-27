"""Unprivileged network lab for the S6 (WAN) scenarios. Standard library only.

Must run inside `unshare -rnm --propagation private` (run_matrix.py arranges
that): the process then owns a fresh network + mount namespace in which it is
"root", so no sudo is involved and everything vanishes when it exits.

Topology (all addresses /24, MTU 1500, TSO/GSO/GRO off so netem sees real packets):

    [this netns: node]  veth-node 10.200.1.1  <---->  veth-gen 10.200.1.2  [ns_p2p_gen: stressgen]

setup() is called once; configure_netem() is called per cell. The `ip netns`
registry needs a writable /run, hence the private tmpfs mounted on it.
"""
import re, subprocess

NS = "ns_p2p_gen"
NODE_DEV, GEN_DEV = "veth-node", "veth-gen"
NODE_IP, GEN_IP = "10.200.1.1", "10.200.1.2"
MTU = 1500


def parse_tc_stats(text):
    """Parse `tc -s qdisc show dev X` text -> dict(kind, sent_bytes, sent_pkts, dropped,
    overlimits, requeues, backlog_bytes, backlog_pkts); missing pieces are None.
    The counters are those of the root qdisc (the first block)."""
    out = dict(kind=None, sent_bytes=None, sent_pkts=None, dropped=None, overlimits=None,
               requeues=None, backlog_bytes=None, backlog_pkts=None)
    m = re.search(r"^qdisc (\S+)", text, re.M)
    if m:
        out["kind"] = m.group(1)
    m = re.search(r"Sent (\d+) bytes (\d+) pkt \(dropped (\d+), overlimits (\d+) requeues (\d+)\)", text)
    if m:
        out.update(zip(("sent_bytes", "sent_pkts", "dropped", "overlimits", "requeues"), map(int, m.groups())))
    m = re.search(r"backlog (\d+)b (\d+)p", text)
    if m:
        out["backlog_bytes"], out["backlog_pkts"] = int(m.group(1)), int(m.group(2))
    return out


class NetLabError(RuntimeError):
    pass


def _run(argv, check=True):
    r = subprocess.run(argv, capture_output=True, text=True)
    if check and r.returncode != 0:
        raise NetLabError(f"{' '.join(argv)} -> rc {r.returncode}: {(r.stderr or r.stdout).strip()}")
    return r


def in_gen(argv):
    """Command prefix that runs `argv` inside the generator namespace."""
    return ["ip", "netns", "exec", NS] + list(argv)


class NetLab:
    def __init__(self):
        self.warnings = []
        self.qdiscs = {}

    def setup(self):
        _run(["mount", "-t", "tmpfs", "none", "/run"])
        _run(["mkdir", "-p", "/run/netns"])
        _run(["ip", "netns", "add", NS])
        _run(["ip", "link", "add", NODE_DEV, "mtu", str(MTU), "type", "veth",
              "peer", "name", GEN_DEV, "mtu", str(MTU), "netns", NS])
        _run(["ip", "addr", "add", f"{NODE_IP}/24", "dev", NODE_DEV])
        _run(in_gen(["ip", "addr", "add", f"{GEN_IP}/24", "dev", GEN_DEV]))
        _run(["ip", "link", "set", "lo", "up"])
        _run(["ip", "link", "set", NODE_DEV, "up"])
        _run(in_gen(["ip", "link", "set", "lo", "up"]))
        _run(in_gen(["ip", "link", "set", GEN_DEV, "up"]))
        for dev, pre in ((NODE_DEV, []), (GEN_DEV, in_gen([]))):
            r = _run(pre + ["ethtool", "-K", dev, "tso", "off", "gso", "off", "gro", "off"], check=False)
            if r.returncode != 0:
                self.warnings.append(f"ethtool -K {dev}: {(r.stderr or r.stdout).strip()}")
        return self.info()

    def _tc(self, dev, pre, args):
        # Always delete first: `replace` on an existing netem keeps its counters, and
        # the per-cell drop statistics (qdisc_stats) must start from zero.
        _run(pre + ["tc", "qdisc", "del", "dev", dev, "root"], check=False)
        if args is not None:
            _run(pre + ["tc", "qdisc", "replace", "dev", dev, "root", "netem"] + args)

    def configure_netem(self, node_delay_ms, gen_delay_ms, node_rate_mbit=None, limit_pkts=None):
        """One-way delays per end (RTT = sum); the rate applies to veth-node egress
        (node -> generator) only. Delay 0 and no rate: default qdisc."""
        def args(delay, rate):
            if not delay and not rate:
                return None
            a = ["delay", f"{delay:g}ms"] if delay else []
            if rate:
                a += ["rate", f"{rate:g}mbit"]
            if limit_pkts:
                a += ["limit", str(int(limit_pkts))]
            return a
        self._tc(NODE_DEV, [], args(node_delay_ms, node_rate_mbit))
        self._tc(GEN_DEV, in_gen([]), args(gen_delay_ms, None))
        self.qdiscs = self.show_qdiscs()
        return self.qdiscs

    def show_qdiscs(self):
        return {NODE_DEV: _run(["tc", "qdisc", "show", "dev", NODE_DEV]).stdout.strip(),
                GEN_DEV: _run(in_gen(["tc", "qdisc", "show", "dev", GEN_DEV])).stdout.strip()}

    def qdisc_stats(self):
        """`tc -s qdisc show` of both ends, parsed (parse_tc_stats) plus the raw text.
        Call after a cell and before the next configure_netem (which resets them).
        A failure is returned as {"error": ...} per end instead of raising."""
        out = {}
        for dev, pre in ((NODE_DEV, []), (GEN_DEV, in_gen([]))):
            r = _run(pre + ["tc", "-s", "qdisc", "show", "dev", dev], check=False)
            if r.returncode != 0:
                out[dev] = dict(error=(r.stderr or r.stdout).strip())
            else:
                out[dev] = dict(parse_tc_stats(r.stdout), raw=r.stdout.strip())
        return out

    def info(self):
        return dict(namespace=NS, node=f"{NODE_DEV} {NODE_IP}/24", gen=f"{GEN_DEV} {GEN_IP}/24",
                    mtu=MTU, warnings=list(self.warnings))
