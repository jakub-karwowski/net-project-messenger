"""Unprivileged network lab for the S6 (WAN) scenarios. Standard library only.

Must run inside `unshare -rnm --propagation private` (run_matrix.py arranges
that): the process then owns a fresh network + mount namespace in which it is
"root", so no sudo is involved and everything vanishes when it exits.

Topology (all addresses /24, MTU 1500, TSO/GSO/GRO off so netem sees real packets):

    [this netns: node]  veth-node 10.200.1.1  <---->  veth-gen 10.200.1.2  [ns_p2p_gen: stressgen]

setup() is called once; configure_netem() is called per cell. The `ip netns`
registry needs a writable /run, hence the private tmpfs mounted on it.
"""
import subprocess

NS = "ns_p2p_gen"
NODE_DEV, GEN_DEV = "veth-node", "veth-gen"
NODE_IP, GEN_IP = "10.200.1.1", "10.200.1.2"
MTU = 1500


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
        if args is None:   # back to the default qdisc
            _run(pre + ["tc", "qdisc", "del", "dev", dev, "root"], check=False)
        else:
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

    def info(self):
        return dict(namespace=NS, node=f"{NODE_DEV} {NODE_IP}/24", gen=f"{GEN_DEV} {GEN_IP}/24",
                    mtu=MTU, warnings=list(self.warnings))
