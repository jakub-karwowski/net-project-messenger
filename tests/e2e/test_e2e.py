"""End-to-end tests: real `app --headless` processes talking over loopback.

    APP_BIN=build/app E2E_IO=threads,pool-sender,poll,epoll,poll-duplex,epoll-duplex,poll-split,epoll-split python3 -m unittest discover tests/e2e

E2E_IO is a comma-separated list of --io modes; every scenario runs once per mode.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tools"))
from node import Node, NodeError, free_port  # noqa: E402

APP_BIN = os.environ.get("APP_BIN", "build/app")
IO_MODES = [m.strip() for m in os.environ.get("E2E_IO", "threads").split(",") if m.strip()]


class E2EBase:
    io = "threads"

    def setUp(self):
        self.nodes = []

    def tearDown(self):
        for n in self.nodes:
            try:
                n.stop()
            except NodeError:
                n.kill()

    def node(self, **kw):
        n = Node(APP_BIN, io=self.io, **kw)
        self.nodes.append(n)
        return n

    def started(self, **kw):
        n = self.node(**kw)
        n.start()
        return n

    def connect(self, a, b):
        """A dials B; returns (conv on A, conv on B)."""
        rid = a.cmd("/connect 127.0.0.1 %d" % b.port)
        dialed = a.expect("peer", what="dialed", timeout=10)
        connected = b.expect("peer", what="connected", timeout=10)
        return dialed["conversation"], connected["conversation"]

    def snapshot(self, n, conv):
        """Switch to `conv` and return its transcript as seen by the render.

        /switch to the current conversation answers only "ok" (no snapshot), so the
        transcript is rebuilt from the event history: a snapshot resets it, a live
        messages event appends. /sync makes sure everything has arrived."""
        n.cmd("/switch %d" % conv)
        n.expect("sync", re=n.cmd("/sync"), timeout=10)
        items = []
        for ev in n.events():
            if ev["event"] == "messages" and ev["conversation"] == conv:
                items = list(ev["items"]) if ev["snapshot"] else items + ev["items"]
        return items


class Scenarios(E2EBase):
    def test_ready_and_clean_shutdown(self):
        n = self.node()
        ready = n.start()
        self.assertEqual(ready["io"], self.io)
        self.assertEqual(ready["listen"], "127.0.0.1:%d" % n.port)
        self.assertEqual(n.stop(), 0)
        self.assertEqual(n.events()[-1]["event"], "bye")
        seqs = [e["seq"] for e in n.events()]
        self.assertEqual(seqs, list(range(1, len(seqs) + 1)))

    def test_stdout_is_only_jsonl(self):
        n = self.started()
        n.cmd("/current")
        n.stop()
        self.assertFalse([e for e in n.events() if e["event"] == "_garbage"])

    def test_connect_events(self):
        a, b = self.started(), self.started()
        ca, cb = self.connect(a, b)
        self.assertGreaterEqual(ca, 1)
        self.assertGreaterEqual(cb, 1)

    def test_message_roundtrip(self):
        a, b = self.started(), self.started()
        ca, cb = self.connect(a, b)
        # B has not switched: the message shows up as a notification.
        a.cmd("/send %d hello from a" % ca)
        b.expect("notification", conversation=cb, timeout=10)
        items = self.snapshot(b, cb)
        self.assertEqual([i["text"] for i in items], ["hello from a"])
        self.assertTrue(items[0]["from"].startswith("peer#"))
        # Reply B -> A.
        b.cmd("/send %d hi back" % cb)
        # A dialed, so ca is A's current conversation: the reply streams in directly.
        a.expect("messages", conversation=ca, snapshot=False, timeout=10,
                 items=[{"from": "peer#1", "text": "hi back"}])
        items = self.snapshot(a, ca)
        self.assertEqual([(i["from"] == "me", i["text"]) for i in items],
                         [(True, "hello from a"), (False, "hi back")])

    def test_conversations_listing(self):
        a, b = self.started(), self.started()
        ca, cb = self.connect(a, b)
        a.cmd("/send %d ping" % ca)
        b.expect("notification", conversation=cb, timeout=10)
        rid = b.cmd("/conversations")
        ev = b.expect("conversations", re=rid)
        self.assertEqual(len(ev["items"]), 1)
        self.assertEqual(ev["items"][0]["conversation"], cb)
        self.assertEqual(ev["items"][0]["unread"], 1)
        self.assertEqual(len(ev["items"][0]["peers"]), 1)

    def test_peer_disconnected_and_reconnect(self):
        a, b = self.started(), self.started()
        ca, cb = self.connect(a, b)
        a.cmd("/send %d before" % ca)
        b.expect("notification", conversation=cb, timeout=10)
        port = b.port
        self.assertEqual(b.stop(), 0)
        a.expect("peer", what="disconnected", conversation=ca, timeout=10)
        # B restarts on the same port; A reconnects into the same conversation.
        b2 = self.node(port=port)
        b2.start()
        rid = a.cmd("/reconnect %d" % ca)
        a.expect("peer", what="dialed", conversation=ca, re=rid, timeout=10)
        a.cmd("/send %d after" % ca)
        items = self.snapshot(a, ca)
        self.assertEqual([i["text"] for i in items], ["before", "after"])

    def test_busy_port(self):
        a = self.started()
        b = self.node(port=a.port)
        ready = b.start_allow_error()
        self.assertIn("error", ready)
        # Still alive and usable: it can dial A.
        rid = b.cmd("/connect 127.0.0.1 %d" % a.port)
        b.expect("peer", what="dialed", timeout=10)
        self.assertEqual(b.stop(), 0)

    def test_echo_mode(self):
        a = self.started(echo=True)
        b = self.started()
        cb, ca = self.connect(b, a)
        n = 20
        for i in range(n):
            b.cmd("/send %d m%d" % (cb, i))
        # Echoes come back asynchronously: poll the snapshot until all arrived.
        import time
        deadline = time.monotonic() + 10
        while True:
            items = self.snapshot(b, cb)
            echoed = [i["text"] for i in items if i["from"] != "me"]
            if len(echoed) >= n or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        self.assertEqual(sorted(echoed), sorted("m%d" % i for i in range(n)))
        # The echoing node stored nothing.
        rid = a.cmd("/conversations")
        ev = a.expect("conversations", re=rid)
        self.assertTrue(all(c["unread"] == 0 for c in ev["items"]))

    def test_stats_counts_frames(self):
        import time
        a, b = self.started(), self.started()
        ca, cb = self.connect(a, b)
        for i in range(5):
            a.cmd("/send %d m%d" % (ca, i))
        deadline = time.monotonic() + 10
        while True:
            rid = b.cmd("/stats")
            m = b.expect("stats", re=rid, timeout=10)["metrics"]
            if m.get("frames_in", 0) >= 6 or time.monotonic() > deadline:
                break
            time.sleep(0.05)
        self.assertGreaterEqual(m["frames_in"], 6)  # ConnectInfo + 5 messages
        self.assertGreater(m["bytes_in"], 0)
        self.assertGreaterEqual(m["accepts"], 1)
        self.assertGreater(m["threads"], 0)
        self.assertIn("t_ns", m)
        rid = a.cmd("/stats")
        self.assertGreaterEqual(a.expect("stats", re=rid)["metrics"]["frames_out"], 5)

    def test_metrics_file(self):
        import json
        import tempfile
        import time
        path = tempfile.mktemp(suffix=".jsonl")
        n = self.started(extra_args=["--metrics=" + path])
        time.sleep(1.3)
        n.stop()
        with open(path) as f:
            rows = [json.loads(line) for line in f if line.strip()]
        os.unlink(path)
        self.assertGreaterEqual(len(rows), 2)
        self.assertLess(rows[0]["t_ns"], rows[-1]["t_ns"])
        for k in ("frames_in", "utime_us", "nvcsw", "maxrss_kb", "threads", "fds"):
            self.assertIn(k, rows[-1])

    def test_sync_barrier_orders_replies(self):
        n = self.started()
        r1 = n.cmd("/current")
        r2 = n.cmd("/sync")
        n.expect("sync", re=r2)
        ev = n.events()
        idx = {e.get("re"): i for i, e in enumerate(ev) if e.get("re")}
        self.assertIn(r1, idx)
        self.assertLess(idx[r1], idx[r2])


def _make(mode):
    return type("Scenarios_" + mode.replace("-", "_"), (Scenarios, unittest.TestCase), {"io": mode})


for _mode in IO_MODES:
    _cls = _make(_mode)
    globals()[_cls.__name__] = _cls
del Scenarios, _cls, _mode  # only the per-mode subclasses are collected

if __name__ == "__main__":
    unittest.main()
