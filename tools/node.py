"""Drive one headless chat node (`app --headless`) over its stdin/stdout JSONL.

Stdlib only. Typical use:

    n = Node("build/app", io="epoll")  # io: threads|pool-sender|poll|epoll|poll-duplex|epoll-duplex|poll-split|epoll-split; n.start()
    rid = n.cmd("/connect 127.0.0.1 9001")
    ev = n.expect("peer", what="dialed")
    n.stop()
"""
import json
import os
import re as _re
import socket
import subprocess
import tempfile
import threading
import time


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class NodeError(Exception):
    pass


class Node:
    def __init__(self, binary, io="threads", port=None, echo=False, extra_args=()):
        self.binary = binary
        self.io = io
        self.port = port if port is not None else free_port()
        self.echo = echo
        self.extra_args = list(extra_args)
        self.proc = None
        self.log_path = None
        self.ready = None
        self.exit_code = None
        self._cv = threading.Condition()
        self._events = []      # full history, in arrival order
        self._pending = []     # not yet consumed by expect()
        self._next_req = 1
        self._eof = False
        self._reader = None
        self.stderr_text = ""

    # --- lifecycle -------------------------------------------------------
    def start(self, timeout=5):
        """Spawn the node and wait for "ready". Raises NodeError if ready carries an
        error (the node keeps running; see start_allow_error)."""
        fd, self.log_path = tempfile.mkstemp(prefix="node-", suffix=".log")
        os.close(fd)
        args = [self.binary, "--headless", "--io=" + self.io,
                "--address=127.0.0.1", "--port=%d" % self.port,
                "--log=" + self.log_path]
        if self.echo:
            args.append("--echo")
        args += self.extra_args
        self.proc = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True, bufsize=1)
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        threading.Thread(target=self._err_loop, daemon=True).start()
        ev = self.expect("ready", timeout=timeout)
        self.ready = ev
        if "error" in ev:
            raise NodeError("node failed to listen: %s" % ev["error"])
        return ev

    def start_allow_error(self, timeout=5):
        """Like start() but returns the ready event even when it carries an error."""
        try:
            return self.start(timeout)
        except NodeError:
            if self.ready is None:
                raise
            return self.ready

    def stop(self, timeout=5):
        """Send /quit, wait for "bye" and exit; returns the exit code (kills on timeout)."""
        if self.proc is None:
            return self.exit_code
        if self.proc.poll() is None:
            try:
                self.proc.stdin.write("/quit\n")
                self.proc.stdin.flush()
            except (BrokenPipeError, ValueError, OSError):
                pass
            try:
                self.expect("bye", timeout=timeout)
            except NodeError:
                pass
            try:
                self.proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
                self.exit_code = -9
                raise NodeError("node did not exit after /quit; killed")
        self.exit_code = self.proc.returncode
        try:
            self.proc.stdin.close()
        except OSError:
            pass
        if self._reader:
            self._reader.join(timeout=2)
        for f in (self.proc.stdout, self.proc.stderr):
            try:
                f.close()
            except (OSError, ValueError):
                pass
        return self.exit_code

    def kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

    # --- io --------------------------------------------------------------
    def _read_loop(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except ValueError:
                ev = {"event": "_garbage", "raw": line}
            with self._cv:
                self._events.append(ev)
                self._pending.append(ev)
                self._cv.notify_all()
        with self._cv:
            self._eof = True
            self._cv.notify_all()

    def _err_loop(self):
        for line in self.proc.stderr:
            self.stderr_text += line

    def send_raw(self, line):
        self.proc.stdin.write(line + "\n")
        self.proc.stdin.flush()

    def cmd(self, text):
        """Send a command with an auto '#rN ' prefix; returns the request id 'rN'."""
        rid = "r%d" % self._next_req
        self._next_req += 1
        self.send_raw("#%s %s" % (rid, text))
        return rid

    def events(self):
        """Every event received so far, in order (including consumed ones)."""
        with self._cv:
            return list(self._events)

    def expect(self, event=None, re=None, timeout=5, **fields):
        """Wait for an event matching all criteria; consume and return it.

        event: value of "event"; re: request id the reply must carry ("re");
        fields: exact match on other keys; text_re=<regex> searches "text".
        Non-matching events stay buffered for later expect() calls."""
        text_re = fields.pop("text_re", None)
        deadline = time.monotonic() + timeout

        def match(ev):
            if event is not None and ev.get("event") != event:
                return False
            if re is not None and ev.get("re") != re:
                return False
            if text_re is not None and not _re.search(text_re, str(ev.get("text", ""))):
                return False
            return all(ev.get(k) == v for k, v in fields.items())

        with self._cv:
            while True:
                for i, ev in enumerate(self._pending):
                    if match(ev):
                        del self._pending[i]
                        return ev
                left = deadline - time.monotonic()
                if self._eof or left <= 0:
                    raise NodeError("timeout waiting for event=%r re=%r %r; pending=%r"
                                    % (event, re, fields, self._pending[-8:]))
                self._cv.wait(left)

    def drain(self):
        """Return and clear all unconsumed events."""
        with self._cv:
            out, self._pending = self._pending, []
            return out

    def take_all(self, event=None, **fields):
        """Non-blocking: consume and return every buffered event matching event/fields."""
        with self._cv:
            hit = [e for e in self._pending
                   if (event is None or e.get("event") == event)
                   and all(e.get(k) == v for k, v in fields.items())]
            self._pending = [e for e in self._pending if e not in hit]
            return hit
