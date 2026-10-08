import ctypes
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from kilix_ralph.claude import ClaudeBridge
from kilix_ralph.store import Store
from kilix_ralph.worker import SCHEMA


FIXTURE = Path(__file__).with_name("claude_cleanup_fixture.py")
STREAM = Path(__file__).with_name("claude_fixture.py")


class CleanupTests(unittest.TestCase):
    def setUp(self):
        # Adopt only this runner's orphaned fixture children so exact waitpid
        # cleanup leaves no zombies. Restore the original setting afterward.
        self.prctl = ctypes.CDLL(None, use_errno=True).prctl
        previous = ctypes.c_int()
        self.assertEqual(self.prctl(37, ctypes.byref(previous), 0, 0, 0), 0)
        self.assertEqual(self.prctl(36, 1, 0, 0, 0), 0)
        self.addCleanup(self.prctl, 36, previous.value, 0, 0, 0)
        self.tmp = tempfile.TemporaryDirectory(prefix="rcc-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "queue")
        self.addCleanup(self.store.close)
        self.record = self.root / "child.json"
        self.child = self.child_fd = None
        self.child_reaped = False
        self.addCleanup(self.cleanup_child)

    def bridge(self, argv, target="claude:cleanup"):
        bridge = ClaudeBridge(self.store, target, self.root, argv=argv)
        self.addCleanup(bridge.close)
        return bridge

    def capture_child(self, bridge):
        if self.child_fd is None and self.record.exists():
            record = json.loads(self.record.read_text())
            self.assertEqual(record["leader"], bridge.process.pid)
            self.assertEqual(record["group"], bridge.process.pid)
            self.assertEqual(record["session"], bridge.process.pid)
            self.child = record["pid"]
            self.child_fd = os.pidfd_open(self.child)
        return self.child_fd is not None

    def cleanup_child(self):
        if self.child_fd is not None:
            try:
                signal.pidfd_send_signal(self.child_fd, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                if not self.child_reaped:
                    os.waitpid(self.child, 0)
            except ChildProcessError:
                pass
            os.close(self.child_fd)
            self.child_fd = None

    def assert_child_killed(self):
        self.assertTrue(select.select([self.child_fd], [], [], 1)[0],
                        "serve returned with the owned descendant still running")
        pid, status = os.waitpid(self.child, 0)
        self.child_reaped = True
        self.assertEqual(pid, self.child)
        self.assertTrue(os.WIFSIGNALED(status))
        self.assertEqual(os.WTERMSIG(status), signal.SIGKILL)

    def descendant_shutdown(self, early):
        mode = "early-child" if early else "term-child"
        bridge = self.bridge([sys.executable, str(FIXTURE), mode, str(self.record)])
        deadline = time.monotonic() + 5
        blocked_observations = 0
        signals = []
        killpg = os.killpg

        def stopping():
            nonlocal blocked_observations
            self.assertLess(time.monotonic(), deadline, "fixture did not become ready")
            if not self.capture_child(bridge):
                return False
            if early:
                if bridge.blocked:
                    blocked_observations += 1
                # Run another whole loop after output EOF: a poll() mutation
                # must reap the early-exited leader before shutdown begins.
                return blocked_observations >= 2
            return True

        def owned_signal(group, sig):
            self.assertEqual(group, bridge.process.pid)
            self.assertIsNone(bridge.process.returncode)
            exited = os.waitid(os.P_PID, group, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if early:
                self.assertIsNotNone(exited)
                self.assertEqual(exited.si_code, os.CLD_EXITED)
                self.assertEqual(exited.si_status, 0)
            elif sig == signal.SIGKILL:
                self.assertIsNotNone(exited, "leader must have exited on TERM")
                self.assertEqual(exited.si_status, signal.SIGTERM)
            signals.append((sig, time.monotonic()))
            killpg(group, sig)

        with patch("kilix_ralph.claude.os.killpg", side_effect=owned_signal):
            bridge.serve(stopping=stopping)
        self.assert_child_killed()
        self.assertEqual([sig for sig, _ in signals], [signal.SIGTERM, signal.SIGKILL])
        self.assertGreaterEqual(signals[1][1] - signals[0][1], 2.8)
        self.assertIsNotNone(bridge.process.returncode)
        self.assertTrue(bridge.process.stdin.closed)
        self.assertTrue(bridge.process.stdout.closed)
        self.assertFalse(bridge.endpoint.exists())

    def test_term_resistant_child_is_killed_after_leader_exits_on_term(self):
        self.descendant_shutdown(early=False)

    def test_early_leader_exit_keeps_group_reserved_through_final_signal(self):
        self.descendant_shutdown(early=True)

    def test_cleanup_exception_still_kills_child_closes_streams_and_preserves_receipt(self):
        bridge = self.bridge([sys.executable, str(FIXTURE), "term-child", str(self.record)])
        request = {"schema": SCHEMA, "target": bridge.target, "operation": "submit",
            "operation_id": "pending", "digest": "a" * 64, "sender": "session:sender",
            "text": "hello", "idle_token": bridge.token}
        bridge.handle(request)
        before = bridge.ledger.get(bridge.target, "pending")
        deadline = time.monotonic() + 5

        def stopping():
            self.assertLess(time.monotonic(), deadline, "fixture did not become ready")
            return self.capture_child(bridge)

        with patch("kilix_ralph.claude.time.sleep", side_effect=RuntimeError("injected grace failure")):
            with self.assertRaisesRegex(RuntimeError, "injected grace failure"):
                bridge.serve(stopping=stopping)
        self.assert_child_killed()
        self.assertEqual(bridge.ledger.get(bridge.target, "pending"), before)
        self.assertIsNotNone(bridge.process.returncode)
        self.assertTrue(bridge.process.stdin.closed)
        self.assertTrue(bridge.process.stdout.closed)
        self.assertFalse(bridge.endpoint.exists())

    def test_normal_shutdown_preserves_empty_pending_and_completed_receipts(self):
        for stage in ("empty", "provider_queued", "completed"):
            with self.subTest(stage=stage):
                received = self.root / (stage + ".json")
                argv = ([sys.executable, str(FIXTURE), "pending", str(received)]
                        if stage == "provider_queued" else [sys.executable, str(STREAM)])
                bridge = self.bridge(argv, target="claude:" + stage)
                request = {"schema": SCHEMA, "target": bridge.target, "operation": "submit",
                    "operation_id": "message", "digest": "a" * 64, "sender": "session:sender",
                    "text": "hello", "idle_token": bridge.token}
                before = None
                deadline = time.monotonic() + 5

                def receipts():
                    return [tuple(row) for row in bridge.ledger.store.db.execute(
                        "SELECT * FROM receipts WHERE target=? ORDER BY id", (bridge.target,))]

                def ready(_):
                    if stage != "empty":
                        self.assertEqual(bridge.handle(request)["status"], "provider_queued")

                def stopping():
                    nonlocal before
                    self.assertLess(time.monotonic(), deadline, "stream fixture did not finish")
                    row = bridge.ledger.get(bridge.target, "message")
                    done = stage == "empty" or (row and row["receipt"]["status"] == stage)
                    if stage == "provider_queued":
                        done = done and received.exists() and received.stat().st_size > 0
                    if done:
                        before = receipts()
                    return bool(done)

                bridge.serve(ready=ready, stopping=stopping)
                self.assertEqual(receipts(), before)
                self.assertEqual(len(before), 0 if stage == "empty" else 1)
                self.assertIsNotNone(bridge.process.returncode)
                self.assertTrue(bridge.process.stdin.closed)
                self.assertTrue(bridge.process.stdout.closed)
                self.assertFalse(bridge.endpoint.exists())
                if stage == "completed":
                    self.assertEqual(bridge.ledger.get(bridge.target, "message")["receipt"]["evidence"]["result"],
                                     "synthetic reply")
                restored = self.bridge(argv, target=bridge.target)
                self.assertEqual(restored.ledger.get(bridge.target, "message"),
                                 bridge.ledger.get(bridge.target, "message"))
                self.assertEqual(bool(restored.blocked), stage == "provider_queued")
                if stage != "empty":
                    request["operation"] = "reconcile"
                    self.assertEqual(restored.handle(request)["status"],
                                     "uncertain" if stage == "provider_queued" else "completed")
                    self.assertEqual(restored.input, b"")

    def test_reaped_leader_cannot_signal_a_replacement_group(self):
        for external_wait in ("popen", "waitpid"):
            with self.subTest(external_wait=external_wait):
                # Exact kernel PID reuse is nondeterministic. Route any unsafe
                # stale-PGID signal to this real, separately owned decoy group.
                decoy = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(15)"],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                    start_new_session=True)
                decoy_fd = os.pidfd_open(decoy.pid)
                try:
                    bridge = self.bridge([sys.executable, "-c", "pass"],
                                         target="claude:reaped-" + external_wait)
                    signals = []
                    killpg = os.killpg

                    def ready(_):
                        if external_wait == "popen":
                            bridge.process.wait(timeout=5)
                        else:
                            deadline = time.monotonic() + 5
                            while os.waitid(os.P_PID, bridge.process.pid,
                                    os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                                self.assertLess(time.monotonic(), deadline)
                                time.sleep(.01)
                            os.waitpid(bridge.process.pid, 0)
                            self.assertIsNone(bridge.process.returncode)

                    def replacement_signal(group, sig):
                        self.assertEqual(group, bridge.process.pid)
                        signals.append(sig)
                        killpg(decoy.pid, sig)

                    with patch("kilix_ralph.claude.os.killpg", side_effect=replacement_signal):
                        bridge.serve(ready=ready, stopping=lambda: True)
                    self.assertFalse(select.select([decoy_fd], [], [], 0)[0],
                                     "replacement group was terminated by a stale PGID signal")
                    self.assertEqual(signals, [], "stale PGID could target a replacement group")
                    self.assertIsNone(os.waitid(os.P_PID, decoy.pid,
                        os.WEXITED | os.WNOHANG | os.WNOWAIT))
                finally:
                    try:
                        signal.pidfd_send_signal(decoy_fd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    decoy.wait()
                    os.close(decoy_fd)
