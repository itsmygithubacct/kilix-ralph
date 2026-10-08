import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from kilix_ralph.claude import ClaudeBridge, call_bridge
from kilix_ralph.store import Error, Store
from kilix_ralph.worker import Worker, SCHEMA

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).with_name("claude_fixture.py")


class ClaudeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="rcb-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.store = Store(self.root / "queue")
        self.addCleanup(self.store.close)
        self.bridge = ClaudeBridge(self.store, "claude:owned", self.root, argv=[sys.executable, str(FIXTURE)])
        self.addCleanup(self.bridge.close)

    def request(self, mid="one", token=None):
        return {"schema": SCHEMA, "target": self.bridge.target, "operation": "submit",
                "operation_id": mid, "digest": "a" * 64, "sender": "session:sender", "text": "hello",
                "idle_token": token or self.bridge.token}

    def acknowledge(self, request):
        old = self.bridge.ledger.get(self.bridge.target, request["operation_id"])
        self.bridge.event({"type": "user", "session_id": self.bridge.session,
            "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.bridge.target + ":" + request["operation_id"])),
            "message": {"content": old["payload"]}})

    def test_busy_and_stale_tokens_cannot_inject(self):
        one = self.request()
        self.assertEqual(self.bridge.handle(one)["status"], "provider_queued")
        size = len(self.bridge.input)
        two = self.request("two")
        self.assertEqual(self.bridge.handle(two)["status"], "not_submitted")
        self.assertEqual(len(self.bridge.input), size)
        self.acknowledge(one)
        self.bridge.event({"type": "result", "subtype": "success", "session_id": self.bridge.session})
        two["idle_token"] = one["idle_token"]
        self.assertEqual(self.bridge.handle(two)["status"], "not_submitted")
        two["idle_token"] = self.bridge.token
        self.assertEqual(self.bridge.handle(two)["status"], "provider_queued")

    def test_result_requires_exact_session_and_ack(self):
        request = self.request()
        self.bridge.handle(request)
        with self.assertRaises(Error):
            self.bridge.event({"type": "result", "session_id": self.bridge.session})
        with self.assertRaises(Error):
            self.bridge.event({"type": "user", "session_id": "wrong"})
        self.assertIsNotNone(self.bridge.active)

    def test_restart_preserves_uncertainty_and_never_resends(self):
        request = self.request()
        self.bridge.handle(request)
        restored = ClaudeBridge(self.store, "claude:owned", self.root, argv=[sys.executable, str(FIXTURE)])
        try:
            self.assertIsNotNone(restored.blocked)
            self.assertEqual(restored.handle(request)["status"], "uncertain")
            self.assertEqual(restored.input, b"")
        finally:
            restored.close()

    def test_payload_collision_refused(self):
        request = self.request()
        self.bridge.handle(request)
        request["text"] = "different"
        with self.assertRaises(Error):
            self.bridge.handle(request)

    def test_real_bridge_worker_roundtrip_and_fifo(self):
        code = """
import json,signal,sys
from kilix_ralph import Store
from kilix_ralph.claude import ClaudeBridge
stopped=False
def stop(*args):
 global stopped
 stopped=True
signal.signal(signal.SIGTERM,stop)
store=Store(sys.argv[1])
bridge=ClaudeBridge(store,'claude:owned',sys.argv[2],argv=[sys.executable,sys.argv[3]])
try:bridge.serve(ready=lambda v:print(json.dumps(v),flush=True),stopping=lambda:stopped)
finally:bridge.close();store.close()
"""
        process = subprocess.Popen([sys.executable, "-c", code, str(self.store.root), str(self.root), str(FIXTURE)],
                                   cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            info = json.loads(process.stdout.readline())
            first = self.store.enqueue("claude:owned", "first", "sender")
            second = self.store.enqueue("claude:owned", "second", "sender")
            worker = Worker(self.store)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                worker.once()
                if self.store.message(second["id"])["status"] == "completed":
                    break
                time.sleep(.03)
            self.assertEqual(self.store.message(first["id"])["status"], "completed")
            self.assertEqual(self.store.message(second["id"])["status"], "completed")
            self.assertEqual(self.store.message(second["id"])["receipt"]["evidence"]["session"], info["session"])
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill();process.wait()
            process.stdout.close();process.stderr.close()
