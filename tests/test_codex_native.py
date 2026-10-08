"""Native queue tests. Real Codex uses only a loopback synthetic model server."""
import http.server
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest

from kilix_ralph.codex import Codex, bind
from kilix_ralph.native import CodexSocket
from kilix_ralph.store import Error, Store
from kilix_ralph.worker import SCHEMA, Worker


class Model(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        def event(kind, **value):
            self.wfile.write(("event: " + kind + "\ndata: " + json.dumps({"type": kind, **value}) + "\n\n").encode())
            self.wfile.flush()
        try:
            event("response.created", response={"id": "resp_test", "object": "response", "status": "in_progress", "output": []})
            self.server.started.set()
            self.server.release.wait(10)
            event("response.completed", response={"id": "resp_test", "object": "response", "status": "completed",
                "output": [], "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}})
        except (BrokenPipeError, ConnectionResetError):
            pass


@unittest.skipUnless(shutil.which("codex"), "Codex app-server binary is required for native integration")
class NativeCodexTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="rnc-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.http = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Model)
        self.http.started, self.http.release = threading.Event(), threading.Event()
        self.http.release.set()
        threading.Thread(target=self.http.serve_forever, daemon=True).start()
        self.addCleanup(self.http.server_close)
        self.addCleanup(self.http.shutdown)
        self.addCleanup(self.http.release.set)
        home = self.root / "codex"
        home.mkdir()
        self.socket = self.root / "api.sock"
        env = {"HOME": str(self.root), "CODEX_HOME": str(home), "PATH": os.environ["PATH"]}
        argv = [os.environ.get("RALPH_TEST_CODEX", shutil.which("codex")), "-c", 'model_provider="mock"',
                "-c", 'model_providers.mock.name="mock"',
                "-c", f'model_providers.mock.base_url="http://127.0.0.1:{self.http.server_port}/v1"',
                "-c", 'model_providers.mock.wire_api="responses"',
                "app-server", "--listen", "unix://" + str(self.socket)]
        self.process = subprocess.Popen(argv, cwd=self.root, env=env, stdin=subprocess.DEVNULL,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.addCleanup(self.stop)
        deadline = time.monotonic() + 10
        while not self.socket.exists():
            if self.process.poll() is not None or time.monotonic() >= deadline:
                self.fail("isolated Codex server did not start")
            time.sleep(.02)
        self.client = CodexSocket(self.socket)
        self.addCleanup(self.client.close)
        self.client.call("initialize", {"clientInfo": {"name": "ralph-test", "version": "0.2.0"},
                                       "capabilities": {"experimentalApi": True}})
        self.client.send({"method": "initialized"})
        self.thread = self.client.call("thread/start", {"model": "test-model", "cwd": str(self.root),
            "approvalPolicy": "never", "sandbox": "read-only"})["thread"]["id"]
        self.store = Store(self.root / "queue")
        self.addCleanup(self.store.close)
        self.target = "codex:" + self.thread
        bind(self.store, self.target, self.thread, self.socket)

    def stop(self):
        self.http.release.set()
        if self.process.poll() is None:
            self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()

    def send(self, mid="native-message"):
        return self.store.enqueue(self.target, "synthetic receiver message", "session:sender", mid)

    def drain(self, mid):
        worker = Worker(self.store)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            worker.once()
            msg = self.store.message(mid)
            if msg["status"] == "completed":
                return msg
            time.sleep(.05)
        self.fail(self.store.message(mid))

    def test_real_worker_delivers_and_matches_completed_turn(self):
        msg = self.send()
        done = self.drain(msg["id"])
        self.assertEqual(done["receipt"]["evidence"]["thread"], self.thread)
        self.assertEqual(done["receipt"]["evidence"]["turn_status"], "completed")

    def test_busy_session_receives_no_steering(self):
        self.http.release.clear()
        self.client.call("turn/start", {"threadId": self.thread, "input": [{"type": "text", "text": "human active turn"}]})
        self.assertTrue(self.http.started.wait(5))
        msg = self.send()
        Worker(self.store).once()
        self.assertEqual(self.store.message(msg["id"])["status"], "queued")
        self.assertEqual(self.client.call("thread/queue/list", {"threadId": self.thread})["data"], [])
        self.http.release.set()
        self.drain(msg["id"])

    def test_human_turn_wins_race_native_queue_waits(self):
        msg = self.send()
        self.http.release.clear()
        adapter = Codex(self.store.root, self.socket, self.thread, self.target)
        self.addCleanup(adapter.close)
        original = adapter.client.call
        human_turn = []
        def raced(method, params):
            if method == "thread/queue/add":
                human_turn.append(self.client.call("turn/start", {"threadId": self.thread,
                    "input": [{"type": "text", "text": "human won boundary"}]})["turn"]["id"])
                self.assertTrue(self.http.started.wait(5))
            return original(method, params)
        adapter.client.call = raced
        result = adapter.handle({"schema": SCHEMA, "target": self.target, "operation": "submit",
            "operation_id": msg["id"], "digest": msg["digest"], "text": msg["text"], "sender": msg["sender"], "idle_token": "old"})
        self.assertEqual(result["status"], "provider_queued")
        pending = self.client.call("thread/queue/list", {"threadId": self.thread})["data"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(adapter.reconcile(msg["id"], msg["digest"])["status"], "provider_queued")
        self.http.release.set()
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            result = adapter.reconcile(msg["id"], msg["digest"])
            if result["status"] == "completed":
                break
            time.sleep(.05)
        self.assertEqual(result["status"], "completed")
        self.assertNotEqual(result["evidence"]["turn"], human_turn[0])

    def test_response_lost_is_reconciled_without_second_add(self):
        msg = self.send()
        adapter = Codex(self.store.root, self.socket, self.thread, self.target)
        self.addCleanup(adapter.close)
        original = adapter.client.call
        def lost(method, params):
            response = original(method, params)
            if method == "thread/queue/add":
                raise Error("injected lost response after acceptance")
            return response
        adapter.client.call = lost
        request = {"schema": SCHEMA, "target": self.target, "operation": "submit",
            "operation_id": msg["id"], "digest": msg["digest"], "text": msg["text"], "sender": msg["sender"]}
        with self.assertRaises(Error):
            adapter.handle(request)
        self.store.claim(msg["id"])
        self.drain(msg["id"])
        turns = self.client.call("thread/turns/list", {"threadId": self.thread, "itemsView": "full"})["data"]
        matches = [item for turn in turns for item in turn["items"] if item.get("clientId") == "kilix-ralph:" + msg["id"]]
        self.assertEqual(len(matches), 1)

    def test_wrong_digest_and_target_refused(self):
        msg = self.send()
        self.drain(msg["id"])
        adapter = Codex(self.store.root, self.socket, self.thread, self.target)
        self.addCleanup(adapter.close)
        with self.assertRaises(Error):
            adapter.reconcile(msg["id"], "0" * 64)
        with self.assertRaises(Error):
            adapter.handle({"schema": SCHEMA, "target": "other", "operation": "observe"})
