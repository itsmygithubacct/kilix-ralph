"""Native readiness vetoes must stop submission before durable provider custody."""
import tempfile
import unittest
from kilix_ralph.codex import Codex
from kilix_ralph.store import Store
from kilix_ralph.worker import SCHEMA


class Client:
    def __init__(self, direct_input, state="idle"):
        self.direct_input = direct_input
        self.state = state
        self.calls = []

    def call(self, method, params):
        self.calls.append(method)
        if method == "initialize":
            return {}
        if method == "thread/read":
            return {"thread": {"id": "thread", "status": {"type": self.state},
                               "canAcceptDirectInput": self.direct_input}}
        if method == "thread/queue/list":
            return {"data": [], "nextCursor": None}
        if method == "thread/queue/add":
            return {"queuedSubmission": {"id": "queue", **params}}
        raise AssertionError(method)

    def send(self, value):
        pass

    def close(self):
        pass


class AdmissionTests(unittest.TestCase):
    def test_explicit_provider_veto_prevents_queue_submission_and_intent(self):
        with tempfile.TemporaryDirectory() as root:
            store = Store(root)
            self.addCleanup(store.close)
            store.add_target("target")
            msg = store.enqueue("target", "hello", "sender", "message")
            client = Client(False)
            adapter = Codex(root, None, "thread", "target", client=client)
            self.addCleanup(adapter.close)
            self.assertNotEqual(adapter.observe()["state"], "idle")
            reply = adapter.handle({"schema": SCHEMA, "target": "target", "operation": "submit",
                "operation_id": msg["id"], "digest": msg["digest"], "text": msg["text"], "sender": msg["sender"]})
            self.assertEqual(reply["status"], "not_submitted")
            self.assertTrue(reply["no_effects"])
            self.assertNotIn("thread/queue/add", client.calls)
            self.assertIsNone(adapter.ledger.get("target", msg["id"]))

    def test_eligible_idle_provider_remains_idle(self):
        with tempfile.TemporaryDirectory() as root:
            adapter = Codex(root, None, "thread", "target", client=Client(True))
            self.addCleanup(adapter.close)
            self.assertEqual(adapter.observe()["state"], "idle")

    def test_non_idle_identity_states_survive_an_input_veto(self):
        for state in ("notLoaded", "active", "unknown"):
            with self.subTest(state=state), tempfile.TemporaryDirectory() as root:
                adapter = Codex(root, None, "thread", "target", client=Client(False, state))
                try:
                    self.assertEqual(adapter.observe()["state"], state)
                finally:
                    adapter.close()
