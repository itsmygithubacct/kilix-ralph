import concurrent.futures
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from kilix_ralph import Error, Store
from kilix_ralph.worker import Worker, rpc

ROOT = Path(__file__).resolve().parents[1]
FIXTURE = Path(__file__).with_name("adapter_fixture.py")


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name) / "state"
        self.now = 1000.
        self.store = Store(self.root, clock=lambda: self.now)
        self.addCleanup(self.store.close)
        self.store.add_target("codex:conversation:generation")
        self.target = "codex:conversation:generation"

    def send(self, text="hello", mid=None, ttl=None):
        return self.store.enqueue(self.target, text, "sender:session", mid, ttl)

    def loop(self, interval=10, limit=None):
        result = self.store.create_loop(self.target, "continue", "sender:session", interval, max_wakes=limit)
        self.store.loop_state(result["id"], "active")
        return result["id"]

    def cli(self, *args):
        return subprocess.run([sys.executable, "-m", "kilix_ralph", "--state-dir", str(self.root), *args],
                              cwd=ROOT, capture_output=True, text=True, timeout=15)

    def test_fifo_one_claim_until_completion(self):
        a, b = self.send(), self.send("second")
        claimed = self.store.receive(self.target)["message"]
        self.assertEqual(claimed["id"], a["id"])
        self.assertEqual(self.store.receive(self.target)["blocked_by"], a["id"])
        self.store.acknowledge(self.target, a["id"])
        self.assertIsNone(self.store.receive(self.target)["message"])
        self.store.acknowledge(self.target, a["id"], completed=True)
        self.assertEqual(self.store.receive(self.target)["message"]["id"], b["id"])

    def test_sender_exit_does_not_remove_mail(self):
        done = self.cli("enqueue", "--target", self.target, "--text", "persist")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertEqual(self.store.receive(self.target)["message"]["text"], "persist")

    def test_dedup_and_conflicting_request(self):
        first = self.send(mid="stable")
        self.assertEqual(self.send(mid="stable")["seq"], first["seq"])
        for text, ttl in (("different", None), ("hello", 10)):
            with self.assertRaises(Error):
                self.send(text, "stable", ttl)
        self.assertEqual(len(self.store.queue()), 1)

    def test_expiry_does_not_reset_on_retry(self):
        self.send(mid="stable", ttl=10)
        self.now += 11
        self.assertEqual(self.send(mid="stable", ttl=10)["expires"], 1010)
        self.assertIsNone(self.store.receive(self.target)["message"])
        self.assertEqual(self.store.message("stable")["status"], "expired")

    def test_no_expiry_or_cancellation_after_claim(self):
        a = self.send(ttl=1)
        self.store.receive(self.target)
        self.now += 100
        self.assertEqual(self.store.queue()[0]["status"], "delivering")
        with self.assertRaises(Error):
            self.store.cancel(a["id"])

    def test_cancel_pending_unblocks_fifo(self):
        a, b = self.send(), self.send("next")
        self.store.cancel(a["id"])
        self.assertEqual(self.store.receive(self.target)["message"]["id"], b["id"])

    def test_recipient_binding_and_ack(self):
        self.store.add_target("other")
        a = self.send()
        self.store.receive(self.target)
        with self.assertRaises(Error):
            self.store.acknowledge("other", a["id"], True)
        with self.assertRaises(Error):
            self.store.add_target(self.target, ["/bin/false"])

    def test_claim_survives_restart_without_resend(self):
        a = self.send()
        self.store.receive(self.target)
        other = Store(self.root)
        try:
            self.assertEqual(other.receive(self.target)["blocked_by"], a["id"])
            self.assertEqual(other.message(a["id"])["text"], "hello")
        finally:
            other.close()

    def test_loop_does_not_accumulate_missed_wakes(self):
        lid = self.loop()
        self.now += 1000
        self.assertEqual(len(self.store.tick()), 1)
        self.now += 1000
        self.assertEqual(self.store.tick(), [])
        self.assertEqual(self.store.loop(lid)["wakes"], 1)

    def test_loop_revisions_and_pause(self):
        lid = self.loop()
        self.now += 10
        mid = self.store.tick()[0]
        self.store.set_next(lid, "new", 1)
        self.assertEqual(self.store.message(mid)["text"], "continue")
        with self.assertRaises(Error):
            self.store.set_next(lid, "stale", 1)
        self.store.loop_state(lid, "paused")
        self.assertEqual(self.store.message(mid)["status"], "cancelled")
        self.now += 100
        self.assertEqual(self.store.tick(), [])
        self.store.loop_state(lid, "active")
        self.now += 10
        self.assertEqual(self.store.message(self.store.tick()[0])["text"], "new")

    def test_stop_discloses_inflight_and_never_restarts(self):
        lid = self.loop()
        self.now += 10
        mid = self.store.receive(self.target)["message"]["id"]
        stopped = self.store.loop_state(lid, "stopped")
        self.assertEqual(stopped["in_flight"], [mid])
        self.store.acknowledge(self.target, mid, True)
        self.now += 100
        self.assertEqual(self.store.tick(), [])
        with self.assertRaises(Error):
            self.store.loop_state(lid, "active")

    def test_limit_and_clock_backward(self):
        lid = self.loop(limit=1)
        self.now -= 100
        self.assertEqual(self.store.tick(), [])
        self.now = 1010
        mid = self.store.tick()[0]
        self.store.receive(self.target)
        self.store.acknowledge(self.target, mid, True)
        self.now += 100
        self.assertEqual(self.store.tick(), [])
        self.assertEqual(self.store.loop(lid)["state"], "stopped")

    def test_recurring_and_one_shot_share_fifo(self):
        first = self.send()
        self.loop()
        self.now += 10
        second = self.store.tick()[0]
        self.assertEqual([m["id"] for m in self.store.queue()], [first["id"], second])

    def test_capacity_and_invalid_values(self):
        for n in range(256):
            self.send(mid=f"m{n}")
        with self.assertRaisesRegex(Error, "queue full"):
            self.send()
        self.assertEqual(self.send(mid="m0")["id"], "m0")
        for value in (float("nan"), float("inf"), 0, -1, True):
            with self.assertRaises(Error):
                self.loop(value)
        for text in ("", " \n", "x\x1by", "x" * 65537):
            with self.assertRaises(Error):
                self.send(text)

    def test_parallel_enqueue_deduplicates(self):
        def submit(_):
            return self.cli("enqueue", "--target", self.target, "--text", "same", "--message-id", "parallel")
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            results = list(pool.map(submit, range(8)))
        self.assertTrue(all(r.returncode == 0 for r in results), [r.stderr for r in results])
        self.assertEqual(len(self.store.queue()), 1)

    def test_parallel_receive_has_one_winner(self):
        self.send()
        def receive(_):
            return self.cli("receive", "--target", self.target, "--idle")
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            results = list(pool.map(receive, range(4)))
        claimed = [r for r in results if r.returncode == 0 and json.loads(r.stdout).get("message")]
        self.assertEqual(len(claimed), 1)

    def test_idle_assertion_required(self):
        done = self.cli("receive", "--target", self.target)
        self.assertNotEqual(done.returncode, 0)

    def test_private_state_and_symlink_rejection(self):
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        self.assertEqual((self.root / "queue.sqlite3").stat().st_mode & 0o777, 0o600)
        other = Path(self.tmp.name) / "other"
        other.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(Error):
            Store(other)

    def test_future_schema_refused(self):
        self.store.db.execute("PRAGMA user_version=999")
        with self.assertRaises(Error):
            Store(self.root)

    def test_prune_only_terminal_receipts(self):
        a, b = self.send(), self.send("keep")
        self.store.cancel(a["id"])
        self.now += 100
        self.assertEqual(self.store.prune(10)["removed"], 1)
        self.assertEqual(self.store.message(b["id"])["status"], "queued")

    def test_full_recipient_does_not_starve_other_loop(self):
        self.loop()
        for n in range(256):
            self.send(mid=f"full{n}")
        self.store.add_target("another")
        other = self.store.create_loop("another", "next", "sender", 10)
        self.store.loop_state(other["id"], "active")
        self.now += 10
        generated = self.store.tick()
        self.assertEqual(len(generated), 1)
        self.assertEqual(self.store.message(generated[0])["target"], "another")

    def test_wake_limit_does_not_abandon_pending_delivery(self):
        lid = self.loop(limit=1)
        self.now += 10
        self.store.tick()
        self.now += 1000
        self.store.tick()
        self.assertEqual(self.store.loop(lid)["state"], "active")

    def test_worker_singleton(self):
        self.assertFalse(self.store.worker_running())
        with self.store.target_lock("ralph:service"):
            self.assertTrue(self.store.worker_running())
        self.assertFalse(self.store.worker_running())

    def test_adapter_enqueue_requires_worker(self):
        self.store.add_target("adapter", [sys.executable, "-c", "pass"])
        result = self.cli("enqueue", "--target", "adapter", "--text", "hello")
        self.assertEqual(result.returncode, 1)
        self.assertIn("no Ralph worker", result.stderr)
        self.assertEqual(self.store.queue("adapter"), [])

    def test_cancel_before_claim_has_no_effects(self):
        msg = self.send()
        self.store.cancel(msg["id"])
        self.assertFalse(self.store.claim(msg["id"]))

    def test_complete_without_claim_is_refused(self):
        msg = self.send()
        with self.assertRaises(Error):
            self.store.acknowledge(self.target, msg["id"], True)

    def test_provider_queue_holds_recipient_and_cannot_expire(self):
        first = self.send(ttl=1)
        second = self.send("next")
        self.assertTrue(self.store.claim(first["id"]))
        self.store.outcome(first["id"], "provider_queued", "native queue receipt", {"native_id": "q1"})
        self.now += 100
        self.assertEqual(self.store.head(self.target)["id"], first["id"])
        self.assertFalse(self.store.claim(second["id"]))
        with self.assertRaises(Error):
            self.store.cancel(first["id"])
        self.store.outcome(first["id"], "accepted", "bound turn", {"turn": "t1"})
        self.store.outcome(first["id"], "completed", "ended", {"turn": "t1"})
        self.assertTrue(self.store.claim(second["id"]))

    def test_schema_one_migrates_with_existing_messages(self):
        first = self.send()
        self.store.db.execute("PRAGMA user_version=1")
        migrated = Store(self.root)
        try:
            self.assertEqual(migrated.db.execute("PRAGMA user_version").fetchone()[0], 2)
            self.assertEqual(migrated.message(first["id"])["text"], "hello")
        finally:
            migrated.close()


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "queue")
        self.addCleanup(self.store.close)
        self.provider = Path(self.tmp.name) / "provider.json"
        self.provider.write_text("{}")
        self.argv = [sys.executable, str(FIXTURE), str(self.provider)]
        self.store.add_target("test:bound-session", self.argv)
        self.target = "test:bound-session"
        self.worker = Worker(self.store)

    def update(self, **values):
        state = json.loads(self.provider.read_text())
        state.update(values)
        self.provider.write_text(json.dumps(state))

    def send(self):
        return self.store.enqueue(self.target, "hello", "sender")

    def test_busy_waiting_unknown_never_submit(self):
        self.send()
        for state in ("busy", "working", "waiting", "unknown", "unreachable", "stopped"):
            self.update(state=state)
            self.worker.once()
            self.assertNotIn("submits", json.loads(self.provider.read_text()))

    def test_race_proves_no_effects_and_keeps_id(self):
        msg = self.send()
        self.update(race=True)
        self.worker.once()
        self.assertEqual(self.store.message(msg["id"])["status"], "queued")
        self.update(race=False)
        self.worker.once()
        self.assertEqual(self.store.message(msg["id"])["status"], "accepted")

    def test_lost_response_reconciles_without_resend(self):
        msg = self.send()
        self.update(lose_response=True)
        self.worker.once()
        self.assertEqual(self.store.message(msg["id"])["status"], "uncertain")
        Worker(self.store).once()
        self.assertEqual(self.store.message(msg["id"])["status"], "accepted")
        self.assertEqual(json.loads(self.provider.read_text())["submits"], 1)

    def test_crash_after_claim_does_not_submit(self):
        msg = self.send()
        self.store.claim(msg["id"])
        self.worker.once()
        self.assertEqual(self.store.message(msg["id"])["status"], "uncertain")
        self.assertNotIn("submits", json.loads(self.provider.read_text()))

    def test_no_second_send_until_completion(self):
        a, b = self.send(), self.send()
        self.worker.once()
        self.worker.once()
        self.assertEqual(self.store.message(b["id"])["status"], "queued")
        self.update(receipts={a["id"]: "completed"})
        self.worker.once()
        self.assertEqual(self.store.message(a["id"])["status"], "completed")
        self.worker.once()
        self.assertEqual(self.store.message(b["id"])["status"], "accepted")

    def test_missing_guard_fails_closed(self):
        msg = self.send()
        calls = []
        def fake(argv, request, timeout):
            calls.append(request["operation"])
            return {"state": "idle", "composer_empty": True, "idle_token": "token"}
        Worker(self.store, transport=fake).once()
        self.assertEqual(calls, ["observe"])
        self.assertEqual(self.store.message(msg["id"])["status"], "queued")

    def test_changed_identity_and_malformed_response(self):
        for code in ('print("{}");', 'print("not-json")', 'print("x"*200000)'):
            with self.assertRaises(Error):
                rpc([sys.executable, "-c", code], {"target": self.target, "operation": "observe"}, 1)

    def test_adapter_timeout_is_bounded(self):
        with self.assertRaises(Error):
            rpc([sys.executable, "-c", "import time; time.sleep(10)"],
                {"target": self.target, "operation": "observe"}, 0.05)

    def test_mailbox_api_cannot_override_adapter_receipts(self):
        msg = self.send()
        for action in (lambda: self.store.receive(self.target),
                       lambda: self.store.acknowledge(self.target, msg["id"])):
            with self.assertRaises(Error):
                action()

    def test_an_unresolved_target_does_not_block_another(self):
        msg = self.send()
        self.store.claim(msg["id"])
        self.store.add_target("other", self.argv)
        other = self.store.enqueue("other", "second", "sender")
        self.worker.once()
        self.assertEqual(self.store.message(msg["id"])["status"], "uncertain")
        self.assertEqual(self.store.message(other["id"])["status"], "accepted")

    def test_two_workers_cannot_submit_same_message(self):
        self.send()
        self.update(delay=0.15)
        def run(_):
            return subprocess.run([sys.executable, "-m", "kilix_ralph", "--state-dir", str(self.store.root),
                                   "serve", "--once"], cwd=ROOT, capture_output=True, text=True, timeout=10)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(run, range(2)))
        self.assertTrue(any(r.returncode == 0 for r in results))
        self.assertEqual(json.loads(self.provider.read_text())["submits"], 1)

    def test_missing_completion_cannot_release_next_message(self):
        first, second = self.send(), self.send()
        self.worker.once()
        self.update(receipts={})
        self.worker.once()
        self.assertEqual(self.store.message(first["id"])["status"], "accepted")
        self.assertEqual(self.store.message(second["id"])["status"], "queued")
        self.assertEqual(json.loads(self.provider.read_text())["submits"], 1)


if __name__ == "__main__":
    unittest.main()
