"""Codex native idle queue adapter. Never calls turn/start or turn/steer."""
import hashlib
import json
from pathlib import Path
import sys

from .native import Ledger, CodexSocket
from .store import Error, identifier, message_text
from .worker import CAPABILITIES, SCHEMA


def default_socket():
    import os
    return Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "app-server-control/app-server-control.sock"


class Codex:
    def __init__(self, root, endpoint, thread, target, client=None):
        self.thread, self.target = identifier(thread), identifier(target)
        self.client = client or CodexSocket(endpoint)
        try:
            self.client.call("initialize", {"clientInfo": {"name": "kilix-ralph", "version": "0.2.0"},
                                          "capabilities": {"experimentalApi": True}})
            self.client.send({"method": "initialized"})
            self.ledger = Ledger(root)
        except BaseException:
            self.client.close()
            raise

    def close(self):
        self.ledger.close()
        self.client.close()

    def thread_info(self):
        info = self.client.call("thread/read", {"threadId": self.thread, "includeTurns": False})["thread"]
        if info.get("id") != self.thread:
            raise Error("Codex returned a different conversation")
        return info

    def pages(self, method, **params):
        cursor = None
        seen = set()
        for _ in range(32):
            result = self.client.call(method, {"threadId": self.thread, "limit": 100, "cursor": cursor, **params})
            if not isinstance(result.get("data"), list):
                raise Error("Codex returned invalid pagination")
            yield from result["data"]
            cursor = result.get("nextCursor")
            if cursor is None:
                return
            if not isinstance(cursor, str) or cursor in seen:
                raise Error("Codex returned a repeated cursor")
            seen.add(cursor)
        raise Error("Codex history exceeds reconciliation bound; inspect before retrying")

    def response(self, mid=None, **values):
        return {"schema": SCHEMA, "target": self.target,
                **({"operation_id": mid} if mid else {}), **values}

    def observe(self):
        info = self.thread_info()
        # Probe the experimental method before allowing any input.
        queue = list(self.pages("thread/queue/list"))
        state = info.get("status", {}).get("type", "unknown")
        if queue or (state == "idle" and info.get("canAcceptDirectInput") is False):
            state = "waiting"
        token = hashlib.sha256(json.dumps([self.thread, info.get("updatedAt"), state]).encode()).hexdigest()
        return self.response(state=state, composer_empty=True, idle_token=token,
                             input_path="native-queue; no terminal composer consumed",
                             capabilities={c: True for c in CAPABILITIES})

    @staticmethod
    def matches(inputs, wire):
        return (isinstance(inputs, list) and len(inputs) == 1 and
                inputs[0].get("type") == "text" and inputs[0].get("text") == wire)

    def reconcile(self, mid, digest):
        old = self.ledger.get(self.target, mid)
        if old is None:
            return self.response(mid, status="uncertain", reason="no native intent record; never resend")
        if old["digest"] != digest:
            raise Error("operation digest does not match native intent")
        prior = old["receipt"]
        if prior and prior.get("status") == "completed":
            return prior
        self.thread_info()  # Never resume/unload a thread as a side effect.
        client_id = "kilix-ralph:" + mid
        for queued in self.pages("thread/queue/list"):
            if queued.get("clientUserMessageId") == client_id:
                if not self.matches(queued.get("input"), old["payload"]):
                    raise Error("native queued message ID has different content")
                return self.ledger.save(self.target, mid, self.response(mid, status="provider_queued",
                    evidence={"provider": "codex", "thread": self.thread, "queued_submission": queued["id"]}))
        for turn in self.pages("thread/turns/list", itemsView="full", sortDirection="desc"):
            for item in turn.get("items", []):
                if item.get("type") == "userMessage" and item.get("clientId") == client_id:
                    if not self.matches(item.get("content"), old["payload"]):
                        raise Error("native user message ID has different content")
                    state = turn.get("status")
                    if state not in ("inProgress", "completed", "failed", "interrupted"):
                        return self.response(mid, status="uncertain", reason="unknown native turn state")
                    return self.ledger.save(self.target, mid, self.response(mid,
                        status="accepted" if state == "inProgress" else "completed",
                        evidence={"provider": "codex", "thread": self.thread, "turn": turn["id"],
                                  "turn_status": state, "turn_succeeded": state == "completed"}))
        # Queue dequeue/history append may straddle this read; absence is never
        # permission to add again, even when a prior adapter process crashed.
        return self.response(mid, status="uncertain", reason="native message not yet found; observation-only recovery")

    def handle(self, request):
        if request.get("schema") != SCHEMA or request.get("target") != self.target:
            raise Error("adapter request has the wrong schema or target")
        operation = request.get("operation")
        if operation == "observe":
            return self.observe()
        mid, digest = identifier(request.get("operation_id")), request.get("digest")
        if operation == "reconcile":
            return self.reconcile(mid, digest)
        if operation != "submit":
            raise Error("unsupported native operation")
        text, sender = message_text(request.get("text")), identifier(request.get("sender"))
        wire = f"[Session message from {sender}; Ralph ID {mid}]\n{text}"
        with self.ledger.store.target_lock(self.target):
            old = self.ledger.get(self.target, mid)
            if old:
                if old["digest"] != digest or old["payload"] != wire:
                    raise Error("native operation ID collision")
                return self.reconcile(mid, digest)
            ready = self.observe()
            if ready["state"] != "idle":
                return self.response(mid, status="not_submitted", no_effects=True)
            self.ledger.intent(self.target, mid, digest, wire)
            # Codex owns the final idle boundary: if a human starts a turn now,
            # this enters its native queue and will NOT steer that active turn.
            result = self.client.call("thread/queue/add", {"threadId": self.thread,
                "clientUserMessageId": "kilix-ralph:" + mid,
                "input": [{"type": "text", "text": wire}]})
            queued = result.get("queuedSubmission", {})
            if queued.get("clientUserMessageId") != "kilix-ralph:" + mid or not self.matches(queued.get("input"), wire):
                raise Error("Codex returned mismatched queue acceptance")
            receipt = self.response(mid, status="provider_queued", evidence={"provider": "codex",
                "thread": self.thread, "queued_submission": queued["id"]})
            return self.ledger.save(self.target, mid, receipt)


def bind(store, target, thread, endpoint):
    adapter = Codex(store.root, endpoint, thread, target)
    try:
        observed = adapter.observe()
    finally:
        adapter.close()
    if observed["state"] == "notLoaded":
        raise Error("thread is not loaded in this server; bind its actual shared app-server")
    argv = [sys.executable, str(Path(__file__).with_name("adapter_entry.py")),
            "--state-dir", str(store.root), "codex", "--socket", str(Path(endpoint).absolute()),
            "--thread", thread, "--target", target]
    return {**store.add_target(target, argv), "provider": "codex", "state": observed["state"]}
