"""Bounded adapter RPC. No shell, terminal injection or inferred idle state."""
import json
import os
import selectors
import signal
import subprocess
import time

from .store import Error, number

SCHEMA = "kilix.ralph.adapter/v1"
CAPABILITIES = ("guarded_idle_submit", "durable_reconciliation", "completion_evidence", "bound_conversation")


def rpc(argv, request, timeout=15):
    """One JSON request/response; limit bytes, time and the owned process group."""
    number(timeout, "adapter timeout")
    payload = memoryview((json.dumps({"schema": SCHEMA, **request}, ensure_ascii=False) + "\n").encode())
    result = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + timeout
    try:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
    except OSError as exc:
        raise Error("cannot start adapter: " + str(exc)) from exc
    try:
        with selectors.DefaultSelector() as selector:
            for stream, events, name in ((process.stdin, selectors.EVENT_WRITE, "stdin"),
                                         (process.stdout, selectors.EVENT_READ, "stdout"),
                                         (process.stderr, selectors.EVENT_READ, "stderr")):
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, events, name)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise Error("adapter timed out")
                for key, _ in selector.select(remaining):
                    stream, name = key.fileobj, key.data
                    if name == "stdin":
                        try:
                            written = os.write(stream.fileno(), payload[:8192])
                            payload = payload[written:]
                        except BrokenPipeError:
                            payload = payload[:0]
                        if not payload:
                            selector.unregister(stream)
                            stream.close()
                    else:
                        chunk = os.read(stream.fileno(), 8192)
                        if not chunk:
                            selector.unregister(stream)
                            stream.close()
                        else:
                            result[name].extend(chunk)
                            if len(result[name]) > 131072:
                                raise Error("adapter output exceeded 128 KiB")
            # Leave the leader waitable until group cleanup; reaping first can
            # let its PID be reused before killpg. This module targets POSIX.
            while True:
                exited = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if exited is not None:
                    break
                if time.monotonic() >= deadline:
                    raise Error("adapter timed out")
                time.sleep(min(0.01, max(0, deadline - time.monotonic())))
        code = exited.si_status if exited.si_code == os.CLD_EXITED else -exited.si_status
        if code:
            raise Error(f"adapter exited {code}")
        response = json.loads(result["stdout"])
        if not isinstance(response, dict) or response.get("schema") != SCHEMA or response.get("target") != request["target"]:
            raise Error("adapter response has the wrong schema or conversation")
        if request["operation"] != "observe" and response.get("operation_id") != request["operation_id"]:
            raise Error("adapter response has the wrong operation ID")
        return response
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        raise Error("invalid or unavailable adapter response: " + str(exc)) from exc
    finally:
        # This RPC owns its process group; helpers may not outlive the request.
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
        for stream in (process.stdin, process.stdout, process.stderr):
            if not stream.closed:
                stream.close()


class Worker:
    def __init__(self, store, timeout=15, transport=rpc):
        self.store, self.timeout, self.transport = store, timeout, transport

    def call(self, binding, operation, **params):
        return self.transport(binding["adapter"], {"operation": operation,
                              "target": binding["id"], **params}, self.timeout)

    def _result(self, message, result, recovering):
        mid = message["id"]
        status = result.get("status")
        if result.get("operation_id") != mid or result.get("target") != message["target"]:
            raise Error("receipt does not match delivery identity")
        # not_submitted is a durable proof of no side effects, not a timeout.
        if status == "not_submitted" and result.get("no_effects") is True:
            if message["status"] == "accepted":
                raise Error("adapter lost previously accepted operation")
            self.store.outcome(mid, "queued", "adapter proved no submission; awaiting fresh idle", result)
        elif status in ("provider_queued", "accepted", "completed"):
            if not isinstance(result.get("evidence"), dict) or not result["evidence"]:
                raise Error("adapter omitted acceptance/completion evidence")
            if status == message["status"] and status in ("provider_queued", "accepted"):
                return
            self.store.outcome(mid, status, "adapter reported " + status, result)
        elif status == "uncertain":
            if message["status"] != "accepted":
                self.store.outcome(mid, "uncertain", "adapter could not reconcile delivery", result)
        else:
            raise Error("adapter returned an invalid delivery outcome")

    def target(self, target):
        binding = self.store.target(target)
        if binding["adapter"] is None:
            return {"target": target, "status": "mailbox", "reason": "receiver must pull at its idle boundary"}
        with self.store.target_lock(target):
            msg = self.store.head(target)
            if msg is None:
                return {"target": target, "status": "empty"}
            recovering = msg["status"] != "queued"
            try:
                if recovering:
                    # Even a crash between claim and RPC is observation-only.
                    result = self.call(binding, "reconcile", operation_id=msg["id"], digest=msg["digest"])
                else:
                    ready = self.call(binding, "observe")
                    caps = ready.get("capabilities", {})
                    if not isinstance(caps, dict) or any(caps.get(c) is not True for c in CAPABILITIES):
                        raise Error("adapter lacks guarded idle delivery or durable lifecycle evidence")
                    token = ready.get("idle_token")
                    if ready.get("state") != "idle" or ready.get("composer_empty") is not True:
                        self.store.pending_reason(msg["id"], "recipient is not proven idle with an empty composer")
                        return self.store.message(msg["id"])
                    if not isinstance(token, str) or not token or len(token) > 1024:
                        raise Error("adapter omitted a bounded idle generation token")
                    if not self.store.claim(msg["id"]):
                        return self.store.message(msg["id"])
                    msg = self.store.message(msg["id"])
                    result = self.call(binding, "submit", operation_id=msg["id"],
                                       digest=msg["digest"], idle_token=token,
                                       sender=msg["sender"], text=msg["text"])
                self._result(msg, result, recovering)
            except Error as exc:
                if msg["status"] == "queued":
                    self.store.pending_reason(msg["id"], str(exc))
                elif msg["status"] != "accepted":
                    self.store.outcome(msg["id"], "uncertain", str(exc))
                else:
                    return {"target": target, "status": "accepted", "id": msg["id"], "error": str(exc)}
            return self.store.message(msg["id"])

    def once(self):
        generated = self.store.tick()
        results = []
        for row in self.store.db.execute("SELECT id FROM targets ORDER BY id").fetchall():
            try:
                results.append(self.target(row[0]))
            except Error as exc:
                results.append({"target": row[0], "error": str(exc)})
        return {"generated": generated, "targets": results}
