"""Owned Claude streaming session. Its single input writer enforces idle-only turns."""
import ctypes
from functools import partial
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import socket
import stat
import struct
import subprocess
import sys
import time
import uuid

from .native import Ledger, UnixRPC
from .store import Error, identifier, message_text
from .worker import CAPABILITIES, SCHEMA


def parent_death(parent):
    if ctypes.CDLL(None).prctl(1, signal.SIGKILL) != 0 or os.getppid() != parent:
        os._exit(127)


class ClaudeBridge:
    def __init__(self, store, target, cwd, model="haiku", effort="low", bypass=False, argv=None):
        self.store, self.target = store, identifier(target)
        self.ledger = Ledger(store.root)
        self.cwd = str(Path(cwd).resolve(strict=True))
        if not Path(self.cwd).is_dir():
            raise Error("Claude working directory must be a directory")
        self.name = hashlib.sha256(target.encode()).hexdigest()[:24]
        self.endpoint = store.root / ("claude-" + self.name + ".sock")
        self.settings = {"cwd": self.cwd, "model": model, "effort": effort, "bypass": bypass}
        with self.ledger.store.transaction():
            self.ledger.store.db.execute("CREATE TABLE IF NOT EXISTS claude_sessions (target TEXT PRIMARY KEY, session TEXT NOT NULL, settings TEXT NOT NULL)")
            row = self.ledger.store.db.execute("SELECT * FROM claude_sessions WHERE target=?", (target,)).fetchone()
            if row:
                if json.loads(row["settings"]) != self.settings:
                    raise Error("Claude target configuration is immutable; use a new target")
                self.session = row["session"]
            else:
                self.session = str(uuid.uuid4())
                self.ledger.store.db.execute("INSERT INTO claude_sessions VALUES (?,?,?)",
                    (target, self.session, json.dumps(self.settings)))
        self.process = None
        self._process_waitable = False
        self.active = None
        self.acknowledged = False
        self.token = uuid.uuid4().hex
        self.output = bytearray()
        self.input = bytearray()
        self.blocked = None
        self._argv_override = argv
        self._refresh_provider_state()

    def _refresh_provider_state(self):
        old = self.ledger.store.db.execute("SELECT receipt FROM receipts WHERE target=?", (self.target,)).fetchall()
        self.blocked = None
        if any(not row[0] or json.loads(row[0]).get("status") != "completed" for row in old):
            self.blocked = "previous Claude turn is unresolved; receipts preserved, no replay"
        executable = shutil.which("claude")
        if self._argv_override is None and not executable:
            raise Error("claude executable not found")
        self.argv = self._argv_override or [executable, "--print", "--input-format", "stream-json",
            "--output-format", "stream-json", "--verbose", "--replay-user-messages",
            "--model", self.settings["model"], "--effort", self.settings["effort"],
            "--resume" if old else "--session-id", self.session]
        if self.settings["bypass"] and self._argv_override is None:
            self.argv.append("--dangerously-skip-permissions")

    def response(self, mid=None, **fields):
        return {"schema": SCHEMA, "target": self.target,
                **({"operation_id": mid} if mid else {}), **fields}

    def receipt(self, mid, status, **evidence):
        return self.ledger.save(self.target, mid, self.response(mid, status=status,
            evidence={"provider": "claude", "session": self.session, **evidence}))

    def handle(self, request):
        if not isinstance(request, dict) or request.get("schema") != SCHEMA or request.get("target") != self.target:
            raise Error("Claude request has the wrong identity")
        op = request.get("operation")
        if op == "observe":
            state = "unknown" if self.blocked else "working" if self.active else "idle"
            return self.response(state=state, reason=self.blocked, composer_empty=True, idle_token=self.token,
                input_path="owned Claude stream; no terminal composer", capabilities={c: True for c in CAPABILITIES})
        mid, digest = identifier(request.get("operation_id")), request.get("digest")
        old = self.ledger.get(self.target, mid)
        if old:
            if old["digest"] != digest:
                raise Error("Claude operation digest mismatch")
            if op == "submit":
                wire = f"[Session message from {identifier(request.get('sender'))}; Ralph ID {mid}]\n{message_text(request.get('text'))}"
                if old["payload"] != wire:
                    raise Error("Claude operation payload mismatch")
            if self.blocked and (not old["receipt"] or old["receipt"].get("status") != "completed"):
                return self.response(mid, status="uncertain", reason=self.blocked)
            return old["receipt"] or self.response(mid, status="uncertain", reason="unresolved Claude intent")
        if op == "reconcile":
            return self.response(mid, status="uncertain", reason="missing Claude intent; never replay")
        if op != "submit":
            raise Error("unsupported Claude operation")
        if self.blocked or self.active or request.get("idle_token") != self.token:
            return self.response(mid, status="not_submitted", no_effects=True)
        text, sender = message_text(request.get("text")), identifier(request.get("sender"))
        wire = f"[Session message from {sender}; Ralph ID {mid}]\n{text}"
        self.ledger.intent(self.target, mid, digest, wire)
        self.active, self.acknowledged = mid, False
        self.token = uuid.uuid4().hex
        self.input.extend((json.dumps({"type": "user", "session_id": self.session,
            "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL, self.target + ":" + mid)),
            "parent_tool_use_id": None, "message": {"role": "user", "content": wire}}) + "\n").encode())
        return self.receipt(mid, "provider_queued", stage="owned stream input pending")

    def event(self, event):
        if not isinstance(event, dict):
            raise Error("Claude returned an invalid event")
        if event.get("session_id") and event["session_id"] != self.session:
            raise Error("Claude changed session identity")
        if not self.active:
            return
        mid = self.active
        if event.get("type") == "user":
            expected_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, self.target + ":" + mid))
            content = event.get("message", {}).get("content")
            if isinstance(content, list) and len(content) == 1 and content[0].get("type") == "text":
                content = content[0].get("text")
            if (event.get("uuid") != expected_uuid or event.get("session_id") != self.session or
                    content != self.ledger.get(self.target, mid)["payload"]):
                raise Error("Claude acknowledgment did not match the submitted message")
            self.acknowledged = True
            self.receipt(mid, "accepted", user_message_uuid=expected_uuid)
        elif event.get("type") == "result":
            if not self.acknowledged or event.get("session_id") != self.session:
                raise Error("Claude result arrived without a bound user acknowledgment")
            self.receipt(mid, "completed", result_id=event.get("uuid"),
                subtype=event.get("subtype"), turn_succeeded=event.get("subtype") == "success" and not event.get("is_error", False),
                result=str(event.get("result", ""))[:8192])
            self.active = None
            self.token = uuid.uuid4().hex

    def _provider_exited(self):
        # WNOWAIT leaves even an exited leader reserved: its PID/PGID cannot
        # be reused while the same-group descendants are still being stopped.
        if not self._process_waitable or self.process.returncode is not None:
            self._process_waitable = False
            return True
        try:
            return os.waitid(os.P_PID, self.process.pid,
                os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None
        except ChildProcessError:
            # An outside waiter consumed the leader. Never signal its old PGID.
            self._process_waitable = False
            return True

    def _signal_provider_group(self, sig):
        self._provider_exited()
        if not self._process_waitable:
            return False
        try:
            os.killpg(self.process.pid, sig)
        except ProcessLookupError:
            return False
        return True

    def _stop_provider(self):
        if not self.process:
            return
        try:
            try:
                self.process.stdin.close()
            finally:
                try:
                    if self._signal_provider_group(signal.SIGTERM):
                        # A leader's exit does not prove its helpers exited.
                        # Keep it unreaped through the entire TERM grace.
                        time.sleep(3)
                finally:
                    self._signal_provider_group(signal.SIGKILL)
        finally:
            self._process_waitable = False
            try:
                self.process.wait()
            finally:
                try:
                    self.process.stdin.close()
                finally:
                    self.process.stdout.close()

    def serve(self, ready=None, stopping=lambda: False):
        with self.ledger.store.target_lock("claude-service:" + self.name):
            # Construction may precede the previous owner's final receipt.
            # Recompute restart admission and argv while ownership is exclusive.
            self._refresh_provider_state()
            if self.endpoint.exists() or self.endpoint.is_symlink():
                info = self.endpoint.lstat()
                if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                    raise Error("refusing to replace an unowned or non-socket endpoint")
                self.endpoint.unlink()  # Singleton lock proves no other bridge owns it.
            listener = socket.socket(socket.AF_UNIX)
            try:
                listener.bind(str(self.endpoint))
                os.chmod(self.endpoint, 0o600)
                listener.listen(8)
                if not self.blocked:
                    self.process = subprocess.Popen(self.argv, cwd=self.cwd, stdin=subprocess.PIPE,
                        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True,
                        preexec_fn=partial(parent_death, os.getpid()))
                    self._process_waitable = True
                    os.set_blocking(self.process.stdout.fileno(), False)
                    os.set_blocking(self.process.stdin.fileno(), False)
                adapter = [sys.executable, str(Path(__file__).with_name("adapter_entry.py")),
                           "--state-dir", str(self.store.root), "claude", "--socket", str(self.endpoint), "--target", self.target]
                self.store.add_target(self.target, adapter)
                if ready:
                    ready({"target": self.target, "session": self.session, "socket": str(self.endpoint), "blocked": self.blocked})
                with selectors.DefaultSelector() as selector:
                    selector.register(listener, selectors.EVENT_READ, "listen")
                    if self.process:
                        selector.register(self.process.stdout, selectors.EVENT_READ, "output")
                    write_registered = False
                    while not stopping():
                        if self.process and self._provider_exited():
                            self.blocked = "Claude exited; restart bridge to inspect preserved receipts"
                        if self.input and self.process and not write_registered and not self.blocked:
                            selector.register(self.process.stdin, selectors.EVENT_WRITE, "input")
                            write_registered = True
                        for key, _ in selector.select(.1):
                            if key.data == "listen":
                                connection, _ = listener.accept()
                                with connection:
                                    connection.settimeout(.5)
                                    _, uid, _ = struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                                    if uid != os.getuid():
                                        continue
                                    raw = bytearray()
                                    rpc = {}
                                    try:
                                        deadline = time.monotonic() + 1
                                        while b"\n" not in raw:
                                            chunk = connection.recv(8192)
                                            if not chunk or len(raw) > 131072 or time.monotonic() > deadline:
                                                raise Error("invalid bridge request")
                                            raw.extend(chunk)
                                        rpc = json.loads(raw.split(b"\n", 1)[0])
                                        if not isinstance(rpc, dict):
                                            rpc = {}
                                            raise Error("invalid bridge RPC object")
                                        if rpc.get("method") != "ralph":
                                            raise Error("unknown bridge method")
                                        answer = {"id": rpc.get("id"), "result": self.handle(rpc["params"])}
                                    except (Error, ValueError, KeyError, OSError) as exc:
                                        answer = {"id": rpc.get("id"), "error": str(exc)[:512]}
                                    try:
                                        connection.sendall((json.dumps(answer) + "\n").encode())
                                    except OSError:
                                        pass  # Intent/receipt already persisted; reconcile, never replay.
                            elif key.data == "input":
                                try:
                                    count = os.write(self.process.stdin.fileno(), self.input[:8192])
                                    del self.input[:count]
                                except OSError:
                                    self.blocked = "Claude input failed; delivery outcome uncertain"
                                if not self.input or self.blocked:
                                    selector.unregister(self.process.stdin)
                                    write_registered = False
                            else:
                                chunk = os.read(self.process.stdout.fileno(), 65536)
                                if not chunk:
                                    selector.unregister(self.process.stdout)
                                    self.blocked = "Claude output closed; delivery outcome uncertain"
                                    continue
                                self.output.extend(chunk)
                                if len(self.output) > 16 * 1024 * 1024:
                                    raise Error("Claude output frame exceeded 16 MiB")
                                while b"\n" in self.output:
                                    line, _, rest = self.output.partition(b"\n")
                                    self.output = bytearray(rest)
                                    try:
                                        self.event(json.loads(line))
                                    except (Error, ValueError, TypeError, AttributeError) as exc:
                                        self.blocked = str(exc)
                                        # Preserve the active receipt; never authorize another turn.
            finally:
                try:
                    listener.close()
                    if self.endpoint.exists():
                        self.endpoint.unlink()
                finally:
                    self._stop_provider()

    def close(self):
        self.ledger.close()


def call_bridge(endpoint, request):
    client = UnixRPC(endpoint)
    try:
        return client.call("ralph", request)
    finally:
        client.close()
