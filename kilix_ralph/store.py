"""Host-local queue. All decisions involving multiple rows are transactions."""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
import uuid


class Error(RuntimeError):
    pass


class QueueFull(Error):
    pass


class Busy(Error):
    pass


ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,191}\Z")
ACTIVE = ("queued", "delivering", "uncertain", "accepted")
TERMINAL = ("completed", "cancelled", "expired")
MAX_TEXT = 65536


def identifier(value):
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise Error("expected a stable ID of 1–192 letters, digits or _ . : @ -")
    return value


def message_text(value):
    if not isinstance(value, str) or not value.strip() or len(value.encode()) > MAX_TEXT:
        raise Error("message must contain 1–65536 UTF-8 bytes and non-whitespace text")
    if any(ord(c) < 32 and c not in "\n\t" or 127 <= ord(c) <= 159 for c in value):
        raise Error("message contains unsupported control characters")
    return value


def number(value, label, positive=True):
    if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
        raise Error(f"{label} must be a finite number")
    if positive and value <= 0:
        raise Error(f"{label} must be positive")
    return value


def private(path, directory=False):
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise Error(f"state must be private, owned and not a symlink: {path}")
    if not directory and info.st_nlink != 1:
        raise Error("state file must not be hard-linked")


class Store:
    def __init__(self, root=None, clock=time.time):
        self.clock = clock
        self.root = Path(root or Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "kilix/ralph").absolute()
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        private(self.root, True)
        path = self.root / "queue.sqlite3"
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        private(path)
        for suffix in ("-wal", "-shm", "-journal"):
            sidecar = Path(str(path) + suffix)
            if sidecar.exists() or sidecar.is_symlink():
                private(sidecar)
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        try:
            self._initialize()
        except BaseException:
            self.db.close()
            raise

    def _initialize(self):
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        with self.transaction():
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise Error("unsupported queue schema; upgrade Ralph")
            for sql in (
                "CREATE TABLE IF NOT EXISTS targets (id TEXT PRIMARY KEY, adapter TEXT NOT NULL)",
                """CREATE TABLE IF NOT EXISTS loops (
                    id TEXT PRIMARY KEY, target TEXT NOT NULL REFERENCES targets(id),
                    sender TEXT NOT NULL, text TEXT NOT NULL, revision INTEGER NOT NULL,
                    interval REAL NOT NULL, due REAL NOT NULL, state TEXT NOT NULL,
                    max_wakes INTEGER, wakes INTEGER NOT NULL DEFAULT 0)""",
                """CREATE TABLE IF NOT EXISTS messages (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
                    target TEXT NOT NULL REFERENCES targets(id), sender TEXT NOT NULL,
                    text TEXT NOT NULL, digest TEXT NOT NULL, created REAL NOT NULL,
                    expires REAL, ttl REAL, status TEXT NOT NULL, reason TEXT NOT NULL,
                    updated REAL NOT NULL, receipt TEXT, loop_id TEXT REFERENCES loops(id),
                    revision INTEGER)""",
                "CREATE INDEX IF NOT EXISTS queue_order ON messages(target,status,seq)",
            ):
                self.db.execute(sql)
            self.db.execute("PRAGMA user_version=1")
        # Persist first creation of the database and WAL directory entries.
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    @contextmanager
    def target_lock(self, target):
        name = hashlib.sha256(identifier(target).encode()).hexdigest() + ".lock"
        path = self.root / name
        fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            private(path)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise Busy("another worker owns this recipient") from exc
            yield
        finally:
            os.close(fd)

    def worker_running(self):
        try:
            with self.target_lock("ralph:service"):
                return False
        except Busy:
            return True

    def add_target(self, target, adapter=None):
        identifier(target)
        if target == "ralph:service":
            raise Error("reserved target identity")
        if adapter is not None and (not isinstance(adapter, list) or not adapter or
                any(not isinstance(v, str) or not v or "\0" in v for v in adapter) or
                not os.path.isabs(adapter[0])):
            raise Error("adapter must be an argv array beginning with an absolute executable path")
        value = json.dumps(adapter, separators=(",", ":"))
        with self.transaction():
            old = self.db.execute("SELECT adapter FROM targets WHERE id=?", (target,)).fetchone()
            if old and old[0] != value:
                raise Error("target binding is immutable; register a new conversation/generation ID")
            self.db.execute("INSERT OR IGNORE INTO targets VALUES (?,?)", (target, value))
        return {"target": target, "mode": "adapter" if adapter else "mailbox"}

    def target(self, target):
        row = self.db.execute("SELECT * FROM targets WHERE id=?", (target,)).fetchone()
        if not row:
            raise Error("unknown target; register an exact conversation identity first")
        return {"id": row["id"], "adapter": json.loads(row["adapter"])}

    def _enqueue(self, target, text, sender, message_id, ttl, loop_id=None, revision=None):
        identifier(target)
        identifier(sender)
        identifier(message_id)
        message_text(text)
        if ttl is not None:
            number(ttl, "ttl")
        self.target(target)
        digest = hashlib.sha256(json.dumps([target, sender, text, ttl, loop_id, revision],
                                          ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
        old = self.db.execute("SELECT digest FROM messages WHERE id=?", (message_id,)).fetchone()
        if old:
            if old[0] != digest:
                raise Error("message ID conflicts with a different immutable request")
            return message_id
        self._expire()
        count, size = self.db.execute("SELECT COUNT(*), COALESCE(SUM(length(CAST(text AS BLOB))),0) FROM messages WHERE status IN ('queued','delivering','uncertain','accepted')").fetchone()
        local = self.db.execute("SELECT COUNT(*) FROM messages WHERE target=? AND status IN ('queued','delivering','uncertain','accepted')", (target,)).fetchone()[0]
        if count >= 4096 or local >= 256 or size + len(text.encode()) > 16 * 1024 * 1024:
            raise QueueFull("queue full; message was not accepted")
        now = self.clock()
        self.db.execute("""INSERT INTO messages
            (id,target,sender,text,digest,created,expires,ttl,status,reason,updated,loop_id,revision)
            VALUES (?,?,?,?,?,?,?,?,'queued','waiting for idle receiver',?,?,?)""",
            (message_id, target, sender, text, digest, now,
             now + ttl if ttl is not None else None, ttl, now, loop_id, revision))
        return message_id

    def enqueue(self, target, text, sender, message_id=None, ttl=None):
        with self.transaction():
            mid = self._enqueue(target, text, sender, message_id or uuid.uuid4().hex, ttl)
        return self.message(mid)

    def _expire(self):
        now = self.clock()
        self.db.execute("UPDATE messages SET status='expired', reason='expired before submission', updated=? WHERE status='queued' AND expires IS NOT NULL AND expires<=?", (now, now))

    def _row(self, row):
        result = dict(row)
        result["receipt"] = json.loads(result["receipt"]) if result["receipt"] else None
        return result

    def message(self, message_id):
        row = self.db.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
        if row is None:
            raise Error("unknown message")
        return self._row(row)

    def queue(self, target=None, history=False):
        with self.transaction():
            self._expire()
        query = "SELECT * FROM messages WHERE 1=1"
        params = []
        if target is not None:
            self.target(target)
            query += " AND target=?"
            params.append(target)
        if not history:
            query += " AND status IN ('queued','delivering','uncertain','accepted')"
        return [self._row(r) for r in self.db.execute(query + " ORDER BY seq", params)]

    def _update(self, mid, status, reason, receipt=None):
        self.db.execute("UPDATE messages SET status=?,reason=?,receipt=?,updated=? WHERE id=?",
                        (status, reason[:512], json.dumps(receipt) if receipt is not None else None, self.clock(), mid))

    def cancel(self, mid):
        with self.transaction():
            self._expire()
            msg = self.message(mid)
            if msg["status"] == "queued":
                self._update(mid, "cancelled", "cancelled before delivery")
            elif msg["status"] not in TERMINAL:
                raise Error("delivery already claimed; cancellation cannot promise recall")
        return self.message(mid)

    def head(self, target):
        with self.transaction():
            self._expire()
            row = self.db.execute("SELECT * FROM messages WHERE target=? AND status IN ('queued','delivering','uncertain','accepted') ORDER BY seq LIMIT 1", (target,)).fetchone()
        return self._row(row) if row else None

    def claim(self, mid):
        with self.transaction():
            self._expire()
            msg = self.message(mid)
            first = self.db.execute("SELECT id FROM messages WHERE target=? AND status IN ('queued','delivering','uncertain','accepted') ORDER BY seq LIMIT 1", (msg["target"],)).fetchone()
            if msg["status"] != "queued" or first is None or first[0] != mid:
                return False
            self._update(mid, "delivering", "durable intent; outcome not yet known")
            return True

    def outcome(self, mid, status, reason, receipt=None):
        if status not in ("queued", "uncertain", "accepted", "completed"):
            raise Error("invalid delivery outcome")
        with self.transaction():
            old = self.message(mid)
            if old["status"] not in ("delivering", "uncertain", "accepted"):
                raise Error("message has no in-flight delivery")
            if old["status"] == "accepted" and status != "completed":
                raise Error("accepted delivery may only advance to completed")
            self._update(mid, status, reason, receipt)
            if status in ("accepted", "completed") and old["status"] not in ("accepted", "completed") and old["loop_id"]:
                self.db.execute("UPDATE loops SET due=?+interval WHERE id=?", (self.clock(), old["loop_id"]))
        return self.message(mid)

    def pending_reason(self, mid, reason):
        with self.transaction():
            self.db.execute("UPDATE messages SET reason=?,updated=? WHERE id=? AND status='queued'", (reason[:512], self.clock(), mid))

    def receive(self, target):
        """Receiver-initiated claim. Caller must be at its own idle boundary."""
        if self.target(target)["adapter"] is not None:
            raise Error("receive is only available for pull mailboxes")
        with self.target_lock(target):
            self.tick()
            msg = self.head(target)
            if msg is None:
                return {"target": target, "message": None}
            if msg["status"] != "queued":
                return {"target": target, "message": None, "blocked_by": msg["id"], "status": msg["status"]}
            if not self.claim(msg["id"]):
                return {"target": target, "message": None}
            # Commit before output. A lost response is not permission to re-deliver.
            return {"target": target, "message": self.message(msg["id"])}

    def acknowledge(self, target, mid, completed=False):
        if self.target(target)["adapter"] is not None:
            raise Error("adapter receipts must come from the adapter")
        with self.target_lock(target):
            msg = self.message(mid)
            if msg["target"] != target:
                raise Error("recipient does not match")
            wanted = "completed" if completed else "accepted"
            if msg["status"] in (wanted, "completed"):
                return msg
            return self.outcome(mid, wanted, "receiver explicitly reported " + wanted,
                                {"verification": "receiver-report", "target": target, "operation_id": mid})

    def create_loop(self, target, text, sender, interval, loop_id=None, max_wakes=None):
        self.target(target)
        message_text(text)
        identifier(sender)
        number(interval, "interval")
        lid = identifier(loop_id or uuid.uuid4().hex)
        if max_wakes is not None and (type(max_wakes) is not int or max_wakes < 1):
            raise Error("max_wakes must be a positive integer")
        with self.transaction():
            if self.db.execute("SELECT 1 FROM loops WHERE id=?", (lid,)).fetchone():
                raise Error("loop ID already exists")
            self.db.execute("INSERT INTO loops VALUES (?,?,?,?,1,?,?,'paused',?,0)",
                            (lid, target, sender, text, interval, self.clock() + interval, max_wakes))
        return self.loop(lid)

    def loop(self, lid):
        row = self.db.execute("SELECT * FROM loops WHERE id=?", (lid,)).fetchone()
        if not row:
            raise Error("unknown loop")
        return dict(row)

    def set_next(self, lid, text, revision):
        message_text(text)
        with self.transaction():
            old = self.loop(lid)
            if old["revision"] != revision:
                raise Error("stale revision; reread the loop before updating")
            self.db.execute("UPDATE loops SET text=?,revision=revision+1 WHERE id=?", (text, lid))
        return self.loop(lid)

    def loop_state(self, lid, state):
        if state not in ("active", "paused", "stopped"):
            raise Error("invalid loop state")
        with self.transaction():
            old = self.loop(lid)
            if old["state"] == "stopped" and state != "stopped":
                raise Error("stopped loops cannot resume; create a new loop")
            if state == "active" and old["state"] != "active":
                self.db.execute("UPDATE loops SET due=?+interval WHERE id=?", (self.clock(), lid))
            self.db.execute("UPDATE loops SET state=? WHERE id=?", (state, lid))
            if state in ("paused", "stopped"):
                self.db.execute("UPDATE messages SET status='cancelled',reason=?,updated=? WHERE loop_id=? AND status='queued'", ("loop " + state, self.clock(), lid))
        result = self.loop(lid)
        result["in_flight"] = [r[0] for r in self.db.execute("SELECT id FROM messages WHERE loop_id=? AND status IN ('delivering','uncertain','accepted')", (lid,))]
        return result

    def tick(self):
        generated = []
        with self.transaction():
            self._expire()
            for row in self.db.execute("SELECT * FROM loops WHERE state='active' AND due<=? ORDER BY due,id", (self.clock(),)).fetchall():
                if self.db.execute("SELECT 1 FROM messages WHERE loop_id=? AND status IN ('queued','delivering','uncertain','accepted')", (row["id"],)).fetchone():
                    continue
                if row["max_wakes"] is not None and row["wakes"] >= row["max_wakes"]:
                    self.db.execute("UPDATE loops SET state='stopped' WHERE id=?", (row["id"],))
                    continue
                mid = uuid.uuid4().hex
                try:
                    self._enqueue(row["target"], row["text"], row["sender"], mid, None, row["id"], row["revision"])
                except QueueFull:
                    continue  # A full recipient must not starve other schedules.
                self.db.execute("UPDATE loops SET wakes=wakes+1,due=?+interval WHERE id=?", (self.clock(), row["id"]))
                generated.append(mid)
        return generated

    def prune(self, age):
        number(age, "age")
        with self.transaction():
            cursor = self.db.execute("DELETE FROM messages WHERE status IN ('completed','cancelled','expired') AND updated<?", (self.clock() - age,))
        return {"removed": cursor.rowcount, "deduplication": "IDs of removed records may be reused"}
