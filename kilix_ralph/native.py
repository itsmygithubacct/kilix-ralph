"""Private provider receipts and same-user Unix JSON-RPC transport."""
import json
import base64
import hashlib
import os
from pathlib import Path
import socket
import stat
import struct
import time

from .store import Error, Store, identifier


class Ledger:
    def __init__(self, root):
        self.store = Store(Path(root) / "providers")
        with self.store.transaction():
            self.store.db.execute("""CREATE TABLE IF NOT EXISTS receipts (
                target TEXT NOT NULL, id TEXT NOT NULL, digest TEXT NOT NULL,
                payload TEXT NOT NULL, receipt TEXT, PRIMARY KEY(target,id))""")

    def close(self):
        self.store.close()

    def get(self, target, mid):
        row = self.store.db.execute("SELECT * FROM receipts WHERE target=? AND id=?", (target, mid)).fetchone()
        if row is None:
            return None
        return {**dict(row), "receipt": json.loads(row["receipt"]) if row["receipt"] else None}

    def intent(self, target, mid, digest, payload):
        identifier(mid)
        if not isinstance(digest, str) or len(digest) != 64:
            raise Error("invalid operation digest")
        with self.store.transaction():
            old = self.get(target, mid)
            if old:
                if old["digest"] != digest or old["payload"] != payload:
                    raise Error("provider receipt conflicts with this operation")
                return False
            self.store.db.execute("INSERT INTO receipts VALUES (?,?,?,?,NULL)", (target, mid, digest, payload))
        return True

    def save(self, target, mid, receipt):
        with self.store.transaction():
            self.store.db.execute("UPDATE receipts SET receipt=? WHERE target=? AND id=?",
                                  (json.dumps(receipt), target, mid))
        return receipt


class UnixRPC:
    def __init__(self, path, timeout=10):
        path = Path(path)
        info = path.lstat()
        # Codex publishes an owned symlink to its short runtime socket path.
        if stat.S_ISLNK(info.st_mode) and info.st_uid == os.getuid():
            path = path.resolve(strict=True)
            info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
            raise Error("provider endpoint must be an owned Unix socket")
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            self.socket.settimeout(timeout)
            self.socket.connect(str(path))
            _, uid, _ = struct.unpack("3i", self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
            if uid != os.getuid():
                raise Error("provider peer has a different UID")
        except BaseException:
            self.socket.close()
            raise
        self.timeout = timeout
        self.buffer = bytearray()
        self.serial = 0

    def close(self):
        self.socket.close()

    def send(self, value):
        self.socket.sendall((json.dumps(value, ensure_ascii=False) + "\n").encode())

    def read(self, deadline):
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Error("provider RPC deadline expired")
            self.socket.settimeout(remaining)
            block = self.socket.recv(65536)
            if not block:
                raise Error("provider closed the connection")
            self.buffer.extend(block)
            if len(self.buffer) > 16 * 1024 * 1024:
                raise Error("provider response exceeded 16 MiB")
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = bytearray(rest)
        value = json.loads(line)
        if not isinstance(value, dict):
            raise Error("provider returned invalid JSON-RPC")
        return value

    def call(self, method, params):
        self.serial += 1
        self.send({"id": self.serial, "method": method, "params": params})
        deadline = time.monotonic() + self.timeout
        for _ in range(4096):
            reply = self.read(deadline)
            if reply.get("id") == self.serial and "method" not in reply:
                if "error" in reply:
                    raise Error("provider rejected " + method + ": " + str(reply["error"])[:512])
                if not isinstance(reply.get("result"), dict):
                    raise Error("provider omitted RPC result")
                return reply["result"]
        raise Error("provider notification limit exceeded")


class CodexSocket(UnixRPC):
    """RFC 6455 over the peer-credential-checked local control socket."""
    def __init__(self, path, timeout=10):
        super().__init__(path, timeout)
        try:
            key = base64.b64encode(os.urandom(16)).decode()
            self.socket.sendall(("GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + key + "\r\n\r\n").encode())
            deadline = time.monotonic() + timeout
            while b"\r\n\r\n" not in self.buffer:
                self.socket.settimeout(max(.001, deadline - time.monotonic()))
                chunk = self.socket.recv(4096)
                if not chunk or time.monotonic() >= deadline or len(self.buffer) > 16384:
                    raise Error("invalid Codex WebSocket handshake")
                self.buffer.extend(chunk)
            headers, _, rest = self.buffer.partition(b"\r\n\r\n")
            self.buffer = bytearray(rest)
            lines = headers.decode("ascii").split("\r\n")
            fields = dict(line.lower().split(":", 1) for line in lines[1:] if ":" in line)
            # Preserve case for the base64 accept value.
            accept = next((line.split(":", 1)[1].strip() for line in lines[1:]
                           if line.lower().startswith("sec-websocket-accept:")), "")
            expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
            if not lines[0].startswith("HTTP/1.1 101 ") or accept != expected or fields.get("upgrade", "").strip() != "websocket":
                raise Error("Codex endpoint did not accept WebSocket transport")
        except BaseException:
            self.close()
            raise

    def frame(self, payload, opcode=1):
        mask = os.urandom(4)
        length = len(payload)
        header = bytes([0x80 | opcode])
        if length < 126:
            header += bytes([0x80 | length])
        elif length <= 65535:
            header += bytes([0x80 | 126]) + struct.pack("!H", length)
        else:
            header += bytes([0x80 | 127]) + struct.pack("!Q", length)
        encoded = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
        self.socket.settimeout(self.timeout)
        self.socket.sendall(header + mask + encoded)

    def send(self, value):
        self.frame(json.dumps(value, ensure_ascii=False).encode())

    def exact(self, count, deadline):
        while len(self.buffer) < count:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise Error("Codex WebSocket timed out")
            self.socket.settimeout(remaining)
            chunk = self.socket.recv(min(65536, count - len(self.buffer)))
            if not chunk:
                raise Error("Codex WebSocket closed")
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:count])
        del self.buffer[:count]
        return result

    def read(self, deadline):
        payload = bytearray()
        fragmented = False
        while True:
            first, second = self.exact(2, deadline)
            opcode, final = first & 15, bool(first & 0x80)
            if first & 0x70 or second & 0x80:
                raise Error("unsupported Codex WebSocket frame flags")
            length = second & 127
            if length == 126:
                length = struct.unpack("!H", self.exact(2, deadline))[0]
            elif length == 127:
                length = struct.unpack("!Q", self.exact(8, deadline))[0]
            if length > 16 * 1024 * 1024 or len(payload) + length > 16 * 1024 * 1024:
                raise Error("Codex WebSocket message exceeds 16 MiB")
            if opcode >= 8 and (length > 125 or not final):
                raise Error("invalid Codex WebSocket control frame")
            part = self.exact(length, deadline)
            if opcode == 8:
                raise Error("Codex closed WebSocket transport")
            if opcode == 9:
                self.frame(part, 10)
                continue
            if opcode == 10:
                continue
            if opcode == 1 and not fragmented:
                fragmented = True
            elif opcode != 0 or not fragmented:
                raise Error("unexpected Codex WebSocket opcode")
            payload.extend(part)
            if final:
                value = json.loads(payload)
                if not isinstance(value, dict):
                    raise Error("Codex returned invalid JSON-RPC")
                return value

