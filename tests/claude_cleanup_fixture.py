"""Bounded process/stream fixtures; no provider, model, or network access."""
import json
import os
from pathlib import Path
import signal
import sys
import time


mode, record = sys.argv[1], Path(sys.argv[2])
signal.signal(signal.SIGTERM, signal.SIG_DFL)
signal.alarm(15)

if mode in ("term-child", "early-child"):
    reader, writer = os.pipe()
    leader = os.getpid()
    child = os.fork()
    if child == 0:
        os.close(reader)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.alarm(15)
        # The early-exit case must expose the leader's output EOF to serve().
        os.close(0)
        os.close(1)
        temporary = record.with_suffix(".tmp")
        temporary.write_text(json.dumps({"pid": os.getpid(), "leader": leader,
            "group": os.getpgrp(), "session": os.getsid(0)}))
        temporary.replace(record)
        os.write(writer, b"1")
        os.close(writer)
        time.sleep(15)
        os._exit(0)
    os.close(writer)
    os.read(reader, 1)
    os.close(reader)
    if mode == "early-child":
        os._exit(0)
    time.sleep(15)
elif mode == "pending":
    record.write_text(sys.stdin.readline())
    time.sleep(15)
else:
    raise SystemExit("unknown fixture mode")
