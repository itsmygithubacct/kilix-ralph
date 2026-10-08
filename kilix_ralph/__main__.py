"""CLI; JSON stdout is the public interface. Errors use stderr and exit 1."""
import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import sys
import time

from .store import Error, MAX_TEXT, Store, number
from .worker import Worker


def read_text(args):
    if args.text is not None:
        return args.text
    if args.message_file == "-":
        raw = sys.stdin.buffer.read(MAX_TEXT + 1)
    else:
        with open(args.message_file, "rb") as stream:
            raw = stream.read(MAX_TEXT + 1)
    if len(raw) > MAX_TEXT:
        raise Error("message file exceeds 65536 bytes")
    return raw.decode("utf-8")


def source():
    # Same-user provenance, not a cross-user authentication claim.
    session = os.environ.get("KITTY_PTY_BROKER_SESSION")
    return "pty:" + session if session else "user:" + str(os.getuid())


def parser():
    p = argparse.ArgumentParser(description="Durable idle-only mailboxes and recurring follow-ups")
    p.add_argument("--state-dir", type=Path, help="private host-local state directory")
    sub = p.add_subparsers(dest="command", required=True)
    target = sub.add_parser("target", help="bind an immutable conversation identity")
    ts = target.add_subparsers(dest="action", required=True)
    add = ts.add_parser("add")
    add.add_argument("--target", required=True, help="stable provider/conversation/generation ID")
    mode = add.add_mutually_exclusive_group(required=True)
    mode.add_argument("--mailbox", action="store_true")
    mode.add_argument("--adapter-command-json", help="JSON argv; first entry must be an absolute executable")
    ts.add_parser("list")
    native = ts.add_parser("codex", help="bind a loaded Codex shared app-server thread")
    native.add_argument("--target", required=True)
    native.add_argument("--thread", required=True)
    native.add_argument("--socket", type=Path)
    claude = sub.add_parser("claude-serve", help="own a Claude streaming session with guarded input")
    claude.add_argument("--target", required=True)
    claude.add_argument("--cwd", type=Path, required=True)
    claude.add_argument("--model", default="haiku")
    claude.add_argument("--effort", choices=("low", "medium", "high"), default="low")
    claude.add_argument("--dangerously-skip-permissions", action="store_true")
    for name in ("enqueue", "create"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--target", required=True)
        text = cmd.add_mutually_exclusive_group(required=True)
        text.add_argument("--text")
        text.add_argument("--message-file")
        if name == "enqueue":
            cmd.add_argument("--message-id")
            cmd.add_argument("--ttl", type=float, help="seconds until an unclaimed message expires")
        else:
            cmd.add_argument("--loop-id")
            cmd.add_argument("--interval", type=float, required=True, help="seconds between wakes")
            cmd.add_argument("--max-wakes", type=int, help="maximum scheduled wakes, including cancelled ones")
    q = sub.add_parser("queue")
    q.add_argument("--target")
    q.add_argument("--history", action="store_true")
    q.add_argument("--json", action="store_true", help="output is always JSON")
    message = sub.add_parser("message")
    ms = message.add_subparsers(dest="action", required=True)
    for name in ("status", "cancel"):
        cmd = ms.add_parser(name)
        cmd.add_argument("message_id")
        cmd.add_argument("--json", action="store_true")
    receive = sub.add_parser("receive", help="pull one message at the receiver's idle boundary; no keyboard injection")
    receive.add_argument("--target", required=True)
    receive.add_argument("--idle", action="store_true", required=True, help="receiver explicitly asserts it is ready for a new turn")
    for name in ("ack", "complete"):
        cmd = sub.add_parser(name)
        cmd.add_argument("message_id")
        cmd.add_argument("--target", required=True)
    for name in ("status", "pause", "resume", "stop", "run"):
        cmd = sub.add_parser(name)
        cmd.add_argument("loop_id")
        cmd.add_argument("--json", action="store_true")
        if name == "run":
            cmd.add_argument("--once", action="store_true")
            cmd.add_argument("--poll", type=float, default=1)
    update = sub.add_parser("set-next")
    update.add_argument("loop_id")
    update.add_argument("--expected-revision", type=int, required=True)
    text = update.add_mutually_exclusive_group(required=True)
    text.add_argument("--text")
    text.add_argument("--message-file")
    serve = sub.add_parser("serve", help="service registered adapters and recurring schedules")
    serve.add_argument("--once", action="store_true")
    serve.add_argument("--poll", type=float, default=1)
    prune = sub.add_parser("prune", help="remove terminal receipts; their IDs cease to be deduplicated")
    prune.add_argument("--older-than", type=float, required=True, help="retention age in seconds")
    return p


def emit(value):
    print(json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)


def service(store, args):
    number(args.poll, "poll interval")
    if args.poll < 0.05:
        raise Error("poll interval must be at least 0.05 seconds")
    stopping = False

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    worker = Worker(store)
    try:
        while not stopping:
            if args.command == "run" and store.loop(args.loop_id)["state"] != "active":
                break
            result = worker.once()
            if args.once:
                return result
            # No prompt content in continuous service logs.
            if result["generated"]:
                emit({"generated": result["generated"]})
            deadline = time.monotonic() + args.poll
            while not stopping and time.monotonic() < deadline:
                time.sleep(min(0.2, max(0, deadline - time.monotonic())))
        return {"worker": "stopped", "pending_messages_preserved": True}
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def dispatch(store, args):
    cmd = args.command
    if cmd == "claude-serve":
        from .claude import ClaudeBridge
        stopping = False
        def stop(signum, frame):
            nonlocal stopping
            stopping = True
        previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
        bridge = None
        try:
            bridge = ClaudeBridge(store, args.target, args.cwd, args.model, args.effort, args.dangerously_skip_permissions)
            bridge.serve(ready=emit, stopping=lambda: stopping)
            return {"bridge": "stopped", "target": args.target}
        finally:
            if bridge:
                bridge.close()
            for sig, handler in previous.items():
                signal.signal(sig, handler)
    if cmd == "target":
        if args.action == "list":
            return [store.target(row[0]) for row in store.db.execute("SELECT id FROM targets ORDER BY id")]
        if args.action == "codex":
            from .codex import bind, default_socket
            return bind(store, args.target, args.thread, args.socket or default_socket())
        adapter = None if args.mailbox else json.loads(args.adapter_command_json)
        return store.add_target(args.target, adapter)
    if cmd == "enqueue":
        existing = args.message_id and store.db.execute("SELECT 1 FROM messages WHERE id=?", (args.message_id,)).fetchone()
        if not existing and store.target(args.target)["adapter"] is not None and not store.worker_running():
            raise Error("no Ralph worker is running; start 'kilix ralph serve' before enqueueing to an adapter")
        return store.enqueue(args.target, read_text(args), source(), args.message_id, args.ttl)
    if cmd == "queue":
        return store.queue(args.target, args.history)
    if cmd == "message":
        return store.cancel(args.message_id) if args.action == "cancel" else store.message(args.message_id)
    if cmd == "receive":
        return store.receive(args.target)
    if cmd in ("ack", "complete"):
        return store.acknowledge(args.target, args.message_id, cmd == "complete")
    if cmd == "create":
        return store.create_loop(args.target, read_text(args), source(), args.interval, args.loop_id, args.max_wakes)
    if cmd == "set-next":
        return store.set_next(args.loop_id, read_text(args), args.expected_revision)
    if cmd == "status":
        return store.loop(args.loop_id)
    if cmd in ("pause", "resume", "stop"):
        return store.loop_state(args.loop_id, {"pause": "paused", "resume": "active", "stop": "stopped"}[cmd])
    if cmd in ("serve", "run"):
        with store.target_lock("ralph:service"):
            if cmd == "run":
                store.loop_state(args.loop_id, "active")
            return service(store, args)
    if cmd == "prune":
        return store.prune(args.older_than)
    raise Error("unknown command")


def main(argv=None):
    args = parser().parse_args(argv)
    store = None
    try:
        store = Store(args.state_dir)
        emit(dispatch(store, args))
        return 0
    except (Error, OSError, ValueError, sqlite3.Error) as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 1
    finally:
        if store is not None:
            store.close()


if __name__ == "__main__":
    sys.exit(main())
