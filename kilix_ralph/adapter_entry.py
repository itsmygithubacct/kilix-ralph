"""Absolute entrypoint used by pinned source installations, without PYTHONPATH."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kilix_ralph.codex import Codex
from kilix_ralph.store import Error


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--state-dir", required=True)
    sub = p.add_subparsers(dest="provider", required=True)
    codex = sub.add_parser("codex")
    codex.add_argument("--socket", required=True)
    codex.add_argument("--thread", required=True)
    codex.add_argument("--target", required=True)
    claude = sub.add_parser("claude")
    claude.add_argument("--socket", required=True)
    claude.add_argument("--target", required=True)
    args = p.parse_args()
    adapter = None
    try:
        raw = sys.stdin.buffer.read(131073)
        if len(raw) > 131072:
            raise Error("adapter request is too large")
        request = json.loads(raw)
        if args.provider == "claude":
            from kilix_ralph.claude import call_bridge
            if request.get("target") != args.target:
                raise Error("wrong Claude target")
            print(json.dumps(call_bridge(args.socket, request)), flush=True)
            return 0
        adapter = Codex(args.state_dir, args.socket, args.thread, args.target)
        print(json.dumps(adapter.handle(request)), flush=True)
        return 0
    except (Error, OSError, ValueError, KeyError, TypeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if adapter:
            adapter.close()


if __name__ == "__main__":
    sys.exit(main())
