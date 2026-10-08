"""Synthetic streaming CLI with explicit acknowledgment and result."""
import json
import sys
import time

for line in sys.stdin:
    message = json.loads(line)
    print(json.dumps(message), flush=True)
    time.sleep(.2)
    print(json.dumps({"type": "result", "subtype": "success", "session_id": message["session_id"],
        "uuid": "synthetic-result", "is_error": False, "result": "synthetic reply"}), flush=True)
