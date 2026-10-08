"""Synthetic provider for subprocess tests. Never connects to a real session."""
import json
from pathlib import Path
import sys
import time

path = Path(sys.argv[1])
state = json.loads(path.read_text())
request = json.load(sys.stdin)
op = request["operation"]
response = {"schema": "kilix.ralph.adapter/v1", "target": request["target"]}
if op == "observe":
    response.update(state=state.get("state", "idle"), composer_empty=True, idle_token="generation-1",
                    capabilities={c: True for c in ("guarded_idle_submit", "durable_reconciliation",
                                  "completion_evidence", "bound_conversation")})
else:
    mid = request["operation_id"]
    response["operation_id"] = mid
    if op == "submit":
        time.sleep(state.get("delay", 0))
        state["submits"] = state.get("submits", 0) + 1
        if state.get("race"):
            response.update(status="not_submitted", no_effects=True)
        else:
            state.setdefault("receipts", {})[mid] = "accepted"
            response.update(status="accepted", evidence={"turn": mid})
        path.write_text(json.dumps(state))
        if state.get("lose_response"):
            sys.exit(2)
    else:
        status = state.get("receipts", {}).get(mid)
        if status:
            response.update(status=status, evidence={"turn": mid})
        else:
            response.update(status="uncertain")
print(json.dumps(response))
