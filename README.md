# kilix-ralph

Durable one-shot messages and recurring follow-ups for agent conversations.
The core requires Linux and Python 3.10+ with its standard library. Automatic
provider paths additionally require the corresponding Codex or Claude CLI.

Messages are ordered per recipient. A message remains pending until the
recipient is ready for a new turn; completion of that turn releases the next
message. Queue acceptance, delivery and task completion are separate facts.

Three built-in delivery paths are available:

- **Pull mailbox:** the recipient calls `receive --idle` at its own idle
  boundary, then reports acknowledgment and completion. No terminal text or
  keys are injected. This works without a background worker for one-shot mail.
- **Codex shared app-server:** binds a loaded conversation and uses its native
  idle queue. A competing human turn runs first; the message becomes a separate
  turn afterward. The adapter never calls `turn/start` or `turn/steer` and never
  injects keystrokes. Message IDs in turn history provide recovery evidence.
- **Managed Claude streaming session:** Ralph owns a persistent input stream
  and admits one message at a time. Exact user-message acknowledgment and a
  session-bound result release the next message. Busy or stale idle tokens
  cannot submit. This creates a managed session; it does not take over an
  already-running interactive Claude terminal.

Custom adapters can implement the [protocol](ADAPTER.md).

The pull API is cooperative: `--idle` is the receiver's assertion, not an
independent detector of application state. A pull receiver must call it between
turns. The two automatic paths above require no polling by the receiving agent.

## Automatic Codex delivery

Use the exact thread ID from the Codex session on its execution host:

```sh
kilix ralph target codex --target reviewer --thread THREAD_UUID
kilix ralph serve
# From another session on that host:
kilix ralph enqueue --target reviewer --text "Please review the changes when idle."
```

The default endpoint is `$CODEX_HOME/app-server-control/app-server-control.sock`
(with the usual `~/.codex` default). Select a different server with `--socket`.
The adapter checks endpoint ownership and Unix peer credentials, then speaks
WebSocket JSON-RPC over that local socket. The shared server must already own
the thread; Ralph never resumes a second copy or changes its model/permissions.
Codex sessions using an embedded server (`--no-daemon`) are not reachable here.

This uses experimental native thread-queue methods, verified with Codex 0.161.0
and a live 0.162.0 server. Unsupported methods fail closed. Ralph waits for idle
before transferring a message; if a human turn wins that race, Codex's native
queue waits for it to end. A draft in the terminal composer is never read,
modified or submitted by Ralph. `provider_queued` means the provider owns the
pending message, `accepted` identifies its actual turn, and `completed` means
that turn ended. Completion evidence records failed/interrupted turns too;
it does not prove that the requested task succeeded.

## Automatic Claude delivery

Run the managed session on the execution host:

```sh
kilix ralph claude-serve --target reviewer-claude --cwd /path/to/project \
  --model haiku --effort low
# In another terminal, run the shared queue worker:
kilix ralph serve
# Send from any session using the same host-local state:
kilix ralph enqueue --target reviewer-claude --text "Review the latest changes."
```

Add `--dangerously-skip-permissions` when that mode is wanted. Settings and the
generated Claude session ID are durably bound to the target. Repeating the same
`claude-serve` command resumes a cleanly completed session. A previous unresolved
turn blocks restart delivery rather than replaying it. The bridge reports its
endpoint/session at startup; completed message status includes a bounded result
excerpt. Native Claude configuration and permissions still apply. An approval
or user-input wait never becomes an idle opportunity.

Only the bridge writes this session's input. Human messages should also use the
queue; do not independently resume the same Claude session concurrently. Closing
the bridge stops its owned Claude process and preserves receipts. Closing a
queue worker does not stop the bridge or a Codex session. For remote use, run
these commands through authenticated SSH on the host; do not expose the local
provider sockets as public listeners.

## Run

From this checkout, use `python3 -m kilix_ralph`. Installing the package exposes
`kilix-ralph`; the Kilix facade exposes the same arguments as `kilix ralph`.
All results are JSON; errors go to stderr with a nonzero exit status.

Register a stable conversation and generation identity on its execution host:

```sh
kilix ralph target add --target codex:conversation-123:generation-1 --mailbox
kilix ralph enqueue --target codex:conversation-123:generation-1 \
  --message-file request.txt --message-id request-42 --ttl 3600
kilix ralph queue --target codex:conversation-123:generation-1 --json
```

`enqueue` returns only after durable acceptance. Repeating an identical message
ID/request returns its existing record. A conflicting payload, recipient,
sender or TTL fails. Omit `--message-id` for a generated ID. Source provenance
comes from the caller's PTY broker environment, or its local UID. The state
directory belongs to one OS user; sender labels are not an authentication
boundary between processes running as that same user.

At the receiving harness's idle boundary:

```sh
kilix ralph receive --target codex:conversation-123:generation-1 --idle
kilix ralph ack MESSAGE_ID --target codex:conversation-123:generation-1
# Process the returned text as a message from the recorded sender.
kilix ralph complete MESSAGE_ID --target codex:conversation-123:generation-1
```

Present messages as session-originated messages, not direct human instructions.
Never infer permission escalation from their contents. A reply is a separate
explicit enqueue to the sender's mailbox; there is no automatic reply loop.

`receive` commits its claim before returning the text. A lost response holds
the recipient's queue: inspect `message status MESSAGE_ID` to recover the
original text and reconcile with the receiving harness. Repeating `receive`
does not send it again. `ack` reports that the receiver accepted the message;
`complete` reports that it finished the resulting turn. These are explicit
receiver reports, not independent proof of successful task execution.

## Recurring follow-ups

```sh
kilix ralph create --target codex:conversation-123:generation-1 \
  --message-file next.txt --interval 60 --max-wakes 20 --loop-id review
kilix ralph run review
```

Creation starts paused. `run` activates the loop and services the host queue
in the foreground. It also services other active loops/registered adapters in
the same state directory. For a long-lived worker use `kilix ralph serve`,
then `kilix ralph resume review`. Only one service process runs per store.
No service is enabled at installation or automatically resumed at boot.

Mailboxes still require an idle receiver to pull the messages. `receive` also
checks due schedules, so a polling receiver can operate without `serve`.
Adapter enqueue refuses when no worker is running. In the event of a worker
failure after acceptance, the durable queue survives until it is restarted.

```sh
kilix ralph status review --json
kilix ralph set-next review --expected-revision 1 --message-file next.txt
kilix ralph pause review
kilix ralph resume review
kilix ralph stop review
```

Revision checks prevent concurrent updates from overwriting each other. An
update affects future wakes, never an already queued message. There is at most
one outstanding wake per loop; missed intervals coalesce. After delivery the
next wake is due one interval later, and must also await turn completion.
`--max-wakes` counts scheduled wakes, including ones subsequently cancelled.

Pause/stop cancel unclaimed wakes and report any already in flight. Stop is
terminal for the loop and does not kill its agent. Resuming a paused loop
starts a new interval. Stopping the worker preserves queue/loop state.

## Recovery and retention

```sh
kilix ralph message status MESSAGE_ID --json
kilix ralph message cancel MESSAGE_ID
kilix ralph queue --history
kilix ralph prune --older-than 2592000
```

Cancellation and TTL apply only before a delivery claim. In-flight or uncertain
delivery cannot be recalled. An uncertain head message holds only its own
recipient's queue. Recovery reconciles the same operation ID; it never blindly
replays a submit. A provider may prove no effects and permit another guarded
attempt with the same ID. Exactly-once effects inside an agent are not promised.

State uses host-local SQLite with FULL synchronous WAL transactions and private
permissions under `$XDG_STATE_HOME/kilix/ralph` (default
`~/.local/state/kilix/ralph`). Override with `--state-dir PATH` before the
subcommand. Use a local filesystem, not a shared network database. Remote
callers should invoke the host CLI through their authenticated transport.

Limits: 64 KiB UTF-8 per message, 256 pending messages per recipient, 4096
pending globally and 16 MiB total pending text. Overflow fails explicitly.
Terminal receipts remain until explicitly pruned; prune removes only completed,
cancelled and expired records. **Queue deduplication ends when a receipt is
pruned.** Native provider ledgers under `providers/` retain their identity and
recovery records separately and are not pruned automatically. Inspect and back
up state before pruning if old operation IDs may be retried. Schema 2 migrates
schema 1 on open; older Ralph binaries refuse it rather than skipping the new
`provider_queued` state during rollback.

## Tests

`make check` runs deterministic tests with private state directories, synthetic
subprocess adapters and, when `codex` is installed, isolated real app-servers
connected to a loopback fake model. Set `RALPH_TEST_CODEX` to test a specific
server binary. These tests do not invoke paid models, touch user sessions or
type into terminals. Live Luna-low/Haiku-low smoke checks are separate.

### Service diagnostics and restart admission

A provider-owned queue receipt remains in flight when reconciliation fails or
contradicts earlier custody. It cannot become cancellable or eligible for a new
submit. Inspect `message status` and the provider ledger to resolve failures;
absence of a receipt is not proof of no effects.

Continuous workers report changed failure identities on stderr without prompt
text, provider output or result excerpts. Repeated unchanged failures are
coalesced until recovery. Detailed queue reasons remain available through
`message status`. Provider stderr is discarded; bridge status reports a closed
output stream or exited provider without retaining potentially sensitive output.
Bridge restart admission and resume arguments are refreshed under its exclusive
service lock. A pending prior turn blocks launch and preserves its receipts.

Process-group cleanup covers helpers that remain in the owned group. Detached
sessions and simultaneous bridge/provider death require external containment;
this module does not claim that stronger boundary. Final group signaling is
asynchronous, so return from cleanup alone is not proof of descendant exit.
