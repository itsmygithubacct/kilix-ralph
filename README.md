# kilix-ralph

Durable one-shot messages and recurring follow-ups for agent conversations.
Python 3.10+ and its standard library are the only runtime dependencies.

Messages are ordered per recipient. A message remains pending until the
recipient is ready for a new turn; completion of that turn releases the next
message. Queue acceptance, delivery and task completion are separate facts.

Two delivery modes are available:

- **Pull mailbox:** the recipient calls `receive --idle` at its own idle
  boundary, then reports acknowledgment and completion. No terminal text or
  keys are injected. This works without a background worker for one-shot mail.
- **Guarded adapter:** a host worker calls a provider adapter that atomically
  checks idle state and submits, then reconciles durable receipts. Adapters
  must implement the [protocol](ADAPTER.md). There is no bundled Codex/Claude
  terminal adapter: the current screen-check/Enter API cannot enforce this
  guard. Registering a target does not attach to an existing TUI automatically.

The pull API is cooperative: `--idle` is the receiver's assertion, not an
independent detector of application state. For an existing interactive agent,
its harness must call it between turns. Ralph does not awaken a stock idle
Codex or Claude terminal by itself in this version.

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
cancelled and expired records. **Deduplication ends when a receipt is pruned.**
Inspect and back up state before pruning if old operation IDs may be retried.

## Tests

`make check` runs deterministic tests with private state directories and
synthetic subprocess adapters. Tests do not invoke models, connect to real
sessions, or type into terminals.
