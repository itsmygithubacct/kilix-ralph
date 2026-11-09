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

Ships in Plebian-OS 0.2.3 as a submodule of
[Kilix](https://github.com/itsmygithubacct/kilix#readme)
(`third_party/kilix-ralph`). Kilix exposes it as `kilix ralph`, which loads
the pinned module without starting the terminal engine. No service is enabled
at installation.

## Quick start

A pull mailbox needs no background worker. On the host where the receiving
session runs:

```sh
kilix ralph target add --target codex:conversation-123:generation-1 --mailbox
kilix ralph enqueue --target codex:conversation-123:generation-1 --text "Run the checks."
# The receiver, at its own idle boundary:
kilix ralph receive --target codex:conversation-123:generation-1 --idle
kilix ralph ack MESSAGE_ID --target codex:conversation-123:generation-1
kilix ralph complete MESSAGE_ID --target codex:conversation-123:generation-1
```

From a checkout, `python3 -m kilix_ralph` takes the same arguments, and
installing the package provides `kilix-ralph`. Every result is JSON.

## Usage

| Command | What it does |
|---|---|
| `target add --target ID --mailbox` / `--adapter-command-json JSON` | Bind a pull mailbox or a guarded adapter to an immutable target |
| `target codex --target ID --thread UUID [--socket PATH]` | Bind a loaded shared app-server thread |
| `target list` | Registered targets |
| `enqueue --target ID --text T\|--message-file F [--message-id ID] [--ttl S]` | Durably accept one message |
| `receive --target ID --idle`, `ack MSG --target ID`, `complete MSG --target ID` | Pull receiver: claim, acknowledge, report completion |
| `queue [--target ID] [--history]`, `message status\|cancel ID` | Inspect the queue; recover or cancel one message |
| `create --target ID --text T\|--message-file F --interval S [--max-wakes N] [--loop-id L]` | A recurring follow-up, created paused |
| `run L`, `status L`, `pause L`, `resume L`, `stop L`, `set-next L --expected-revision N --text T\|--message-file F` | Drive one loop |
| `serve [--once]` | The worker for adapters and schedules; one per store |
| `prune --older-than SECONDS` | Remove terminal receipts (their IDs stop deduplicating) |

`--state-dir PATH` goes before the subcommand. The managed streaming-session
bridge is described under the second automatic path below.

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

## How it works

```
 sender ──enqueue──> SQLite store (WAL, FULL sync, 0700)  $XDG_STATE_HOME/kilix/ralph
                          │ one queue per recipient, head of line only
          ┌───────────────┼──────────────────────────┐
   pull: receive --idle   adapter (JSON argv,        native provider paths:
   ack / complete         15 s, 128 KiB bounds)      shared app-server queue,
                                                     managed streaming session
                          │
                 accepted ──> completed ──> next message released
```

A message stays pending until its recipient is ready for a new turn, and
completion of that turn releases the next one. Each step (accepted by the
queue, claimed, acknowledged, completed) is a separate durable fact, so a lost
response or a crashed worker leaves a record to reconcile rather than a blind
resend. Loops add at most one outstanding wake each. The pull-mailbox details
are in [docs/mailbox.md](docs/mailbox.md), loops in
[docs/recurring.md](docs/recurring.md), and recovery, limits and pruning in
[docs/recovery.md](docs/recovery.md).

## Tests

`make check` runs deterministic tests with private state directories, synthetic
subprocess adapters and, when `codex` is installed, isolated real app-servers
connected to a loopback fake model. Set `RALPH_TEST_CODEX` to test a specific
server binary. These tests do not invoke paid models, touch user sessions or
type into terminals. Live Luna-low/Haiku-low smoke checks are separate.

## Documentation

| Document | Contents |
|---|---|
| [docs/mailbox.md](docs/mailbox.md) | Running the CLI, registering targets, enqueueing, pull receivers |
| [docs/recurring.md](docs/recurring.md) | Recurring follow-ups: create, run, pause, update, stop |
| [docs/recovery.md](docs/recovery.md) | Cancellation, recovery, state location, limits, pruning, service diagnostics |
| [ADAPTER.md](ADAPTER.md) | The guarded delivery adapter protocol |

## Related repositories

- [kilix](https://github.com/itsmygithubacct/kilix#readme): pins this module
  and exposes it as `kilix ralph`.
- [kitty-pty-broker](https://github.com/itsmygithubacct/kitty-pty-broker#readme):
  its session environment is the sender provenance Ralph records.

## Part of Plebian-OS

kilix-ralph is one of the repositories behind
[Plebian-OS](https://github.com/itsmygithubacct/plebian-os#readme), a Debian
install whose desktop is [Kilix](https://github.com/itsmygithubacct/kilix#readme)
running the [Kilix 95](https://github.com/itsmygithubacct/kilix-95#readme)
desktop through [Pleb](https://github.com/itsmygithubacct/pleb#readme).
It ships inside Kilix as the `third_party/kilix-ralph` submodule and is
reached through `kilix ralph`. The map of every repository and how it ships
is on the
[components page](https://github.com/itsmygithubacct/plebian-os/blob/main/docs/components.md).

## License

MIT. See [LICENSE](LICENSE).
