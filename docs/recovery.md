# Recovery, retention and service diagnostics

What can be cancelled or recovered, where state lives, the queue limits,
pruning, and how workers report and restart.

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

## Service diagnostics and restart admission

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
