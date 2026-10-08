# Guarded delivery adapter protocol v1

A registered adapter is trusted code running as the queue's OS user. Its
executable and fixed arguments are registered as a JSON argv array; no shell
is used. Target bindings are immutable. Use a new target ID for a replacement
conversation/generation, never silently retarget an existing mailbox.

```sh
kilix ralph target add --target provider:conversation:generation \
  --adapter-command-json '["/absolute/path/provider-adapter", "--profile", "work"]'
kilix ralph serve
```

Each invocation receives one UTF-8 JSON object on stdin and returns one JSON
object on stdout. Diagnostics belong on stderr. Both output streams are bounded
to 128 KiB, and invocations have a 15-second deadline. The worker owns and cleans
up the invocation process group, including helpers. An adapter must contact an
independently managed provider; do not launch a persistent provider as a child
of the RPC process. Requests never contain shell commands derived from prompts.

Every request and response carries `schema: "kilix.ralph.adapter/v1"` and the
exact registered `target`. A response with mismatched identity or schema is an
error. The adapter must independently bind that identity to the actual provider
conversation, including a generation if IDs can be reused. Echoing the request
target without checking the provider is not a valid implementation.

## Observe

Request adds `operation: "observe"`. Example response:

```json
{
  "schema": "kilix.ralph.adapter/v1",
  "target": "provider:conversation:generation",
  "state": "idle",
  "composer_empty": true,
  "idle_token": "provider-generation-73",
  "capabilities": {
    "guarded_idle_submit": true,
    "durable_reconciliation": true,
    "completion_evidence": true,
    "bound_conversation": true
  }
}
```

Only exactly `idle` with an empty composer permits a claim. Working, waiting
for user/permission input, unreachable, unknown and stopped all defer. Silence
and absence of a spinner are insufficient evidence. The token binds the observed
idle generation; it must become stale when another turn or human input begins.

## Submit

Request adds `operation: "submit"`, `operation_id`, `digest`, `idle_token`,
`sender` and `text`. The digest binds the immutable queue request; the adapter
must durably reject reuse of an operation ID for a different digest/target.
Preserve text literally and label its sender; do not execute it as a command.

The provider must atomically check conversation identity, idle generation and
empty composer at the submission boundary. A failed guard must have no effects.
This cannot be implemented by reading a screen and then sending Enter as a
separate operation. Neither busy-session steering nor deferred input counts
as idle-only submission. There is no opt-out flag that weakens this requirement.

A provider-native queue is also valid when it guarantees a separate future
turn and never steers a busy turn or consumes a terminal draft. In this path,
`composer_empty: true` means no terminal composer is involved in the submission;
it does not assert that every attached frontend has an empty draft. The native
queue owns the final idle boundary if a competing turn invalidates the observed
idle token. Codex's native thread queue uses this path. Ordinary application
input buffered during a turn is not an equivalent guarantee.

Possible responses include the schema, target and matching `operation_id`:

- `status: "not_submitted", no_effects: true`: durable proof the operation
  neither submitted nor left input behind. The worker may obtain a fresh idle
  token and attempt the same operation ID later. An invocation still running
  elsewhere or a missing receipt alone cannot establish this proof.
- `status: "accepted", evidence: {...}`: provider acceptance for a specific
  turn. Evidence must be nonempty and identify the provider receipt/turn.
- `status: "provider_queued", evidence: {...}`: the provider or its exclusive
  bridge owns the pending message; it has not yet been acknowledged as a turn.
  This holds the recipient queue and requires reconciliation, not another send.
- `status: "completed", evidence: {...}`: that accepted turn has ended, with
  provider evidence. This is not a claim that its task was successful.
- `status: "uncertain"`: unknown outcome; hold the queue for reconciliation.

The queue commits an intent before calling submit. RPC errors, malformed
responses, timeout and process death therefore become uncertain delivery,
never a fresh message ID or a blind retry.

## Reconcile

Request adds `operation: "reconcile"`, `operation_id` and `digest`. This is
observation-only: never submit text or keys. Return the same statuses above.
After durable provider acceptance, the only forward transition is completion;
losing a receipt must not authorize replay. A recovered incomplete queue intent
always calls reconcile first, even if the worker may have died before submit.

The worker will not deliver the next message to that recipient until the prior
turn is completed. An old idle token or a momentarily unchanged idle screen
cannot release the next message. Adapter implementations must test human input
races, provider restarts, accepted-but-response-lost, stale generations and
completion evidence against their real provider. Capability flags are a
contract assertion, not independent verification by Ralph.

## Bundled providers

`target codex` binds a loaded shared app-server thread. Native queue acceptance
is reconciled through `clientUserMessageId` in the provider queue and `clientId`
in full turn items. Payloads must also match exactly. Missing history, unloaded
threads, failed pagination and lost receipts never authorize replay. The
adapter does not resume threads or change model/approval settings.

`claude-serve` owns a streaming-input CLI process and private local RPC socket.
One writer guards the idle token, stores intent before input, verifies the
replayed user message UUID/content/session, then waits for a bound result. A
bridge restart with an unresolved receipt blocks delivery. It does not claim
that a newly resumed process can determine whether a lost prior result occurred.
Both built-ins keep provider receipts separately from prunable queue history.
