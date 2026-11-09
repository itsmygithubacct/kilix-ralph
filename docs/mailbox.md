# Pull mailboxes

Registering a target, enqueueing, and the receiver's `receive`, `ack` and
`complete` reports at its idle boundary.

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
