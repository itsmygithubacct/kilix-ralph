# Recurring follow-ups

Loops that enqueue the next message on an interval, and how they are paused,
updated and stopped.

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
