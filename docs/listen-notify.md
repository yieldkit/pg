# LISTEN and NOTIFY

`notifyAfterCommit` sends `NOTIFY` once the write commits. `listenWithRetry` keeps a `LISTEN` open across reconnects.

<!-- prettier-ignore -->
```ts
import { listenWithRetry, makePgLive, notifyAfterCommit, PgClient } from '@yieldkit/pg';
import { Deferred, Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const program = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const pg = yield* PgClient.PgClient;
  const listening = yield* Deferred.make<void>();

  yield* Effect.forkScoped(
    listenWithRetry('note_created', (payload) => Effect.log(`note ${payload} created`), {
      onStatusChange: (status) =>
        status._tag === 'Connected' ? Deferred.succeed(listening, undefined) : Effect.void,
    }),
  );
  yield* Deferred.await(listening);                           // a NOTIFY sent before LISTEN is confirmed is never delivered

  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* sql`INSERT INTO note ${sql.insert({ id: 1, body: 'hello' })}`;
      yield* notifyAfterCommit(sql, pg, 'note_created', '1'); // sent only after COMMIT
    }),
  );
});

// A real service keeps the scope open; here the listener stops as soon as program returns.
await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(makePgLive())));
```

## Notify

`notifyAfterCommit(sql, pg, channel, payload)` runs the `NOTIFY` after the surrounding `sql.withTransaction` commits. If it then fails, it logs `pg.notify.failed` (`channel`, `payloadBytes`, `errorMessage`) and the write stands.

A bad channel or payload fails with `PgChannelError` before the commit:

- a channel must match `[A-Za-z_][A-Za-z0-9_]{0,62}`;
- a payload must be shorter than 8000 bytes of UTF-8 and must not contain a NUL character. An empty payload is fine.

`validateNotifyChannel` and `validateNotifyPayload` run the same checks without connecting:

<!-- prettier-ignore -->
```ts
import { validateNotifyChannel, validateNotifyPayload } from '@yieldkit/pg';

validateNotifyChannel('note created');                   // fails with PgChannelError: a space
validateNotifyPayload('note_created', 'x'.repeat(8000)); // fails with PgChannelError: 8000 bytes
```

## Listen

`listenWithRetry(channel, handler, options?)` listens until its fiber is interrupted. The handler runs one payload at a time; the rest wait in a buffer of `bufferCapacity`:

```ts
import { listenWithRetry } from '@yieldkit/pg';
import { Effect, Queue } from 'effect';

const listen = Effect.gen(function* () {
  const jobs = yield* Queue.dropping<string>(10_000); // offer never waits
  yield* listenWithRetry('job_queued', (payload) => Effect.asVoid(Queue.offer(jobs, payload)), {
    bufferCapacity: 4096, // default LISTEN_BUFFER_CAPACITY, 1024
    onStatusChange: (status) => Effect.log(`job_queued listener: ${status._tag}`),
  });
});
```

Keep the handler fast, for example by handing the payload to a queue that never blocks, as above.

- When the buffer is full, the listener lets the handler catch up once, then drops the newest payloads, counts them in `pg_listen_dropped_total` and logs `pg.listen.overflow` once per overflow. Size `bufferCapacity` for the largest burst a slow handler must absorb.
- When the connection drops, the handler still gets the payloads already buffered.
- A failing handler ends the session. Payloads still buffered are discarded, counted and logged as `pg.listen.backlog_discarded`, and a new session starts.

Each listener holds one pooled connection while it runs, so size `maxConnections` for the listeners plus the queries.

## Reconnects

Every session that ends, cleanly or not, is followed by a new one. The delay starts at `LISTEN_RETRY_BASE` (500 ms), doubles up to `LISTEN_RETRY_CAP` (30 seconds) and is multiplied by a random factor from 0.8 to 1.2. It resets only after a session that stayed up for `LISTEN_STABLE_AFTER` (5 seconds), so a server that keeps dropping the listener gets growing delays.

The first session logs `pg.listen.connected`, later ones `pg.listen.reconnected`. Each end logs `pg.listen.disconnected` (`reason: 'closed'` or `errorMessage`), each failure `pg.listen.failed`, and each wait `pg.listen.retry_scheduled` at debug.

## Status

`onStatusChange` receives every `ListenStatus`, in order, on the listener fiber, which waits for it. Keep it short.

| `ListenStatus`                                 | When                                                                                           |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `Connecting`                                   | Once, before the first attempt                                                                 |
| `Connected { since }`                          | PostgreSQL confirmed `LISTEN`; `since` is epoch milliseconds                                   |
| `Reconnecting { attempt, delayMs, lastError }` | Before each backoff sleep; `attempt` starts at 1, `lastError` is `undefined` after a clean end |

## Metrics

| Metric                    | Export              | Kind    | Value                                                                    |
| ------------------------- | ------------------- | ------- | ------------------------------------------------------------------------ |
| `pg_listen_connected`     | `pgListenConnected` | gauge   | `1` while `LISTEN` is confirmed, `0` otherwise                           |
| `pg_listen_dropped_total` | `pgListenDropped`   | counter | Payloads dropped because the buffer was full or discarded with a session |

Both carry a `channel` attribute and are removed when the listener fiber ends, so dynamic channel names leave no series behind.

## Limits

- Payloads sent while the listener reconnects are never delivered by PostgreSQL, and are not counted in `pg_listen_dropped_total`.
- Channel names are case-sensitive, because `LISTEN` is sent with the name quoted. From SQL use `pg_notify('NoteCreated', ...)` or `NOTIFY "NoteCreated"`. An unquoted `NOTIFY NoteCreated` goes to `notecreated`.
- `listenWithRetry` fails only with `PgChannelError`, for an invalid channel or a `bufferCapacity` that is not an integer of at least 1.
- `lastError` and the `errorMessage` of `pg.listen.failed` carry the driver's message for a connection failure. For anything the handler raises (failure, defect or thrown exception) they carry only the `_tag` or error name, so payload content never reaches the logs. A `SqlError` from the handler's own query is reported as `SqlError`.
- A defect or exception in `onStatusChange` is logged as `pg.listen.status_callback_failed`, and the listener goes on.
- The metrics assume one listener per channel per process. Two listeners on one channel share the series.
- PgBouncer in transaction mode does not keep `LISTEN` (see [Behind PgBouncer](pool-and-timeouts.md#behind-pgbouncer)).
