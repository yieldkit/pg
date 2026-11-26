# Advisory locks

`withAdvisoryXactLock(key, effect)` runs `effect` in a transaction that holds a PostgreSQL advisory lock on `key`:

```ts
import { withAdvisoryXactLock } from '@yieldkit/pg';
import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const settleInvoice = (invoiceId: string) =>
  withAdvisoryXactLock(
    `invoice:${invoiceId}`,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE invoice SET status = 'settled' WHERE id = ${invoiceId} AND status = 'open'`;
    }),
  );
```

Callers with the same key, in any fiber or process, run one at a time. Different keys never block each other, apart from rare hash collisions. The lock is released when the transaction commits, rolls back or is interrupted.

## Keys

A key is one string or a pair of strings:

<!-- prettier-ignore -->
```ts
import { withAdvisoryXactLock } from '@yieldkit/pg';
import { Effect } from 'effect';

const accountId = 'acct_42';
const checksum = '9f2c';

withAdvisoryXactLock(`import:${accountId}`, Effect.void);           // hashtextextended(key, 0)
withAdvisoryXactLock([`import:${accountId}`, checksum], Effect.void); // hashtext of each half
```

## Nesting

Inside an outer `sql.withTransaction` the call nests as a savepoint:

- If `effect` succeeds, the lock is held until the outer transaction ends.
- If `effect` fails, the rollback to the savepoint releases the lock while the outer transaction is still open, unless the outer transaction already held the same key.

## Limits

- Run the queries the lock protects through the same `SqlClient` inside `effect`.
- The guarantee covers the database work in that transaction. If the session ends while `effect` still runs, for example after the 30 second `idle_in_transaction_session_timeout` while it waits on something other than SQL, another caller can enter. Make side effects outside the database idempotent, or fence them with a value checked in the same transaction.
- The wait for the lock is bounded by the session `lock_timeout` (5 seconds by default), then fails with `SqlError`.
- Each caller waiting for a lock holds a pooled connection. Many callers queued on one key can use up the pool and delay unrelated queries; keep `maxConnections` above the number of callers that wait on a hot key, or serialise them before they reach the database.
- A key that is not a non-empty string or exactly two non-empty strings (such as `null` from an untyped caller) fails with `PgConfigError` before any statement runs.
- The call, lock wait included, runs in the `pg.withAdvisoryXactLock` span.
