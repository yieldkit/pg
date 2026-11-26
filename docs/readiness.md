# Readiness

`checkReadiness` runs `SELECT 1` through the `PgClient` in context, with retries, for a health endpoint or a startup gate:

```ts
import { checkReadiness } from '@yieldkit/pg';
import { Effect } from 'effect';

const health = checkReadiness().pipe(
  Effect.map(({ latencyMs, pool }) => ({ status: 200, body: { latencyMs, pool } })),
  Effect.catchTag('PgReadinessError', (error) =>
    Effect.succeed({ status: 503, body: { error: error.message } }),
  ),
);
```

It returns `{ latencyMs, attempts, pool }`. `latencyMs` runs from the first probe to the answer, retries included, on the monotonic clock. `pool` is `PgClient.poolStats`: `idle`, `used`, `pending`, `min` and `max`.

## Options

<!-- prettier-ignore -->
```ts
import { checkReadiness } from '@yieldkit/pg';

const ready = checkReadiness({
  retries: 2,                  // READINESS_RETRIES; up to 3 attempts
  baseDelay: '100 millis',     // READINESS_BASE_DELAY; doubles per retry, with jitter
  attemptTimeout: '2 seconds', // READINESS_ATTEMPT_TIMEOUT; includes the wait for a pooled connection
});
// Effect<PgReadiness, PgReadinessError, PgClient>
```

Each retry logs `pg.readiness.retry` (warning, `attempt`, `delayMs`). When every attempt fails it logs `pg.readiness.failed` and fails with `PgReadinessError { message, attempts }`. The message names the kind of failure (`SQL error ConnectionError`, `attempt timed out`), never the URI, host or password.

`ping` runs a single `SELECT 1` on the `SqlClient`, without retries or stats, and fails with `SqlError`.

## Limits

- Call it outside any transaction. Inside `sql.withTransaction` it probes the transaction's connection and reports ready even when the pool has none free.
- After `attemptTimeout`, the driver may take up to about 5 seconds more to cancel a stalled query before the next attempt.
- A `retries` that is not an integer of at least 0, or a `baseDelay` or `attemptTimeout` that is not a finite duration above zero, fails with `attempts: 0` before any probe.
- It runs in the `pg.checkReadiness` span.
