# Pool and timeouts

Every option has a default, so most services set only a few:

```ts
import { makePgLive } from '@yieldkit/pg';

const PgLive = makePgLive({
  applicationName: 'billing-worker',
  maxConnections: 20,
  statementTimeout: '1 minute',
  searchPath: 'billing, public',
});
```

## Options

Every `PgLayerOptions` field, with its default. `makePgLive` takes `PgLiveOptions`: the same without `url`.

<!-- prettier-ignore -->
```ts
import { makePgLayer } from '@yieldkit/pg';
import { Redacted } from 'effect';

const dbUri = 'postgres://app:s3cret@db:5432/app';

makePgLayer({
  url: Redacted.make(dbUri),              // required; never logged
  applicationName: undefined,             // application_name in pg_stat_activity;
                                          // '@effect/sql-pg' (DEFAULT_APPLICATION_NAME)
  maxConnections: 10,                     // DEFAULT_MAX_CONNECTIONS; an integer >= 1
  minConnections: 1,                      // kept open; 0 to maxConnections
  statementTimeout: '15 seconds',         // session statement_timeout; false sends none
  lockTimeout: '5 seconds',               // session lock_timeout; false sends none
  idleInTransactionTimeout: '30 seconds', // session idle_in_transaction_session_timeout; false sends none
  searchPath: undefined,                  // e.g. 'app, public'; the server or role setting
  acquireTimeout: '5 seconds',            // wait for a free pooled connection
  connectTimeout: '5 seconds',            // to connect and authenticate a new session
  idleTimeout: '30 seconds',              // an unused pooled connection stays open this long
  keepAlive: '10 seconds',                // idle time before TCP keepalive probes start
  slowQueryLog: undefined,                // { thresholdMs }; see slow-query-logging.md
  tls: 'disable',                         // see tls.md
  tlsCa: undefined,                       // PEM bundle, only with 'verify-full'
});
```

`acquireTimeout` also covers opening a new session, so with the equal defaults an unresponsive server can show up as `Timed out waiting for a pooled connection`. Set `acquireTimeout` above `connectTimeout` to tell the two apart.

## Session parameters

The session timeouts and `searchPath` are sent as startup parameters on every physical connection. `sessionStartupParameters(slowQueryLog, timeouts)` returns what is sent:

<!-- prettier-ignore -->
```ts
import { sessionStartupParameters } from '@yieldkit/pg';
import { Effect } from 'effect';

Effect.runSync(sessionStartupParameters({ thresholdMs: 500 }));
// { statement_timeout: '15000ms', lock_timeout: '5000ms',
//   idle_in_transaction_session_timeout: '30000ms', log_min_duration_statement: '500' }

Effect.runSync(sessionStartupParameters(undefined, { statementTimeout: '1 minute', searchPath: 'app, public' }));
// { statement_timeout: '60000ms', lock_timeout: '5000ms',
//   idle_in_transaction_session_timeout: '30000ms', search_path: 'app, public' }
```

A timeout set to `false` is not sent, so the server, database or role setting applies. `undefined` means the default.

`lock_timeout` also bounds the wait in [`withAdvisoryXactLock`](advisory-locks.md).

## Behind PgBouncer

PgBouncer refuses unknown startup parameters, and the layer fails with `unsupported startup parameter: statement_timeout` without retrying. Send none, and set the timeouts on the role or database:

<!-- prettier-ignore -->
```ts
import { makePgLayer } from '@yieldkit/pg';
import { Redacted } from 'effect';

makePgLayer({
  url: Redacted.make('postgres://app:s3cret@pgbouncer:6432/app'),
  statementTimeout: false,                // ALTER ROLE app SET statement_timeout = '15s' instead
  lockTimeout: false,
  idleInTransactionTimeout: false,
  // leave searchPath and slowQueryLog unset: they are startup parameters too
});
```

Alternatively, list the names in PgBouncer's `ignore_startup_parameters`. It then drops them silently, so they take effect only through role or database settings either way. `application_name` is always accepted.

Transaction mode does not keep session state (`SET`, `LISTEN`, session advisory locks). Run `listenWithRetry` and `runMigrations` against PostgreSQL directly or through a session-mode pool.

## Validation

The validators are exported for code that builds its own settings:

<!-- prettier-ignore -->
```ts
import { formatPgTimeoutMs, validatePgSchemaName, validatePgSearchPath } from '@yieldkit/pg';

formatPgTimeoutMs('statementTimeout', '1.5 millis'); // Effect succeeding with '2ms'
validatePgSearchPath('app, public');                 // Effect succeeding with 'app, public'
validatePgSchemaName('App');                         // fails with PgConfigError
```

## Limits

Every check runs before a socket is opened and fails with `PgConfigError`.

- `maxConnections` must be an integer of at least 1. `minConnections` must be an integer from 0 to `maxConnections`.
- Every duration must parse, be finite and be greater than zero. PostgreSQL reads `0` as "no limit", so use `false` instead.
- The session timeouts must be at most 2,147,483,647 ms. Fractions of a millisecond are rounded up.
- `keepAlive` must be from 1 to 2,147,483,647 whole seconds.
- `applicationName` must not be empty. The driver always sends a name.
- `searchPath` is a comma-separated list of lowercase unquoted schema names (`[a-z_][a-z0-9_]*`, up to 63 characters). Uppercase, dots, quotes and empty entries are rejected.
- Even with `minConnections: 0`, the layer connects once at startup for its probe query.
