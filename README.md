# @yieldkit/pg

[![npm](https://img.shields.io/npm/v/@yieldkit/pg)](https://www.npmjs.com/package/@yieldkit/pg)
[![CI](https://github.com/yieldkit/pg/actions/workflows/ci.yml/badge.svg)](https://github.com/yieldkit/pg/actions/workflows/ci.yml)
[![license](https://img.shields.io/npm/l/@yieldkit/pg)](LICENSE)

PostgreSQL for Effect on `@effect/sql-pg`: a logged connection pool that checks its settings at startup, `LISTEN`/`NOTIFY`, advisory locks, a readiness probe and SQL migrations with node-pg-migrate.

The layers provide `@effect/sql-pg`'s `PgClient` and Effect's `SqlClient`, so repositories keep yielding `SqlClient`. Logs go through `@yieldkit/logger` with the connection URI and password removed.

## Install

```bash
pnpm add @yieldkit/pg @effect/sql-pg effect
```

`effect` and `@effect/sql-pg` 4.1.6 or a later 4.1 patch are peer dependencies, so your code and this package share one copy of each. `pg`, `node-pg-migrate` and [`@yieldkit/logger`](https://github.com/yieldkit/logger) come with the package. It runs on Node.js 22.18+, 24.11+ and 26+, and on Bun, against PostgreSQL 15 to 19. CI runs the integration tests on PostgreSQL 19 and 15.

| Import                 | Contents                                                                                       |
| ---------------------- | ---------------------------------------------------------------------------------------------- |
| `@yieldkit/pg`         | Pool layers, readiness, `LISTEN`/`NOTIFY`, advisory locks, migrations and the errors           |
| `@yieldkit/pg/testing` | `startPostgresContainer`, `migrateTo` and `logMessages` for tests against PostgreSQL in Docker |

`startPostgresContainer` also needs `@testcontainers/postgresql`, an optional peer dependency (`pnpm add -D @testcontainers/postgresql`). `@yieldkit/pg` itself never loads it.

## Quick start

<!-- prettier-ignore -->
```ts
import { checkReadiness, makePgLive } from '@yieldkit/pg';
import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const PgLive = makePgLive({ applicationName: 'api' }); // URI from DATABASE_URL

const notes = Effect.gen(function* () {
  yield* checkReadiness();                             // SELECT 1, with retries
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ id: number; body: string }>`SELECT id, body FROM note`;
}).pipe(Effect.provide(PgLive));
```

The layer opens one connection while it is built, so a bad URI, a refused port or a wrong password fails at startup rather than on the first query. There is no default URI: without `DATABASE_URL`, `makePgLive` fails with `PgConfigError: DATABASE_URL is not set.`

## Common tasks

### Configure the pool in code

`makePgLayer` takes the URI as a `Redacted` and reads neither `PG_SSL_MODE` nor `PG_SLOW_QUERY_LOG`. Every session gets a `statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout`; set one to `false` to send none, as PgBouncer requires.

```ts
import { makePgLayer } from '@yieldkit/pg';
import { Redacted } from 'effect';

const PgLive = makePgLayer({
  url: Redacted.make('postgres://app:s3cret@db.example.com:5432/app'),
  tls: 'verify-full',
  maxConnections: 20,
  statementTimeout: '1 minute',
});
```

TLS defaults to `disable`; see [TLS](docs/tls.md). The URI may not set [TLS, pool or target parameters](docs/connection.md#reserved-query-parameters); others, such as `schema`, are ignored.

### Run migrations at deploy time

```ts
import { runMigrations } from '@yieldkit/pg';
import { join } from 'node:path';

const migrate = runMigrations({ migrationsDir: join(import.meta.dirname, '../migrations') });
// Effect<ReadonlyArray<string>, MigrationError | ConfigError>: the names applied
```

The run applies every pending `.sql` file under node-pg-migrate's advisory lock, on its own session with its own timeouts. A second deploy waits for the first.

### Notify listeners after a commit

<!-- prettier-ignore -->
```ts
import { listenWithRetry, notifyAfterCommit, PgClient } from '@yieldkit/pg';
import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const createNote = (id: number, body: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const pg = yield* PgClient.PgClient;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`INSERT INTO note ${sql.insert({ id, body })}`;
        yield* notifyAfterCommit(sql, pg, 'note_created', String(id)); // sent only after COMMIT
      }),
    );
  });

const listener = listenWithRetry('note_created', (id) => Effect.log(`note ${id} created`));
```

`listenWithRetry` reconnects with backoff, buffers payloads for a slow handler and reports `pg_listen_*` metrics.

### Run one job at a time across processes

```ts
import { withAdvisoryXactLock } from '@yieldkit/pg';
import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const settle = (invoiceId: string) =>
  withAdvisoryXactLock(
    `invoice:${invoiceId}`,
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE invoice SET status = 'settled' WHERE id = ${invoiceId}`;
    }),
  );
```

The lock lives as long as the transaction around `effect`.

### Test against PostgreSQL in Docker

```ts
import { makePgLayer, ping } from '@yieldkit/pg';
import { startPostgresContainer } from '@yieldkit/pg/testing';
import { Effect } from 'effect';

const postgres = await startPostgresContainer(); // postgres:19.0-trixie on a tmpfs
await Effect.runPromise(ping.pipe(Effect.provide(makePgLayer({ url: postgres.connectionUri }))));
await postgres.stop();
```

## Documentation

| Page                                             | Covers                                                                    |
| ------------------------------------------------ | ------------------------------------------------------------------------- |
| [Connection](docs/connection.md)                 | `makePgLive` and `makePgLayer`, environment variables, URI rules, startup |
| [TLS](docs/tls.md)                               | Modes, `PG_SSL_MODE`, private CAs                                         |
| [Pool and timeouts](docs/pool-and-timeouts.md)   | Every option and default, session parameters, PgBouncer                   |
| [Slow-query logging](docs/slow-query-logging.md) | `slowQueryLog` and the privilege it needs                                 |
| [Migrations](docs/migrations.md)                 | `runMigrations` options, file format, how a run works                     |
| [LISTEN and NOTIFY](docs/listen-notify.md)       | `notifyAfterCommit`, `listenWithRetry`, reconnects, metrics               |
| [Advisory locks](docs/advisory-locks.md)         | `withAdvisoryXactLock` keys and nesting                                   |
| [Readiness](docs/readiness.md)                   | `checkReadiness` and `ping`                                               |
| [Logs and errors](docs/logs-and-errors.md)       | Log events, spans and error types                                         |
| [Testing](docs/testing.md)                       | `startPostgresContainer`, `migrateTo` and `logMessages`                   |

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). `pnpm test` runs the unit tests. `pnpm test:integration` starts PostgreSQL and PgBouncer with Testcontainers, so it needs Docker and `openssl` on `PATH`.

## License

[MIT](LICENSE)
