# Migrations

`runMigrations` applies a directory of `.sql` files with node-pg-migrate, under an advisory lock, and returns the names it applied:

```ts
import { runMigrations } from '@yieldkit/pg';
import { Effect } from 'effect';
import { join } from 'node:path';

const migrate = Effect.gen(function* () {
  const applied = yield* runMigrations({
    migrationsDir: join(import.meta.dirname, '../migrations'), // pass an absolute path
  });
  yield* Effect.log(`applied ${applied.length} migrations`);
});
```

It logs `pg.migration.applied` (`direction`, `name`) per migration, or `pg.migration.up_to_date` when none were pending.

## Options

<!-- prettier-ignore -->
```ts
import { runMigrations } from '@yieldkit/pg';

const applied = runMigrations({
  migrationsDir: '/srv/app/migrations',  // required; an absolute path
  databaseUrl: undefined,                // a Redacted URI; default DATABASE_URL, which must then be set
  direction: 'up',                       // or 'down'
  force: false,                          // must be true for 'down', which drops data
  count: undefined,                      // a positive integer or Infinity; default all pending up, one down
  tls: undefined,                        // default PG_SSL_MODE, then 'disable'
  tlsCa: undefined,                      // PEM bundle, only with 'verify-full'
  applicationName: undefined,            // shows the run in pg_stat_activity; '@effect/sql-pg'
  migrationsTable: 'schema_migrations',  // MIGRATIONS_TABLE
  migrationsSchema: 'public',            // MIGRATIONS_SCHEMA; must already exist
  lockTimeout: '10 seconds',             // MIGRATION_LOCK_TIMEOUT; per statement
  statementTimeout: '10 minutes',        // MIGRATION_STATEMENT_TIMEOUT; per statement
});
// Effect<ReadonlyArray<string>, MigrationError | ConfigError>
```

To roll back the last migration:

```ts
import { runMigrations } from '@yieldkit/pg';

const rolledBack = runMigrations({
  migrationsDir: '/srv/app/migrations',
  direction: 'down',
  force: true,
});
```

## File format

Each migration is one file or a pair, named so they sort in order. Only `*.sql` files (lowercase extension) are read. `0001_create_note.sql`:

```sql
-- Up Migration
CREATE TABLE note (id bigint PRIMARY KEY, body text NOT NULL);

-- Down Migration
DROP TABLE note;
```

- `0001_create_note.sql`: the up migration, split by `-- Up Migration` and `-- Down Migration` marker lines when present.
- `0001_create_note.up.sql` and `0001_create_note.down.sql`: the two halves of one migration named `0001_create_note`.

Both forms record the same name, so a migration can switch form without being applied again. Each file runs in one transaction with its history row, so don't write `BEGIN`, `COMMIT` or `ROLLBACK`.

## How a run works

A run uses its own node-postgres session, not the pool, with exactly the host, port, user, database and password the pool uses (see [Connection](connection.md#the-connection-uri)), decoded the same way.

1. Connect (5 second timeout). Clear `statement_timeout`, `lock_timeout` and, on PostgreSQL 17+, `transaction_timeout`, so a role setting cannot cut the lock wait short.
2. Take node-pg-migrate's advisory lock with `pg_advisory_lock`. A second deploy waits here, then logs `pg.migration.lock_acquired` with `waitedMs`.
3. Set `lock_timeout`, `statement_timeout` and a 60 second `idle_in_transaction_session_timeout`, so a stuck `ALTER TABLE` fails instead of queueing every query on that table. `transaction_timeout` stays cleared.
4. Run each file in its own transaction, then unlock and disconnect.

An interrupted run closes the session. PostgreSQL rolls back and frees the lock once the statement in flight ends or reaches `statement_timeout`.

## Runner output

node-pg-migrate's output is buffered and logged as `pg.migration.runner` (`level`, `line`) when the run ends: at debug after a success, at error otherwise.

- The buffer keeps the first 500 lines (`MIGRATION_LOG_BUFFER_LIMIT`). One more line carries `droppedLines` when it overflowed.
- A line over 4000 characters (`MIGRATION_LOG_LINE_MAX_CHARS`) keeps its start and end around a `…[N characters omitted]…` marker.
- A successful run logs no SQL.

## Limits

- Everything fails with `MigrationError { message, direction }`, with the URI and password removed from the message. An invalid `PG_SSL_MODE` fails with `ConfigError`.
- These fail before connecting:
  - `direction: 'down'` without `force: true`, or a direction other than `up` or `down`;
  - a `count` that is not a positive integer or `Infinity`;
  - a timeout that is not a positive duration of at most 2,147,483,647 ms;
  - a history table or schema name that is not a lowercase unquoted identifier;
  - no `databaseUrl` and no `DATABASE_URL`, a malformed URI or a reserved query parameter (see [Connection](connection.md));
  - an empty `applicationName`;
  - `PGREPLICATION` in the environment (a replication session) or a `PGSSLNEGOTIATION` other than `postgres` (a different TLS handshake). No option overrides either.
- Don't set `NODE_PG_FORCE_NATIVE`: the native client ignores the TLS settings, so `runMigrations` refuses it (and without `pg-native`, `pg` itself throws on import).
- A relative `migrationsDir` resolves from the working directory. Dotfiles and non-`.sql` files are ignored.
- A `.down.sql` without its `.up.sql`, or a pair next to a single file of the same name, fails the run.
- Name files with letters, digits, `_`, `-` and `.` only. An apostrophe makes the history insert fail.
- Applied migrations must still be in file order (node-pg-migrate's `checkOrder`).
- History rows are ordered by `run_on`, a timestamp in the session's time zone. In a zone with daylight saving time, two deploys inside the repeated autumn hour can record out of order, and every later run then fails its order check. A database that runs in UTC cannot hit this. Do not change the time zone of a database that already has history: old and new rows would no longer sort by when they ran.
- PgBouncer in transaction mode can't hold the migration lock (see [Behind PgBouncer](pool-and-timeouts.md#behind-pgbouncer)).
- node-postgres's `PG*` environment variables (`PGHOST`, `PGSSLMODE` and the rest) have no effect.
- `lockTimeout` and `statementTimeout` never limit the wait for the migration lock itself.
- A failed statement is logged with its SQL text, the whole file for a `.sql` migration, with only the URI and password removed. Keep other secrets out of migration files.
