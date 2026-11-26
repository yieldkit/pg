# Testing

`@yieldkit/pg/testing` runs tests against a real PostgreSQL in Docker. `startPostgresContainer` needs the optional peer `@testcontainers/postgresql`, and the example captures log lines with `@yieldkit/logger/testing`:

```bash
pnpm add -D @testcontainers/postgresql @yieldkit/logger vitest
```

```ts
import { makeTestLogger } from '@yieldkit/logger/testing';
import { makePgLayer, ping } from '@yieldkit/pg';
import { logMessages, startPostgresContainer, type PostgresContainer } from '@yieldkit/pg/testing';
import { Effect, Layer } from 'effect';
import { afterAll, beforeAll, expect, it } from 'vitest';

let postgres: PostgresContainer;
beforeAll(async () => {
  postgres = await startPostgresContainer();
});
afterAll(() => postgres.stop());

it('opens a pool', async () => {
  const logger = makeTestLogger();
  const PgTest = Layer.provideMerge(makePgLayer({ url: postgres.connectionUri }), logger.layer);
  await Effect.runPromise(ping.pipe(Effect.provide(PgTest)));
  expect(logMessages(logger.records)).toContain('pg.client.ready');
});
```

`connectionUri` is `Redacted` because it holds the password, and is invalid after `stop()` resolves. `logMessages(records)` returns the string messages of `makeTestLogger` records.

## Container options

<!-- prettier-ignore -->
```ts
import { startPostgresContainer } from '@yieldkit/pg/testing';

const postgres = await startPostgresContainer({
  image: 'postgres:19.0-trixie', // POSTGRES_TEST_IMAGE
  database: 'test',              // letters, digits, '_', '.' and '-' only
  username: 'test',
  password: 'test',
  tmpfsSize: '512m',             // tmpfs that holds the data directory
});
```

Another image must accept the official image's `POSTGRES_*` variables, as `pgvector/pgvector` does.

## Testing a data migration

`migrateTo({ databaseUrl, migrationsDir, count })` moves a database up or down until exactly `count` migrations are applied, and returns the names applied or rolled back.

<!-- prettier-ignore -->
```ts
import { migrateTo } from '@yieldkit/pg/testing';
import { join } from 'node:path';
import { it } from 'vitest';

const migrationsDir = join(import.meta.dirname, '../migrations');

it('migrates note rows', async () => {
  const databaseUrl = postgres.connectionUri; // from beforeAll above
  await migrateTo({ databaseUrl, migrationsDir, count: 1 }); // then insert rows in the old shape
  await migrateTo({ databaseUrl, migrationsDir, count: 2 }); // assert the transformed rows
  await migrateTo({ databaseUrl, migrationsDir, count: 1 }); // check the down migration
});
```

`count: 0` rolls everything back. `migrationsTable` and `migrationsSchema` work as in `runMigrations`.

## Limits

- Without `@testcontainers/postgresql`, or without Docker, `startPostgresContainer` rejects with `PgContainerError`.
- `PGDATA` is set to `/var/lib/postgresql/pgdata` on the tmpfs, so an image that seeds a cluster elsewhere at build time starts empty.
- `migrateTo` rolls back with `force: true`, so it is for test databases only.
- `migrateTo` rejects with `MigrationError` when the directory has fewer than `count` migrations, after applying all of them.
- `migrateTo` takes its TLS mode from `PG_SSL_MODE`. It has no `tls` or `tlsCa` option.
- `migrateTo` reads the applied count before the run takes the migration lock. Don't run it next to other migrations of the same database.
