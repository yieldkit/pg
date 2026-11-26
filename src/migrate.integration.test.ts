import type { StructuredLogRecord } from '@yieldkit/logger';
import type { Socket } from 'node:net';

import { captureLogs, makeTestLogger } from '@yieldkit/logger/testing';
import { Effect, Fiber, Layer, Redacted, Schedule } from 'effect';
import { PG_MIGRATE_LOCK_ID } from 'node-pg-migrate';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createConnection, createServer } from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it, vi } from 'vite-plus/test';

import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer, ping } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { runMigrations } from './migrate.ts';
import { MIGRATION_LOG_LINE_MAX_CHARS } from './policy.ts';
import { migrateTo, startPostgresContainer } from './testing/index.ts';

/**
 * `server_version_num` of PostgreSQL 17, the first release with `transaction_timeout`.
 */
const TRANSACTION_TIMEOUT_MIN_VERSION = 170_000;

/**
 * `application_name` the lock cases give their runs, so they can find the session in
 * `pg_stat_activity`.
 */
const MIGRATION_APPLICATION_NAME = 'app-migrate';

/**
 * Migration that creates `widget`, shared by the lock and timeout cases.
 */
const CREATE_WIDGET_SQL = `-- Up Migration
CREATE TABLE widget (id INT PRIMARY KEY);
-- Down Migration
DROP TABLE widget;
`;

/**
 * How long the lock-wait cases leave a run blocked before checking it is still waiting. Three times
 * the `lockTimeout` they pass, so a lock timeout on the wait would have fired by then.
 */
const BLOCKED_FOR_MS = 1500;

/**
 * How long the holder keeps the migration lock in the `transaction_timeout` case: well past the
 * role's 1 second limit.
 */
const TRANSACTION_TIMEOUT_HELD_FOR_MS = 3000;

/**
 * `lockTimeout` and `statementTimeout` used by the cases that expect a timeout.
 */
const SHORT_TIMEOUT = '500 millis';

/**
 * Upper bound for interrupting a run mid-statement. Well below the 3 second statement and the 5
 * second bound on the unlock, so it fails if the interrupt waits for either.
 */
const INTERRUPT_WITHIN_MS = 1000;

/**
 * Upper bound for a run that should fail on a short timeout, with headroom for a slow machine.
 */
const TIMEOUT_FAILURE_WITHIN_MS = 5000;

/**
 * Allows headroom beyond the migration client's close deadline.
 */
const HALF_OPEN_CLOSE_WITHIN = '8 seconds';

/**
 * Reads `waitedMs` from a serialized `pg.migration.lock_acquired` record.
 */
const WAITED_MS_PATTERN = /"waitedMs":(\d+)/;

/**
 * Delay between polls of a condition the lock cases wait on.
 */
const POLL_INTERVAL = '100 millis';

/**
 * Polls before giving up on a condition, about 5 seconds at `POLL_INTERVAL`.
 */
const POLL_ATTEMPTS = 50;

/**
 * Retries `check` until it returns true, failing after `POLL_ATTEMPTS` polls.
 */
const pollUntil = (label: string, check: () => Promise<boolean>): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const done = yield* Effect.promise(check);
      if (!done) {
        return yield* Effect.fail(`timed out waiting for ${label}`);
      }

      return undefined;
    }).pipe(Effect.retry({ times: POLL_ATTEMPTS, schedule: Schedule.spaced(POLL_INTERVAL) })),
  );

/**
 * Waits real time. Used only where elapsed time is the property under test, such as a lock wait
 * outliving `lockTimeout`.
 */
const waitFor = (millis: number): Promise<void> => Effect.runPromise(Effect.sleep(millis));

/**
 * True when a `MIGRATION_APPLICATION_NAME` session in the client's database is waiting on a lock.
 */
const migrationSessionWaitsOnLock = async (client: Client): Promise<boolean> => {
  const result = await client.query<{ wait_event_type: string | null }>(
    `SELECT wait_event_type FROM pg_stat_activity
     WHERE datname = current_database() AND application_name = $1`,
    [MIGRATION_APPLICATION_NAME],
  );
  return result.rows.length === 1 && result.rows[0]?.wait_event_type === 'Lock';
};

/**
 * True when a `MIGRATION_APPLICATION_NAME` session in any database is running a `pg_sleep`
 * statement.
 */
const migrationSessionIsActive = async (client: Client): Promise<boolean> => {
  const result = await client.query(
    `SELECT 1 FROM pg_stat_activity
     WHERE application_name = $1 AND state = 'active' AND query LIKE '%pg_sleep%'`,
    [MIGRATION_APPLICATION_NAME],
  );
  return result.rows.length === 1;
};

/**
 * True when a fresh session can take the migration advisory lock. Releases it again on success.
 */
const migrationLockIsFree = (url: Redacted.Redacted): Promise<boolean> =>
  withClient(url, async (probe) => {
    const result = await probe.query<{ locked: boolean }>(
      'SELECT pg_try_advisory_lock($1) AS locked',
      [PG_MIGRATE_LOCK_ID],
    );
    return result.rows[0]?.locked === true;
  });

/**
 * Opens a connected node-postgres client for `url`.
 */
const connect = async (url: Redacted.Redacted): Promise<Client> => {
  const client = new Client({ connectionString: Redacted.value(url) });
  await client.connect();
  return client;
};

/**
 * Runs `body` with a connected client for `url` and always ends the client.
 */
const withClient = async <A>(
  url: Redacted.Redacted,
  body: (client: Client) => Promise<A>,
): Promise<A> => {
  const client = await connect(url);
  try {
    return await body(client);
  } finally {
    await client.end();
  }
};

/**
 * Forwards PostgreSQL traffic without replying to the client's FIN.
 */
const withHalfOpenProxy = async (
  databaseUrl: Redacted.Redacted,
  body: (proxyUrl: Redacted.Redacted) => Promise<void>,
): Promise<void> => {
  const target = new URL(Redacted.value(databaseUrl));
  const upstreamOptions = {
    host: target.hostname,
    port: Number(target.port),
    allowHalfOpen: true,
  };
  const sockets = new Set<Socket>();
  const server = createServer({ allowHalfOpen: true }, (client) => {
    const upstream = createConnection(upstreamOptions);
    sockets.add(client);
    sockets.add(upstream);
    client.on('error', () => undefined);
    upstream.on('error', () => undefined);
    client.pipe(upstream, { end: false });
    upstream.pipe(client, { end: false });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  try {
    const address = server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('Expected the proxy to listen on a TCP port.');
    }

    target.hostname = '127.0.0.1';
    target.port = String(address.port);
    await body(Redacted.make(target.toString()));
  } finally {
    for (const socket of sockets) {
      socket.destroy();
    }

    await new Promise<void>((resolve) => {
      server.close(() => resolve());
    });
  }
};

/**
 * Writes `files` (name without `.sql` to SQL text) into a new temporary directory.
 */
const writeMigrationsDir = async (files: Readonly<Record<string, string>>): Promise<string> => {
  const dir = await mkdtemp(path.join(tmpdir(), 'pg-test-'));
  await Promise.all(
    Object.entries(files).map(([name, sql]) => writeFile(path.join(dir, `${name}.sql`), sql)),
  );
  return dir;
};

/**
 * Names recorded in `schema_migrations`, in id order.
 */
const recordedNames = (url: Redacted.Redacted): Promise<ReadonlyArray<string>> =>
  withClient(url, async (client) => {
    const result = await client.query<{ name: string }>(
      'SELECT name FROM schema_migrations ORDER BY id',
    );
    return result.rows.map((row) => row.name);
  });

/**
 * Two migrations that turn `account.full_name` into `first_name` and `last_name`, with a down
 * migration that rebuilds `full_name` from them.
 */
const SPLIT_NAME_MIGRATIONS = {
  '0001_create_account': `-- Up Migration
CREATE TABLE account (id SERIAL PRIMARY KEY, full_name TEXT NOT NULL);
-- Down Migration
DROP TABLE account;
`,
  '0002_split_name': `-- Up Migration
ALTER TABLE account ADD COLUMN first_name TEXT, ADD COLUMN last_name TEXT;
UPDATE account SET first_name = split_part(full_name, ' ', 1), last_name = split_part(full_name, ' ', 2);
ALTER TABLE account DROP COLUMN full_name;
-- Down Migration
ALTER TABLE account ADD COLUMN full_name TEXT;
UPDATE account SET full_name = first_name || ' ' || last_name;
ALTER TABLE account DROP COLUMN first_name, DROP COLUMN last_name;
`,
};

/**
 * User name and password of a role that does not exist. PostgreSQL quotes the user name in its
 * authentication error, so the password appears in the server's message and must be redacted.
 */
const LEAKED_SECRET = 'leak-s3cret-user';

/**
 * Password of the role that owns the runner output database, chosen so it can be searched for.
 */
const RUNNER_ROLE_PASSWORD = 'runner-s3cret';

/**
 * Lines `pg.migration.runner` logged in `records`, as `[level, line]` pairs.
 */
const runnerLines = (
  records: ReadonlyArray<StructuredLogRecord>,
): ReadonlyArray<readonly [string, string]> =>
  records.flatMap((record) => {
    const line = record.annotations.line;
    return record.message === 'pg.migration.runner' && typeof line === 'string'
      ? [[record.level, line] as const]
      : [];
  });

describe('runMigrations', () => {
  let postgres: PostgresContainer;
  let migrationsDir: string;

  /**
   * Creates an empty database named `name` and returns its URI, so each case gets its own migration
   * history and advisory lock space.
   */
  const createDatabase = async (name: string): Promise<Redacted.Redacted> => {
    await withClient(postgres.connectionUri, (client) => client.query(`CREATE DATABASE ${name}`));
    return Redacted.make(Redacted.value(postgres.connectionUri).replace(/\/[^/]+$/, `/${name}`));
  };

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
    migrationsDir = await mkdtemp(path.join(tmpdir(), 'pg-test-'));
    await writeFile(
      path.join(migrationsDir, '0001_create_widget.sql'),
      `-- Up Migration
CREATE TABLE widget (id INT PRIMARY KEY);
-- Down Migration
DROP TABLE widget;
`,
    );
  });

  afterAll(async () => {
    await rm(migrationsDir, { recursive: true, force: true });
    await postgres.stop();
  });

  it('applies SQL files and skips them on the second up run', async () => {
    const first = await Effect.runPromise(
      runMigrations({ databaseUrl: postgres.connectionUri, migrationsDir, direction: 'up' }),
    );
    const second = await Effect.runPromise(
      runMigrations({ databaseUrl: postgres.connectionUri, migrationsDir, direction: 'up' }),
    );
    expect(first).toEqual(['0001_create_widget']);
    expect(second).toEqual([]);
  });

  it('closes its client socket when the peer never answers its FIN', async () => {
    const databaseUrl = await createDatabase('app_half_open_migration');
    await withHalfOpenProxy(databaseUrl, async (proxyUrl) => {
      const end = vi.spyOn(Client.prototype, 'end');
      try {
        const applied = await Effect.runPromise(
          runMigrations({ databaseUrl: proxyUrl, migrationsDir }).pipe(
            Effect.timeout(HALF_OPEN_CLOSE_WITHIN),
          ),
        );
        expect(applied).toEqual(['0001_create_widget']);
        expect(end.mock.contexts[0]).toHaveProperty('connection.stream.destroyed', true);
      } finally {
        end.mockRestore();
      }
    });
  });

  it('finishes counting migrations when the peer never answers its FIN', async () => {
    const databaseUrl = await createDatabase('app_half_open_count');
    await withHalfOpenProxy(databaseUrl, async (proxyUrl) => {
      const count = Effect.promise(() =>
        migrateTo({ databaseUrl: proxyUrl, migrationsDir, count: 0 }),
      );
      await expect(
        Effect.runPromise(count.pipe(Effect.timeout(HALF_OPEN_CLOSE_WITHIN))),
      ).resolves.toEqual([]);
    });
  });

  it('logs the migrations that commit before a later migration fails', async () => {
    const databaseUrl = await createDatabase('app_partial_migration');
    const dir = await writeMigrationsDir({
      '0001_existing': 'SELECT 1;',
      '0002_committed': 'SELECT 2;',
      '0003_invalid': 'SELECT nonexistent_column;',
    });
    try {
      await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir, count: 1 }));
      const run = runMigrations({ databaseUrl, migrationsDir: dir });
      const { value, logs } = await Effect.runPromise(captureLogs(Effect.flip(run)));
      expect(value.message).toContain('nonexistent_column');
      expect(await recordedNames(databaseUrl)).toEqual(['0001_existing', '0002_committed']);
      const applied = logs
        .filter((record) => record.message === 'pg.migration.applied')
        .map((record) => record.annotations.name);
      expect(applied).toEqual(['0002_committed']);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('rolls the applied migration back', async () => {
    const databaseUrl = await createDatabase('app_rollback');
    await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir }));
    const down = await Effect.runPromise(
      runMigrations({ databaseUrl, migrationsDir, direction: 'down', force: true }),
    );
    expect(down).toEqual(['0001_create_widget']);
    expect(await recordedNames(databaseUrl)).toEqual([]);
  });

  it('ignores files that are not .sql instead of loading or failing on them', async () => {
    const databaseUrl = await createDatabase('app_non_sql');
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    try {
      await writeFile(path.join(dir, 'README.md'), '# Migrations\n');
      await writeFile(path.join(dir, 'helper.ts'), 'export const helper = 1;\n');
      await writeFile(path.join(dir, '0002_script.js'), 'exports.up = () => {};\n');
      const applied = await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir }));
      expect(applied).toEqual(['0001_create_widget']);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('rejects a single SQL file with a down marker and no up marker', async () => {
    const databaseUrl = await createDatabase('app_down_marker_only');
    const dir = await writeMigrationsDir({
      '0001_note': `CREATE TABLE note (id integer);
-- Down Migration
DROP TABLE note;
`,
    });
    try {
      const error = await Effect.runPromise(
        Effect.flip(runMigrations({ databaseUrl, migrationsDir: dir })),
      );
      expect(error._tag).toBe('MigrationError');
      expect(error.message).toContain('0001_note.sql: Down marker requires an Up marker');
      const tables = await withClient(databaseUrl, (client) =>
        client.query(
          `SELECT to_regclass('public.note') AS note,
                  to_regclass('public.schema_migrations') AS history`,
        ),
      );
      expect(tables.rows).toEqual([{ note: null, history: null }]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('pairs NNNN_x.up.sql with NNNN_x.down.sql instead of running the down file on up', async () => {
    const databaseUrl = await createDatabase('app_paired');
    const dir = await writeMigrationsDir({
      '0001_orders.up': 'CREATE TABLE orders (id INT PRIMARY KEY);',
      '0001_orders.down': 'CREATE TABLE down_file_ran (id INT); DROP TABLE orders;',
    });
    const tables = () =>
      withClient(databaseUrl, async (client) => {
        const result = await client.query<{ table_name: string }>(
          `SELECT table_name FROM information_schema.tables
           WHERE table_schema = 'public' AND table_name <> 'schema_migrations'
           ORDER BY table_name`,
        );
        return result.rows.map((row) => row.table_name);
      });
    try {
      const up = await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir }));
      expect(up).toEqual(['0001_orders']);
      expect(await tables()).toEqual(['orders']);

      const down = await Effect.runPromise(
        runMigrations({ databaseUrl, migrationsDir: dir, direction: 'down', force: true }),
      );
      expect(down).toEqual(['0001_orders']);
      expect(await tables()).toEqual(['down_file_ran']);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('connects to a percent-encoded host, as the pool does', async () => {
    const databaseUrl = await createDatabase('app_encoded_host');
    const encoded = Redacted.make(
      Redacted.value(databaseUrl).replace(
        /@([^:/])([^:/]*)/,
        (_match, first: string, rest: string) => `@%${first.charCodeAt(0).toString(16)}${rest}`,
      ),
    );
    expect(Redacted.value(encoded)).toContain('@%');
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    try {
      const applied = await Effect.runPromise(
        runMigrations({ databaseUrl: encoded, migrationsDir: dir }),
      );
      expect(applied).toEqual(['0001_create_widget']);
      await Effect.runPromise(
        ping.pipe(
          Effect.provide(makePgLayer({ url: encoded }).pipe(Layer.provide(makeTestLogger().layer))),
        ),
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('migrates the schema the pool uses even when PGOPTIONS sets another search_path', async () => {
    const databaseUrl = await createDatabase('app_pgoptions');
    await withClient(databaseUrl, (client) => client.query('CREATE SCHEMA other'));
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    const previous = process.env.PGOPTIONS;
    process.env.PGOPTIONS = '-c search_path=other';
    try {
      await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir }));
    } finally {
      if (previous === undefined) {
        delete process.env.PGOPTIONS;
      } else {
        process.env.PGOPTIONS = previous;
      }

      await rm(dir, { recursive: true, force: true });
    }

    const schemas = await withClient(databaseUrl, (client) =>
      client.query<{ table_schema: string }>(
        `SELECT table_schema FROM information_schema.tables WHERE table_name = 'widget'`,
      ),
    );
    expect(schemas.rows.map((row) => row.table_schema)).toEqual(['public']);
  });

  it('does not record a migration whose SQL fails', async () => {
    const admin = new Client({ connectionString: Redacted.value(postgres.connectionUri) });
    await admin.connect();
    try {
      await admin.query('CREATE DATABASE app_bad_migration');
    } finally {
      await admin.end();
    }

    const databaseUrl = Redacted.make(
      Redacted.value(postgres.connectionUri).replace(/\/[^/]+$/, '/app_bad_migration'),
    );
    const dir = await mkdtemp(path.join(tmpdir(), 'pg-test-bad-'));
    await writeFile(
      path.join(dir, '0001_ok.sql'),
      `-- Up Migration
CREATE TABLE ok_widget (id INT PRIMARY KEY);
-- Down Migration
DROP TABLE ok_widget;
`,
    );
    await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir, direction: 'up' }));
    await writeFile(
      path.join(dir, '0002_bad.sql'),
      `-- Up Migration
THIS IS NOT SQL;
-- Down Migration
SELECT 1;
`,
    );
    const error = await Effect.runPromise(
      Effect.flip(runMigrations({ databaseUrl, migrationsDir: dir, direction: 'up' })),
    );
    expect(error._tag).toBe('MigrationError');
    const client = new Client({ connectionString: Redacted.value(databaseUrl) });
    await client.connect();
    try {
      const recorded = await client.query<{ name: string }>('SELECT name FROM schema_migrations');
      expect(recorded.rows.map((row) => row.name)).toContain('0001_ok');
      expect(recorded.rows.map((row) => row.name)).not.toContain('0002_bad');
    } finally {
      await client.end();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('fails with a redacted MigrationError when tls require meets plaintext', async () => {
    const password = 'tls-probe-s3cret';
    const parsed = new URL(Redacted.value(postgres.connectionUri));
    parsed.password = password;
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make(parsed.toString()),
          migrationsDir,
          direction: 'up',
          tls: 'require',
        }),
      ),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain('does not support SSL');
    expect(error.message).not.toContain('password authentication');
    expect(error.message).not.toContain(password);
  });

  it('applies only the first file with count 1', async () => {
    const databaseUrl = await createDatabase('app_count');
    const dir = await writeMigrationsDir(SPLIT_NAME_MIGRATIONS);
    try {
      const applied = await Effect.runPromise(
        runMigrations({ databaseUrl, migrationsDir: dir, count: 1 }),
      );
      expect(applied).toEqual(['0001_create_account']);
      expect(await recordedNames(databaseUrl)).toEqual(['0001_create_account']);
      const rest = await Effect.runPromise(
        runMigrations({ databaseUrl, migrationsDir: dir, count: Infinity }),
      );
      expect(rest).toEqual(['0002_split_name']);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('records history in a custom table and schema, which migrateTo also reads', async () => {
    const databaseUrl = await createDatabase('app_custom_history');
    await withClient(databaseUrl, (client) => client.query('CREATE SCHEMA ops'));
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    const history = { migrationsTable: 'app_migrations', migrationsSchema: 'ops' };
    try {
      const applied = await Effect.runPromise(
        runMigrations({ databaseUrl, migrationsDir: dir, ...history }),
      );
      expect(applied).toEqual(['0001_create_widget']);
      const recorded = await withClient(databaseUrl, (client) =>
        client.query<{ name: string }>('SELECT name FROM ops.app_migrations'),
      );
      expect(recorded.rows.map((row) => row.name)).toEqual(['0001_create_widget']);
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 1, ...history })).toEqual(
        [],
      );
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('fails migrateTo when the directory has fewer migrations than requested', async () => {
    const databaseUrl = await createDatabase('app_migrate_to_short');
    const dir = await writeMigrationsDir(SPLIT_NAME_MIGRATIONS);
    try {
      const error = await migrateTo({ databaseUrl, migrationsDir: dir, count: 5 }).then(
        () => undefined,
        (cause: unknown) => cause,
      );
      expect(error).toMatchObject({ _tag: 'MigrationError' });
      expect(String(error)).toContain('only 2');
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('moves data through migrateTo up and down', async () => {
    const databaseUrl = await createDatabase('app_migrate_to');
    const dir = await writeMigrationsDir(SPLIT_NAME_MIGRATIONS);
    try {
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 1 })).toEqual([
        '0001_create_account',
      ]);
      await withClient(databaseUrl, (client) =>
        client.query(`INSERT INTO account (full_name) VALUES ('Ada Lovelace'), ('Grace Hopper')`),
      );
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 2 })).toEqual([
        '0002_split_name',
      ]);
      const split = await withClient(databaseUrl, (client) =>
        client.query('SELECT first_name, last_name FROM account ORDER BY id'),
      );
      expect(split.rows).toEqual([
        { first_name: 'Ada', last_name: 'Lovelace' },
        { first_name: 'Grace', last_name: 'Hopper' },
      ]);
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 2 })).toEqual([]);
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 1 })).toEqual([
        '0002_split_name',
      ]);
      const restored = await withClient(databaseUrl, (client) =>
        client.query('SELECT full_name FROM account ORDER BY id'),
      );
      expect(restored.rows).toEqual([{ full_name: 'Ada Lovelace' }, { full_name: 'Grace Hopper' }]);
      expect(await migrateTo({ databaseUrl, migrationsDir: dir, count: 0 })).toEqual([
        '0001_create_account',
      ]);
      expect(await recordedNames(databaseUrl)).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('logs runner output at debug after a successful run', async () => {
    const databaseUrl = await createDatabase('app_runner_debug');
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    try {
      const quiet = await Effect.runPromise(
        captureLogs(runMigrations({ databaseUrl, migrationsDir: dir })),
      );
      expect(runnerLines(quiet.logs)).toEqual([]);
      await withClient(databaseUrl, (client) => client.query('DROP TABLE widget'));
      await withClient(databaseUrl, (client) => client.query('DELETE FROM schema_migrations'));
      const verbose = await Effect.runPromise(
        captureLogs(runMigrations({ databaseUrl, migrationsDir: dir }), { minLevel: 'Debug' }),
      );
      const lines = runnerLines(verbose.logs);
      expect(lines.length).toBeGreaterThan(0);
      expect(lines.every(([level]) => level === 'DEBUG')).toBe(true);
      expect(lines.map(([, line]) => line)).toContain('> - 0001_create_widget');
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('logs redacted runner output at error after a failed run', async () => {
    await withClient(postgres.connectionUri, async (client) => {
      await client.query(`CREATE ROLE runner_owner LOGIN PASSWORD '${RUNNER_ROLE_PASSWORD}'`);
      await client.query('CREATE DATABASE app_runner_error OWNER runner_owner');
    });
    const parsed = new URL(Redacted.value(postgres.connectionUri));
    parsed.username = 'runner_owner';
    parsed.password = RUNNER_ROLE_PASSWORD;
    parsed.pathname = '/app_runner_error';
    const databaseUrl = Redacted.make(parsed.toString());
    const dir = await writeMigrationsDir({
      '0001_bad': `-- Up Migration
SELECT '${RUNNER_ROLE_PASSWORD}' FROM missing_table;
-- ${'padding '.repeat(2000)}
-- Down Migration
SELECT 1;
`,
    });
    try {
      const { value: error, logs } = await Effect.runPromise(
        captureLogs(Effect.flip(runMigrations({ databaseUrl, migrationsDir: dir }))),
      );
      expect(error.message).not.toContain(RUNNER_ROLE_PASSWORD);
      const lines = runnerLines(logs);
      expect(lines.length).toBeGreaterThan(0);
      expect(lines.every(([level]) => level === 'ERROR')).toBe(true);
      expect(lines.some(([, line]) => line.includes('missing_table'))).toBe(true);
      expect(lines.some(([, line]) => line.includes('characters omitted'))).toBe(true);
      expect(lines.every(([, line]) => line.length <= MIGRATION_LOG_LINE_MAX_CHARS + 64)).toBe(
        true,
      );
      expect(lines.some(([, line]) => line.includes('***'))).toBe(true);
      expect(JSON.stringify(logs)).not.toContain(RUNNER_ROLE_PASSWORD);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('redacts the password from a server message that quotes it', async () => {
    const parsed = new URL(Redacted.value(postgres.connectionUri));
    parsed.username = LEAKED_SECRET;
    parsed.password = LEAKED_SECRET;
    const error = await Effect.runPromise(
      Effect.flip(runMigrations({ databaseUrl: Redacted.make(parsed.toString()), migrationsDir })),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain('password authentication failed');
    expect(error.message).toContain('***');
    expect(error.message).not.toContain(LEAKED_SECRET);
  });

  it('omits the password from a connection failure', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make('postgres://app:s3cret@127.0.0.1:1/app'),
          migrationsDir,
          direction: 'up',
        }),
      ),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).not.toContain('s3cret');
  });

  it('waits for a held migration lock past lockTimeout and reports its application_name', async () => {
    const databaseUrl = await createDatabase('app_lock_wait');
    await withClient(postgres.connectionUri, (client) =>
      client.query(`ALTER DATABASE app_lock_wait SET lock_timeout = '200ms'`),
    );
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    const holder = await connect(databaseUrl);
    const harness = makeTestLogger();
    try {
      await holder.query('SELECT pg_advisory_lock($1)', [PG_MIGRATE_LOCK_ID]);
      const run = Effect.runFork(
        runMigrations({
          databaseUrl,
          migrationsDir: dir,
          lockTimeout: SHORT_TIMEOUT,
          applicationName: MIGRATION_APPLICATION_NAME,
        }).pipe(Effect.provide(harness.layer)),
      );
      await pollUntil('the migration session to wait on the lock', () =>
        migrationSessionWaitsOnLock(holder),
      );
      await waitFor(BLOCKED_FOR_MS);
      expect(run.pollUnsafe()).toBeUndefined();
      expect(await migrationSessionWaitsOnLock(holder)).toBe(true);
      await holder.query('SELECT pg_advisory_unlock($1)', [PG_MIGRATE_LOCK_ID]);
      const applied = await Effect.runPromise(Fiber.join(run));
      expect(applied).toEqual(['0001_create_widget']);
      expect(await recordedNames(databaseUrl)).toEqual(['0001_create_widget']);
      const acquired = harness.records.find(
        (record) => record.message === 'pg.migration.lock_acquired',
      );
      const waitedMs = WAITED_MS_PATTERN.exec(JSON.stringify(acquired))?.[1];
      expect(Number(waitedMs)).toBeGreaterThanOrEqual(BLOCKED_FOR_MS);
    } finally {
      await holder.end();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('waits for a held migration lock past the role transaction_timeout', async (context) => {
    const version = await withClient(postgres.connectionUri, async (client) => {
      const result = await client.query<{ server_version_num: string }>('SHOW server_version_num');
      return Number(result.rows[0]?.server_version_num);
    });
    // `transaction_timeout` exists from PostgreSQL 17; older servers run the rest of the suite.
    context.skip(
      version < TRANSACTION_TIMEOUT_MIN_VERSION,
      'transaction_timeout needs PostgreSQL 17',
    );
    await withClient(postgres.connectionUri, async (client) => {
      await client.query("CREATE ROLE txn_timeout_owner LOGIN PASSWORD 'txn-timeout'");
      await client.query("ALTER ROLE txn_timeout_owner SET transaction_timeout = '1s'");
      await client.query('CREATE DATABASE app_txn_timeout OWNER txn_timeout_owner');
    });
    const parsed = new URL(Redacted.value(postgres.connectionUri));
    parsed.username = 'txn_timeout_owner';
    parsed.password = 'txn-timeout';
    parsed.pathname = '/app_txn_timeout';
    const holderUrl = Redacted.make(
      Redacted.value(postgres.connectionUri).replace(/\/[^/]+$/, '/app_txn_timeout'),
    );
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    const holder = await connect(holderUrl);
    const harness = makeTestLogger();
    try {
      await holder.query('SELECT pg_advisory_lock($1)', [PG_MIGRATE_LOCK_ID]);
      const run = Effect.runFork(
        runMigrations({
          databaseUrl: Redacted.make(parsed.toString()),
          migrationsDir: dir,
          applicationName: MIGRATION_APPLICATION_NAME,
        }).pipe(Effect.provide(harness.layer)),
      );
      await pollUntil('the migration session to wait on the lock', () =>
        migrationSessionWaitsOnLock(holder),
      );
      await waitFor(TRANSACTION_TIMEOUT_HELD_FOR_MS);
      await holder.query('SELECT pg_advisory_unlock($1)', [PG_MIGRATE_LOCK_ID]);
      const applied = await Effect.runPromise(Fiber.join(run));
      expect(applied).toEqual(['0001_create_widget']);
      const acquired = harness.records.find(
        (record) => record.message === 'pg.migration.lock_acquired',
      );
      const waitedMs = WAITED_MS_PATTERN.exec(JSON.stringify(acquired))?.[1];
      expect(Number(waitedMs)).toBeGreaterThan(1000);
    } finally {
      await holder.end();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('lets two concurrent runs migrate a fresh database once', async () => {
    const databaseUrl = await createDatabase('app_concurrent');
    const files = {
      '0001_create_widget': CREATE_WIDGET_SQL,
      '0002_create_gadget': `-- Up Migration
CREATE TABLE gadget (id INT PRIMARY KEY);
-- Down Migration
DROP TABLE gadget;
`,
    };
    const dir = await writeMigrationsDir(files);
    try {
      const run = runMigrations({ databaseUrl, migrationsDir: dir });
      const results = await Effect.runPromise(Effect.all([run, run], { concurrency: 2 }));
      const fileNames = Object.keys(files);
      expect(results.flat().toSorted()).toEqual(fileNames);
      const recorded = await recordedNames(databaseUrl);
      expect(recorded).toEqual(fileNames);
      expect(new Set(recorded).size).toBe(fileNames.length);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('fails a migration that waits longer than lockTimeout for a table lock', async () => {
    const databaseUrl = await createDatabase('app_lock_timeout');
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    await Effect.runPromise(runMigrations({ databaseUrl, migrationsDir: dir }));
    await writeFile(
      path.join(dir, '0002_alter_widget.sql'),
      `-- Up Migration
ALTER TABLE widget ADD COLUMN label TEXT;
-- Down Migration
ALTER TABLE widget DROP COLUMN label;
`,
    );
    const holder = await connect(databaseUrl);
    try {
      await holder.query('BEGIN');
      await holder.query('LOCK TABLE widget IN ACCESS EXCLUSIVE MODE');
      const startedAt = Date.now();
      const error = await Effect.runPromise(
        Effect.flip(runMigrations({ databaseUrl, migrationsDir: dir, lockTimeout: SHORT_TIMEOUT })),
      );
      expect(Date.now() - startedAt).toBeLessThan(TIMEOUT_FAILURE_WITHIN_MS);
      expect(error._tag).toBe('MigrationError');
      expect(error.message).toContain('lock timeout');
      await holder.query('ROLLBACK');
      expect(await recordedNames(databaseUrl)).toEqual(['0001_create_widget']);
    } finally {
      await holder.end();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('fails a migration statement that runs longer than statementTimeout', async () => {
    const databaseUrl = await createDatabase('app_statement_timeout');
    const dir = await writeMigrationsDir({
      '0001_sleep': `-- Up Migration
SELECT pg_sleep(2);
-- Down Migration
SELECT 1;
`,
    });
    try {
      const error = await Effect.runPromise(
        Effect.flip(
          runMigrations({ databaseUrl, migrationsDir: dir, statementTimeout: SHORT_TIMEOUT }),
        ),
      );
      expect(error._tag).toBe('MigrationError');
      expect(error.message).toContain('statement timeout');
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('releases the migration lock when a waiting run is interrupted', async () => {
    const databaseUrl = await createDatabase('app_lock_interrupt');
    const dir = await writeMigrationsDir({ '0001_create_widget': CREATE_WIDGET_SQL });
    const holder = await connect(databaseUrl);
    try {
      await holder.query('SELECT pg_advisory_lock($1)', [PG_MIGRATE_LOCK_ID]);
      const run = Effect.runFork(
        runMigrations({
          databaseUrl,
          migrationsDir: dir,
          applicationName: MIGRATION_APPLICATION_NAME,
        }),
      );
      await pollUntil('the migration session to wait on the lock', () =>
        migrationSessionWaitsOnLock(holder),
      );
      await Effect.runPromise(Fiber.interrupt(run));
      await holder.query('SELECT pg_advisory_unlock($1)', [PG_MIGRATE_LOCK_ID]);
      await pollUntil('the migration lock to be released', () => migrationLockIsFree(databaseUrl));
      expect(await recordedNames(databaseUrl).catch(() => [])).toEqual([]);
    } finally {
      await holder.end();
      await rm(dir, { recursive: true, force: true });
    }
  });

  it('ends the session without waiting when a run is interrupted mid-migration', async () => {
    const databaseUrl = await createDatabase('app_interrupt_running');
    const dir = await writeMigrationsDir({
      '0001_sleep': `-- Up Migration
SELECT pg_sleep(3);
-- Down Migration
SELECT 1;
`,
    });
    try {
      const run = Effect.runFork(
        runMigrations({
          databaseUrl,
          migrationsDir: dir,
          applicationName: MIGRATION_APPLICATION_NAME,
        }),
      );
      await withClient(postgres.connectionUri, (observer) =>
        pollUntil('the migration statement to run', () => migrationSessionIsActive(observer)),
      );
      const interruptedAt = Date.now();
      await Effect.runPromise(Fiber.interrupt(run));
      expect(Date.now() - interruptedAt).toBeLessThan(INTERRUPT_WITHIN_MS);
      await pollUntil('the migration lock to be released', () => migrationLockIsFree(databaseUrl));
      expect(await recordedNames(databaseUrl)).toEqual([]);
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
