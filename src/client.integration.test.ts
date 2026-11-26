import { makeTestLogger } from '@yieldkit/logger/testing';
import { Deferred, Effect, Exit, Fiber, Layer, Redacted } from 'effect';
import { SqlClient, SqlError } from 'effect/sql';
import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer, ping } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { omitUndefined } from './omit-undefined.ts';
import { reachableStrings, renderExit, withSpanRecorder } from './secret-scan.fixture.ts';
import { logMessages, startPostgresContainer } from './testing/index.ts';

const readSlowQuerySetting = (
  url: PostgresContainer['connectionUri'],
  slowQueryLog?: { readonly thresholdMs: number },
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        setting: string;
      }>`SELECT setting FROM pg_settings WHERE name = 'log_min_duration_statement'`;
      const setting = rows[0]?.setting;
      if (setting === undefined) {
        return yield* Effect.fail('log_min_duration_statement returned no row');
      }

      return setting;
    }),
  ).pipe(
    Effect.provide(
      makePgLayer({
        url,
        applicationName: 'app-slow-query-test',
        ...omitUndefined({ slowQueryLog }),
      }),
    ),
  );

/**
 * Mount point of the test container's tmpfs; the official images from PostgreSQL 18 on keep their
 * data directory under it.
 */
const TMPFS_MOUNT_PATH = '/var/lib/postgresql/';

/**
 * Password swapped into the container URI so a leak is detectable. The session fails at the TLS
 * handshake, before the password would be checked.
 */
const TLS_PROBE_PASSWORD = 'tls-probe-s3cret';

/**
 * The container URI with `TLS_PROBE_PASSWORD` as its password.
 */
const withProbePassword = (url: Redacted.Redacted): Redacted.Redacted => {
  const parsed = new URL(Redacted.value(url));
  parsed.password = TLS_PROBE_PASSWORD;
  return Redacted.make(parsed.toString());
};

/**
 * Password of a role that does not exist. It differs from the user name, database and host.
 */
const LEAKED_PASSWORD = 'Zq9-leak-pw';

/**
 * User name of that role. It contains the password because PostgreSQL quotes only the user name in
 * its authentication error: this is the one way a real server puts the password into the message
 * the client redacts.
 */
const LEAKED_USER = `app_${LEAKED_PASSWORD}`;

/**
 * Login role without superuser, used to check the slow-query privilege error.
 */
const PLAIN_ROLE = 'plain_app';

/**
 * Password of `PLAIN_ROLE`.
 */
const PLAIN_ROLE_PASSWORD = 'plain-pw';

/**
 * The container URI with its user and password replaced.
 */
const withCredentials = (
  url: Redacted.Redacted,
  user: string,
  password: string,
): Redacted.Redacted => {
  const parsed = new URL(Redacted.value(url));
  parsed.username = user;
  parsed.password = password;
  return Redacted.make(parsed.toString());
};

describe('makePgLayer', () => {
  let postgres: PostgresContainer;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
  });

  afterAll(async () => {
    await postgres.stop();
  });

  it('logs session create, disconnect, and pool close', async () => {
    const harness = makeTestLogger();
    await Effect.runPromise(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-test',
          }),
        ),
      ).pipe(Effect.provide(harness.layer)),
    );
    const logged = logMessages(harness.records);
    expect(logged).toContain('pg.client.opening');
    expect(logged).toContain('pg.connection.created');
    expect(logged).toContain('pg.client.ready');
    expect(logged).toContain('pg.connection.disconnected');
    expect(logged).toContain('pg.client.closed');
    expect(logged.indexOf('pg.connection.disconnected')).toBeLessThan(
      logged.indexOf('pg.client.closed'),
    );
    expect(JSON.stringify(harness.records)).not.toContain(Redacted.value(postgres.connectionUri));
  });

  it('logs the TLS mode and connects with tls disable', async () => {
    const harness = makeTestLogger();
    await Effect.runPromise(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-tls-disable-test',
            tls: 'disable',
          }),
        ),
      ).pipe(Effect.provide(harness.layer)),
    );
    const opening = harness.records.find((record) => record.message === 'pg.client.opening');
    expect(JSON.stringify(opening)).toContain('"tls":"disable"');
    expect(logMessages(harness.records)).toContain('pg.client.ready');
  });

  it('refuses a plaintext fallback when tls is require', async () => {
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: withProbePassword(postgres.connectionUri),
            applicationName: 'app-tls-require-test',
            tls: 'require',
          }),
        ),
      ).pipe(Effect.provide(harness.layer)),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(logMessages(harness.records)).not.toContain('pg.client.ready');
    const failed = harness.records.find((record) => record.message === 'pg.connection.failed');
    expect(JSON.stringify(failed)).toContain('Server refused TLS');
    expect(JSON.stringify(failed)).not.toContain('password authentication');
    expect(String(exit)).toContain('Server refused TLS');
    expect(renderExit(exit)).not.toContain(TLS_PROBE_PASSWORD);
    expect(reachableStrings(harness.records).join('\n')).not.toContain(TLS_PROBE_PASSWORD);
  });

  it('leaves slow-query logging off unless a threshold is configured', async () => {
    const disabled = await Effect.runPromise(readSlowQuerySetting(postgres.connectionUri));
    const enabled = await Effect.runPromise(
      readSlowQuerySetting(postgres.connectionUri, { thresholdMs: 250 }),
    );
    expect(disabled).toBe('-1');
    expect(enabled).toContain('250');
  });

  it('applies the default session timeouts', async () => {
    const settings = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{
            name: string;
            setting: string;
          }>`SELECT name, setting FROM pg_settings
            WHERE name IN ('statement_timeout', 'lock_timeout', 'idle_in_transaction_session_timeout')`;
          return Object.fromEntries(rows.map((row) => [row.name, row.setting]));
        }),
      ).pipe(
        Effect.provide(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-timeout-test',
          }),
        ),
      ),
    );
    expect(settings.statement_timeout).toBe('15000');
    expect(settings.lock_timeout).toBe('5000');
    expect(settings.idle_in_transaction_session_timeout).toBe('30000');
  });

  it('cancels a statement that exceeds statement_timeout', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        Effect.scoped(
          Effect.gen(function* () {
            const sql = yield* SqlClient.SqlClient;
            yield* sql`SELECT pg_sleep(3)`;
          }),
        ).pipe(
          Effect.provide(
            makePgLayer({
              url: postgres.connectionUri,
              applicationName: 'app-cancel-test',
              statementTimeout: '1 second',
            }),
          ),
        ),
      ),
    );
    expect(SqlError.isSqlError(error) && error.reason._tag === 'StatementTimeoutError').toBe(true);
  });

  it('answers ping and fails a second checkout while the only connection is busy', async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* ping;
          const release = yield* Deferred.make<void>();
          const held = yield* Effect.forkChild(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`SELECT 1`;
                yield* Deferred.await(release);
              }),
            ),
          );
          yield* Effect.yieldNow;
          const blocked = yield* sql`SELECT 1`.pipe(Effect.flip);
          yield* Deferred.succeed(release, undefined);
          yield* Fiber.join(held).pipe(Effect.ignore);
          expect(blocked._tag).toBe('SqlError');
        }),
      ).pipe(
        Effect.provide(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-pool-test',
            maxConnections: 1,
            acquireTimeout: '300 millis',
          }),
        ),
      ),
    );
  });

  it('keeps the data directory on the tmpfs mount', async () => {
    const [directory, mounts] = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const dirRows = yield* sql<{ data_directory: string }>`SHOW data_directory`;
          const mountRows = yield* sql<{
            mounts: string;
          }>`SELECT pg_read_file('/proc/mounts') AS mounts`;
          return [dirRows[0]?.data_directory, mountRows[0]?.mounts] as const;
        }),
      ).pipe(
        Effect.provide(
          makePgLayer({ url: postgres.connectionUri, applicationName: 'app-tmpfs-test' }),
        ),
      ),
    );
    expect(directory?.startsWith(TMPFS_MOUNT_PATH)).toBe(true);
    let servingMount = '';
    let filesystem: string | undefined;
    for (const line of (mounts ?? '').split('\n')) {
      const [, mountPoint, type] = line.split(' ');
      if (mountPoint === undefined || mountPoint.length <= servingMount.length) {
        continue;
      }

      const mountPrefix = mountPoint === '/' ? '/' : `${mountPoint}/`;
      if (mountPoint === directory || directory?.startsWith(mountPrefix)) {
        servingMount = mountPoint;
        filesystem = type;
      }
    }

    expect(filesystem).toBe('tmpfs');
  });

  it('removes the password a failed-login message quotes from logs and the returned error', async () => {
    const harness = makeTestLogger();
    const { exit, spans } = await Effect.runPromise(
      withSpanRecorder(
        Effect.scoped(
          Layer.build(
            makePgLayer({
              url: withCredentials(postgres.connectionUri, LEAKED_USER, LEAKED_PASSWORD),
            }),
          ),
        ).pipe(Effect.provide(harness.layer)),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    const failures = harness.records.filter(
      (record) =>
        record.message === 'pg.client.failed' || record.message === 'pg.connection.failed',
    );
    expect(failures.length).toBeGreaterThan(0);
    for (const failure of failures) {
      expect(failure.annotations.user).toBe(LEAKED_USER);
      expect(String(failure.annotations.errorMessage)).toContain(
        'password authentication failed for user "app_***"',
      );
    }

    const loggedOutsideUser = reachableStrings(harness.records).filter(
      (value) => value !== LEAKED_USER,
    );
    expect(loggedOutsideUser.join('\n')).not.toContain(LEAKED_PASSWORD);
    const rendered = renderExit(exit);
    expect(rendered).toContain('password authentication failed for user "app_***"');
    expect(rendered).not.toContain(LEAKED_PASSWORD);
    expect(spans.map((span) => span.renderedAttributes).join('\n')).not.toContain(LEAKED_PASSWORD);
  });

  it('fails with a PgConfigError naming the privilege when the role may not set slowQueryLog', async () => {
    const admin = new Client({ connectionString: Redacted.value(postgres.connectionUri) });
    await admin.connect();
    try {
      await admin.query(`CREATE ROLE ${PLAIN_ROLE} LOGIN PASSWORD '${PLAIN_ROLE_PASSWORD}'`);
    } finally {
      await admin.end();
    }

    const url = withCredentials(postgres.connectionUri, PLAIN_ROLE, PLAIN_ROLE_PASSWORD);
    await Effect.runPromise(
      Effect.scoped(Layer.build(makePgLayer({ url }))).pipe(Effect.provide(makeTestLogger().layer)),
    );
    const exit = await Effect.runPromiseExit(
      Effect.scoped(Layer.build(makePgLayer({ url, slowQueryLog: { thresholdMs: 500 } }))).pipe(
        Effect.provide(makeTestLogger().layer),
      ),
    );
    expect(String(exit)).toContain('PgConfigError');
    expect(String(exit)).toContain('GRANT SET ON PARAMETER log_min_duration_statement');
  });
});
