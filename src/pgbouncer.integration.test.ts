import { makeTestLogger } from '@yieldkit/logger/testing';
import { Effect, Exit, Layer, Redacted, Schedule } from 'effect';
import { SqlClient } from 'effect/sql';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PgLayerOptions } from './client.ts';
import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { logMessages, startPostgresContainer } from './testing/index.ts';

/**
 * PgBouncer image, pinned. Its default config refuses every startup parameter except a few such as
 * `application_name`.
 */
const PGBOUNCER_IMAGE = 'edoburu/pgbouncer:v1.26.1-p0';

/**
 * Port PgBouncer listens on inside its container.
 */
const PGBOUNCER_PORT = 5432;

/**
 * Polls before giving up on PgBouncer accepting connections, about 15 seconds.
 */
const READY_ATTEMPTS = 30;

/**
 * Delay between polls of PgBouncer's port.
 */
const READY_POLL_INTERVAL = '500 millis';

/**
 * Runs the `docker` command line with `args` and resolves with its standard output.
 */
const docker = async (...args: ReadonlyArray<string>): Promise<string> => {
  const { stdout } = await promisify(execFile)('docker', args);
  return stdout;
};

/**
 * Resolves once PgBouncer accepts TCP connections inside container `name`, polling up to
 * `READY_ATTEMPTS` times.
 */
const waitForPgBouncer = (name: string): Promise<void> =>
  Effect.runPromise(
    Effect.tryPromise({
      try: () => docker('exec', name, 'nc', '-z', '127.0.0.1', String(PGBOUNCER_PORT)),
      catch: () => `PgBouncer in ${name} is not accepting connections`,
    }).pipe(
      Effect.retry({
        schedule: Schedule.spaced(READY_POLL_INTERVAL),
        times: READY_ATTEMPTS,
      }),
      Effect.asVoid,
    ),
  );

/**
 * Builds `makePgLayer(options)`, runs a parameterized query and reads `statement_timeout` through
 * it, and returns the exit (`'<42> <statement_timeout>'` on success) and the captured log records.
 */
const readStatementTimeout = async (options: PgLayerOptions) => {
  const harness = makeTestLogger();
  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const echoed = yield* sql<{ n: number }>`SELECT ${41}::int + 1 AS n`;
      const rows = yield* sql<{ statement_timeout: string }>`SHOW statement_timeout`;
      return `${String(echoed[0]?.n)} ${String(rows[0]?.statement_timeout)}`;
    }).pipe(Effect.provide(makePgLayer(options).pipe(Layer.provide(harness.layer)))),
  );
  return { exit, records: harness.records };
};

describe('makePgLayer through PgBouncer in transaction mode', () => {
  let postgres: PostgresContainer;
  let containerName: string;
  let bouncerUrl: Redacted.Redacted;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
    const upstream = new URL(Redacted.value(postgres.connectionUri));
    upstream.hostname = 'host.docker.internal';
    containerName = `pg-test-pgbouncer-${process.pid}`;
    await docker(
      'run',
      '-d',
      '--rm',
      '--name',
      containerName,
      '--add-host=host.docker.internal:host-gateway',
      '-p',
      `127.0.0.1::${String(PGBOUNCER_PORT)}`,
      '-e',
      `DATABASE_URL=${upstream.toString()}`,
      '-e',
      'AUTH_TYPE=scram-sha-256',
      '-e',
      'POOL_MODE=transaction',
      PGBOUNCER_IMAGE,
    );
    const mapped = (await docker('port', containerName, String(PGBOUNCER_PORT))).trim();
    const port = mapped.split(':').at(-1) ?? '';
    const bouncer = new URL(Redacted.value(postgres.connectionUri));
    bouncer.hostname = '127.0.0.1';
    bouncer.port = port;
    bouncerUrl = Redacted.make(bouncer.toString());
    await waitForPgBouncer(containerName);
  });

  afterAll(async () => {
    await docker('rm', '-f', containerName).catch(() => '');
    await postgres.stop();
  });

  it('fails without retrying when the default session timeouts are sent', async () => {
    const { exit, records } = await readStatementTimeout({ url: bouncerUrl });
    expect(Exit.isFailure(exit)).toBe(true);
    expect(JSON.stringify(records)).toContain('unsupported startup parameter');
    expect(logMessages(records)).not.toContain('pg.client.reconnecting');
  });

  it('connects when every session timeout is false, leaving the server setting', async () => {
    const { exit } = await readStatementTimeout({
      url: bouncerUrl,
      statementTimeout: false,
      lockTimeout: false,
      idleInTransactionTimeout: false,
    });
    expect(exit).toEqual(Exit.succeed('42 0'));
  });
});
