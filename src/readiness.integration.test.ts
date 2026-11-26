import { PgClient } from '@effect/sql-pg';
import { makeTestLogger } from '@yieldkit/logger/testing';
import { Duration, Effect, Exit, Layer, Redacted } from 'effect';
import { createServer } from 'node:net';
import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer } from './client.ts';
import { PgReadinessError } from './errors.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { checkReadiness } from './readiness.ts';
import { logMessages, startPostgresContainer } from './testing/index.ts';

/**
 * Retries the readiness failure cases ask for, so they expect three attempts.
 */
const FAILING_READINESS_RETRIES = 2;

/**
 * Password in the unreachable URI, so a leak into the error message is detectable.
 */
const UNREACHABLE_PASSWORD = 'unreachable-s3cret';

/**
 * A duration string `Duration.Input` does not accept, as untyped config could supply.
 */
const UNPARSEABLE_DURATION: string = '100ms';

/**
 * Claims that any string is a `Duration.Input`, as untyped config would. Used only to pass the
 * malformed value `checkReadiness` must reject at run time.
 */
const isClaimedDurationInput = (_value: string): _value is Duration.Input & string => true;

/**
 * Connection URI of `database` on the same server as `url`.
 */
const withDatabase = (url: Redacted.Redacted, database: string): Redacted.Redacted => {
  const parsed = new URL(Redacted.value(url));
  parsed.pathname = `/${database}`;
  return Redacted.make(parsed.toString());
};

/**
 * A local TCP port nothing listens on: the OS hands out a free port, which is released right away.
 */
const closedPort = (): Promise<number> =>
  new Promise((resolve, reject) => {
    const server = createServer();
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      const port = typeof address === 'object' && address !== null ? address.port : 0;
      server.close(() => {
        resolve(port);
      });
    });
  });

describe('checkReadiness', () => {
  let postgres: PostgresContainer;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
  });

  afterAll(async () => {
    await postgres.stop();
  });

  it('reports pool stats and latency when the database answers', async () => {
    const readiness = await Effect.runPromise(
      Effect.scoped(
        checkReadiness().pipe(
          Effect.provide(
            makePgLayer({
              url: postgres.connectionUri,
              applicationName: 'app-readiness-test',
              maxConnections: 4,
            }),
          ),
        ),
      ),
    );
    expect(readiness.attempts).toBe(1);
    expect(readiness.latencyMs).toBeGreaterThanOrEqual(0);
    expect(readiness.pool.max).toBe(4);
    expect(readiness.pool.pending).toBe(0);
  });

  it('fails with PgReadinessError after retries + 1 attempts when the database is gone', async () => {
    const admin = new Client({ connectionString: Redacted.value(postgres.connectionUri) });
    await admin.connect();
    await admin.query('CREATE DATABASE readiness_gone');
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            makePgLayer({
              url: withDatabase(postgres.connectionUri, 'readiness_gone'),
              applicationName: 'app-readiness-failure-test',
              maxConnections: 1,
            }),
          );
          yield* Effect.promise(() => admin.query('DROP DATABASE readiness_gone WITH (FORCE)'));
          return yield* checkReadiness({
            retries: FAILING_READINESS_RETRIES,
            baseDelay: '10 millis',
            attemptTimeout: '1 second',
          }).pipe(Effect.provide(context), Effect.flip);
        }),
      ).pipe(Effect.provide(harness.layer)),
    );
    await admin.end();
    expect(Exit.isSuccess(exit)).toBe(true);
    const error = Exit.isSuccess(exit) ? exit.value : undefined;
    expect(error).toBeInstanceOf(PgReadinessError);
    expect(error?.attempts).toBe(FAILING_READINESS_RETRIES + 1);
    expect(error?.message).not.toContain('test:test');
    expect(error?.message).not.toContain(Redacted.value(postgres.connectionUri));
    const logged = logMessages(harness.records);
    expect(logged.filter((message) => message === 'pg.readiness.retry')).toHaveLength(
      FAILING_READINESS_RETRIES,
    );
    expect(logged).toContain('pg.readiness.failed');
  });

  it('reports the pool stats shape', async () => {
    const readiness = await Effect.runPromise(
      Effect.scoped(
        checkReadiness().pipe(
          Effect.provide(
            makePgLayer({
              url: postgres.connectionUri,
              applicationName: 'app-readiness-stats-test',
              maxConnections: 3,
            }),
          ),
        ),
      ),
    );
    expect(Object.keys(readiness.pool).toSorted()).toEqual([
      'idle',
      'max',
      'min',
      'pending',
      'used',
    ]);
    expect(readiness.pool.max).toBe(3);
    expect(readiness.pool.min).toBeLessThanOrEqual(readiness.pool.max);
    expect(readiness.pool.idle + readiness.pool.used).toBeGreaterThanOrEqual(1);
  });

  it('rejects an invalid retries, delay or timeout option before any probe runs', async () => {
    const errors = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const negative = yield* checkReadiness({ retries: -1 }).pipe(Effect.flip);
          const fractional = yield* checkReadiness({ retries: 1.5 }).pipe(Effect.flip);
          const badDelay = yield* checkReadiness({
            baseDelay: Duration.infinity,
          }).pipe(Effect.flip);
          const zeroTimeout = yield* checkReadiness({ attemptTimeout: 0 }).pipe(Effect.flip);
          const unparseable = yield* checkReadiness({
            baseDelay: isClaimedDurationInput(UNPARSEABLE_DURATION) ? UNPARSEABLE_DURATION : 0,
          }).pipe(Effect.flip);
          return [negative, fractional, badDelay, zeroTimeout, unparseable];
        }).pipe(
          Effect.provide(
            makePgLayer({
              url: postgres.connectionUri,
              applicationName: 'app-readiness-retries-test',
              maxConnections: 1,
            }),
          ),
        ),
      ),
    );
    for (const error of errors) {
      expect(error).toBeInstanceOf(PgReadinessError);
      expect(error.attempts).toBe(0);
    }
  });

  it('fails with PgReadinessError after retries + 1 attempts against an unreachable port', async () => {
    const port = await closedPort();
    const harness = makeTestLogger();
    const url = Redacted.make(`postgres://app:${UNREACHABLE_PASSWORD}@127.0.0.1:${port}/app`);
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(PgClient.layer({ url, connectTimeout: '1 second' }));
          return yield* checkReadiness({
            retries: FAILING_READINESS_RETRIES,
            baseDelay: '10 millis',
            attemptTimeout: '2 seconds',
          }).pipe(Effect.provide(context), Effect.flip);
        }),
      ).pipe(Effect.provide(harness.layer)),
    );
    expect(Exit.isSuccess(exit)).toBe(true);
    const error = Exit.isSuccess(exit) ? exit.value : undefined;
    expect(error).toBeInstanceOf(PgReadinessError);
    expect(error?.attempts).toBe(FAILING_READINESS_RETRIES + 1);
    expect(error?.message).not.toContain(UNREACHABLE_PASSWORD);
    expect(error?.message).not.toContain(String(port));
    expect(JSON.stringify(harness.records)).not.toContain(UNREACHABLE_PASSWORD);
    expect(logMessages(harness.records)).toContain('pg.readiness.failed');
  });
});
