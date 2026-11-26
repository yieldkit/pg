import { makeTestLogger } from '@yieldkit/logger/testing';
import { Effect, Layer, Schedule } from 'effect';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { ListenStatus } from './listen.ts';
import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { listenWithRetry } from './listen.ts';
import { LISTEN_STABLE_AFTER } from './policy.ts';
import { logMessages, startPostgresContainer } from './testing/index.ts';

const waitForLog = (records: ReadonlyArray<{ readonly message: unknown }>, message: string) =>
  Effect.gen(function* () {
    if (!logMessages(records).includes(message)) {
      return yield* Effect.fail(`missing ${message}`);
    }

    return undefined;
  }).pipe(
    Effect.retry({
      times: 50,
      schedule: Schedule.spaced('100 millis'),
    }),
  );

describe('listenWithRetry', () => {
  let postgres: PostgresContainer;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
  });

  afterAll(async () => {
    await postgres.stop();
  });

  it('logs connect, disconnect, and reconnect when a payload handler fails', async () => {
    const harness = makeTestLogger();
    const layer = makePgLayer({
      url: postgres.connectionUri,
      applicationName: 'app-listen-test',
    });
    let dropped = false;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* Effect.forkScoped(
            listenWithRetry('app_events', () => {
              if (!dropped) {
                dropped = true;
                return Effect.fail('handler dropped the payload');
              }

              return Effect.void;
            }).pipe(Effect.provide(harness.layer)),
          );
          yield* waitForLog(harness.records, 'pg.listen.connected');
          yield* sql`SELECT pg_notify('app_events', 'ping')`;
          yield* waitForLog(harness.records, 'pg.listen.disconnected');
          yield* waitForLog(harness.records, 'pg.listen.reconnected');
        }),
      ).pipe(Effect.provide(Layer.provideMerge(layer, harness.layer))),
    );
    const logged = logMessages(harness.records);
    expect(logged).toContain('pg.listen.connected');
    expect(logged).toContain('pg.listen.disconnected');
    expect(logged).toContain('pg.listen.reconnected');
    expect(logged.indexOf('pg.listen.connected')).toBeLessThan(
      logged.indexOf('pg.listen.disconnected'),
    );
    expect(logged.indexOf('pg.listen.disconnected')).toBeLessThan(
      logged.indexOf('pg.listen.reconnected'),
    );
  });

  it('logs a closed listen without reconnecting when the scope ends', async () => {
    const harness = makeTestLogger();
    const layer = makePgLayer({
      url: postgres.connectionUri,
      applicationName: 'app-listen-close-test',
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.forkScoped(
            listenWithRetry('app_events', () => Effect.void).pipe(Effect.provide(harness.layer)),
          );
          yield* waitForLog(harness.records, 'pg.listen.connected');
        }),
      ).pipe(Effect.provide(Layer.provideMerge(layer, harness.layer))),
    );
    const logged = logMessages(harness.records);
    expect(logged).toContain('pg.listen.disconnected');
    expect(logged).not.toContain('pg.listen.reconnected');
  });

  it('reconnects from the base delay when the backend is terminated after a stable session', async () => {
    const harness = makeTestLogger();
    const layer = makePgLayer({
      url: postgres.connectionUri,
      applicationName: 'app-listen-terminate-test',
    });
    const statuses: Array<ListenStatus> = [];
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* Effect.forkScoped(
            listenWithRetry('app_terminate', () => Effect.void, {
              onStatusChange: (status) => Effect.sync(() => statuses.push(status)),
            }).pipe(Effect.provide(harness.layer)),
          );
          yield* waitForLog(harness.records, 'pg.listen.connected');
          yield* Effect.sleep(LISTEN_STABLE_AFTER);
          yield* Effect.sleep('500 millis');
          yield* sql`
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE query = 'LISTEN "app_terminate"'
          `;
          yield* waitForLog(harness.records, 'pg.listen.reconnected');
        }),
      ).pipe(Effect.provide(Layer.provideMerge(layer, harness.layer))),
    );
    const reconnecting = statuses.find((status) => status._tag === 'Reconnecting');
    expect(reconnecting?._tag === 'Reconnecting' && reconnecting.attempt).toBe(1);
    expect(reconnecting?._tag === 'Reconnecting' && reconnecting.delayMs).toBeLessThanOrEqual(600);
    expect(statuses.filter((status) => status._tag === 'Connected')).toHaveLength(2);
  });
});
