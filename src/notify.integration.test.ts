import { PgClient } from '@effect/sql-pg';
import { Effect, Queue, Stream } from 'effect';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PostgresContainer } from './testing/index.ts';

import { makePgLayer } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { notifyAfterCommit } from './notify.ts';
import { startPostgresContainer } from './testing/index.ts';

describe('notifyAfterCommit', () => {
  let postgres: PostgresContainer;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
  });

  afterAll(async () => {
    await postgres.stop();
  });

  it('delivers a payload published after the transaction commits', async () => {
    const payload = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const pg = yield* PgClient.PgClient;
          const received = yield* Queue.unbounded<string>();
          const notifications = yield* pg.listen('app_events');
          yield* Effect.forkScoped(
            Stream.fromQueue(notifications).pipe(
              Stream.runForEach((note) => Queue.offer(received, note.payload).pipe(Effect.asVoid)),
            ),
          );
          yield* sql.withTransaction(notifyAfterCommit(sql, pg, 'app_events', '{"ok":true}'));
          return yield* Queue.take(received);
        }),
      ).pipe(
        Effect.provide(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-notify-test',
          }),
        ),
      ),
    );
    expect(payload).toBe('{"ok":true}');
  });

  it('does not deliver a notify when the transaction rolls back', async () => {
    const { rollback, delivered } = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const pg = yield* PgClient.PgClient;
          const queue = yield* Queue.unbounded<string>();
          const notifications = yield* pg.listen('app_events');
          yield* Effect.forkScoped(
            Stream.fromQueue(notifications).pipe(
              Stream.runForEach((note) => Queue.offer(queue, note.payload).pipe(Effect.asVoid)),
            ),
          );
          const rollbackReason = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                yield* notifyAfterCommit(sql, pg, 'app_events', 'should-not-send');
                return yield* Effect.fail('rollback');
              }),
            )
            .pipe(Effect.flip);
          yield* sql.withTransaction(notifyAfterCommit(sql, pg, 'app_events', 'marker'));
          const payloads: Array<string> = [];
          for (;;) {
            const payload = yield* Queue.take(queue);
            payloads.push(payload);
            if (payload === 'marker') {
              return { rollback: rollbackReason, delivered: payloads };
            }
          }
        }),
      ).pipe(
        Effect.provide(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-notify-rollback-test',
          }),
        ),
      ),
    );
    expect(rollback).toBe('rollback');
    expect(delivered).toEqual(['marker']);
  });
});
