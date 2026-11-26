import { Deferred, Effect, Exit, Fiber, Schema } from 'effect';
import { SqlClient } from 'effect/sql';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PostgresContainer } from './testing/index.ts';

import { withAdvisoryXactLock } from './advisory-lock.ts';
import { makePgLayer } from './client.ts';
import { PgConfigError } from './errors.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { startPostgresContainer } from './testing/index.ts';

/**
 * Pool size that lets a holder and a waiter use separate connections at once.
 */
const POOL_SIZE = 4;

/**
 * How long a case waits to be sure a blocked fiber would have run by now.
 */
const BLOCKED_FOR = '500 millis';

/**
 * Claims that any array of strings is a pair key, as a JavaScript caller or a cast would. Used only
 * to pass the malformed key `withAdvisoryXactLock` must reject at run time.
 */
const isClaimedPairKey = (_key: ReadonlyArray<string>): _key is readonly [string, string] => true;

/**
 * A one-element array, which `hashtext(NULL)` would turn into no lock at all.
 */
const ONE_ELEMENT_KEY: ReadonlyArray<string> = ['only-one'];

/**
 * Runs `program` with a scoped pool against the shared container.
 */
const runWithPool = <A, E>(
  postgres: PostgresContainer,
  program: Effect.Effect<A, E, SqlClient.SqlClient>,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      program.pipe(
        Effect.provide(
          makePgLayer({
            url: postgres.connectionUri,
            applicationName: 'app-advisory-lock-test',
            maxConnections: POOL_SIZE,
          }),
        ),
      ),
    ),
  );

/**
 * Starts a fiber that takes the lock, records `events`, and holds the transaction until `release`
 * resolves. Resolves once the lock is held.
 */
const holdLock = (
  key: string | readonly [string, string],
  events: Array<string>,
  release: Deferred.Deferred<void>,
) =>
  Effect.gen(function* () {
    const held = yield* Deferred.make<void>();
    const fiber = yield* Effect.forkChild(
      withAdvisoryXactLock(
        key,
        Effect.gen(function* () {
          events.push('first acquired');
          yield* Deferred.succeed(held, undefined);
          yield* Deferred.await(release);
          events.push('first committing');
        }),
      ),
    );
    yield* Deferred.await(held);
    return fiber;
  });

describe('withAdvisoryXactLock', () => {
  let postgres: PostgresContainer;

  beforeAll(async () => {
    postgres = await startPostgresContainer({ image: INTEGRATION_IMAGE });
  });

  afterAll(async () => {
    await postgres.stop();
  });

  it('runs a second fiber on the same key only after the first commits', async () => {
    const events: Array<string> = [];
    await runWithPool(
      postgres,
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const first = yield* holdLock('invoice:1', events, release);
        const second = yield* Effect.forkChild(
          withAdvisoryXactLock(
            'invoice:1',
            Effect.sync(() => events.push('second acquired')),
          ),
        );
        yield* Effect.sleep(BLOCKED_FOR);
        expect(events).toEqual(['first acquired']);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(second);
      }),
    );
    expect(events).toEqual(['first acquired', 'first committing', 'second acquired']);
  });

  it('does not block a different key', async () => {
    const events: Array<string> = [];
    await runWithPool(
      postgres,
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const first = yield* holdLock('invoice:1', events, release);
        yield* withAdvisoryXactLock(
          'invoice:2',
          Effect.sync(() => events.push('other acquired')),
        );
        expect(events).toEqual(['first acquired', 'other acquired']);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
      }),
    );
  });

  it('serializes the two-key variant on the same pair', async () => {
    const events: Array<string> = [];
    await runWithPool(
      postgres,
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const first = yield* holdLock(['csv-repeat:ws1', 'abc'], events, release);
        const same = yield* Effect.forkChild(
          withAdvisoryXactLock(
            ['csv-repeat:ws1', 'abc'],
            Effect.sync(() => events.push('same pair acquired')),
          ),
        );
        yield* withAdvisoryXactLock(
          ['csv-repeat:ws1', 'def'],
          Effect.sync(() => events.push('other pair acquired')),
        );
        yield* Effect.sleep(BLOCKED_FOR);
        expect(events).toEqual(['first acquired', 'other pair acquired']);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
        yield* Fiber.join(same);
      }),
    );
    expect(events).toEqual([
      'first acquired',
      'other pair acquired',
      'first committing',
      'same pair acquired',
    ]);
  });

  it('releases the lock and rolls back when the effect fails', async () => {
    const outcome = await runWithPool(
      postgres,
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE IF NOT EXISTS lock_rollback_probe (id INT)`;
        const failed = yield* withAdvisoryXactLock(
          'invoice:rollback',
          sql`INSERT INTO lock_rollback_probe VALUES (1)`.pipe(
            Effect.andThen(Effect.fail('boom' as const)),
          ),
        ).pipe(Effect.exit);
        const reacquired = yield* withAdvisoryXactLock(
          'invoice:rollback',
          Effect.succeed('ok'),
        ).pipe(Effect.timeout('2 seconds'));
        const rows = yield* sql<{
          total: number;
        }>`SELECT count(*)::int AS total FROM lock_rollback_probe`;
        return { failed, reacquired, total: rows[0]?.total };
      }),
    );
    expect(Exit.isFailure(outcome.failed)).toBe(true);
    expect(outcome.reacquired).toBe('ok');
    expect(outcome.total).toBe(0);
  });

  it('rejects an empty key or a one-element pair before running any statement', async () => {
    const errors = await runWithPool(
      postgres,
      Effect.gen(function* () {
        const single = yield* withAdvisoryXactLock('', Effect.void).pipe(Effect.flip);
        const pair = yield* withAdvisoryXactLock(['a', ''], Effect.void).pipe(Effect.flip);
        const short = yield* withAdvisoryXactLock(
          isClaimedPairKey(ONE_ELEMENT_KEY) ? ONE_ELEMENT_KEY : 'unreachable',
          Effect.void,
        ).pipe(Effect.flip);
        return [single, pair, short];
      }),
    );
    expect(errors.every((error) => Schema.is(PgConfigError)(error))).toBe(true);
  });
});
