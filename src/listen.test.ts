import type { PgConnection } from '@effect/sql-pg';

import { PgClient } from '@effect/sql-pg';
import { makeTestLogger } from '@yieldkit/logger/testing';
import { Data, Deferred, Effect, Fiber, Metric, Queue, Random } from 'effect';
import { SqlError } from 'effect/sql';
import { TestClock } from 'effect/testing';
import { describe, expect, it } from 'vite-plus/test';

import type { ListenStatus } from './listen.ts';

import { listenWithRetry, pgListenConnected, pgListenDropped } from './listen.ts';
import { logMessages } from './testing/index.ts';

const CHANNEL = 'app_events';

const RANDOM_SEED = 'listen-backoff';

const BASE_MS = 500;

const CAP_MS = 30_000;

const JITTER_MIN = 0.8;

const JITTER_MAX = 1.2;

/**
 * Turns of the scheduler given to the listener after each step so it reaches its next sleep.
 */
const SETTLE_TURNS = 50;

/**
 * Time the blocked handler takes to finish after the transport drops.
 */
const HANDLER_DRAIN_MS = 6000;

const connectionLost = new SqlError.SqlError({
  reason: new SqlError.ConnectionError({
    message: 'connection lost',
    cause: new Error('ECONNRESET'),
  }),
});

/**
 * Failure raised by the test payload handler when it rejects a payload.
 */
class HandlerRejectedError extends Data.TaggedError('HandlerRejectedError')<{
  readonly message: string;
}> {}

type NotificationQueue = Queue.Queue<PgConnection.Notification, SqlError.SqlError>;

/**
 * What the stub does on one `listen` call: fail before `LISTEN` is confirmed, or confirm it and
 * hand back a queue the test controls.
 */
type ListenPlan = 'fail' | 'confirm';

type Harness = {
  readonly statuses: Array<ListenStatus>;
  readonly queues: Array<NotificationQueue>;
  readonly calls: { count: number };
  readonly stub: Pick<PgClient.PgClient, 'listen'>;
};

/**
 * Stub `PgClient` whose `listen` follows `plans` in order, then fails on every later call.
 */
const makeHarness = (plans: ReadonlyArray<ListenPlan>): Harness => {
  const statuses: Array<ListenStatus> = [];
  const queues: Array<NotificationQueue> = [];
  const calls = { count: 0 };
  const stub: Pick<PgClient.PgClient, 'listen'> = {
    listen: () =>
      Effect.gen(function* () {
        const plan = plans[calls.count] ?? 'fail';
        calls.count += 1;
        if (plan === 'fail') {
          return yield* connectionLost;
        }

        const queue = yield* Queue.unbounded<PgConnection.Notification, SqlError.SqlError>();
        queues.push(queue);
        return queue;
      }),
  };
  return { statuses, queues, calls, stub };
};

/**
 * Narrows the stub to `PgClient` by the one member `listenWithRetry` uses. Calling any other member
 * would fail the test, which is what a stub should do.
 */
const isListenOnlyPgClient = (stub: Pick<PgClient.PgClient, 'listen'>): stub is PgClient.PgClient =>
  typeof stub.listen === 'function';

/**
 * The stub as the `PgClient` service.
 */
const asPgClient = (stub: Pick<PgClient.PgClient, 'listen'>): PgClient.PgClient => {
  if (!isListenOnlyPgClient(stub)) {
    throw new Error('stub must implement listen');
  }

  return stub;
};

/**
 * Queue handed to the listener on its `index`-th confirmed session. Dies when there is none yet.
 */
const sessionQueue = (harness: Harness, index: number): Effect.Effect<NotificationQueue> => {
  const queue = harness.queues[index];
  return queue === undefined ? Effect.die(`no session ${index} yet`) : Effect.succeed(queue);
};

/**
 * Forks `listenWithRetry` against the harness stub with a seeded `Random`, recording statuses.
 */
const forkListener = (
  harness: Harness,
  handlePayload: (payload: string) => Effect.Effect<void, unknown> = () => Effect.void,
) =>
  listenWithRetry(CHANNEL, handlePayload, {
    onStatusChange: (status) => Effect.sync(() => harness.statuses.push(status)),
  }).pipe(
    Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
    Random.withSeed(RANDOM_SEED),
    Effect.forkChild,
  );

/**
 * Lets forked fibers run until they block on the clock or a queue.
 */
const settle = Effect.gen(function* () {
  for (let turn = 0; turn < SETTLE_TURNS; turn += 1) {
    yield* Effect.yieldNow;
  }
});

/**
 * Advances the test clock and lets the listener react.
 */
const advance = (millis: number) => Effect.andThen(TestClock.adjust(`${millis} millis`), settle);

const lastStatus = (harness: Harness): ListenStatus | undefined => harness.statuses.at(-1);

/**
 * The `Reconnecting` status most recently reported. Fails the test when the last status is another.
 */
const lastReconnecting = (harness: Harness) => {
  const status = lastStatus(harness);
  if (status?._tag !== 'Reconnecting') {
    throw new Error(`expected Reconnecting, got ${status?._tag ?? 'nothing'}`);
  }

  return status;
};

/**
 * Asserts `delayMs` is `expectedMs` with jitter applied.
 */
const expectJittered = (delayMs: number, expectedMs: number): void => {
  expect(delayMs).toBeGreaterThanOrEqual(Math.floor(expectedMs * JITTER_MIN));
  expect(delayMs).toBeLessThanOrEqual(Math.ceil(expectedMs * JITTER_MAX));
};

const gaugeValue = Effect.map(
  Metric.value(Metric.withAttributes(pgListenConnected, { channel: CHANNEL })),
  (state) => state.value,
);

const droppedValue = Effect.map(
  Metric.value(Metric.withAttributes(pgListenDropped, { channel: CHANNEL })),
  (state) => state.count,
);

/**
 * Names of the metric series registered for `CHANNEL`.
 */
const channelSeries = Effect.map(Metric.snapshot, (snapshots) =>
  snapshots
    .filter((snapshot) => snapshot.attributes?.channel === CHANNEL)
    .map((snapshot) => snapshot.id),
);

/**
 * Offers `payloads` to the listener's `index`-th session queue, as PostgreSQL would deliver them.
 */
const deliver = (harness: Harness, index: number, payloads: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const queue = yield* sessionQueue(harness, index);
    for (const payload of payloads) {
      yield* Queue.offer(queue, { channel: CHANNEL, payload, processId: 1 });
    }
  });

/**
 * Runs a test program with a fresh test clock, metric registry and in-memory logger, which captures
 * debug lines and keeps them off the console.
 */
const runTest = <A, E>(
  program: Effect.Effect<A, E>,
  logger = makeTestLogger({ minLevel: 'Debug' }),
): Promise<A> =>
  Effect.runPromise(
    program.pipe(
      Effect.provide(TestClock.layer()),
      Effect.provideService(Metric.MetricRegistry, new Map()),
      Effect.provide(logger.layer),
    ),
  );

describe('listenWithRetry backoff', () => {
  it('doubles the delay after each failed attempt and caps it at 30 seconds', async () => {
    const harness = makeHarness([]);
    const delays = await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        const observed: Array<number> = [];
        for (let attempt = 1; attempt <= 10; attempt += 1) {
          const status = lastReconnecting(harness);
          expect(status.attempt).toBe(attempt);
          expect(status.lastError).toContain('connection lost');
          observed.push(status.delayMs);
          yield* advance(status.delayMs);
        }

        yield* Fiber.interrupt(fiber);
        return observed;
      }),
    );
    delays.forEach((delayMs, index) => {
      expectJittered(delayMs, Math.min(BASE_MS * 2 ** index, CAP_MS));
    });
    expect(harness.calls.count).toBe(11);
  });

  it('starts again from 500 ms after a session that stayed up for 5 seconds', async () => {
    const harness = makeHarness(['fail', 'fail', 'fail', 'confirm']);
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          yield* advance(lastReconnecting(harness).delayMs);
        }

        expect(lastStatus(harness)?._tag).toBe('Connected');
        yield* advance(6000);
        yield* Queue.fail(yield* sessionQueue(harness, 0), connectionLost);
        yield* settle;
        const status = lastReconnecting(harness);
        expect(status.attempt).toBe(1);
        expectJittered(status.delayMs, BASE_MS);
        yield* Fiber.interrupt(fiber);
      }),
    );
  });

  it('keeps backing off when a session confirms and drops within 5 seconds', async () => {
    const harness = makeHarness(['fail', 'fail', 'fail', 'confirm']);
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        for (let attempt = 1; attempt <= 3; attempt += 1) {
          yield* advance(lastReconnecting(harness).delayMs);
        }

        expect(lastStatus(harness)?._tag).toBe('Connected');
        yield* advance(1000);
        yield* Queue.fail(yield* sessionQueue(harness, 0), connectionLost);
        yield* settle;
        const status = lastReconnecting(harness);
        expect(status.attempt).toBe(4);
        expectJittered(status.delayMs, BASE_MS * 2 ** 3);
        yield* Fiber.interrupt(fiber);
      }),
    );
  });

  it('clears the gauge on disconnect and excludes handler drain time from session uptime', async () => {
    const harness = makeHarness(['fail', 'confirm']);
    await runTest(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const handle = Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
        const fiber = yield* forkListener(harness, () => handle);
        yield* settle;
        yield* advance(lastReconnecting(harness).delayMs);
        yield* deliver(harness, 0, ['blocked']);
        yield* Deferred.await(started);
        yield* Queue.fail(yield* sessionQueue(harness, 0), connectionLost);
        yield* settle;
        const gaugeAfterDrop = yield* gaugeValue;
        yield* advance(HANDLER_DRAIN_MS);
        yield* Deferred.succeed(release, undefined);
        yield* settle;
        expect({ gaugeAfterDrop, attempt: lastReconnecting(harness).attempt }).toEqual({
          gaugeAfterDrop: 0,
          attempt: 2,
        });
        yield* Fiber.interrupt(fiber);
      }),
    );
  });
});

describe('listenWithRetry sessions', () => {
  it('reconnects after the notification queue shuts down cleanly', async () => {
    const harness = makeHarness(['confirm', 'confirm']);
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        yield* Queue.shutdown(yield* sessionQueue(harness, 0));
        yield* settle;
        const status = lastReconnecting(harness);
        expect(status.lastError).toBeUndefined();
        yield* advance(status.delayMs);
        expect(harness.calls.count).toBe(2);
        expect(lastStatus(harness)?._tag).toBe('Connected');
        yield* Fiber.interrupt(fiber);
      }),
    );
  });

  it('reports Connecting, Connected, Reconnecting, Connected and tracks the gauge', async () => {
    const harness = makeHarness(['confirm', 'confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    const gauges = await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness, (payload) =>
          payload === 'bad'
            ? Effect.fail(new HandlerRejectedError({ message: 'handler rejected bad' }))
            : Effect.void,
        );
        yield* settle;
        const connected = yield* gaugeValue;
        yield* Queue.offer(yield* sessionQueue(harness, 0), {
          channel: CHANNEL,
          payload: 'bad',
          processId: 1,
        });
        yield* settle;
        const reconnecting = yield* gaugeValue;
        yield* advance(lastReconnecting(harness).delayMs);
        const reconnected = yield* gaugeValue;
        yield* Fiber.interrupt(fiber);
        const interrupted = yield* gaugeValue;
        return { connected, reconnecting, reconnected, interrupted };
      }),
      logger,
    );
    expect(harness.statuses.map((status) => status._tag)).toEqual([
      'Connecting',
      'Connected',
      'Reconnecting',
      'Connected',
    ]);
    const reconnecting = harness.statuses[2];
    expect(reconnecting?._tag === 'Reconnecting' && reconnecting.lastError).toBe(
      'HandlerRejectedError',
    );
    expect(gauges).toEqual({ connected: 1, reconnecting: 0, reconnected: 1, interrupted: 0 });
    const logged = logMessages(logger.records);
    expect(logged).toEqual([
      'pg.listen.connected',
      'pg.listen.disconnected',
      'pg.listen.failed',
      'pg.listen.retry_scheduled',
      'pg.listen.reconnected',
      'pg.listen.disconnected',
    ]);
  });

  it('describes a non-Error handler failure by its _tag in pg.listen.disconnected', async () => {
    const harness = makeHarness(['confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness, () => Effect.fail({ _tag: 'PayloadRejected' }));
        yield* settle;
        yield* Queue.offer(yield* sessionQueue(harness, 0), {
          channel: CHANNEL,
          payload: 'any',
          processId: 1,
        });
        yield* settle;
        yield* Fiber.interrupt(fiber);
      }),
      logger,
    );
    const disconnected = logger.records.find(
      (record) => record.message === 'pg.listen.disconnected',
    );
    expect(disconnected?.annotations.errorMessage).toBe('PayloadRejected');
  });

  it('resets the gauge when interrupted while onStatusChange is still running', async () => {
    const harness = makeHarness(['confirm']);
    const gauges = await runTest(
      Effect.gen(function* () {
        const fiber = yield* listenWithRetry(CHANNEL, () => Effect.void, {
          onStatusChange: (status) => (status._tag === 'Connected' ? Effect.never : Effect.void),
        }).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
          Effect.forkChild,
        );
        yield* settle;
        const connected = yield* gaugeValue;
        yield* Fiber.interrupt(fiber);
        const interrupted = yield* gaugeValue;
        return { connected, interrupted };
      }),
    );
    expect(gauges).toEqual({ connected: 1, interrupted: 0 });
  });

  it('stops reconnecting once the fiber is interrupted', async () => {
    const harness = makeHarness([]);
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        expect(harness.calls.count).toBe(1);
        yield* Fiber.interrupt(fiber);
        yield* advance(CAP_MS * 10);
        expect(harness.calls.count).toBe(1);
        expect(yield* gaugeValue).toBe(0);
      }),
    );
  });

  it('fails with PgChannelError before calling listen for an invalid channel', async () => {
    const harness = makeHarness([]);
    const error = await runTest(
      Effect.flip(
        listenWithRetry('bad channel', () => Effect.void).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
        ),
      ),
    );
    expect(error._tag).toBe('PgChannelError');
    expect(harness.calls.count).toBe(0);
  });

  it('keeps the handler error message out of logs and the status', async () => {
    const harness = makeHarness(['confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness, (payload) =>
          Effect.fail(new HandlerRejectedError({ message: `rejected ${payload}` })),
        );
        yield* settle;
        yield* deliver(harness, 0, ['secret-payload']);
        yield* settle;
        yield* Fiber.interrupt(fiber);
      }),
      logger,
    );
    expect(JSON.stringify(logger.records)).not.toContain('secret-payload');
    expect(JSON.stringify(harness.statuses)).not.toContain('secret-payload');
  });

  const handlerOutcomes: ReadonlyArray<
    readonly [string, (error: SqlError.SqlError) => Effect.Effect<void, SqlError.SqlError>]
  > = [
    ['fails', (error) => Effect.fail(error)],
    ['dies', (error) => Effect.die(error)],
    [
      'throws',
      (error) => {
        throw error;
      },
    ],
  ];
  for (const [name, fail] of handlerOutcomes) {
    it(`keeps the payload out of logs and the status when the handler ${name} with a SqlError`, async () => {
      const harness = makeHarness(['confirm']);
      const logger = makeTestLogger({ minLevel: 'Debug' });
      await runTest(
        Effect.gen(function* () {
          const fiber = yield* forkListener(harness, (payload) =>
            fail(
              new SqlError.SqlError({
                reason: new SqlError.ConstraintError({
                  message: `invalid input syntax for type uuid: "${payload}"`,
                  cause: new Error(`invalid input syntax for type uuid: "${payload}"`),
                }),
              }),
            ),
          );
          yield* settle;
          yield* deliver(harness, 0, ['secret-payload']);
          yield* settle;
          yield* Fiber.interrupt(fiber);
        }),
        logger,
      );
      expect(logMessages(logger.records)).toContain('pg.listen.failed');
      expect(JSON.stringify(logger.records)).not.toContain('secret-payload');
      expect(JSON.stringify(harness.statuses)).not.toContain('secret-payload');
      expect(JSON.stringify(harness.statuses)).toContain('SqlError');
    });
  }

  it('keeps listening when onStatusChange dies', async () => {
    const harness = makeHarness(['confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    await runTest(
      Effect.gen(function* () {
        const fiber = yield* listenWithRetry(CHANNEL, () => Effect.void, {
          onStatusChange: (status) =>
            status._tag === 'Connecting' ? Effect.die(new Error('callback bug')) : Effect.void,
        }).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
          Effect.forkChild,
        );
        yield* settle;
        expect(harness.calls.count).toBe(1);
        yield* Fiber.interrupt(fiber);
      }),
      logger,
    );
    expect(logMessages(logger.records)).toContain('pg.listen.status_callback_failed');
    expect(logMessages(logger.records)).toContain('pg.listen.connected');
  });

  for (const tag of ['Connecting', 'Connected', 'Reconnecting'] as const) {
    it(`keeps listening and delivers when onStatusChange throws on ${tag}`, async () => {
      const harness = makeHarness(['fail', 'confirm']);
      const logger = makeTestLogger({ minLevel: 'Debug' });
      const handled: Array<string> = [];
      await runTest(
        Effect.gen(function* () {
          const fiber = yield* listenWithRetry(
            CHANNEL,
            (payload) => Effect.sync(() => handled.push(payload)),
            {
              onStatusChange: (status) => {
                harness.statuses.push(status);
                if (status._tag === tag) {
                  throw new Error('callback bug');
                }

                return Effect.void;
              },
            },
          ).pipe(
            Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
            Effect.forkChild,
          );
          yield* settle;
          yield* advance(lastReconnecting(harness).delayMs);
          yield* deliver(harness, 0, ['first']);
          yield* settle;
          yield* Fiber.interrupt(fiber);
        }),
        logger,
      );
      expect(handled).toEqual(['first']);
      expect(harness.calls.count).toBe(2);
      expect(logMessages(logger.records)).toContain('pg.listen.status_callback_failed');
    });
  }
});

/**
 * Most `settle` rounds a test waits for the handler to catch up with a burst.
 */
const MAX_SETTLE_ROUNDS = 20;

/**
 * Notifications sent in each tick of the flood test.
 */
const FLOOD_NOTIFICATIONS_PER_TICK = 16;

/**
 * How many ticks the flood test delivers while the handler is blocked.
 */
const FLOOD_TICKS = 400;

/**
 * Well above what one tick leaves queued and far below a backlog that grows for the entire flood.
 */
const MAX_DRIVER_BACKLOG_NOTIFICATIONS = 64;

describe('listenWithRetry buffering', () => {
  it('drops notifications past bufferCapacity while the handler is slow, and counts them', async () => {
    const harness = makeHarness(['confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    const handled: Array<string> = [];
    const { dropped } = await runTest(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const fiber = yield* listenWithRetry(
          CHANNEL,
          (payload) =>
            Deferred.await(release).pipe(Effect.andThen(Effect.sync(() => handled.push(payload)))),
          { bufferCapacity: 2 },
        ).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
          Effect.forkChild,
        );
        yield* settle;
        yield* deliver(harness, 0, ['1', '2', '3', '4', '5', '6']);
        yield* settle;
        const droppedWhileBlocked = yield* droppedValue;
        yield* Deferred.succeed(release, undefined);
        yield* settle;
        yield* Fiber.interrupt(fiber);
        return { dropped: droppedWhileBlocked };
      }),
      logger,
    );
    expect(dropped).toBeGreaterThanOrEqual(3);
    expect(handled.length + dropped).toBe(6);
    expect(handled[0]).toBe('1');
    const overflows = logMessages(logger.records).filter((name) => name === 'pg.listen.overflow');
    expect(overflows).toHaveLength(1);
  });

  it('keeps the driver queue backlog bounded during a flood with a stalled handler', async () => {
    const harness = makeHarness(['confirm']);
    const backlog = await runTest(
      Effect.gen(function* () {
        const fiber = yield* listenWithRetry(CHANNEL, () => Effect.never, {
          bufferCapacity: 1,
        }).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
          Effect.forkChild,
        );
        yield* settle;
        const queue = yield* sessionQueue(harness, 0);
        // The driver delivers a batch synchronously on each socket read.
        const flood = Effect.promise(
          () =>
            new Promise<void>((resolve) => {
              let tick = 0;
              const sendBatch = () => {
                for (let index = 0; index < FLOOD_NOTIFICATIONS_PER_TICK; index += 1) {
                  Queue.offerUnsafe(queue, { channel: CHANNEL, payload: 'event', processId: 1 });
                }

                tick += 1;
                if (tick < FLOOD_TICKS) {
                  setImmediate(sendBatch);
                } else {
                  setImmediate(resolve);
                }
              };
              setImmediate(sendBatch);
            }),
        );
        yield* flood;
        const queued = Queue.sizeUnsafe(queue);
        yield* Fiber.interrupt(fiber);
        return queued;
      }),
    );
    expect(backlog).toBeLessThanOrEqual(MAX_DRIVER_BACKLOG_NOTIFICATIONS);
  });

  it('delivers a burst larger than bufferCapacity to a handler that keeps up', async () => {
    const harness = makeHarness(['confirm']);
    const handled: Array<string> = [];
    const burst = Array.from({ length: 50 }, (_, index) => String(index));
    const dropped = await runTest(
      Effect.gen(function* () {
        const fiber = yield* listenWithRetry(
          CHANNEL,
          (payload) => Effect.sync(() => handled.push(payload)),
          { bufferCapacity: 2 },
        ).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
          Effect.forkChild,
        );
        yield* settle;
        yield* deliver(harness, 0, burst);
        for (
          let round = 0;
          round < MAX_SETTLE_ROUNDS && handled.length < burst.length;
          round += 1
        ) {
          yield* settle;
        }

        const count = yield* droppedValue;
        yield* Fiber.interrupt(fiber);
        return count;
      }),
      makeTestLogger(),
    );
    expect(dropped).toBe(0);
    expect(handled).toEqual(burst);
  });

  it('counts and logs the backlog discarded when the handler fails', async () => {
    const harness = makeHarness(['confirm']);
    const logger = makeTestLogger({ minLevel: 'Debug' });
    const dropped = await runTest(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const fiber = yield* forkListener(harness, (payload) =>
          payload === 'bad'
            ? Deferred.await(release).pipe(
                Effect.andThen(Effect.fail(new HandlerRejectedError({ message: 'bad' }))),
              )
            : Effect.void,
        );
        yield* settle;
        yield* deliver(harness, 0, ['bad', 'a', 'b']);
        yield* settle;
        yield* Deferred.succeed(release, undefined);
        yield* settle;
        const count = yield* droppedValue;
        yield* Fiber.interrupt(fiber);
        return count;
      }),
      logger,
    );
    expect(dropped).toBe(2);
    const discarded = logger.records.find(
      (record) => record.message === 'pg.listen.backlog_discarded',
    );
    expect(discarded?.annotations.discarded).toBe(2);
  });

  it('rejects a buffer capacity below 1 before calling listen', async () => {
    const harness = makeHarness([]);
    const error = await runTest(
      Effect.flip(
        listenWithRetry(CHANNEL, () => Effect.void, { bufferCapacity: 0 }).pipe(
          Effect.provideService(PgClient.PgClient, asPgClient(harness.stub)),
        ),
      ),
    );
    expect(error._tag).toBe('PgChannelError');
    expect(harness.calls.count).toBe(0);
  });

  it('finishes the payload in flight and the buffered ones when the connection drops', async () => {
    const harness = makeHarness(['confirm']);
    const handled: Array<string> = [];
    await runTest(
      Effect.gen(function* () {
        const release = yield* Deferred.make<void>();
        const fiber = yield* forkListener(harness, (payload) =>
          Deferred.await(release).pipe(Effect.andThen(Effect.sync(() => handled.push(payload)))),
        );
        yield* settle;
        yield* deliver(harness, 0, ['first', 'second']);
        yield* settle;
        yield* Queue.fail(yield* sessionQueue(harness, 0), connectionLost);
        yield* settle;
        yield* Deferred.succeed(release, undefined);
        yield* settle;
        expect(lastReconnecting(harness).lastError).toContain('connection lost');
        yield* Fiber.interrupt(fiber);
      }),
    );
    expect(handled).toEqual(['first', 'second']);
  });

  it('removes the metric series for the channel when the listener ends', async () => {
    const harness = makeHarness(['confirm']);
    const series = await runTest(
      Effect.gen(function* () {
        const fiber = yield* forkListener(harness);
        yield* settle;
        const before = yield* channelSeries;
        yield* Fiber.interrupt(fiber);
        const after = yield* channelSeries;
        return { before, after };
      }),
    );
    expect(series.before).toContain('pg_listen_connected');
    expect(series.after).toEqual([]);
  });
});
