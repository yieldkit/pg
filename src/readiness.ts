import type { Pool } from 'effect';

import { PgClient } from '@effect/sql-pg';
import { logError, logWarning } from '@yieldkit/logger';
import { Cause, Clock, Duration, Effect, Ref, Schedule } from 'effect';
import { SqlError } from 'effect/sql';

import { positiveFiniteMillis } from './duration.ts';
import { PgReadinessError } from './errors.ts';
import { READINESS_ATTEMPT_TIMEOUT, READINESS_BASE_DELAY, READINESS_RETRIES } from './policy.ts';

/**
 * Options for `checkReadiness`.
 */
export type ReadinessOptions = {
  /**
   * Retries after the first failed probe. Must be an integer >= 0. Defaults to 2.
   */
  readonly retries?: number | undefined;

  /**
   * Delay before the first retry; each further retry doubles it, with jitter. Defaults to 100
   * milliseconds.
   */
  readonly baseDelay?: Duration.Input | undefined;

  /**
   * How long one `SELECT 1` may take, including waiting for a pooled connection. Defaults to 2
   * seconds.
   */
  readonly attemptTimeout?: Duration.Input | undefined;
};

/**
 * Result of a successful `checkReadiness`.
 */
export type PgReadiness = {
  /**
   * Milliseconds from the start of the first probe to the successful answer, including retry
   * delays.
   */
  readonly latencyMs: number;

  /**
   * Probes that ran, including the successful one. 1 means the first probe answered.
   */
  readonly attempts: number;

  /**
   * Pool occupancy after the probe: `idle`, `used`, `pending`, `min` and `max` connections.
   */
  readonly pool: Pool.Stats;
};

/**
 * Message used when `retries` is rejected before any probe runs.
 */
const INVALID_RETRIES_MESSAGE = 'PostgreSQL readiness retries must be an integer >= 0.';

/**
 * Message used when `baseDelay` or `attemptTimeout` is rejected before any probe runs.
 */
const invalidDurationMessage = (name: string): string =>
  `PostgreSQL readiness ${name} must be a finite duration greater than zero.`;

/**
 * Fails with `PgReadinessError` (`attempts: 0`) unless `input` is a finite duration greater than
 * zero, so a bad value from untyped config fails typed instead of dying or sleeping forever.
 */
const assertReadinessDuration = (
  name: string,
  input: Duration.Input,
): Effect.Effect<Duration.Input, PgReadinessError> =>
  positiveFiniteMillis(input) === undefined
    ? Effect.fail(new PgReadinessError({ message: invalidDurationMessage(name), attempts: 0 }))
    : Effect.succeed(input);

/**
 * Short, secret-free label for why a probe failed. Driver messages are not copied because they may
 * name the host or the user.
 */
const describeProbeFailure = (error: unknown): string => {
  if (SqlError.isSqlError(error)) {
    return `SQL error ${error.reason._tag}`;
  }

  if (Cause.isTimeoutError(error)) {
    return 'attempt timed out';
  }

  return 'unexpected failure';
};

/**
 * Runs `SELECT 1` through the pool under `attemptTimeout`, counting each attempt in `attempts`.
 */
const probeOnce = (
  client: PgClient.PgClient,
  attempts: Ref.Ref<number>,
  attemptTimeout: Duration.Input,
) =>
  Ref.update(attempts, (count) => count + 1).pipe(
    Effect.andThen(client`SELECT 1`),
    Effect.timeout(attemptTimeout),
  );

/**
 * Probes the pool with `SELECT 1`, retrying with jittered exponential backoff. Returns `latencyMs`,
 * the number of `attempts` and the pool stats from `PgClient.poolStats`. Each attempt runs under
 * `attemptTimeout`, which starts cancelling the probe; the driver may take up to about 5 seconds
 * more to cancel and drain a stalled query before the next attempt. Each retry logs
 * `pg.readiness.retry` (warning, with `attempt` and `delayMs`). When every attempt fails it logs
 * `pg.readiness.failed` (error) and fails with `PgReadinessError`, whose `attempts` is `retries +
 * 1` and whose message names the failure kind but never the URI or password. A `retries` that is
 * not an integer >= 0, or a `baseDelay` or `attemptTimeout` that is not a finite duration greater
 * than zero, fails with `attempts: 0` before any probe. `latencyMs` is measured on the monotonic
 * clock. The work runs in the `pg.checkReadiness` span. Use `ping` for a single probe without
 * retries or stats.
 *
 * Call it outside any transaction: inside `sql.withTransaction` the probe reuses the transaction's
 * connection and reports ready even when the pool has no free connection.
 */
export const checkReadiness = Effect.fn('pg.checkReadiness')(function* (
  options: ReadinessOptions = {},
): Effect.fn.Return<PgReadiness, PgReadinessError, PgClient.PgClient> {
  const retries = options.retries ?? READINESS_RETRIES;
  if (!Number.isInteger(retries) || retries < 0) {
    return yield* new PgReadinessError({ message: INVALID_RETRIES_MESSAGE, attempts: 0 });
  }

  const baseDelay = yield* assertReadinessDuration(
    'baseDelay',
    options.baseDelay ?? READINESS_BASE_DELAY,
  );
  const attemptTimeout = yield* assertReadinessDuration(
    'attemptTimeout',
    options.attemptTimeout ?? READINESS_ATTEMPT_TIMEOUT,
  );
  const client = yield* PgClient.PgClient;
  const attempts = yield* Ref.make(0);
  const startedAt = yield* Clock.monotonicTimeNanos;
  const retrySchedule = Schedule.max([
    Schedule.exponential(baseDelay, 2),
    Schedule.recurs(retries),
  ]).pipe(
    Schedule.jittered,
    Schedule.modifyDelay(({ duration }) =>
      Ref.get(attempts).pipe(
        Effect.flatMap((attempt) =>
          logWarning('pg.readiness.retry', { attempt, delayMs: Duration.toMillis(duration) }),
        ),
        Effect.as(duration),
      ),
    ),
  );
  yield* probeOnce(client, attempts, attemptTimeout).pipe(
    Effect.retry(retrySchedule),
    Effect.catch((error) =>
      Ref.get(attempts).pipe(
        Effect.flatMap((made) => failReadiness(describeProbeFailure(error), made)),
      ),
    ),
  );
  const made = yield* Ref.get(attempts);
  const finishedAt = yield* Clock.monotonicTimeNanos;
  const pool = yield* client.poolStats;
  const latencyMs = Math.round(Duration.toMillis(Duration.nanos(finishedAt - startedAt)));
  return { latencyMs, attempts: made, pool };
});

/**
 * Logs `pg.readiness.failed` and fails with `PgReadinessError` for `reason` after `attempts`.
 */
const failReadiness = (reason: string, attempts: number): Effect.Effect<never, PgReadinessError> =>
  logError('pg.readiness.failed', { attempts, reason }).pipe(
    Effect.andThen(
      Effect.fail(
        new PgReadinessError({
          message: `PostgreSQL readiness check failed after ${String(attempts)} attempt(s): ${reason}.`,
          attempts,
        }),
      ),
    ),
  );
