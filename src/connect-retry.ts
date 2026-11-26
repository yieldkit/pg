import type { LogFields } from '@yieldkit/logger';

import { logInfo } from '@yieldkit/logger';
import { Effect, Predicate, Schedule } from 'effect';
import { SqlError } from 'effect/sql';

import { CONNECT_RETRY_BASE, CONNECT_RETRY_CAP, CONNECT_RETRY_TIMES } from './policy.ts';

/**
 * Start of the error a pooler such as PgBouncer sends when it refuses a startup parameter
 * (`unsupported startup parameter: statement_timeout`). Retrying cannot help.
 */
const UNSUPPORTED_STARTUP_PARAMETER = 'unsupported startup parameter';

/**
 * True when the driver error behind `error` says the server refused a startup parameter.
 */
const isStartupParameterRejection = (error: SqlError.SqlError): boolean => {
  const { cause } = error.reason;
  return cause instanceof Error && cause.message.startsWith(UNSUPPORTED_STARTUP_PARAMETER);
};

/**
 * SQLSTATE `cannot_connect_now`: the server is starting up, shutting down or in recovery.
 */
const CANNOT_CONNECT_NOW = '57P03';

/**
 * True when the driver error behind `error` carries SQLSTATE `57P03`.
 */
const isCannotConnectNow = (error: SqlError.SqlError): boolean =>
  Predicate.hasProperty(error.reason.cause, 'code') &&
  error.reason.cause.code === CANNOT_CONNECT_NOW;

/**
 * True for an error `@effect/sql-pg` classifies as a connection error (a refused, reset or closed
 * socket, a connect timeout, a failed TLS negotiation or SQLSTATE class `08`), and for a server
 * that answers with `57P03` because it is starting up, shutting down or in recovery. Authentication
 * errors and every other SQLSTATE are not retried, nor is `53300` (too many connections), nor a
 * pooler's refusal of a startup parameter, which fails the same way every time.
 */
export const isRetryableConnectError = (error: unknown): boolean =>
  SqlError.isSqlError(error) &&
  ((error.reason._tag === 'ConnectionError' && !isStartupParameterRejection(error)) ||
    isCannotConnectNow(error));

/**
 * Jittered exponential delay from 250 ms, capped at 2 seconds, between retried connects.
 */
const connectRetrySchedule = Schedule.min([
  Schedule.exponential(CONNECT_RETRY_BASE, 2),
  Schedule.spaced(CONNECT_RETRY_CAP),
]).pipe(Schedule.jittered);

/**
 * Retries `open` when PostgreSQL refuses the TCP session or is not ready yet (`57P03`). Logs
 * `pg.client.reconnecting` once per retry that will actually run: not after the last attempt, and
 * not for a failure that is not retried. The last failure is returned unchanged. An authentication
 * error fails on the first attempt.
 */
export const retryConnection = <A, E, R>(
  open: Effect.Effect<A, E, R>,
  fields: LogFields,
): Effect.Effect<A, E, R> =>
  open.pipe(
    Effect.retry({
      while: (error) => isRetryableConnectError(error),
      times: CONNECT_RETRY_TIMES,
      schedule: connectRetrySchedule.pipe(
        Schedule.tap(({ input, attempt }) =>
          isRetryableConnectError(input) && attempt <= CONNECT_RETRY_TIMES
            ? logInfo('pg.client.reconnecting', fields)
            : Effect.void,
        ),
      ),
    }),
  );
