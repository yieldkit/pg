import type { Duration } from 'effect';

import { Effect } from 'effect';

import { positiveFiniteMillis } from './duration.ts';
import { PgConfigError } from './errors.ts';
import { omitUndefined } from './omit-undefined.ts';
import { IDLE_IN_TRANSACTION_TIMEOUT, LOCK_TIMEOUT, STATEMENT_TIMEOUT } from './policy.ts';

export { DEFAULT_SLOW_QUERY_THRESHOLD_MS } from './policy.ts';

/**
 * Valid PostgreSQL unquoted schema identifier grammar (lowercase ASCII letters, digits, and
 * underscores, up to 63 bytes).
 */
const PG_SCHEMA_IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]{0,62}$/;

/**
 * Largest millisecond value PostgreSQL accepts for `statement_timeout`, `lock_timeout`,
 * `idle_in_transaction_session_timeout` and `log_min_duration_statement` (a signed 32-bit int).
 */
export const MAX_PG_TIMEOUT_MS = 2_147_483_647;

/**
 * How long a statement may run before PostgreSQL records it as slow. `0` records every statement.
 * Omit the option to leave the server default (`-1`, off).
 */
export type SlowQueryLogOptions = {
  /**
   * Threshold in milliseconds. Must be an integer >= 0.
   */
  readonly thresholdMs: number;
};

/**
 * Validates a PostgreSQL schema identifier for `search_path` and DDL helpers. Rejects uppercase,
 * dots, quotes, whitespace, empty strings, and names longer than 63 bytes with `PgConfigError`.
 */
export const validatePgSchemaName = (schema: string): Effect.Effect<string, PgConfigError> => {
  if (!PG_SCHEMA_IDENTIFIER_PATTERN.test(schema)) {
    return Effect.fail(
      new PgConfigError({
        message:
          `Invalid PostgreSQL schema name "${schema}". ` +
          'Expected lowercase letters, digits, and underscores starting with a letter or underscore.',
      }),
    );
  }

  return Effect.succeed(schema);
};

/**
 * Validates a comma-separated PostgreSQL `search_path` value so every entry is a safe unquoted
 * schema identifier. Returns `undefined` when `searchPath` is omitted.
 */
export const validatePgSearchPath = (
  searchPath: string | undefined,
): Effect.Effect<string | undefined, PgConfigError> => {
  if (searchPath === undefined) {
    return Effect.undefined;
  }

  if (searchPath.trim().length === 0) {
    return Effect.fail(new PgConfigError({ message: 'PostgreSQL search_path must not be empty.' }));
  }

  const parts = searchPath.split(',').map((part) => part.trim());
  return Effect.forEach(parts, validatePgSchemaName).pipe(
    Effect.map((validated) => validated.join(', ')),
  );
};

/**
 * Converts a positive `Duration.Input` into a PostgreSQL millisecond GUC literal (`"<ms>ms"`),
 * rounding fractional milliseconds up. Rejects zero, negative, infinite, unparseable, or values
 * above 2,147,483,647 ms with `PgConfigError`.
 */
export const formatPgTimeoutMs = (
  name: string,
  input: Duration.Input,
): Effect.Effect<string, PgConfigError> => {
  const millis = positiveFiniteMillis(input);
  const rounded = millis === undefined ? undefined : Math.ceil(millis);
  if (rounded === undefined || rounded > MAX_PG_TIMEOUT_MS) {
    return Effect.fail(
      new PgConfigError({
        message: `${name} must be a positive duration of at most ${String(MAX_PG_TIMEOUT_MS)} ms.`,
      }),
    );
  }

  return Effect.succeed(`${String(rounded)}ms`);
};

/**
 * Returns the `log_min_duration_statement` startup value, or `undefined` when slow-query logging is
 * off. A threshold that is not an integer from 0 to 2,147,483,647 fails with `PgConfigError` and
 * does not connect. `0` is accepted and means PostgreSQL logs every statement.
 */
export const slowQueryDurationSetting = (
  slowQueryLog: SlowQueryLogOptions | undefined,
): Effect.Effect<string | undefined, PgConfigError> => {
  if (slowQueryLog === undefined) {
    return Effect.undefined;
  }

  const { thresholdMs } = slowQueryLog;
  if (!Number.isInteger(thresholdMs) || thresholdMs < 0 || thresholdMs > MAX_PG_TIMEOUT_MS) {
    return Effect.fail(
      new PgConfigError({
        message:
          `slowQueryLog.thresholdMs must be an integer from 0 to ${String(MAX_PG_TIMEOUT_MS)} ` +
          `(got ${String(thresholdMs)}). 0 logs every statement; omit slowQueryLog to leave ` +
          'logging off.',
      }),
    );
  }

  return Effect.succeed(String(thresholdMs));
};

/**
 * One session timeout: a duration to send as a startup parameter, or `false` to send nothing and
 * leave the server, database or role setting in effect.
 */
export type SessionTimeout = Duration.Input | false;

/**
 * Optional session limits and search path. Omitted (`undefined`) fields use the package defaults;
 * `false` sends no startup parameter for that timeout. Each duration must be positive and at most
 * 2,147,483,647 ms; zero (which PostgreSQL reads as "no limit"), negative, infinite or unparseable
 * values fail with `PgConfigError`.
 */
export type SessionTimeouts = {
  /**
   * `statement_timeout` for every session. Defaults to `STATEMENT_TIMEOUT` (15 seconds). `false`
   * sends none.
   */
  readonly statementTimeout?: SessionTimeout | undefined;

  /**
   * `lock_timeout` for every session. Defaults to `LOCK_TIMEOUT` (5 seconds). `false` sends none.
   */
  readonly lockTimeout?: SessionTimeout | undefined;

  /**
   * `idle_in_transaction_session_timeout` for every session. Defaults to
   * `IDLE_IN_TRANSACTION_TIMEOUT` (30 seconds). `false` sends none.
   */
  readonly idleInTransactionTimeout?: SessionTimeout | undefined;

  /**
   * Optional PostgreSQL `search_path` startup value (validated comma-separated schema names).
   */
  readonly searchPath?: string | undefined;
};

/**
 * Formats one session timeout: `undefined` uses `fallback`, `false` returns `undefined` (send
 * nothing), anything else goes through `formatPgTimeoutMs`.
 */
const formatSessionTimeout = (
  name: string,
  input: SessionTimeout | undefined,
  fallback: Duration.Input,
): Effect.Effect<string | undefined, PgConfigError> =>
  input === false ? Effect.undefined : formatPgTimeoutMs(name, input ?? fallback);

/**
 * Session startup parameters sent with every physical connection, with the timeouts as millisecond
 * literals (`'15000ms'`). A timeout set to `false` is left out, as are `log_min_duration_statement`
 * and `search_path` unless configured, so with all three timeouts `false` and neither of the others
 * set the result is empty. Invalid timeouts, thresholds or schema names fail with `PgConfigError`.
 */
export const sessionStartupParameters = Effect.fnUntraced(function* (
  slowQueryLog: SlowQueryLogOptions | undefined,
  timeouts: SessionTimeouts = {},
): Effect.fn.Return<Readonly<Record<string, string>>, PgConfigError> {
  const slowQueryMs = yield* slowQueryDurationSetting(slowQueryLog);
  const searchPath = yield* validatePgSearchPath(timeouts.searchPath);
  const statementTimeout = yield* formatSessionTimeout(
    'statementTimeout',
    timeouts.statementTimeout,
    STATEMENT_TIMEOUT,
  );
  const lockTimeout = yield* formatSessionTimeout(
    'lockTimeout',
    timeouts.lockTimeout,
    LOCK_TIMEOUT,
  );
  const idleInTransactionTimeout = yield* formatSessionTimeout(
    'idleInTransactionTimeout',
    timeouts.idleInTransactionTimeout,
    IDLE_IN_TRANSACTION_TIMEOUT,
  );
  return omitUndefined({
    statement_timeout: statementTimeout,
    lock_timeout: lockTimeout,
    idle_in_transaction_session_timeout: idleInTransactionTimeout,
    log_min_duration_statement: slowQueryMs,
    search_path: searchPath,
  });
});
