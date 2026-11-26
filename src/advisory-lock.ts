import type { SqlError } from 'effect/sql';

import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

import { PgConfigError } from './errors.ts';

/**
 * Lock key: one string, or a pair of strings that PostgreSQL hashes into its two-integer key space.
 */
export type AdvisoryLockKey = string | readonly [string, string];

/**
 * Message used when a key is not a non-empty string or a pair of non-empty strings.
 */
const INVALID_KEY_MESSAGE =
  'Advisory lock keys must be a non-empty string or a pair of non-empty strings.';

/**
 * Number of parts in a pair key.
 */
const PAIR_KEY_LENGTH = 2;

/**
 * True for a non-empty string.
 */
const isNonEmptyString = (part: unknown): boolean => typeof part === 'string' && part.length > 0;

/**
 * True when `key` is a non-empty string or an array of exactly two non-empty strings. Checked at
 * run time because a one-element array (reachable through a cast or plain JavaScript) would hash
 * `NULL` and make `pg_advisory_xact_lock` return without taking any lock, and because `null` or
 * another non-array value from such a caller must fail as `PgConfigError`, not as a defect.
 */
const isValidKey = (key: AdvisoryLockKey): boolean => {
  if (typeof key === 'string') {
    return isNonEmptyString(key);
  }

  const parts: unknown = key;
  // Checked by index: `every` skips the holes of a sparse array such as `['ns', , ]`.
  return (
    Array.isArray(parts) &&
    parts.length === PAIR_KEY_LENGTH &&
    isNonEmptyString(parts[0]) &&
    isNonEmptyString(parts[1])
  );
};

/**
 * Takes the transaction-scoped lock for `key` on the current connection. A single key hashes to one
 * 64-bit value; a pair hashes each half to a 32-bit value.
 */
const takeLock = (
  sql: SqlClient.SqlClient,
  key: AdvisoryLockKey,
): Effect.Effect<unknown, SqlError.SqlError> => {
  if (typeof key === 'string') {
    return sql`SELECT pg_advisory_xact_lock(hashtextextended(${key}, 0))`;
  }

  const [first, second] = key;
  return sql`SELECT pg_advisory_xact_lock(hashtext(${first}), hashtext(${second}))`;
};

/**
 * Runs `effect` inside a transaction that holds a PostgreSQL advisory lock on `key`. Fibers, and
 * processes, using the same key run their transactions one at a time; the next one starts after the
 * holder's transaction commits or rolls back. The lock is released by the transaction ending, so a
 * failure or interruption of `effect` rolls back and releases it. Different keys never block each
 * other, apart from rare hash collisions.
 *
 * The guarantee covers the database work in that transaction, not side effects outside it. If the
 * session ends while `effect` is still running (for example after
 * `idle_in_transaction_session_timeout` while `effect` waits on something other than SQL), the lock
 * is released and another caller can enter while the first one's non-SQL work goes on. Make
 * external side effects idempotent or fence them with a value checked in the same transaction.
 *
 * The helper owns the transaction on purpose: a `pg_advisory_xact_lock` taken outside a transaction
 * is released as soon as its own statement ends, which would silently protect nothing. Inside an
 * outer `sql.withTransaction`, this call nests as a savepoint. If `effect` succeeds, the lock stays
 * held until the OUTER transaction commits or rolls back, not when `effect` finishes; if it fails,
 * the rollback to the savepoint releases the lock while the outer transaction is still open, unless
 * the outer transaction already held the same key: then the lock stays held until it ends. Run
 * every query the lock protects through the same `SqlClient` inside `effect`.
 *
 * `key` is either one string, hashed with `hashtextextended(key, 0)`, or a pair of strings hashed
 * with `hashtext` each. An empty string, an empty half of a pair, or anything else that is not a
 * string or exactly two strings (such as `null` from an untyped caller) fails with `PgConfigError`
 * before any statement runs. The wait for the lock is bounded by the session `lock_timeout`. The
 * whole call, lock wait included, runs in the `pg.withAdvisoryXactLock` span.
 */
export const withAdvisoryXactLock: <A, E, R>(
  key: AdvisoryLockKey,
  effect: Effect.Effect<A, E, R>,
) => Effect.Effect<A, E | SqlError.SqlError | PgConfigError, R | SqlClient.SqlClient> = Effect.fn(
  'pg.withAdvisoryXactLock',
)(function* <A, E, R>(
  key: AdvisoryLockKey,
  effect: Effect.Effect<A, E, R>,
): Effect.fn.Return<A, E | SqlError.SqlError | PgConfigError, R | SqlClient.SqlClient> {
  if (!isValidKey(key)) {
    return yield* new PgConfigError({ message: INVALID_KEY_MESSAGE });
  }

  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(takeLock(sql, key).pipe(Effect.andThen(effect)));
});
