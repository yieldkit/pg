import { Effect, Schema } from 'effect';
import { SqlClient } from 'effect/sql';
import { describe, expect, it } from 'vite-plus/test';

import type { AdvisoryLockKey } from './advisory-lock.ts';

import { withAdvisoryXactLock } from './advisory-lock.ts';
import { PgConfigError } from './errors.ts';

/**
 * Claims that any value is a lock key, as a JavaScript caller or a cast would.
 */
const isClaimedKey = (_key: unknown): _key is AdvisoryLockKey => true;

/**
 * Claims that any value is a `SqlClient`. The key check fails before the client is used.
 */
const isClaimedSqlClient = (_client: unknown): _client is SqlClient.SqlClient => true;

/**
 * `value` passed off as a lock key.
 */
const asKey = (value: unknown): AdvisoryLockKey => {
  if (!isClaimedKey(value)) {
    throw new Error('unreachable');
  }

  return value;
};

/**
 * `value` passed off as a `SqlClient`.
 */
const asSqlClient = (value: unknown): SqlClient.SqlClient => {
  if (!isClaimedSqlClient(value)) {
    throw new Error('unreachable');
  }

  return value;
};

describe('withAdvisoryXactLock', () => {
  it('fails with PgConfigError for a null key instead of dying', async () => {
    const error = await Effect.runPromise(
      Effect.flip(withAdvisoryXactLock(asKey(null), Effect.void)).pipe(
        Effect.provideService(SqlClient.SqlClient, asSqlClient({})),
      ),
    );
    expect(Schema.is(PgConfigError)(error)).toBe(true);
  });
});
