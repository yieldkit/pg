import { makeTestLogger } from '@yieldkit/logger/testing';
import { Effect } from 'effect';
import { SqlError } from 'effect/sql';
import { describe, expect, it } from 'vite-plus/test';

import { isRetryableConnectError, retryConnection } from './connect-retry.ts';

const connectionError = new SqlError.SqlError({
  reason: new SqlError.ConnectionError({
    message: 'connection refused',
    cause: new Error('ECONNREFUSED'),
  }),
});

const authenticationError = new SqlError.SqlError({
  reason: new SqlError.AuthenticationError({
    message: 'password authentication failed',
    cause: new Error('28P01'),
  }),
});

/**
 * A startup failure the driver reports as an `ErrorResponse` with SQLSTATE `code`, which
 * `@effect/sql-pg` classifies as `UnknownError` unless the code starts with `08`.
 */
const startupErrorWithCode = (code: string) =>
  new SqlError.SqlError({
    reason: new SqlError.UnknownError({
      message: 'PgConnection: Failed to connect',
      cause: Object.assign(new Error('the database system is starting up'), { code }),
    }),
  });

/**
 * Runs `retryConnection` on an attempt that always fails with `error` and returns how many attempts
 * ran and how many `pg.client.reconnecting` lines were logged.
 */
const countRetries = async (error: SqlError.SqlError) => {
  const harness = makeTestLogger();
  let attempts = 0;
  const open = Effect.suspend(() => {
    attempts += 1;
    return Effect.fail(error);
  });
  await Effect.runPromise(
    Effect.flip(retryConnection(open, {})).pipe(Effect.provide(harness.layer)),
  );
  const reconnecting = harness.records.filter(
    (record) => record.message === 'pg.client.reconnecting',
  ).length;
  return { attempts, reconnecting };
};

describe('retryConnection', () => {
  it('retries a refused connect and returns the later success', async () => {
    let attempts = 0;
    const open = Effect.suspend(() => {
      attempts += 1;
      if (attempts < 2) {
        return Effect.fail(connectionError);
      }

      return Effect.succeed('ready');
    });
    const result = await Effect.runPromise(retryConnection(open, { applicationName: 'app-test' }));
    expect(result).toBe('ready');
    expect(attempts).toBe(2);
    expect(isRetryableConnectError(connectionError)).toBe(true);
  });

  it('does not retry an authentication failure', async () => {
    let attempts = 0;
    const open = Effect.suspend(() => {
      attempts += 1;
      return Effect.fail(authenticationError);
    });
    const error = await Effect.runPromise(Effect.flip(retryConnection(open, {})));
    expect(error.reason._tag).toBe('AuthenticationError');
    expect(attempts).toBe(1);
    expect(isRetryableConnectError(authenticationError)).toBe(false);
  });

  it('does not retry a pooler refusing a startup parameter', async () => {
    const refused = new SqlError.SqlError({
      reason: new SqlError.ConnectionError({
        message: 'PgConnection: Failed to connect',
        cause: new Error('unsupported startup parameter: statement_timeout'),
      }),
    });
    let attempts = 0;
    const open = Effect.suspend(() => {
      attempts += 1;
      return Effect.fail(refused);
    });
    await Effect.runPromise(Effect.flip(retryConnection(open, {})));
    expect(attempts).toBe(1);
  });

  it('retries a server that is starting up (57P03) but not a full one (53300)', async () => {
    expect(await countRetries(startupErrorWithCode('57P03'))).toEqual({
      attempts: 4,
      reconnecting: 3,
    });
    expect(await countRetries(startupErrorWithCode('53300'))).toEqual({
      attempts: 1,
      reconnecting: 0,
    });
    expect(await countRetries(startupErrorWithCode('28P01'))).toEqual({
      attempts: 1,
      reconnecting: 0,
    });
  });

  it('logs pg.client.reconnecting once per retry that runs, never for a failure it gives up on', async () => {
    expect(await countRetries(connectionError)).toEqual({ attempts: 4, reconnecting: 3 });
    expect(await countRetries(authenticationError)).toEqual({ attempts: 1, reconnecting: 0 });
  });
});
