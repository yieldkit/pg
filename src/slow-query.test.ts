import { Effect } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import {
  formatPgTimeoutMs,
  sessionStartupParameters,
  slowQueryDurationSetting,
  validatePgSchemaName,
  validatePgSearchPath,
} from './slow-query.ts';

describe('slowQueryDurationSetting', () => {
  it('stays unset when logging is omitted and rejects a negative threshold', async () => {
    const off = await Effect.runPromise(slowQueryDurationSetting(undefined));
    expect(off).toBeUndefined();

    const invalid = await Effect.runPromise(
      Effect.flip(slowQueryDurationSetting({ thresholdMs: -1 })),
    );
    expect(invalid._tag).toBe('PgConfigError');
    expect(invalid.message).toContain('integer from 0 to');
  });

  it('adds log_min_duration_statement only when a threshold is set', async () => {
    const disabled = await Effect.runPromise(sessionStartupParameters(undefined));
    expect(disabled.log_min_duration_statement).toBeUndefined();
    expect(disabled.statement_timeout).toBe('15000ms');
    expect(disabled.lock_timeout).toBe('5000ms');
    expect(disabled.idle_in_transaction_session_timeout).toBe('30000ms');

    const enabled = await Effect.runPromise(sessionStartupParameters({ thresholdMs: 250 }));
    expect(enabled.log_min_duration_statement).toBe('250');
  });

  it('validates searchPath and forwards it in session startup parameters', async () => {
    const validated = await Effect.runPromise(validatePgSchemaName('effect_cluster'));
    expect(validated).toBe('effect_cluster');

    const multi = await Effect.runPromise(validatePgSearchPath('effect_cluster, public'));
    expect(multi).toBe('effect_cluster, public');

    const bad = await Effect.runPromise(Effect.flip(validatePgSearchPath('effect.cluster')));
    expect(bad._tag).toBe('PgConfigError');

    const params = await Effect.runPromise(
      sessionStartupParameters(undefined, {
        searchPath: 'effect_cluster',
        lockTimeout: '10 seconds',
      }),
    );
    expect(params.search_path).toBe('effect_cluster');
    expect(params.lock_timeout).toBe('10000ms');
  });

  it('rejects a zero, unparseable or too large session timeout instead of sending it', async () => {
    const cases = [
      { statementTimeout: 0 },
      { lockTimeout: '0 millis' },
      { idleInTransactionTimeout: 'Infinity' },
      { statementTimeout: 2_147_483_648 },
    ] as const;
    const errors = await Promise.all(
      cases.map((timeouts) =>
        Effect.runPromise(Effect.flip(sessionStartupParameters(undefined, timeouts))),
      ),
    );
    expect(errors.map((error) => error._tag)).toEqual(cases.map(() => 'PgConfigError'));
  });

  it('sends no startup parameter for a timeout set to false, for poolers such as PgBouncer', async () => {
    const none = await Effect.runPromise(
      sessionStartupParameters(undefined, {
        statementTimeout: false,
        lockTimeout: false,
        idleInTransactionTimeout: false,
      }),
    );
    expect(none).toEqual({});

    const some = await Effect.runPromise(
      sessionStartupParameters(undefined, { statementTimeout: false }),
    );
    expect(some).toEqual({
      lock_timeout: '5000ms',
      idle_in_transaction_session_timeout: '30000ms',
    });
  });

  it('says an empty searchPath is empty instead of naming an invalid schema', async () => {
    const error = await Effect.runPromise(Effect.flip(validatePgSearchPath(' ')));
    expect(error.message).toContain('must not be empty');
  });

  it('rejects a slow-query threshold above the PostgreSQL int32 limit', async () => {
    const largest = await Effect.runPromise(
      slowQueryDurationSetting({ thresholdMs: 2_147_483_647 }),
    );
    expect(largest).toBe('2147483647');

    const tooLarge = await Effect.runPromise(
      Effect.flip(slowQueryDurationSetting({ thresholdMs: 2_147_483_648 })),
    );
    expect(tooLarge._tag).toBe('PgConfigError');
  });

  it('formats positive durations into PostgreSQL millisecond GUC literals', async () => {
    expect(await Effect.runPromise(formatPgTimeoutMs('statementTimeout', '10 minutes'))).toBe(
      '600000ms',
    );
    expect(await Effect.runPromise(formatPgTimeoutMs('lockTimeout', '10 seconds'))).toBe('10000ms');

    const zero = await Effect.runPromise(Effect.flip(formatPgTimeoutMs('lockTimeout', '0 millis')));
    expect(zero._tag).toBe('PgConfigError');

    const huge = await Effect.runPromise(
      Effect.flip(formatPgTimeoutMs('lockTimeout', 2_147_483_648)),
    );
    expect(huge._tag).toBe('PgConfigError');
  });
});
