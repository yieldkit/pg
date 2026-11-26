import { SqlError } from 'effect/sql';
import { describe, expect, it } from 'vite-plus/test';

import { redactStartupError } from './startup-error.ts';

const RAW_URL = 'postgres://app:Sekr3tPW@db.internal:5432/app';

describe('redactStartupError', () => {
  it('masks the password in the constraint name of a UniqueViolation', () => {
    const error = new SqlError.SqlError({
      reason: new SqlError.UniqueViolation({
        message: 'duplicate key value violates unique constraint',
        constraint: 'login_Sekr3tPW_key',
        cause: new Error('duplicate key value violates unique constraint'),
      }),
    });
    const redacted = redactStartupError(error, RAW_URL);
    expect(SqlError.isSqlError(redacted) && redacted.reason._tag).toBe('UniqueViolation');
    expect(JSON.stringify(redacted)).not.toContain('Sekr3tPW');
    expect(
      SqlError.isSqlError(redacted) &&
        redacted.reason._tag === 'UniqueViolation' &&
        redacted.reason.constraint,
    ).not.toContain('Sekr3tPW');
  });
});
