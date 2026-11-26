import { SqlError } from 'effect/sql';

import { omitUndefined } from './omit-undefined.ts';
import { redactDatabaseSecrets } from './target.ts';

/**
 * Copy of the driver's `Error` with the URI and password removed from its message, its stack and
 * every other string field the server sent (`detail`, `hint`, ...). Non-string fields such as
 * `code` are kept, so callers can still match on the SQLSTATE.
 */
const redactDriverError = (cause: Error, rawUrl: string): Error => {
  const redacted = new Error(redactDatabaseSecrets(cause.message, rawUrl));
  for (const [key, value] of Object.entries(cause)) {
    Reflect.set(
      redacted,
      key,
      typeof value === 'string' ? redactDatabaseSecrets(value, rawUrl) : value,
    );
  }

  redacted.name = cause.name;
  if (cause.stack !== undefined) {
    redacted.stack = redactDatabaseSecrets(cause.stack, rawUrl);
  }

  return redacted;
};

/**
 * Rebuilds `reason` as the same kind of reason with `cause` in place of its driver error and the
 * URI and password removed from its own message (and from a `UniqueViolation`'s constraint name).
 * An empty message stays unset. Indexing the constructor table by `reason._tag` fails to compile if
 * a reason kind is added and not listed.
 */
const withReasonCause = (
  reason: SqlError.SqlErrorReason,
  cause: unknown,
  rawUrl: string,
): SqlError.SqlErrorReason => {
  const fields = {
    cause,
    ...omitUndefined({
      message:
        reason.message.length > 0 ? redactDatabaseSecrets(reason.message, rawUrl) : undefined,
      operation: reason.operation,
    }),
  };
  if (reason._tag === 'UniqueViolation') {
    return new SqlError.UniqueViolation({
      ...fields,
      constraint: redactDatabaseSecrets(reason.constraint, rawUrl),
    });
  }

  const plainReasons = {
    ConnectionError: SqlError.ConnectionError,
    AuthenticationError: SqlError.AuthenticationError,
    AuthorizationError: SqlError.AuthorizationError,
    SqlSyntaxError: SqlError.SqlSyntaxError,
    ConstraintError: SqlError.ConstraintError,
    DeadlockError: SqlError.DeadlockError,
    SerializationError: SqlError.SerializationError,
    LockTimeoutError: SqlError.LockTimeoutError,
    StatementTimeoutError: SqlError.StatementTimeoutError,
    UnknownError: SqlError.UnknownError,
  };
  return new plainReasons[reason._tag](fields);
};

/**
 * Returns a startup `SqlError` rebuilt with the URI and password removed from the driver error
 * behind it, so a server message that echoes them (for example a user name equal to the password)
 * does not reach the caller's logs or `Cause.pretty`. The reason kind and SQLSTATE `code` stay the
 * same. Any other error is returned unchanged.
 */
export const redactStartupError = <E>(error: E, rawUrl: string): E | SqlError.SqlError => {
  if (!SqlError.isSqlError(error)) {
    return error;
  }

  const { reason } = error;
  const cause =
    reason.cause instanceof Error ? redactDriverError(reason.cause, rawUrl) : reason.cause;
  return new SqlError.SqlError({ reason: withReasonCause(reason, cause, rawUrl) });
};
