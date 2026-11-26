import type { PgClient } from '@effect/sql-pg';
import type { SqlClient } from 'effect/sql';

import { logError } from '@yieldkit/logger';
import { Effect } from 'effect';

import { PgChannelError } from './errors.ts';
import { MAX_NOTIFY_PAYLOAD_BYTES, NOTIFY_CHANNEL_PATTERN } from './policy.ts';

/**
 * UTF-8 byte length of a `NOTIFY` payload, which is what PostgreSQL counts against the 8000-byte
 * cap.
 */
const payloadByteLength = (payload: string): number => Buffer.byteLength(payload, 'utf8');

/**
 * Rejects a channel that PostgreSQL would not accept as an identifier. Does not connect. An empty
 * string, a space, or more than 63 characters fails with `PgChannelError`.
 */
export const validateNotifyChannel = (channel: string): Effect.Effect<string, PgChannelError> => {
  if (!NOTIFY_CHANNEL_PATTERN.test(channel)) {
    return Effect.fail(
      new PgChannelError({
        channel,
        message:
          'Notify channel must match [A-Za-z_][A-Za-z0-9_]{0,62} (1 to 63 characters, no spaces).',
      }),
    );
  }

  return Effect.succeed(channel);
};

/**
 * The NUL character, which PostgreSQL text values cannot contain.
 */
const NUL_CHARACTER = '\u0000';

/**
 * Rejects a payload PostgreSQL would refuse at `NOTIFY` time, so the caller learns before its
 * transaction commits rather than losing the event after: one of 8000 UTF-8 bytes or more, or one
 * containing a NUL character. Does not connect. An empty payload is accepted.
 */
export const validateNotifyPayload = (
  channel: string,
  payload: string,
): Effect.Effect<string, PgChannelError> => {
  const bytes = payloadByteLength(payload);
  if (bytes >= MAX_NOTIFY_PAYLOAD_BYTES) {
    return Effect.fail(
      new PgChannelError({
        channel,
        message:
          `Notify payload is ${String(bytes)} bytes; ` +
          `it must be shorter than ${String(MAX_NOTIFY_PAYLOAD_BYTES)}.`,
      }),
    );
  }

  if (payload.includes(NUL_CHARACTER)) {
    return Effect.fail(
      new PgChannelError({ channel, message: 'Notify payload must not contain a NUL character.' }),
    );
  }

  return Effect.succeed(payload);
};

/**
 * Logs `pg.notify.failed` and succeeds when `notify` fails, so a committed write is not rolled
 * back.
 */
export const settleNotify = (
  notify: Effect.Effect<void, { readonly message: string }>,
  channel: string,
  payloadBytes: number,
): Effect.Effect<void> =>
  notify.pipe(
    Effect.catch((error) =>
      logError('pg.notify.failed', {
        channel,
        payloadBytes,
        errorMessage: error.message,
      }),
    ),
  );

/**
 * Sends `payload` on `channel` after the surrounding transaction commits. Call this inside
 * `sql.withTransaction`. A commit that then fails to notify logs `pg.notify.failed` and does not
 * fail the already-committed transaction. An invalid channel, a payload of 8000 bytes or more, or a
 * payload containing a NUL character fails with `PgChannelError` before the hook is registered.
 */
export const notifyAfterCommit = Effect.fnUntraced(function* (
  sql: SqlClient.SqlClient,
  pg: PgClient.PgClient,
  channel: string,
  payload: string,
): Effect.fn.Return<void, PgChannelError> {
  const validChannel = yield* validateNotifyChannel(channel);
  yield* validateNotifyPayload(validChannel, payload);
  yield* sql.afterCommit(
    settleNotify(pg.notify(validChannel, payload), validChannel, payloadByteLength(payload)),
  );
});
