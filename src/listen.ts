import type { PgConnection } from '@effect/sql-pg';
import type { Scope } from 'effect';

import { PgClient } from '@effect/sql-pg';
import { describeError, logDebug, logError, logInfo, logWarning } from '@yieldkit/logger';
import {
  Cause,
  Clock,
  Duration,
  Effect,
  Exit,
  Metric,
  Predicate,
  Queue,
  Random,
  Stream,
} from 'effect';
import { SqlError } from 'effect/sql';

import { PgChannelError } from './errors.ts';
import { validateNotifyChannel } from './notify.ts';
import {
  LISTEN_BUFFER_CAPACITY,
  LISTEN_RETRY_BASE,
  LISTEN_RETRY_CAP,
  LISTEN_STABLE_AFTER,
} from './policy.ts';

/**
 * `LISTEN_RETRY_BASE` in milliseconds.
 */
const LISTEN_RETRY_BASE_MS = Duration.toMillis(LISTEN_RETRY_BASE);

/**
 * `LISTEN_RETRY_CAP` in milliseconds.
 */
const LISTEN_RETRY_CAP_MS = Duration.toMillis(LISTEN_RETRY_CAP);

/**
 * `LISTEN_STABLE_AFTER` in milliseconds.
 */
const LISTEN_STABLE_AFTER_MS = Duration.toMillis(LISTEN_STABLE_AFTER);

/**
 * Lowest factor applied to a reconnect delay, so listeners that dropped together spread out.
 */
const LISTEN_JITTER_MIN = 0.8;

/**
 * Highest factor applied to a reconnect delay.
 */
const LISTEN_JITTER_MAX = 1.2;

/**
 * Largest doubling exponent used for the backoff. Anything above it is past the cap anyway, and
 * bounding it keeps the arithmetic finite however long a listener keeps failing.
 */
const LISTEN_MAX_BACKOFF_EXPONENT = 30;

/**
 * Health of one `listenWithRetry` fiber, reported through `ListenOptions.onStatusChange`.
 * `Connecting` is reported once before the first attempt. `Connected.since` is the epoch
 * milliseconds at which PostgreSQL confirmed `LISTEN`. `Reconnecting` is reported before each
 * backoff sleep: `attempt` counts the consecutive sessions that failed or ended before they were
 * stable (starting at 1), `delayMs` is the jittered sleep before the next attempt, and `lastError`
 * describes the failure, or is `undefined` when the session ended without one. A transport failure
 * is described by its message; anything the handler raises (a failure, a defect or a thrown
 * exception, a `SqlError` from its own query included) only by its `_tag` or error name, so payload
 * content a handler puts in its error message never reaches logs or this status.
 */
export type ListenStatus =
  | { readonly _tag: 'Connecting' }
  | { readonly _tag: 'Connected'; readonly since: number }
  | {
      readonly _tag: 'Reconnecting';
      readonly attempt: number;
      readonly delayMs: number;
      readonly lastError: string | undefined;
    };

/**
 * Optional hooks and limits for `listenWithRetry`.
 */
export type ListenOptions = {
  /**
   * Called on every status change, in order, on the listener fiber. It should return quickly; the
   * listener waits for it before going on. A defect it raises, or an exception it throws, is logged
   * as `pg.listen.status_callback_failed` and does not stop the listener.
   */
  readonly onStatusChange?: ((status: ListenStatus) => Effect.Effect<void>) | undefined;

  /**
   * Notifications held for a handler that is slower than the senders. Defaults to
   * `LISTEN_BUFFER_CAPACITY` (1024); must be an integer >= 1. When the buffer is full, newer
   * notifications are dropped, counted in `pgListenDropped` and logged as `pg.listen.overflow` once
   * per overflow. Keep handlers fast (hand the payload to a queue or another fiber) so the buffer
   * stays empty.
   */
  readonly bufferCapacity?: number | undefined;
};

/**
 * Gauge `pg_listen_connected`, tagged with `channel`. It is `1` while that channel's `LISTEN` is
 * confirmed and `0` while connecting or waiting to reconnect; the series is removed when the
 * listener fiber ends. It assumes one listener per channel per process: two listeners on one
 * channel share the series, so the first to disconnect sets it to `0`, and the first to stop
 * removes it, while the other is still running.
 */
export const pgListenConnected = Metric.gauge('pg_listen_connected', {
  description: 'Whether the LISTEN session for a channel is confirmed (1) or not (0).',
});

/**
 * Counter `pg_listen_dropped_total`, tagged with `channel`: notifications dropped because the
 * listener's buffer was full, plus those still buffered when a session ended. The series is removed
 * when the listener fiber ends.
 */
export const pgListenDropped = Metric.counter('pg_listen_dropped_total', {
  description: 'Notifications a LISTEN session dropped because its handler fell behind.',
});

/**
 * What one listen session left behind. `confirmedAtNanos` is set once PostgreSQL confirms `LISTEN`.
 */
type SessionRecord = {
  /**
   * Monotonic nanoseconds at which `LISTEN` was confirmed, or `undefined` if it never was.
   */
  confirmedAtNanos: bigint | undefined;

  /**
   * Monotonic nanoseconds at which the notification pump stopped.
   */
  endedAtNanos: bigint | undefined;

  /**
   * Description of the failure that ended the session, or `undefined` when it ended cleanly.
   */
  errorMessage: string | undefined;
};

/**
 * Per-channel context shared by every session of one `listenWithRetry` fiber.
 */
type ListenContext<E> = {
  /**
   * Validated channel name.
   */
  readonly channel: string;

  /**
   * Payload handler supplied by the caller.
   */
  readonly handlePayload: (payload: string) => Effect.Effect<void, E>;

  /**
   * Validated buffer capacity.
   */
  readonly bufferCapacity: number;

  /**
   * `pgListenConnected` tagged with this channel.
   */
  readonly gauge: Metric.Metric<number, unknown>;

  /**
   * `pgListenDropped` tagged with this channel.
   */
  readonly dropped: Metric.Metric<number, unknown>;

  /**
   * Reports a status change to the caller, or does nothing when no callback was given.
   */
  readonly report: (status: ListenStatus) => Effect.Effect<void>;
};

/**
 * Listens on `channel` until the fiber is interrupted, opening a new `LISTEN` after every session
 * that ends, whether it failed or ended cleanly. Logs `pg.listen.connected`, then
 * `pg.listen.disconnected`, `pg.listen.retry_scheduled` (debug) and `pg.listen.reconnected` for
 * later sessions, and `pg.listen.failed` for each failure. An invalid channel or buffer capacity
 * fails with `PgChannelError` before any socket is used. A payload handler that fails ends that
 * listen session and starts another. Each listener holds one pooled connection while it runs, so
 * the pool needs a connection per listener on top of those its queries use.
 *
 * Notifications wait in a buffer of `bufferCapacity` while the handler runs, one at a time. A full
 * buffer drops newer notifications (`pg.listen.overflow`, `pgListenDropped`), so a flood of
 * `NOTIFY` cannot grow memory without bound. When the connection drops, the handler still gets the
 * notifications already buffered. When the handler fails, the ones still buffered are discarded,
 * counted and logged as `pg.listen.backlog_discarded`. Notifications sent while the listener
 * reconnects are never delivered by PostgreSQL.
 *
 * Reconnect delays grow from `LISTEN_RETRY_BASE` (500 ms), doubling up to `LISTEN_RETRY_CAP` (30
 * seconds), each multiplied by a random factor between 0.8 and 1.2. The backoff goes back to the
 * base only after a session that confirmed `LISTEN` and stayed up for `LISTEN_STABLE_AFTER` (5
 * seconds), measured on the monotonic clock. A session that confirms and drops sooner counts as a
 * failure, so a server that keeps accepting and dropping the listener is retried with growing
 * delays rather than every 500 ms. `options.onStatusChange` and the `pgListenConnected` gauge
 * report the listener's health.
 */
export const listenWithRetry: <E>(
  channel: string,
  handlePayload: (payload: string) => Effect.Effect<void, E>,
  options?: ListenOptions,
) => Effect.Effect<void, PgChannelError, PgClient.PgClient> = Effect.fnUntraced(function* <E>(
  channel: string,
  handlePayload: (payload: string) => Effect.Effect<void, E>,
  options: ListenOptions = {},
): Effect.fn.Return<void, PgChannelError, PgClient.PgClient> {
  const validChannel = yield* validateNotifyChannel(channel);
  const bufferCapacity = yield* validateBufferCapacity(
    validChannel,
    options.bufferCapacity ?? LISTEN_BUFFER_CAPACITY,
  );
  const context: ListenContext<E> = {
    channel: validChannel,
    handlePayload,
    bufferCapacity,
    gauge: Metric.withAttributes(pgListenConnected, { channel: validChannel }),
    dropped: Metric.withAttributes(pgListenDropped, { channel: validChannel }),
    report: makeReporter(validChannel, options.onStatusChange),
  };
  return yield* retrySessions(context).pipe(
    Effect.ensuring(Effect.andThen(Metric.remove(context.gauge), Metric.remove(context.dropped))),
  );
});

/**
 * Runs sessions forever, sleeping with backoff between them. Only interruption ends it.
 */
const retrySessions = Effect.fnUntraced(function* <E>(
  context: ListenContext<E>,
): Effect.fn.Return<never, never, PgClient.PgClient> {
  yield* Metric.update(context.gauge, 0);
  yield* context.report({ _tag: 'Connecting' });
  let reconnect = false;
  let attempt = 0;
  for (;;) {
    const session = yield* runSession(context, reconnect);
    const endedAtNanos = session.endedAtNanos ?? (yield* Clock.monotonicTimeNanos);
    reconnect = true;
    attempt = isStableSession(session, endedAtNanos) ? 1 : attempt + 1;
    const delayMs = yield* backoffDelayMillis(attempt);
    yield* logDebug('pg.listen.retry_scheduled', { channel: context.channel, attempt, delayMs });
    yield* context.report({
      _tag: 'Reconnecting',
      attempt,
      delayMs,
      lastError: session.errorMessage,
    });
    yield* Effect.sleep(Duration.millis(delayMs));
  }
});

/**
 * Rejects a buffer capacity that is not an integer >= 1. Does not connect.
 */
const validateBufferCapacity = (
  channel: string,
  capacity: number,
): Effect.Effect<number, PgChannelError> => {
  if (!Number.isInteger(capacity) || capacity < 1) {
    return Effect.fail(
      new PgChannelError({
        channel,
        message: `bufferCapacity must be an integer >= 1 (got ${String(capacity)}).`,
      }),
    );
  }

  return Effect.succeed(capacity);
};

/**
 * Wraps the caller's status callback so a defect it raises, or an exception it throws before
 * returning an effect, is logged as `pg.listen.status_callback_failed` instead of ending the
 * listener. Interruption still stops it.
 */
const makeReporter =
  (channel: string, onStatusChange: ((status: ListenStatus) => Effect.Effect<void>) | undefined) =>
  (status: ListenStatus): Effect.Effect<void> =>
    onStatusChange === undefined
      ? Effect.void
      : Effect.suspend(() => onStatusChange(status)).pipe(
          Effect.catchDefect((defect) =>
            logError('pg.listen.status_callback_failed', {
              channel,
              status: status._tag,
              errorMessage: describeFailure(defect),
            }),
          ),
        );

/**
 * Runs one listen session and reports how it ended. A failure logs `pg.listen.failed`; an interrupt
 * that comes from the notification queue shutting down counts as a clean end. Interrupting the
 * calling fiber still stops it.
 */
const runSession = Effect.fnUntraced(function* <E>(
  context: ListenContext<E>,
  reconnect: boolean,
): Effect.fn.Return<SessionRecord, never, PgClient.PgClient> {
  const session: SessionRecord = {
    confirmedAtNanos: undefined,
    endedAtNanos: undefined,
    errorMessage: undefined,
  };
  const exit = yield* Effect.exit(listenOnce(context, reconnect, session));
  if (Exit.isSuccess(exit) || Cause.hasInterruptsOnly(exit.cause)) {
    return session;
  }

  const errorMessage = describeFailure(Cause.squash(exit.cause));
  yield* logError('pg.listen.failed', { channel: context.channel, errorMessage });
  return { ...session, errorMessage };
});

/**
 * One confirmed `LISTEN`. Records when PostgreSQL confirmed it, reports `Connected`, sets the gauge
 * and logs `pg.listen.connected` or `pg.listen.reconnected`. A forked pump moves notifications into
 * the bounded buffer while the handler takes them one at a time. When the connection ends, the pump
 * closes the buffer with that outcome, so the handler finishes the payload in flight and the ones
 * already buffered before the session ends. A failing handler ends the session at once. Logs
 * `pg.listen.disconnected` (and `pg.listen.backlog_discarded` when notifications were still
 * buffered). The gauge returns to `0` when the pump exits, even while the handler is draining.
 */
const listenOnce = Effect.fnUntraced(function* <E>(
  context: ListenContext<E>,
  reconnect: boolean,
  session: SessionRecord,
): Effect.fn.Return<void, HandlerFailure | SqlError.SqlError, PgClient.PgClient | Scope.Scope> {
  const { channel, gauge } = context;
  let failureMessage: string | undefined;
  const pg = yield* PgClient.PgClient;
  const notifications = yield* pg.listen(channel);
  const buffer = yield* Queue.dropping<string, SqlError.SqlError | Cause.Done>(
    context.bufferCapacity,
  );
  session.confirmedAtNanos = yield* Clock.monotonicTimeNanos;
  const since = yield* Clock.currentTimeMillis;
  const clearGauge = Metric.update(gauge, 0);
  yield* Effect.addFinalizer(() => clearGauge);
  yield* Metric.update(gauge, 1);
  yield* context.report({ _tag: 'Connected', since });
  yield* logInfo(reconnect ? 'pg.listen.reconnected' : 'pg.listen.connected', { channel });
  yield* Effect.addFinalizer(() =>
    Effect.andThen(
      discardBacklog(context, buffer),
      logInfo('pg.listen.disconnected', {
        channel,
        ...(failureMessage === undefined ? { reason: 'closed' } : { errorMessage: failureMessage }),
      }),
    ),
  );
  yield* Stream.fromQueue(notifications).pipe(
    Stream.runForEach(makeEnqueue(context, buffer)),
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        session.endedAtNanos = yield* Clock.monotonicTimeNanos;
        yield* clearGauge;
        return yield* Exit.isSuccess(exit)
          ? Queue.end(buffer)
          : Queue.failCause(buffer, exit.cause);
      }),
    ),
    Effect.forkScoped,
  );
  yield* Queue.take(buffer).pipe(
    Effect.flatMap((payload) => runHandler(context, payload)),
    Effect.forever,
    Effect.catchIf(Cause.isDone, () => Effect.void),
    Effect.tapError((error) =>
      Effect.sync(() => {
        failureMessage = describeFailure(error);
      }),
    ),
  );
}, Effect.scoped);

/**
 * Builds the pump step that moves one notification into `buffer`. The pump receives a whole socket
 * read at once, so when the buffer first fills it yields once to let the handler take a payload.
 * During an overflow it drops immediately when the buffer is full so the driver queue can drain. A
 * dropped notification is counted in `pgListenDropped`, and `pg.listen.overflow` is logged once per
 * overflow: again only after the buffer has drained empty.
 */
const makeEnqueue = <E>(
  context: ListenContext<E>,
  buffer: Queue.Queue<string, SqlError.SqlError | Cause.Done>,
) => {
  let overflowing = false;
  let droppedTotal = 0;

  /**
   * Offers `payload`, ending the current overflow once the buffer had drained empty.
   */
  const offer = (payload: string): boolean => {
    if (!Queue.offerUnsafe(buffer, payload)) {
      return false;
    }

    if (Queue.sizeUnsafe(buffer) <= 1) {
      overflowing = false;
    }

    return true;
  };

  /**
   * Counts a notification the full buffer could not take, and logs the first of an overflow.
   */
  const drop = (): Effect.Effect<void> => {
    droppedTotal += 1;
    const firstOfOverflow = !overflowing;
    overflowing = true;
    const count = Metric.update(context.dropped, 1);
    return firstOfOverflow
      ? Effect.andThen(
          count,
          logWarning('pg.listen.overflow', {
            channel: context.channel,
            bufferCapacity: context.bufferCapacity,
            droppedTotal,
          }),
        )
      : count;
  };

  return (notification: PgConnection.Notification): Effect.Effect<void> =>
    Effect.suspend(() => {
      if (offer(notification.payload)) {
        return Effect.void;
      }

      if (overflowing) {
        return drop();
      }

      return Effect.andThen(
        Effect.yieldNow,
        Effect.suspend(() => (offer(notification.payload) ? Effect.void : drop())),
      );
    });
};

/**
 * Counts and logs the notifications still in `buffer` when a session ends, which are discarded.
 */
const discardBacklog = <E>(
  context: ListenContext<E>,
  buffer: Queue.Queue<string, SqlError.SqlError | Cause.Done>,
): Effect.Effect<void> =>
  Effect.suspend(() => {
    const discarded = Queue.sizeUnsafe(buffer);
    if (discarded <= 0) {
      return Effect.void;
    }

    return Effect.andThen(
      Metric.update(context.dropped, discarded),
      logWarning('pg.listen.backlog_discarded', { channel: context.channel, discarded }),
    );
  });

/**
 * Discriminant of `HandlerFailure`. Only `runHandler` builds one, and only its result reaches
 * `describeFailure` as a handler error.
 */
const HANDLER_FAILURE_TAG = 'PgListenHandlerFailure';

/**
 * A failure or defect raised by the caller's payload handler, reduced to its `_tag` or error name.
 * Keeping it apart from the transport's `SqlError` matters because a handler that writes the
 * payload to the database fails with a `SqlError` too, and its message often quotes the payload.
 */
type HandlerFailure = {
  /**
   * Always `HANDLER_FAILURE_TAG`.
   */
  readonly _tag: typeof HANDLER_FAILURE_TAG;

  /**
   * `_tag` or error name of what the handler raised. Never its message.
   */
  readonly description: string;
};

/**
 * Wraps whatever the handler raised in a `HandlerFailure` that keeps only its `_tag` or name.
 */
const handlerFailure = (raised: unknown): HandlerFailure => ({
  _tag: HANDLER_FAILURE_TAG,
  description: describeCallerFailure(raised),
});

/**
 * True for a `HandlerFailure`, checked structurally.
 */
const isHandlerFailure = (error: unknown): error is HandlerFailure =>
  Predicate.isTagged(error, HANDLER_FAILURE_TAG) &&
  Predicate.hasProperty(error, 'description') &&
  Predicate.isString(error.description);

/**
 * Names something the caller's code raised by its `_tag` or error name only, never its message,
 * which may quote payload content.
 */
const describeCallerFailure = (error: unknown): string => {
  if (Predicate.hasProperty(error, '_tag') && Predicate.isString(error._tag)) {
    return error._tag;
  }

  return error instanceof Error ? error.name : 'unknown failure';
};

/**
 * Runs the payload handler, turning its failures and defects (an exception it throws before
 * returning an effect included) into a `HandlerFailure`, so none can be mistaken for a transport
 * error. Interruption passes through.
 */
const runHandler = <E>(
  context: ListenContext<E>,
  payload: string,
): Effect.Effect<void, HandlerFailure> =>
  Effect.suspend(() => context.handlePayload(payload)).pipe(
    Effect.mapError(handlerFailure),
    Effect.catchDefect((defect) => Effect.fail(handlerFailure(defect))),
  );

/**
 * Describes why a session ended. A transport `SqlError` keeps its message; a handler failure is
 * described by the `_tag` or error name of what the handler raised (a handler's own `SqlError`
 * included), and anything else (a status callback) the same way, so payload content never reaches
 * logs or `ListenStatus.lastError`.
 */
const describeFailure = (error: unknown): string => {
  if (isHandlerFailure(error)) {
    return error.description;
  }

  if (SqlError.isSqlError(error)) {
    return describeError(error);
  }

  return describeCallerFailure(error);
};

/**
 * True when the session confirmed `LISTEN` and stayed up for at least `LISTEN_STABLE_AFTER` on the
 * monotonic clock, which resets the backoff. A session that never confirmed, or dropped sooner, is
 * not stable.
 */
const isStableSession = (session: SessionRecord, endedAtNanos: bigint): boolean =>
  session.confirmedAtNanos !== undefined &&
  Duration.toMillis(Duration.nanos(endedAtNanos - session.confirmedAtNanos)) >=
    LISTEN_STABLE_AFTER_MS;

/**
 * Delay in whole milliseconds before reconnect `attempt` (1-based): `min(LISTEN_RETRY_BASE *
 * 2^(attempt - 1), LISTEN_RETRY_CAP)` times a factor drawn from Effect's `Random` between
 * `LISTEN_JITTER_MIN` and `LISTEN_JITTER_MAX`.
 */
const backoffDelayMillis = Effect.fnUntraced(function* (attempt: number): Effect.fn.Return<number> {
  const exponent = Math.min(Math.max(attempt - 1, 0), LISTEN_MAX_BACKOFF_EXPONENT);
  const capped = Math.min(LISTEN_RETRY_BASE_MS * 2 ** exponent, LISTEN_RETRY_CAP_MS);
  const factor = yield* Random.nextBetween(LISTEN_JITTER_MIN, LISTEN_JITTER_MAX);
  return Math.round(capped * factor);
});
