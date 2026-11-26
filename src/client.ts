import type { LogFields } from '@yieldkit/logger';
import type { Duration } from 'effect';

import { PgClient } from '@effect/sql-pg';
import { describeError, logError, logInfo } from '@yieldkit/logger';
import { Config, Effect, Layer, Redacted, Schema } from 'effect';
import { SqlClient, SqlError } from 'effect/sql';

import type { SessionTimeout, SlowQueryLogOptions } from './slow-query.ts';
import type { PgTlsMode } from './tls.ts';

import { retryConnection } from './connect-retry.ts';
import { positiveFiniteMillis } from './duration.ts';
import { PgConfigError } from './errors.ts';
import { omitUndefined } from './omit-undefined.ts';
import {
  ACQUIRE_TIMEOUT,
  CONNECT_TIMEOUT,
  DEFAULT_MAX_CONNECTIONS,
  DEFAULT_MIN_CONNECTIONS,
  DEFAULT_SLOW_QUERY_THRESHOLD_MS,
  IDLE_TIMEOUT,
  KEEP_ALIVE,
} from './policy.ts';
import { MAX_PG_TIMEOUT_MS, sessionStartupParameters } from './slow-query.ts';
import { redactStartupError } from './startup-error.ts';
import {
  CODE_URI_LABEL,
  databaseTargetFields,
  parseConnectionUri,
  redactDatabaseSecrets,
  resolveApplicationName,
  resolveConnectionUri,
} from './target.ts';
import {
  assertPgTlsMode,
  assertTlsCa,
  DEFAULT_PG_TLS_MODE,
  pgTlsOptions,
  resolvePgTlsMode,
  warnIfTlsDisabledInProduction,
} from './tls.ts';

/**
 * Options for a logged PostgreSQL pool. `url` is redacted and is never written to logs. Every
 * duration must be finite and greater than zero; anything else fails with `PgConfigError` before a
 * socket is opened.
 */
export type PgLayerOptions = {
  /**
   * Redacted postgres URI, parsed by `parseConnectionUri`. The driver gets the resolved host, port,
   * user, database and password, never the URI, and the URI is never written to logs.
   */
  readonly url: Redacted.Redacted;

  /**
   * `application_name` for `pg_stat_activity`. Omit to send `@effect/sql-pg`
   * (`DEFAULT_APPLICATION_NAME`): the driver always sends a name, so there is no way to send none.
   * An empty name fails with `PgConfigError`.
   */
  readonly applicationName?: string | undefined;

  /**
   * Pool size. Must be an integer >= 1. Defaults to 10.
   */
  readonly maxConnections?: number | undefined;

  /**
   * Sessions kept open. Must be an integer from 0 to `maxConnections`. Defaults to 1. The layer
   * opens one session for its probe query, even with `0`, so a refused connection fails at startup;
   * it is ready once that session answered, and the rest of the `minConnections` sessions open in
   * the background.
   */
  readonly minConnections?: number | undefined;

  /**
   * When set, PostgreSQL logs statements at least this slow. Omit to leave the server or role
   * setting in effect. Setting `log_min_duration_statement` needs superuser or, on PostgreSQL 15
   * and later, `GRANT SET ON PARAMETER log_min_duration_statement`; without it every session is
   * refused and the layer fails with `PgConfigError`.
   */
  readonly slowQueryLog?: SlowQueryLogOptions | undefined;

  /**
   * `statement_timeout` sent as a startup parameter of each session. Defaults to 15 seconds.
   * `false` sends nothing, so the server or role setting applies; use it behind a pooler such as
   * PgBouncer that refuses unknown startup parameters.
   */
  readonly statementTimeout?: SessionTimeout | undefined;

  /**
   * `lock_timeout` sent as a startup parameter of each session. Defaults to 5 seconds. `false`
   * sends nothing.
   */
  readonly lockTimeout?: SessionTimeout | undefined;

  /**
   * `idle_in_transaction_session_timeout` sent as a startup parameter of each session. Defaults to
   * 30 seconds. `false` sends nothing.
   */
  readonly idleInTransactionTimeout?: SessionTimeout | undefined;

  /**
   * Optional PostgreSQL `search_path` for each session (validated schema identifiers).
   */
  readonly searchPath?: string | undefined;

  /**
   * How long a statement waits for a free connection. Defaults to 5 seconds.
   */
  readonly acquireTimeout?: Duration.Input | undefined;

  /**
   * How long a new session may take to connect and authenticate. Defaults to 5 seconds.
   */
  readonly connectTimeout?: Duration.Input | undefined;

  /**
   * How long an unused pooled session stays open. Defaults to 30 seconds.
   */
  readonly idleTimeout?: Duration.Input | undefined;

  /**
   * Delay before TCP keepalive probes start on an idle session. Defaults to 10 seconds. Must be
   * from 1 to 2,147,483,647 whole seconds (fractions are dropped).
   */
  readonly keepAlive?: Duration.Input | undefined;

  /**
   * Transport security. Defaults to `disable` (plaintext); this layer does not read `PG_SSL_MODE`.
   * `require` and `verify-full` fail the connection when the server does not offer TLS.
   */
  readonly tls?: PgTlsMode | undefined;

  /**
   * PEM certificates to trust instead of Node's CAs, for a server certificate issued by a private
   * CA. Only valid with `tls: 'verify-full'`; any other mode fails with `PgConfigError`.
   */
  readonly tlsCa?: string | undefined;
};

/**
 * Process-level options for `makePgLive`: every `makePgLayer` option except `url` and `tls`, which
 * come from `DATABASE_URL` and `PG_SSL_MODE` unless `tls` is given here.
 */
export type PgLiveOptions = Omit<PgLayerOptions, 'url' | 'tls'> & {
  /**
   * Overrides `PG_SSL_MODE`. Omit to read that variable, which defaults to `disable`. An invalid
   * `PG_SSL_MODE` fails with `ConfigError`.
   */
  readonly tls?: PgTlsMode | undefined;

  /**
   * Overrides `PG_SLOW_QUERY_LOG`. Omit to read that flag, which defaults to off.
   */
  readonly slowQueryLog?: SlowQueryLogOptions | undefined;
};

/**
 * Row returned by the startup probe `SELECT pg_backend_pid()`.
 */
const PidRow = Schema.Struct({
  /**
   * Backend process id of the session that served the probe.
   */
  pid: Schema.Int,
});

/**
 * PostgreSQL SQLSTATE for "insufficient privilege".
 */
const INSUFFICIENT_PRIVILEGE = '42501';

/**
 * Server parameter `slowQueryLog` sets, named in the privilege error.
 */
const SLOW_QUERY_PARAMETER = 'log_min_duration_statement';

/**
 * Logged value of `slowQueryLog` when no threshold is sent and the server or role setting applies.
 */
const SERVER_DEFAULT = 'server-default';

/**
 * Message of the `PgConfigError` raised when the role may not set `log_min_duration_statement`.
 */
const SLOW_QUERY_PERMISSION_MESSAGE =
  `slowQueryLog needs permission to set ${SLOW_QUERY_PARAMETER}: connect as a superuser, ` +
  `GRANT SET ON PARAMETER ${SLOW_QUERY_PARAMETER} to the role (PostgreSQL 15+), or omit ` +
  'slowQueryLog and set it with ALTER ROLE or ALTER DATABASE instead.';

/**
 * The driver's `Error` behind a `SqlError`, or `undefined` for anything else.
 */
const sqlErrorCause = (error: unknown): Error | undefined => {
  if (!SqlError.isSqlError(error)) {
    return undefined;
  }

  const { cause } = error.reason;
  return cause instanceof Error ? cause : undefined;
};

/**
 * Describes a driver failure with the server's own message after the driver's summary, such as
 * `PgConnection: Failed to connect: password authentication failed for user "app"`. The result
 * still contains whatever the server sent, so redact it before logging.
 */
const describePgFailure = (error: unknown): string => {
  const summary = describeError(error);
  const cause = sqlErrorCause(error);
  if (cause === undefined || cause.message.length === 0 || cause.message === summary) {
    return summary;
  }

  return `${summary}: ${cause.message}`;
};

/**
 * Logs a driver failure after removing the connection URI and password from the message.
 */
const logPgFailure = (
  event: string,
  error: unknown,
  rawUrl: string,
  fields: LogFields,
): Effect.Effect<void> =>
  logError(event, {
    ...fields,
    errorMessage: redactDatabaseSecrets(describePgFailure(error), rawUrl),
  });

/**
 * True when PostgreSQL refused the session because the role may not set
 * `log_min_duration_statement`, which `slowQueryLog` sends at startup.
 */
const isSlowQueryPermissionError = (error: unknown): boolean => {
  const cause = sqlErrorCause(error);
  return (
    cause !== undefined &&
    'code' in cause &&
    cause.code === INSUFFICIENT_PRIVILEGE &&
    cause.message.includes(SLOW_QUERY_PARAMETER)
  );
};

/**
 * Replaces the opaque startup `SqlError` with a `PgConfigError` that names the missing privilege
 * when `slowQueryLog` caused it. Other errors pass through unchanged.
 */
const explainSlowQueryPermission = <E>(error: E): E | PgConfigError =>
  isSlowQueryPermissionError(error)
    ? new PgConfigError({ message: SLOW_QUERY_PERMISSION_MESSAGE })
    : error;

/**
 * Rejects a pool size that is not an integer >= 1. Does not connect.
 */
const assertMaxConnections = (maxConnections: number): Effect.Effect<number, PgConfigError> => {
  if (!Number.isInteger(maxConnections) || maxConnections < 1) {
    return Effect.fail(
      new PgConfigError({
        message: `maxConnections must be an integer >= 1 (got ${String(maxConnections)}).`,
      }),
    );
  }

  return Effect.succeed(maxConnections);
};

/**
 * Rejects a `minConnections` that is not an integer from 0 to `maxConnections`. Does not connect.
 */
const assertMinConnections = (
  minConnections: number,
  maxConnections: number,
): Effect.Effect<number, PgConfigError> => {
  if (!Number.isInteger(minConnections) || minConnections < 0 || minConnections > maxConnections) {
    return Effect.fail(
      new PgConfigError({
        message:
          `minConnections must be an integer from 0 to maxConnections (${String(maxConnections)}) ` +
          `(got ${String(minConnections)}).`,
      }),
    );
  }

  return Effect.succeed(minConnections);
};

/**
 * Rejects a pool duration that is not finite and greater than zero. Does not connect.
 */
const assertPoolDuration = (
  name: string,
  input: Duration.Input,
): Effect.Effect<Duration.Input, PgConfigError> =>
  positiveFiniteMillis(input) === undefined
    ? Effect.fail(
        new PgConfigError({ message: `${name} must be a finite duration greater than zero.` }),
      )
    : Effect.succeed(input);

/**
 * Largest keepalive delay the driver accepts, in whole seconds.
 */
const MAX_KEEP_ALIVE_SECONDS = 2_147_483_647;

/**
 * Rejects a keepalive delay the driver would refuse at connect time: it truncates to whole seconds
 * and accepts 1 to 2,147,483,647. Does not connect.
 */
const assertKeepAlive = (input: Duration.Input): Effect.Effect<Duration.Input, PgConfigError> => {
  const millis = positiveFiniteMillis(input);
  const seconds = millis === undefined ? 0 : Math.floor(millis / 1000);
  return seconds >= 1 && seconds <= MAX_KEEP_ALIVE_SECONDS
    ? Effect.succeed(input)
    : Effect.fail(
        new PgConfigError({
          message: `keepAlive must be from 1 to ${String(MAX_KEEP_ALIVE_SECONDS)} whole seconds.`,
        }),
      );
};

/**
 * Validated client-side pool timings passed to `PgClient.make`.
 */
type PoolTimings = {
  /**
   * How long a statement waits for a free connection.
   */
  readonly acquireTimeout: Duration.Input;

  /**
   * How long a new session may take to connect and authenticate.
   */
  readonly connectTimeout: Duration.Input;

  /**
   * How long an unused pooled session stays open.
   */
  readonly idleTimeout: Duration.Input;

  /**
   * Delay before TCP keepalive probes start on an idle session.
   */
  readonly keepAlive: Duration.Input;
};

/**
 * Applies the defaults to the client-side pool timings and validates each one.
 */
const resolvePoolTimings = Effect.fnUntraced(function* (
  options: PgLayerOptions,
): Effect.fn.Return<PoolTimings, PgConfigError> {
  const acquireTimeout = yield* assertPoolDuration(
    'acquireTimeout',
    options.acquireTimeout ?? ACQUIRE_TIMEOUT,
  );
  const connectTimeout = yield* assertPoolDuration(
    'connectTimeout',
    options.connectTimeout ?? CONNECT_TIMEOUT,
  );
  const idleTimeout = yield* assertPoolDuration('idleTimeout', options.idleTimeout ?? IDLE_TIMEOUT);
  const keepAlive = yield* assertKeepAlive(options.keepAlive ?? KEEP_ALIVE);
  return { acquireTimeout, connectTimeout, idleTimeout, keepAlive };
});

/**
 * Opens one session and returns its backend pid. The caller logs success and retries a refused
 * connect or a server that is not ready yet.
 */
const probeConnection = Effect.fnUntraced(function* (
  client: PgClient.PgClient & SqlClient.SqlClient,
): Effect.fn.Return<number, SqlError.SqlError | PgConfigError> {
  const rows = yield* client`SELECT pg_backend_pid() AS pid`;
  const decoded = yield* Schema.decodeUnknownEffect(Schema.Array(PidRow))(rows).pipe(
    Effect.mapError(
      () =>
        new PgConfigError({
          message: 'PostgreSQL probe returned an unexpected pid row.',
        }),
    ),
  );
  const first = decoded[0];
  if (first === undefined) {
    return yield* new PgConfigError({ message: 'PostgreSQL probe returned no backend pid.' });
  }

  return first.pid;
});

/**
 * Builds a pooled client, logs opening and ready, and logs closed when the scope ends. Pool
 * construction failures log `pg.client.failed` and do not include the URI. `label` is how error
 * messages refer to the URI.
 */
const openLoggedClient = Effect.fnUntraced(function* (options: PgLayerOptions, label: string) {
  const applicationName = yield* resolveApplicationName(options.applicationName);
  const maxConnections = yield* assertMaxConnections(
    options.maxConnections ?? DEFAULT_MAX_CONNECTIONS,
  );
  const minConnections = yield* assertMinConnections(
    options.minConnections ?? DEFAULT_MIN_CONNECTIONS,
    maxConnections,
  );
  const timings = yield* resolvePoolTimings(options);
  const tls = yield* assertPgTlsMode(options.tls ?? DEFAULT_PG_TLS_MODE);
  const tlsCa = yield* assertTlsCa(tls, options.tlsCa);
  const { target, port, password } = yield* parseConnectionUri({ url: options.url, label });
  const fields = databaseTargetFields(target, applicationName);
  const rawUrl = Redacted.value(options.url);
  const startupParameters = yield* sessionStartupParameters(options.slowQueryLog, {
    statementTimeout: options.statementTimeout,
    lockTimeout: options.lockTimeout,
    idleInTransactionTimeout: options.idleInTransactionTimeout,
    searchPath: options.searchPath,
  });
  yield* logInfo('pg.client.opening', {
    ...fields,
    maxConnections,
    slowQueryLog: startupParameters.log_min_duration_statement ?? SERVER_DEFAULT,
    tls,
  });
  yield* warnIfTlsDisabledInProduction('pg.client.tls_disabled', tls, fields);

  const client = yield* PgClient.make({
    host: target.host,
    port,
    username: target.user,
    database: target.database,
    ...omitUndefined({ password }),
    ssl: pgTlsOptions(tls, target.host, tlsCa),
    applicationName,
    maxConnections,
    minConnections,
    ...timings,
    startupParameters,
    spanAttributes: {
      'db.system.name': 'postgresql',
      'db.namespace': target.database,
      'server.address': target.host,
    },
  }).pipe(
    Effect.tapError((error) => logPgFailure('pg.client.failed', error, rawUrl, fields)),
    Effect.mapError(explainSlowQueryPermission),
    Effect.mapError((error) => redactStartupError(error, rawUrl)),
  );

  yield* Effect.addFinalizer(() => logInfo('pg.client.closed', fields));
  const backendPid = yield* retryConnection(probeConnection(client), fields).pipe(
    Effect.tapError((error) => logPgFailure('pg.connection.failed', error, rawUrl, fields)),
    Effect.mapError(explainSlowQueryPermission),
    Effect.mapError((error) => redactStartupError(error, rawUrl)),
  );
  yield* Effect.addFinalizer(() =>
    logInfo('pg.connection.disconnected', { ...fields, backendPid }),
  );
  yield* logInfo('pg.connection.created', { ...fields, backendPid });
  yield* logInfo('pg.client.ready', { ...fields, maxConnections });
  return client;
});

/**
 * Builds a pool layer that provides Effect's `PgClient` and `SqlClient`. Logs `pg.client.opening`,
 * `pg.connection.created` (backend pid), `pg.client.ready`, and `pg.client.closed` when the layer
 * scope ends. Connection and authentication failures log `pg.client.failed` or
 * `pg.connection.failed` with the server's reason and with the URI and password removed. The driver
 * connects with the host, port, user, database and password `parseConnectionUri` resolved, the same
 * ones `runMigrations` uses. An empty or malformed URI (no host, a raw `@` in the path, query or
 * fragment, bad percent-encoding, a NUL character, port 0, no database, no user anywhere, a
 * non-postgres protocol), an invalid pool size, duration, keepalive or slow-query threshold, an
 * empty `applicationName`, `tlsCa` without `verify-full`, or a URI query parameter that sets TLS,
 * pool settings or the connection target fails with `PgConfigError` before a socket is opened.
 * Session timeouts set to `false` are not sent, which a pooler such as PgBouncer needs; a pooler
 * refusing a startup parameter fails the build without retries. A role that may not set
 * `log_min_duration_statement` while `slowQueryLog` is set fails with `PgConfigError` naming the
 * privilege. TLS follows `tls` (default `disable`); with `require` or `verify-full`, a server
 * without TLS fails the build with `SqlError` instead of falling back to plaintext. `disable` while
 * `NODE_ENV` is production logs `pg.client.tls_disabled` as a warning. Other startup failures are
 * the driver's `SqlError` with the same reason kind and SQLSTATE `code`, rebuilt so the URI and
 * password are removed from the driver error's message, stack and string fields.
 *
 * Redaction covers this package's log lines, its own errors and that startup `SqlError`. A
 * `SqlError` from a query after startup, and the spans `@effect/sql-pg` ends itself (startup spans
 * included), carry the server's text unchanged; PostgreSQL never echoes the password, only the user
 * name. The build opens one session for `SELECT pg_backend_pid()`, which `pg.client.ready` proves;
 * the rest of the `minConnections` sessions open in the background.
 */
export const makePgLayer = (
  options: PgLayerOptions,
): Layer.Layer<PgClient.PgClient | SqlClient.SqlClient, SqlError.SqlError | PgConfigError> =>
  PgClient.layerFrom(openLoggedClient(options, CODE_URI_LABEL));

/**
 * Reads `PG_SLOW_QUERY_LOG` (default false) and, when true, `PG_SLOW_QUERY_THRESHOLD_MS`. A false
 * flag returns `undefined` so the server or role setting stays in effect. A threshold that is not
 * an integer from 0 to `MAX_PG_TIMEOUT_MS` fails with `ConfigError` naming the variable.
 */
const readSlowQueryLog = Effect.fnUntraced(function* (): Effect.fn.Return<
  SlowQueryLogOptions | undefined,
  Config.ConfigError
> {
  const enabled = yield* Config.Boolean('PG_SLOW_QUERY_LOG').pipe(Config.withDefault(false));
  if (!enabled) {
    return undefined;
  }

  const thresholdMs = yield* Config.schema(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: MAX_PG_TIMEOUT_MS })),
    'PG_SLOW_QUERY_THRESHOLD_MS',
  ).pipe(Config.withDefault(DEFAULT_SLOW_QUERY_THRESHOLD_MS));
  return { thresholdMs };
});

/**
 * Pool layer reading the URI from `DATABASE_URL`. There is no fallback: an unset `DATABASE_URL`
 * fails with `PgConfigError` naming the variable, and URI errors name it too. Slow-query logging
 * stays off unless `slowQueryLog` is passed or `PG_SLOW_QUERY_LOG` is true. TLS follows `tls`, then
 * `PG_SSL_MODE` (case-insensitive), then `disable`. Every other option behaves as in `makePgLayer`.
 * Invalid environment values fail with `ConfigError`, invalid options with `PgConfigError`, both
 * before a socket is opened.
 */
export const makePgLive = (
  options: PgLiveOptions = {},
): Layer.Layer<
  PgClient.PgClient | SqlClient.SqlClient,
  Config.ConfigError | SqlError.SqlError | PgConfigError
> =>
  Layer.unwrap(
    Effect.gen(function* () {
      const uri = yield* resolveConnectionUri(undefined);
      const slowQueryLog = options.slowQueryLog ?? (yield* readSlowQueryLog());
      const tls = yield* resolvePgTlsMode(options.tls);
      return PgClient.layerFrom(
        openLoggedClient({ ...options, url: uri.url, slowQueryLog, tls }, uri.label),
      );
    }),
  );

/**
 * Runs `SELECT 1` on the current pool. A refused or saturated pool fails with `SqlError`. Does not
 * open its own connection.
 */
export const ping: Effect.Effect<void, SqlError.SqlError, SqlClient.SqlClient> = Effect.gen(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`SELECT 1`;
  },
).pipe(Effect.asVoid, Effect.withSpan('pg.ping'));
