import type { Config, Scope } from 'effect';
import type { RunnerOption } from 'node-pg-migrate';
import type { ClientConfig } from 'pg';

import { describeError, logInfo } from '@yieldkit/logger';
import { Clock, Duration, Effect, Exit, Option, Redacted } from 'effect';
import { PG_MIGRATE_LOCK_ID, runner } from 'node-pg-migrate';
import { getMigrationFilePaths } from 'node-pg-migrate/migration';
import { readFile } from 'node:fs/promises';
import { Client } from 'pg';

import type { PgConfigError } from './errors.ts';
import type { RunnerLogger } from './migration-log.ts';
import type { PgTlsMode } from './tls.ts';

import { MigrationError } from './errors.ts';
import { endMigrationClient, migrationClientConfig } from './migration-client.ts';
import {
  emitRunnerLog,
  logAppliedMigrations,
  logCommittedMigrationsOnFailure,
  makeRunnerLogBuffer,
} from './migration-log.ts';
import { omitUndefined } from './omit-undefined.ts';
import {
  MIGRATION_CLIENT_END_TIMEOUT,
  MIGRATION_IDLE_IN_TRANSACTION_TIMEOUT,
  MIGRATION_LOCK_TIMEOUT,
  MIGRATION_STATEMENT_TIMEOUT,
  MIGRATIONS_SCHEMA,
  MIGRATIONS_TABLE,
} from './policy.ts';
import { validatePgSchemaName } from './slow-query.ts';
import {
  parseConnectionUri,
  redactDatabaseSecrets,
  resolveApplicationName,
  resolveConnectionUri,
} from './target.ts';
import {
  assertTlsCa,
  pgTlsOptions,
  resolvePgTlsMode,
  warnIfTlsDisabledInProduction,
} from './tls.ts';

/**
 * Direction passed to `node-pg-migrate`. Anything else is rejected before connecting.
 */
export type MigrationDirection = 'up' | 'down';

/**
 * Options for one migration run. `databaseUrl` falls back to `DATABASE_URL`; there is no default
 * URI. `migrationsDir` must be an existing directory of `.sql` files. Relative paths are resolved
 * by `node-pg-migrate` from the process working directory, so pass an absolute path.
 */
export type RunMigrationsOptions = {
  /**
   * Redacted connection URI. Omit to read `DATABASE_URL`; when that is unset too, the run fails
   * with `MigrationError` naming the variable.
   */
  readonly databaseUrl?: Redacted.Redacted | undefined;

  /**
   * Directory of migration files. Pass an absolute path; `node-pg-migrate` resolves relative paths
   * from the working directory. Only `*.sql` files are read; dotfiles and every other file (a
   * README, a helper script, a `.js` or `.ts` file) are ignored. A blank path fails with
   * `MigrationError` before connecting.
   */
  readonly migrationsDir: string;

  /**
   * Name of the history table. Defaults to `MIGRATIONS_TABLE` (`schema_migrations`). Must be a
   * lowercase unquoted identifier; anything else fails with `MigrationError` before connecting.
   */
  readonly migrationsTable?: string | undefined;

  /**
   * Schema of the history table, which must already exist. Defaults to `MIGRATIONS_SCHEMA`
   * (`public`). Must be a lowercase unquoted identifier; anything else fails with `MigrationError`
   * before connecting. The migrations themselves run with the session's `search_path`.
   */
  readonly migrationsSchema?: string | undefined;

  /**
   * `up` applies pending files. `down` rolls back. Defaults to `up`. Any other value fails before
   * connecting.
   */
  readonly direction?: MigrationDirection | undefined;

  /**
   * Transport security for the migration session. Omit to read `PG_SSL_MODE`, which defaults to
   * `disable`. `require` and `verify-full` fail with `MigrationError` when the server does not
   * offer TLS.
   */
  readonly tls?: PgTlsMode | undefined;

  /**
   * PEM certificates to trust instead of Node's CAs. Only valid with `tls: 'verify-full'`; any
   * other mode fails with `MigrationError` before connecting.
   */
  readonly tlsCa?: string | undefined;

  /**
   * Allows `direction: 'down'`, which drops application data. Without `force: true` a down run
   * fails with `MigrationError` before connecting. Ignored for `up`.
   */
  readonly force?: boolean | undefined;

  /**
   * How many migration files to apply (`up`) or roll back (`down`). Must be a positive integer or
   * `Infinity`; anything else fails with `MigrationError` before connecting. Omit to keep the
   * runner's defaults: every pending file for `up`, one file for `down`.
   */
  readonly count?: number | undefined;

  /**
   * `application_name` the migration session reports to PostgreSQL, so it can be found in
   * `pg_stat_activity` while it waits for the migration lock. Omit to send `@effect/sql-pg`
   * (`DEFAULT_APPLICATION_NAME`), the same name the pool sends by default; `PGAPPNAME` is never
   * read. An empty name fails with `MigrationError` before connecting.
   */
  readonly applicationName?: string | undefined;

  /**
   * Session `lock_timeout` for migration statements, applied after the migration lock is held.
   * Defaults to `MIGRATION_LOCK_TIMEOUT` (10 seconds). It never limits the wait for the migration
   * lock itself. Zero, negative, infinite or unparseable values, or more than 2147483647 ms, fail
   * with `MigrationError` before connecting. Fractions of a millisecond are rounded up.
   */
  readonly lockTimeout?: Duration.Input | undefined;

  /**
   * Session `statement_timeout` for migration statements, applied after the migration lock is held.
   * Defaults to `MIGRATION_STATEMENT_TIMEOUT` (10 minutes). It never limits the wait for the
   * migration lock itself. Zero, negative, infinite or unparseable values, or more than 2147483647
   * ms, fail with `MigrationError` before connecting. Fractions of a millisecond are rounded up.
   */
  readonly statementTimeout?: Duration.Input | undefined;
};

/**
 * Validated session timeouts for migration statements, in whole milliseconds.
 */
type MigrationTimeouts = {
  /**
   * `lock_timeout` in milliseconds, always a positive integer.
   */
  readonly lockTimeoutMs: number;

  /**
   * `statement_timeout` in milliseconds, always a positive integer.
   */
  readonly statementTimeoutMs: number;
};

/**
 * Everything a validated run needs to open its session and call the runner.
 */
type MigrationPlan = {
  /**
   * Validated direction passed to the runner.
   */
  readonly direction: MigrationDirection;

  /**
   * Node-postgres settings for the owned migration session, including the password.
   */
  readonly clientConfig: ClientConfig;

  /**
   * Session timeouts applied once the migration lock is held.
   */
  readonly timeouts: MigrationTimeouts;

  /**
   * Number of files to apply or roll back, or `undefined` for the runner's default.
   */
  readonly count: number | undefined;

  /**
   * Validated history table name.
   */
  readonly migrationsTable: string;

  /**
   * Validated schema of the history table.
   */
  readonly migrationsSchema: string;

  /**
   * Removes the URI and password from a message.
   */
  readonly redact: (message: string) => string;

  /**
   * Builds a `MigrationError` for `direction` with the URI and password removed from the cause.
   */
  readonly toError: (cause: unknown) => MigrationError;
};

/**
 * Directions `node-pg-migrate` accepts. Used to reject anything else before a connection is opened.
 */
const MIGRATION_DIRECTIONS = new Set<MigrationDirection>(['up', 'down']);

/**
 * Names `node-pg-migrate` must skip in the migrations directory: dotfiles and anything that does
 * not end in lowercase `.sql` (the `sql` loader pairs `.up.sql` and `.down.sql` case-sensitively).
 * `[\s\S]` instead of `.` so a name with a newline cannot slip past. The runner builds
 * `^<pattern>$` from it, so the alternatives sit in one group to keep both anchored. Without it,
 * the runner would load `.js` and `.ts` files as migrations and fail the whole run on any other
 * file.
 */
const NON_SQL_FILE_PATTERN = String.raw`(?:\.[\s\S]*|(?![\s\S]*\.sql$)[\s\S]*)`;

/**
 * The marker check skips split SQL files, which the loader reads as separate directions.
 */
const SINGLE_SQL_IGNORE_PATTERN = String.raw`(?:${NON_SQL_FILE_PATTERN}|[\s\S]*\.(?:up|down)\.sql)`;

/**
 * Matches the Up marker grammar used by the SQL loader.
 */
const UP_MIGRATION_MARKER = /^\s*--[\s-]*up\s+migration/im;

/**
 * Matches the Down marker grammar used by the SQL loader.
 */
const DOWN_MIGRATION_MARKER = /^\s*--[\s-]*down\s+migration/im;

/**
 * Loads `.sql` files with `node-pg-migrate`'s `sql` loader instead of its default `legacySql` one.
 * Both read a single `NNNN_name.sql` the same way (and give it the same id, so existing history
 * still matches), but `sql` also pairs `NNNN_name.up.sql` with `NNNN_name.down.sql` into one
 * migration. The legacy loader runs each half as its own up migration, down file first.
 */
const SQL_LOADER_STRATEGIES: NonNullable<RunnerOption['migrationLoaderStrategies']> = [
  { extensions: ['.sql'], loader: 'sql' },
];

/**
 * Rejects a direction other than `up` or `down` before any connection is opened.
 */
const assertDirection = (
  direction: MigrationDirection,
): Effect.Effect<MigrationDirection, MigrationError> => {
  if (!MIGRATION_DIRECTIONS.has(direction)) {
    return Effect.fail(
      new MigrationError({
        message: 'Migration direction must be up or down.',
        direction: 'up',
      }),
    );
  }

  return Effect.succeed(direction);
};

/**
 * Rejects `down` without `force: true`, before any connection is opened.
 */
const assertDownAllowed = (
  direction: MigrationDirection,
  force: boolean | undefined,
): Effect.Effect<void, MigrationError> => {
  if (direction === 'down' && force !== true) {
    return Effect.fail(
      new MigrationError({
        message: 'Refusing to run down migrations without force: true; they drop application data.',
        direction,
      }),
    );
  }

  return Effect.void;
};

/**
 * Rejects a `count` that is not a positive integer or `Infinity`, before any connection is opened.
 */
const assertCount = (
  count: number | undefined,
  direction: MigrationDirection,
): Effect.Effect<number | undefined, MigrationError> => {
  if (count !== undefined && count !== Number.POSITIVE_INFINITY && !isPositiveInteger(count)) {
    return Effect.fail(
      new MigrationError({
        message: 'Migration count must be a positive integer or Infinity.',
        direction,
      }),
    );
  }

  return Effect.succeed(count);
};

/**
 * True for an integer of at least 1. `NaN`, fractions, zero and negatives are not.
 */
const isPositiveInteger = (value: number): boolean => Number.isInteger(value) && value > 0;

/**
 * Rewraps a `PgConfigError` as a `MigrationError` for `direction`. The message is already free of
 * the URI and password.
 */
const toMigrationError = (error: PgConfigError, direction: MigrationDirection): MigrationError =>
  new MigrationError({ message: error.message, direction });

/**
 * Names of the migrations the runner reported as executed, in runner order.
 */
const appliedNames = (
  migrations: ReadonlyArray<{ readonly name: string }>,
): ReadonlyArray<string> => migrations.map((migration) => migration.name);

/**
 * Applies or rolls back SQL migrations with `node-pg-migrate`. Logs `pg.migration.applied` for each
 * migration the runner executed, or `pg.migration.up_to_date` when none were pending. History is
 * stored in `migrationsTable` (default `public.schema_migrations`). Connection failures and runner
 * failures become `MigrationError` with the URI and password removed. `direction` other than `up`
 * or `down`, an invalid `lockTimeout` or `statementTimeout`, `direction: 'down'` without `force:
 * true`, a `count` that is not a positive integer or `Infinity`, an invalid history table or schema
 * name, `tlsCa` without `verify-full`, an unset `DATABASE_URL` when no `databaseUrl` is given, a
 * malformed URI or a URI query parameter rejected by the connection URI policy, or `PGREPLICATION`
 * in the environment, fails the same way before connecting. The session is built from the parsed
 * URI exactly like the pool's (see `migrationClientConfig`), so `PGHOST`, `PGPORT`, `PGUSER`,
 * `PGPASSWORD`, `PGOPTIONS`, `PGAPPNAME` and the other libpq variables cannot point it elsewhere
 * than the pool or change how it talks to the server; `PGREPLICATION` and a `PGSSLNEGOTIATION`
 * other than `postgres`, which no setting overrides, fail before connecting. Don't set
 * `NODE_PG_FORCE_NATIVE`: it loads the native client, which ignores the TLS settings. Without
 * `pg-native` installed, `pg` reads it when it is loaded, so importing `@yieldkit/pg` itself throws
 * `Cannot find module 'pg-native'`; with `pg-native` installed, `runMigrations` fails with
 * `MigrationError` before connecting. TLS follows `tls`, then `PG_SSL_MODE`, then `disable`; an
 * invalid `PG_SSL_MODE` fails with `ConfigError`. With `require` or `verify-full`, a server without
 * TLS fails with `MigrationError` instead of falling back to plaintext. `disable` while `NODE_ENV`
 * is production logs `pg.migration.tls_disabled`.
 *
 * A single SQL migration with a Down marker but no Up marker fails with `MigrationError` before
 * connecting.
 *
 * The run owns one session, named `applicationName` (default `@effect/sql-pg`), with TCP keepalive
 * on. It takes the `node-pg-migrate` advisory lock itself with `lock_timeout`, `statement_timeout`
 * and (on PostgreSQL 17 and later) `transaction_timeout` at `0`, so a second deploy waits for the
 * first however long it takes, and only then sets `lock_timeout`, `statement_timeout` and
 * `idle_in_transaction_session_timeout` for the session before calling the runner with `noLock`;
 * `transaction_timeout` stays `0`. Setting the timeouts before the lock would make the lock wait
 * itself fail after `lock_timeout`; setting them after bounds every migration statement without
 * ever cutting the wait short. Logs `pg.migration.lock_acquired` with `waitedMs` once the lock is
 * held. Interrupting the run ends the session without waiting for the server. PostgreSQL notices
 * the closed session, rolls back the open migration and releases the lock once the statement in
 * flight finishes or reaches `statement_timeout`; a run interrupted while waiting for the lock
 * releases it as soon as the wait ends.
 *
 * Everything the runner prints is buffered, passed through `redactDatabaseSecrets` and logged as
 * `pg.migration.runner` with `level` and `line` once the run ends: at debug on success, at error on
 * failure or interruption. The buffer keeps the first `MIGRATION_LOG_BUFFER_LIMIT` lines, each cut
 * to `MIGRATION_LOG_LINE_MAX_CHARS`, and a further `pg.migration.runner` line carries
 * `droppedLines` when it overflowed. A successful run never logs SQL, because `verbose` is off. A
 * failed statement is logged with its SQL text, which for a `.sql` migration is the whole file;
 * only the database URL and password are redacted from it, so keep other secrets out of migration
 * files.
 */
export const runMigrations = Effect.fn('pg.runMigrations')(function* (
  options: RunMigrationsOptions,
): Effect.fn.Return<ReadonlyArray<string>, MigrationError | Config.ConfigError> {
  const plan = yield* planMigrations(options);
  yield* assertSingleFileMarkers(options.migrationsDir, plan);
  const runnerLog = makeRunnerLogBuffer(plan.redact);
  const applied = yield* Effect.scoped(
    applyMigrations(plan, options.migrationsDir, runnerLog.logger),
  ).pipe(Effect.onExit((exit) => emitRunnerLog(runnerLog, Exit.isSuccess(exit))));
  const names = appliedNames(applied);
  if (names.length === 0) {
    yield* logInfo('pg.migration.up_to_date', {
      direction: plan.direction,
      migrationsDir: options.migrationsDir,
    });
    return names;
  }

  yield* logAppliedMigrations(names, plan.direction);
  return names;
});

/**
 * Rejects a Down marker without an Up marker because the SQL loader would run both sections on up.
 */
const assertSingleFileMarkers = Effect.fnUntraced(function* (dir: string, plan: MigrationPlan) {
  // Let the runner report file-access failures through its usual error path.
  const files = yield* Effect.promise(() =>
    getMigrationFilePaths(dir, { ignorePattern: SINGLE_SQL_IGNORE_PATTERN }).catch(() => []),
  );
  const invalid = yield* Effect.findFirst(files, (file) =>
    Effect.promise(async (signal) => {
      const sql = await readFile(file, { encoding: 'utf8', signal }).catch(() => '');
      return DOWN_MIGRATION_MARKER.test(sql) && !UP_MIGRATION_MARKER.test(sql);
    }),
  );
  if (Option.isSome(invalid)) {
    return yield* plan.toError(new Error(`${invalid.value}: Down marker requires an Up marker.`));
  }

  return undefined;
});

/**
 * Validates the options and resolves the URI and TLS mode, failing before any connection is opened.
 */
const planMigrations = Effect.fnUntraced(function* (
  options: RunMigrationsOptions,
): Effect.fn.Return<MigrationPlan, MigrationError | Config.ConfigError> {
  const direction = yield* assertDirection(options.direction ?? 'up');
  yield* assertDownAllowed(direction, options.force);
  const count = yield* assertCount(options.count, direction);
  if (options.migrationsDir.trim().length === 0) {
    return yield* new MigrationError({ message: 'migrationsDir must not be empty.', direction });
  }

  const timeouts = yield* resolveMigrationTimeouts(options, direction);
  /**
   * Rewraps a `PgConfigError` for this run's direction.
   */
  const fromConfigError = (error: PgConfigError): MigrationError =>
    toMigrationError(error, direction);
  const migrationsTable = yield* validatePgSchemaName(
    options.migrationsTable ?? MIGRATIONS_TABLE,
  ).pipe(Effect.mapError(fromConfigError));
  const migrationsSchema = yield* validatePgSchemaName(
    options.migrationsSchema ?? MIGRATIONS_SCHEMA,
  ).pipe(Effect.mapError(fromConfigError));
  const uri = yield* resolveConnectionUri(options.databaseUrl).pipe(
    Effect.catchTag('PgConfigError', (error) => Effect.fail(fromConfigError(error))),
  );
  const parsed = yield* parseConnectionUri(uri).pipe(Effect.mapError(fromConfigError));
  const { target } = parsed;
  const tls = yield* resolvePgTlsMode(options.tls).pipe(
    Effect.catchTag('PgConfigError', (error) => Effect.fail(fromConfigError(error))),
  );
  const tlsCa = yield* assertTlsCa(tls, options.tlsCa).pipe(Effect.mapError(fromConfigError));
  const applicationName = yield* resolveApplicationName(options.applicationName).pipe(
    Effect.mapError(fromConfigError),
  );
  const clientConfig = yield* migrationClientConfig(parsed, {
    applicationName,
    ssl: pgTlsOptions(tls, target.host, tlsCa),
  }).pipe(Effect.mapError(fromConfigError));
  yield* warnIfTlsDisabledInProduction('pg.migration.tls_disabled', tls, {
    host: target.host,
    port: target.port,
    database: target.database,
  });
  const rawUrl = Redacted.value(uri.url);
  /**
   * Removes the URI and password from `message`.
   */
  const redact = (message: string): string => redactDatabaseSecrets(message, rawUrl);
  return {
    direction,
    timeouts,
    count,
    migrationsTable,
    migrationsSchema,
    redact,
    clientConfig,
    toError: (cause: unknown) =>
      new MigrationError({
        message: redact(describeError(cause)),
        direction,
      }),
  };
});

/**
 * Validates `lockTimeout` and `statementTimeout`, applying the defaults for omitted values.
 */
const resolveMigrationTimeouts = Effect.fnUntraced(function* (
  options: RunMigrationsOptions,
  direction: MigrationDirection,
): Effect.fn.Return<MigrationTimeouts, MigrationError> {
  const lockTimeoutMs = yield* toTimeoutMillis(
    'lockTimeout',
    options.lockTimeout ?? MIGRATION_LOCK_TIMEOUT,
    direction,
  );
  const statementTimeoutMs = yield* toTimeoutMillis(
    'statementTimeout',
    options.statementTimeout ?? MIGRATION_STATEMENT_TIMEOUT,
    direction,
  );
  return { lockTimeoutMs, statementTimeoutMs };
});

/**
 * Largest value PostgreSQL accepts for `lock_timeout` and `statement_timeout`, in milliseconds.
 */
const MAX_SESSION_TIMEOUT_MS = 2_147_483_647;

/**
 * Converts a duration to whole milliseconds, rounding up. PostgreSQL treats `0` as "no timeout", so
 * zero, negative, infinite and unparseable values are rejected instead of silently disabling the
 * bound. Values above `MAX_SESSION_TIMEOUT_MS` are rejected here instead of failing at the `SET`
 * after the lock is held.
 */
const toTimeoutMillis = (
  name: string,
  input: Duration.Input,
  direction: MigrationDirection,
): Effect.Effect<number, MigrationError> => {
  const millis = Option.match(Duration.fromInput(input), {
    onNone: () => Number.NaN,
    onSome: Duration.toMillis,
  });
  const rounded = Math.ceil(millis);
  if (!Number.isFinite(millis) || millis <= 0 || rounded > MAX_SESSION_TIMEOUT_MS) {
    return Effect.fail(
      new MigrationError({
        message:
          `Migration ${name} must be a positive duration ` +
          `of at most ${MAX_SESSION_TIMEOUT_MS} ms.`,
        direction,
      }),
    );
  }

  return Effect.succeed(rounded);
};

/**
 * Opens the migration session, takes the migration lock, applies the statement timeouts and runs
 * `node-pg-migrate` on that session. The session and lock are released when the scope closes.
 */
const applyMigrations = Effect.fnUntraced(function* (
  plan: MigrationPlan,
  migrationsDir: string,
  logger: RunnerLogger,
): Effect.fn.Return<ReadonlyArray<{ readonly name: string }>, MigrationError, Scope.Scope> {
  const connect = connectMigrationClient(plan.clientConfig, plan.toError);
  const client = yield* Effect.acquireRelease(connect, endMigrationClient, { interruptible: true });
  yield* acquireMigrationLock(client, plan);
  yield* query(client, migrationTimeoutStatements(plan.timeouts), plan.toError);
  const run = Effect.tryPromise({
    try: () =>
      runner({
        dbClient: client,
        noLock: true,
        dir: migrationsDir,
        direction: plan.direction,
        ...omitUndefined({ count: plan.count }),
        migrationsTable: plan.migrationsTable,
        migrationsSchema: plan.migrationsSchema,
        ignorePattern: NON_SQL_FILE_PATTERN,
        migrationLoaderStrategies: SQL_LOADER_STRATEGIES,
        singleTransaction: false,
        checkOrder: true,
        verbose: false,
        logger,
      }),
    catch: plan.toError,
  });
  const table = `"${plan.migrationsSchema}"."${plan.migrationsTable}"`;
  return yield* logCommittedMigrationsOnFailure(client, table, plan.direction, run);
});

/**
 * Connects a new node-postgres client. A constructor that throws (node-postgres validates some
 * settings while building it) fails with `MigrationError` instead of a defect. A failed connect, or
 * an interrupted one, ends the half-open client so its socket does not linger until the server's
 * authentication timeout.
 */
const connectMigrationClient = (
  config: ClientConfig,
  toError: (cause: unknown) => MigrationError,
): Effect.Effect<Client, MigrationError> =>
  Effect.try({ try: () => new Client(config), catch: toError }).pipe(
    Effect.flatMap((client) =>
      Effect.callback<Client, MigrationError>((resume) => {
        client.on('error', ignoreSessionError);
        client.connect().then(
          () => resume(Effect.succeed(client)),
          (cause: unknown) => {
            endQuietly(client);
            resume(Effect.fail(toError(cause)));
          },
        );
        return Effect.sync(() => endQuietly(client));
      }),
    ),
  );

/**
 * Ends `client` without waiting and ignores the outcome; used on paths that already failed.
 */
const endQuietly = (client: Client): void => {
  client.end().catch(() => undefined);
};

/**
 * Listener for node-postgres `'error'` events. A session the server drops (terminated backend,
 * `idle_in_transaction_session_timeout`, network reset) emits `'error'`, which would crash the
 * process without a listener. The query in flight still rejects, so the run still fails with
 * `MigrationError`.
 */
const ignoreSessionError = (): void => undefined;

/**
 * Waits for the `node-pg-migrate` advisory lock with no session timeouts, so a run queued behind
 * another deploy waits however long that deploy takes. Registers an unlock for when the scope
 * closes after a success or failure. An interrupted run skips the unlock, because the runner may
 * still be using the session; ending the session releases the lock instead, once the server
 * finishes the statement in flight.
 */
const acquireMigrationLock = Effect.fnUntraced(function* (
  client: Client,
  plan: MigrationPlan,
): Effect.fn.Return<void, MigrationError, Scope.Scope> {
  yield* query(client, NO_SESSION_TIMEOUTS, plan.toError);
  const startedAt = yield* Clock.monotonicTimeNanos;
  yield* query(client, 'SELECT pg_advisory_lock($1)', plan.toError, [PG_MIGRATE_LOCK_ID]);
  yield* Effect.addFinalizer((exit) =>
    Exit.hasInterrupts(exit) ? Effect.void : releaseMigrationLock(client, plan),
  );
  const acquiredAt = yield* Clock.monotonicTimeNanos;
  yield* logInfo('pg.migration.lock_acquired', {
    direction: plan.direction,
    waitedMs: Math.round(Duration.toMillis(Duration.nanos(acquiredAt - startedAt))),
  });
});

/**
 * Releases the migration advisory lock, ignoring errors (mapped through `plan.toError`) and giving
 * up after `MIGRATION_CLIENT_END_TIMEOUT`. A failed migration leaves its transaction aborted, so
 * the unlock can fail; ending the session right after releases the lock anyway.
 */
const releaseMigrationLock = (client: Client, plan: MigrationPlan): Effect.Effect<void> =>
  Effect.tryPromise({
    try: () => client.query('SELECT pg_advisory_unlock($1)', [PG_MIGRATE_LOCK_ID]),
    catch: plan.toError,
  }).pipe(Effect.timeout(MIGRATION_CLIENT_END_TIMEOUT), Effect.ignore);

/**
 * Clears session timeouts a role or database default may set, so they cannot cut off the lock wait:
 * `statement_timeout`, `lock_timeout` and, on PostgreSQL 17 and later, `transaction_timeout`. The
 * `WHERE` guard selects no row on older servers, which have no such setting, so nothing fails
 * there. `transaction_timeout` stays `0` for the whole session: `statement_timeout` and
 * `lock_timeout` already bound each migration statement, and a role value would otherwise cut off a
 * long migration file.
 */
const NO_SESSION_TIMEOUTS =
  'SET statement_timeout = 0; SET lock_timeout = 0; ' +
  "SELECT set_config('transaction_timeout', '0', false) " +
  "WHERE current_setting('transaction_timeout', true) IS NOT NULL";

/**
 * Session settings applied once the migration lock is held. The values are validated integers, so
 * interpolating them is safe.
 */
const migrationTimeoutStatements = (timeouts: MigrationTimeouts): string =>
  [
    `SET lock_timeout = '${timeouts.lockTimeoutMs}ms'`,
    `SET statement_timeout = '${timeouts.statementTimeoutMs}ms'`,
    `SET idle_in_transaction_session_timeout = '${MIGRATION_IDLE_IN_TRANSACTION_TIMEOUT}'`,
  ].join('; ');

/**
 * Runs one query on the migration session, mapping a failure through `toError`.
 */
const query = (
  client: Client,
  text: string,
  toError: (cause: unknown) => MigrationError,
  values: ReadonlyArray<unknown> = [],
): Effect.Effect<void, MigrationError> =>
  Effect.tryPromise({ try: () => client.query(text, [...values]), catch: toError }).pipe(
    Effect.asVoid,
  );
