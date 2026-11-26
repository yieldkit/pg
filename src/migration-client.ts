import type { Client, ClientConfig } from 'pg';

import { Duration, Effect, Redacted } from 'effect';

import type { ParsedConnectionUri } from './target.ts';
import type { PgTlsOptions } from './tls.ts';

import { PgConfigError } from './errors.ts';
import {
  KEEP_ALIVE,
  MIGRATION_CLIENT_END_TIMEOUT,
  MIGRATION_CONNECT_TIMEOUT_MS,
} from './policy.ts';

/**
 * Startup `options` sent by the migration session. node-postgres reads `PGOPTIONS` whenever
 * `options` is empty, which would let a shell setting such as `-c search_path=other` move the
 * migrations to another schema than the pool uses. A single space is non-empty, so the variable is
 * ignored, and PostgreSQL parses it as no options at all.
 */
const NO_STARTUP_OPTIONS = ' ';

/**
 * Environment variable node-postgres reads to open a replication (walsender) session. There is no
 * config value that overrides it, so its presence is rejected instead.
 */
const PG_REPLICATION_ENV = 'PGREPLICATION';

/**
 * Environment variable node-postgres reads to choose how TLS is negotiated. `ClientConfig` has no
 * field to override it and the pool always uses the standard `postgres` negotiation, so any other
 * value is rejected.
 */
const PG_SSL_NEGOTIATION_ENV = 'PGSSLNEGOTIATION';

/**
 * Environment variable that makes `pg` load its native (libpq) client. That client ignores the TLS
 * settings this package passes, so `verify-full` would become libpq's default `prefer`.
 */
const PG_FORCE_NATIVE_ENV = 'NODE_PG_FORCE_NATIVE';

/**
 * The only `PGSSLNEGOTIATION` value accepted: the standard negotiation the pool uses.
 */
const STANDARD_SSL_NEGOTIATION = 'postgres';

/**
 * Client encoding of the migration session. node-postgres reads `PGCLIENT_ENCODING` whenever it is
 * empty; the pool always speaks UTF-8. node-postgres uses it only to decode text it receives.
 */
const CLIENT_ENCODING = 'utf8';

/**
 * `KEEP_ALIVE` in milliseconds, the delay before TCP keepalive probes start on an idle session.
 */
const KEEP_ALIVE_MS = Duration.toMillis(KEEP_ALIVE);

/**
 * PostgreSQL error code for a missing table.
 */
const UNDEFINED_TABLE = '42P01';

/**
 * True for the PostgreSQL error raised when a query names a table that does not exist.
 */
export const isUndefinedTable = (cause: unknown): boolean =>
  typeof cause === 'object' && cause !== null && 'code' in cause && cause.code === UNDEFINED_TABLE;

/**
 * Settings of a migration session that do not come from the URI.
 */
export type MigrationSessionSettings = {
  /**
   * `application_name` to report. Must be non-empty: node-postgres reads `PGAPPNAME` for an empty
   * one. Pass `resolveApplicationName(option)` so the default matches the pool's.
   */
  readonly applicationName: string;

  /**
   * TLS settings from `pgTlsOptions`.
   */
  readonly ssl: PgTlsOptions;
};

/**
 * Password callback used when the URI has none. A function is never treated as missing, so
 * node-postgres does not fall back to `PGPASSWORD` or `~/.pgpass`, matching the pool, which sends
 * no password either.
 */
const noPassword = (): string => '';

/**
 * Fails with `PgConfigError` when `env` sets a variable node-postgres would act on with no config
 * value to override it: `PGREPLICATION` (opens a replication session), a `PGSSLNEGOTIATION` other
 * than `postgres` (changes the TLS handshake the pool uses) or `NODE_PG_FORCE_NATIVE` (loads the
 * native client, which ignores the TLS settings). Without `pg-native` installed, `pg` already
 * throws on that variable when it is loaded, so this check only runs when `pg-native` is
 * installed.
 */
const assertNoDriverOverrides = (
  env: Readonly<Record<string, string | undefined>>,
): Effect.Effect<void, PgConfigError> => {
  if ((env[PG_REPLICATION_ENV] ?? '').length > 0) {
    return Effect.fail(
      new PgConfigError({
        message: `${PG_REPLICATION_ENV} must not be set for migrations; it opens a replication session.`,
      }),
    );
  }

  const negotiation = env[PG_SSL_NEGOTIATION_ENV] ?? '';
  if (negotiation.length > 0 && negotiation !== STANDARD_SSL_NEGOTIATION) {
    return Effect.fail(
      new PgConfigError({
        message:
          `${PG_SSL_NEGOTIATION_ENV} must be unset or '${STANDARD_SSL_NEGOTIATION}' for ` +
          'migrations; the pool always uses the standard TLS negotiation.',
      }),
    );
  }

  if ((env[PG_FORCE_NATIVE_ENV] ?? '').length > 0) {
    return Effect.fail(
      new PgConfigError({
        message: `${PG_FORCE_NATIVE_ENV} must not be set for migrations; the native client ignores the TLS settings this package passes.`,
      }),
    );
  }

  return Effect.void;
};

/**
 * Builds the node-postgres config for a migration session from the same parsed URI the pool uses,
 * so it connects to the same host (a decoded Unix socket directory included), port, database and
 * user. Every setting node-postgres would otherwise take from a `PG*` variable is explicit and
 * non-empty (node-postgres treats an empty value as missing): host, port, user, database, password
 * (a callback when the URI has none), `options`, `application_name` and `client_encoding`. So
 * `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`, `PGPASSWORD`, `PGOPTIONS`, `PGAPPNAME`,
 * `PGCLIENT_ENCODING`, `PGSSLMODE` and `PGCONNECT_TIMEOUT` cannot change the session. TCP keepalive
 * starts after `KEEP_ALIVE`, so a connection dropped without a reset fails the run instead of
 * hanging it. Fails with `PgConfigError` when `env` sets `PGREPLICATION`, a `PGSSLNEGOTIATION`
 * other than `postgres` or `NODE_PG_FORCE_NATIVE`, which no config value overrides.
 */
export const migrationClientConfig = (
  parsed: ParsedConnectionUri,
  settings: MigrationSessionSettings,
  env: Readonly<Record<string, string | undefined>> = process.env,
): Effect.Effect<ClientConfig, PgConfigError> => {
  const { target, port, password } = parsed;
  return assertNoDriverOverrides(env).pipe(
    Effect.as({
      host: target.host,
      port,
      user: target.user,
      database: target.database,
      password: password === undefined ? noPassword : Redacted.value(password),
      options: NO_STARTUP_OPTIONS,
      application_name: settings.applicationName,
      client_encoding: CLIENT_ENCODING,
      binary: false,
      ssl: settings.ssl,
      connectionTimeoutMillis: MIGRATION_CONNECT_TIMEOUT_MS,
      keepAlive: true,
      keepAliveInitialDelayMillis: KEEP_ALIVE_MS,
    }),
  );
};

/**
 * Ends the migration client and releases any session locks. Destroys its stream if closing fails or
 * the peer does not close within `MIGRATION_CLIENT_END_TIMEOUT`.
 */
export const endMigrationClient = (client: Client): Effect.Effect<void> =>
  Effect.tryPromise({ try: () => client.end(), catch: () => undefined }).pipe(
    Effect.timeout(MIGRATION_CLIENT_END_TIMEOUT),
    Effect.catch(() =>
      Effect.sync(() => {
        client.connection.stream.destroy();
      }),
    ),
  );
