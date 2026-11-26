import type { LogFields } from '@yieldkit/logger';

import { logWarning } from '@yieldkit/logger';
import { Config, Effect, Option, Schema } from 'effect';
import { isIP } from 'node:net';

import { PgConfigError } from './errors.ts';

/**
 * Transport security for PostgreSQL sessions.
 *
 * - `disable`: plaintext. The driver never sends `SSLRequest`.
 * - `require`: TLS is mandatory but the server certificate is not verified. A server that does not
 *   offer TLS fails the connection instead of falling back to plaintext.
 * - `verify-full`: TLS is mandatory, the certificate chain must verify against Node's trusted CAs (or
 *   the `tlsCa` option when given), and the certificate must match the URI host.
 *
 * Only `verify-full` protects the password against an active attacker: with `disable` or `require`,
 * whoever answers the connection can ask for the password in clear text.
 */
export type PgTlsMode = 'disable' | 'require' | 'verify-full';

/**
 * TLS options handed to `@effect/sql-pg` (`ssl`) and node-postgres (`ssl`). `false` disables TLS
 * and also stops node-postgres from reading `PGSSLMODE`.
 */
export type PgTlsOptions =
  | false
  | {
      /**
       * Whether the server certificate chain and host name must verify.
       */
      readonly rejectUnauthorized: boolean;

      /**
       * SNI and certificate host name. Only set for a DNS host, because RFC 6066 forbids an IP
       * address here.
       */
      readonly servername?: string;

      /**
       * PEM certificates trusted instead of Node's CAs. Only set for `verify-full` with `tlsCa`.
       */
      readonly ca?: string;
    };

/**
 * Mode used when neither the `tls` option nor `PG_SSL_MODE` is set. Plaintext, so local compose and
 * test containers work without certificates.
 */
export const DEFAULT_PG_TLS_MODE: PgTlsMode = 'disable';

/**
 * Environment variable read by `makePgLive` and `runMigrations` when no `tls` option is passed.
 */
export const PG_SSL_MODE_ENV = 'PG_SSL_MODE';

/**
 * Every accepted `PgTlsMode`, used to validate code options and `PG_SSL_MODE`.
 */
const PG_TLS_MODES: ReadonlySet<string> = new Set<PgTlsMode>(['disable', 'require', 'verify-full']);

/**
 * `NODE_ENV` values treated as production for the plaintext warning.
 */
const PRODUCTION_ENVS = new Set(['production', 'prod']);

/**
 * Square brackets `URL.hostname` keeps around an IPv6 literal.
 */
const IPV6_BRACKETS_PATTERN = /^\[(.*)\]$/;

/**
 * Narrows a string to `PgTlsMode`.
 */
const isPgTlsMode = (value: string): value is PgTlsMode => PG_TLS_MODES.has(value);

/**
 * Parses a case-insensitive `PG_SSL_MODE` value, ignoring surrounding whitespace. Returns
 * `undefined` for anything other than `disable`, `require` or `verify-full`, including libpq modes
 * such as `prefer` or `verify-ca` that this package does not support.
 */
export const parsePgTlsMode = (raw: string): PgTlsMode | undefined => {
  const normalized = raw.trim().toLowerCase();
  return isPgTlsMode(normalized) ? normalized : undefined;
};

/**
 * `PG_SSL_MODE` text that `parsePgTlsMode` accepts.
 */
const PgTlsModeInputSchema = Schema.String.check(
  Schema.makeFilter((raw) => parsePgTlsMode(raw) !== undefined, {
    message: `Expected ${PG_SSL_MODE_ENV} to be one of disable, require, verify-full`,
  }),
);

/**
 * Reads `PG_SSL_MODE` case-insensitively. An unset variable yields `DEFAULT_PG_TLS_MODE`
 * (`disable`); any other value fails with `ConfigError` naming the accepted modes.
 */
export const PgTlsModeConfig: Config.Config<PgTlsMode> = Config.schema(
  PgTlsModeInputSchema,
  PG_SSL_MODE_ENV,
).pipe(
  Config.map((raw): PgTlsMode => parsePgTlsMode(raw) ?? DEFAULT_PG_TLS_MODE),
  Config.withDefault(DEFAULT_PG_TLS_MODE),
);

/**
 * Uses the explicit `tls` option when given, otherwise reads `PG_SSL_MODE`. Fails with
 * `ConfigError` for an invalid environment value and with `PgConfigError` for an invalid option.
 */
export const resolvePgTlsMode = (
  tls: PgTlsMode | undefined,
): Effect.Effect<PgTlsMode, Config.ConfigError | PgConfigError> =>
  tls === undefined ? PgTlsModeConfig : assertPgTlsMode(tls);

/**
 * Rejects a `tls` option outside `PgTlsMode`, which only a JavaScript or cast caller can pass. Does
 * not connect.
 */
export const assertPgTlsMode = (tls: string): Effect.Effect<PgTlsMode, PgConfigError> => {
  if (!isPgTlsMode(tls)) {
    return Effect.fail(
      new PgConfigError({ message: 'tls must be one of disable, require, verify-full.' }),
    );
  }

  return Effect.succeed(tls);
};

/**
 * Rejects `tlsCa` unless `mode` is `verify-full`, the only mode that checks the certificate, and
 * rejects an empty `tlsCa`. Returns the CA bundle, or `undefined` when none was given. Does not
 * connect or parse the PEM text; a bundle the TLS stack cannot use fails the connection.
 */
export const assertTlsCa = (
  mode: PgTlsMode,
  ca: string | undefined,
): Effect.Effect<string | undefined, PgConfigError> => {
  if (ca === undefined) {
    return Effect.undefined;
  }

  if (mode !== 'verify-full') {
    return Effect.fail(
      new PgConfigError({ message: "tlsCa only applies with tls 'verify-full'." }),
    );
  }

  if (ca.trim().length === 0) {
    return Effect.fail(new PgConfigError({ message: 'tlsCa must not be empty.' }));
  }

  return Effect.succeed(ca);
};

/**
 * Maps a mode to the `ssl` value both drivers take. `host` is the URI host name; brackets around an
 * IPv6 literal are ignored. `ca`, when given, replaces Node's trusted CAs for `verify-full`; check
 * it with `assertTlsCa` first, because other modes ignore it.
 *
 * Both drivers treat an object as "TLS required": `@effect/sql-pg` only falls back to plaintext
 * when `ssl` is unset and the URI says `sslmode=prefer` (`internal/config.ts`, `sslOptional`), and
 * fails with "Server refused TLS" otherwise. node-postgres ends the connection with "The server
 * does not support SSL connections" whenever `ssl` is truthy and the server answers `N`.
 */
export const pgTlsOptions = (mode: PgTlsMode, host: string, ca?: string): PgTlsOptions => {
  if (mode === 'disable') {
    return false;
  }

  if (mode === 'require') {
    return { rejectUnauthorized: false };
  }

  const verified = verifyFullOptions(host);
  return ca === undefined ? verified : { ...verified, ca };
};

/**
 * True when `NODE_ENV` is `production` or `prod`, ignoring case. A missing or unreadable value
 * counts as not production, so this never fails.
 */
const isProductionEnv: Effect.Effect<boolean> = Config.option(Config.String('NODE_ENV')).pipe(
  Effect.map((env) =>
    Option.match(env, {
      onNone: () => false,
      onSome: (value) => PRODUCTION_ENVS.has(value.trim().toLowerCase()),
    }),
  ),
  Effect.catchTag('ConfigError', () => Effect.succeed(false)),
);

/**
 * Logs `event` at warning level when `mode` is `disable` and `NODE_ENV` is production. Never fails,
 * because plaintext stays a supported, explicit choice.
 */
export const warnIfTlsDisabledInProduction = Effect.fnUntraced(function* (
  event: string,
  mode: PgTlsMode,
  fields: LogFields,
): Effect.fn.Return<void> {
  if (mode !== 'disable') {
    return;
  }

  const production = yield* isProductionEnv;
  if (!production) {
    return;
  }

  yield* logWarning(event, {
    ...fields,
    tls: mode,
    hint:
      "Pass tls: 'verify-full' (or 'require') to encrypt PostgreSQL connections, or set " +
      `${PG_SSL_MODE_ENV}=verify-full where it is read (makePgLive, runMigrations without tls).`,
  });
});

/**
 * `verify-full` options. `servername` is only set for a DNS name; for an IP address both drivers
 * check the certificate against the connect host instead.
 */
const verifyFullOptions = (host: string): Exclude<PgTlsOptions, false> => {
  const bareHost = host.replace(IPV6_BRACKETS_PATTERN, '$1');
  if (isIP(bareHost) !== 0 || bareHost.length === 0) {
    return { rejectUnauthorized: true };
  }

  return { rejectUnauthorized: true, servername: bareHost };
};
