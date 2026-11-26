import type { LogFields } from '@yieldkit/logger';

import { Config, Effect, Option, Redacted, Result } from 'effect';

import { PgConfigError } from './errors.ts';
import { DEFAULT_APPLICATION_NAME } from './policy.ts';

/**
 * Host, port, database, and user a session connects with, resolved from a connection URI. The pool
 * and the migration session both connect with exactly these values, and they are what the logs
 * show. The password is never included.
 */
export type DatabaseTarget = {
  /**
   * Host from the URI, percent-decoded, so `%2Fcloudsql%2Fdb` becomes the Unix socket directory
   * `/cloudsql/db`. An IPv6 literal is returned without its brackets (`::1`). A host starting with
   * `/` is a Unix socket directory for both drivers.
   */
  readonly host: string;

  /**
   * Port from the URI, or `5432` when the URI omits it. Always an integer from 1 to 65535.
   */
  readonly port: string;

  /**
   * Database name from the URI path, percent-decoded. Never empty.
   */
  readonly database: string;

  /**
   * User from the URI, percent-decoded. When the URI has none, the OS user from `USER`, then
   * `USERNAME`, skipping empty values. `PGUSER` is never read.
   */
  readonly user: string;
};

/**
 * Protocols accepted by `describeDatabaseTarget`.
 */
const POSTGRES_PROTOCOLS = new Set(['postgres:', 'postgresql:']);

/**
 * Port used when the URI does not include one.
 */
const DEFAULT_POSTGRES_PORT = '5432';

/**
 * URI query parameters that would set or weaken TLS. TLS comes only from `PG_SSL_MODE` or the `tls`
 * option. `uselibpqcompat` is included because node-postgres uses it to change what `sslmode`
 * means.
 */
const URI_TLS_PARAMETERS: ReadonlySet<string> = new Set([
  'ssl',
  'sslmode',
  'sslcert',
  'sslkey',
  'sslrootcert',
  'sslcrl',
  'sslpassword',
  'sslnegotiation',
  'sslcompression',
  'sslsni',
  'requiressl',
  'gssencmode',
  'uselibpqcompat',
]);

/**
 * URI query parameters that would replace session settings, timeouts or the process name this
 * package sets on every pooled session.
 */
const URI_POOL_PARAMETERS: ReadonlySet<string> = new Set([
  'options',
  'application_name',
  'statement_timeout',
  'lock_timeout',
  'idle_in_transaction_session_timeout',
  'query_timeout',
  'connect_timeout',
  'keepalives',
  'keepalives_idle',
  'target_session_attrs',
]);

/**
 * URI query parameters that repeat the host, port, user, password or database from the URI
 * authority and path, and would silently replace them.
 */
const URI_TARGET_PARAMETERS: ReadonlySet<string> = new Set([
  'host',
  'hostaddr',
  'port',
  'user',
  'password',
  'dbname',
]);

/**
 * URI query parameters that change the wire protocol the driver speaks: a replication session,
 * another client encoding, or binary result parsing.
 */
const URI_PROTOCOL_PARAMETERS: ReadonlySet<string> = new Set([
  'replication',
  'client_encoding',
  'binary',
]);

/**
 * Name of the environment variable `makePgLive` and `runMigrations` read when no URI is passed in
 * code. Error messages name it only when the URI came from there.
 */
export const DATABASE_URL_ENV = 'DATABASE_URL';

/**
 * How error messages refer to a URI that was passed in code.
 */
export const CODE_URI_LABEL = 'The connection URI';

/**
 * Text that replaces the whole connection URI in a redacted message.
 */
const REDACTED_URL = '[redacted-database-url]';

/**
 * Text that replaces the password in a redacted message.
 */
const REDACTED_PASSWORD = '***';

/**
 * Shape every accepted URI has, used in error messages. Never contains caller input.
 */
const EXPECTED_URI_SHAPE = 'postgres://user:password@host:port/database';

/**
 * Leading slash of a URI path, removed to get the database name.
 */
const LEADING_SLASH_PATTERN = /^\//;

/**
 * Square brackets `URL.hostname` keeps around an IPv6 literal. Both drivers want the bare address.
 */
const IPV6_BRACKETS_PATTERN = /^\[(.*)\]$/;

/**
 * Highest TCP port number.
 */
const MAX_PORT = 65_535;

/**
 * Environment variables `@effect/sql-pg` reads, in order, for the user when the URI has none.
 */
const OS_USER_ENVS: ReadonlyArray<string> = ['USER', 'USERNAME'];

/**
 * Message for a URI with a raw `@` after the host, which an unencoded `/`, `?` or `#` in the user
 * or password can leave behind. `URL` ends the authority at the first `/`, `?` or `#`, so the rest
 * of the user or password, including the `@` that should end it, lands in the path, query or
 * fragment. Names no part of the URI.
 */
const ambiguousAuthorityMessage = (label: string): string =>
  `${label} has a raw '@' after the host, which usually means its user or password contains an ` +
  "unencoded '/', '?', '#' or '@'. Percent-encode those characters in the user and password " +
  "(for example '/' as %2F and '@' as %40), and write an '@' in the database name or a query " +
  'value as %40.';

/**
 * A connection URI and the name error messages use for it: `DATABASE_URL` when it was read from the
 * environment, `CODE_URI_LABEL` when it was passed in code.
 */
export type ConnectionUri = {
  /**
   * Redacted URI, including the password.
   */
  readonly url: Redacted.Redacted;

  /**
   * Subject of error messages about this URI. Never the URI itself.
   */
  readonly label: string;
};

/**
 * A parsed connection URI: the loggable target plus the numeric port and decoded password the
 * drivers need. `makePgLayer` and `runMigrations` both connect with these fields and nothing else
 * from the URI, so the pool and the migration session always reach the same server as the same
 * user.
 */
export type ParsedConnectionUri = {
  /**
   * Host, port, database and user, safe to log.
   */
  readonly target: DatabaseTarget;

  /**
   * `target.port` as a number, an integer from 1 to 65535.
   */
  readonly port: number;

  /**
   * Decoded password, or `undefined` when the URI has none.
   */
  readonly password: Redacted.Redacted | undefined;
};

/**
 * One group of rejected URI parameters and the error message that names the offending ones.
 */
type RejectedUriParameterGroup = {
  /**
   * Lowercase parameter names in the group.
   */
  readonly parameters: ReadonlySet<string>;

  /**
   * Builds the error message from the URI's label and the offending names, in URI order. Never
   * includes a value.
   */
  readonly message: (label: string, names: ReadonlyArray<string>) => string;
};

/**
 * Rejected URI parameter groups, checked in this order; the first group with a match fails.
 */
const REJECTED_URI_PARAMETER_GROUPS: ReadonlyArray<RejectedUriParameterGroup> = [
  {
    parameters: URI_TLS_PARAMETERS,
    message: (label, names) =>
      `${label} must not set TLS parameters (${names.join(', ')}). ` +
      'Use PG_SSL_MODE or the tls option instead.',
  },
  {
    parameters: URI_POOL_PARAMETERS,
    message: (label, names) =>
      `${label} must not set pool-owned parameters (${names.join(', ')}). ` +
      'Use the pool options instead.',
  },
  {
    parameters: URI_TARGET_PARAMETERS,
    message: (label, names) =>
      `${label} must not repeat connection target parameters (${names.join(', ')}). ` +
      'Put the host, port, user, password and database in the URI itself.',
  },
  {
    parameters: URI_PROTOCOL_PARAMETERS,
    message: (label, names) =>
      `${label} must not set protocol parameters (${names.join(', ')}). ` +
      'They change how the driver talks to the server and are not supported.',
  },
];

/**
 * Percent-decodes one URI component, or returns `undefined` when its percent-encoding is malformed
 * (`decodeURIComponent` throws `URIError`).
 */
const decodeUriComponent = (value: string): string | undefined => {
  try {
    return decodeURIComponent(value);
  } catch {
    return undefined;
  }
};

/**
 * Every form of the URI's password that could appear in a message, longest first: the text as
 * written in the URI and its decoded form when that decodes. Empty when the URI has no password or
 * does not parse.
 */
const passwordForms = (rawUrl: string): ReadonlyArray<string> => {
  let raw: string;
  try {
    raw = new URL(rawUrl).password;
  } catch {
    return [];
  }

  if (raw.length === 0) {
    return [];
  }

  const decoded = decodeUriComponent(raw);
  const forms = new Set([raw, ...(decoded === undefined || decoded.length === 0 ? [] : [decoded])]);
  return [...forms].toSorted((left, right) => right.length - left.length);
};

/**
 * Removes a connection URI and its password from a driver error message: the whole URI becomes
 * `[redacted-database-url]`, then every occurrence of the password, both percent-encoded as `URL`
 * serializes it and decoded, becomes `***`. A password whose percent-encoding is malformed (a bare
 * `%`) is masked as written. A URI that does not parse only has the whole URI replaced.
 *
 * Masking is exact-value: no prefix, truncation or context matching. Keep the password out of the
 * user name: the user name is logged as `user`, and the server echoes it (cut to 63 bytes) in
 * authentication errors. A very short password also masks unrelated text that contains it.
 */
export const redactDatabaseSecrets = (message: string, rawUrl: string): string => {
  const withoutUrl = rawUrl.length > 0 ? message.split(rawUrl).join(REDACTED_URL) : message;
  return passwordForms(rawUrl).reduce(
    (redacted, password) => redacted.split(password).join(REDACTED_PASSWORD),
    withoutUrl,
  );
};

/**
 * Rejects connection URI query parameters that this package owns, so each setting has exactly one
 * source. Fails with `PgConfigError` naming the offending parameters (lowercased, deduplicated, in
 * URI order) and starting with `label`; the message never includes the URI, a parameter value or
 * the password.
 *
 * - TLS parameters (`sslmode`, `ssl`, `sslrootcert`, ...): node-postgres merges the parsed URI over
 *   its explicit config (`Object.assign({}, config, parse(connectionString))`), so `?ssl=no-verify`
 *   would replace a verifying `ssl` object. Neither driver follows libpq, where `require` encrypts
 *   without verifying: both verify on `require`, and node-postgres warns that it treats `require`
 *   as `verify-full`. They also disagree with each other: `@effect/sql-pg` falls back to plaintext
 *   on `prefer` and rejects `no-verify`, while node-postgres verifies on `prefer` and turns
 *   verification off on `no-verify`. Use `PG_SSL_MODE` or the `tls` option instead.
 * - Pool parameters (`options`, `application_name`, the timeouts, ...): node-postgres lets them
 *   override pool settings, and `@effect/sql-pg` passes `options` straight through as a startup
 *   option next to the session timeouts this package sets.
 * - Target parameters (`host`, `port`, `user`, `password`, `dbname`, `hostaddr`): `@effect/sql-pg`
 *   applies them after the URI authority, so the server it connects to would differ from the host
 *   and database this package logs.
 * - Protocol parameters (`replication`, `client_encoding`, `binary`): they would open a walsender
 *   session, change the client encoding, or change how results are parsed.
 *
 * Parameter names are compared case-insensitively.
 */
export const assertConnectionUriPolicy = (
  parsed: URL,
  label: string = CODE_URI_LABEL,
): Effect.Effect<void, PgConfigError> => {
  const names = [...new Set([...parsed.searchParams.keys()].map((name) => name.toLowerCase()))];
  for (const group of REJECTED_URI_PARAMETER_GROUPS) {
    const rejected = names.filter((name) => group.parameters.has(name));
    if (rejected.length > 0) {
      return Effect.fail(new PgConfigError({ message: group.message(label, rejected) }));
    }
  }

  return Effect.void;
};

/**
 * Uses `url` when given (labelled `CODE_URI_LABEL`), otherwise reads `DATABASE_URL` (labelled
 * `DATABASE_URL`). There is no fallback URI: an unset `DATABASE_URL` fails with `PgConfigError`
 * naming the variable. A config provider that fails for another reason fails with `ConfigError`.
 */
export const resolveConnectionUri = Effect.fnUntraced(function* (
  url: Redacted.Redacted | undefined,
): Effect.fn.Return<ConnectionUri, PgConfigError | Config.ConfigError> {
  if (url !== undefined) {
    return { url, label: CODE_URI_LABEL };
  }

  const fromEnv = yield* Config.option(Config.Redacted(DATABASE_URL_ENV));
  if (Option.isNone(fromEnv)) {
    return yield* new PgConfigError({
      message: `${DATABASE_URL_ENV} is not set. Set it to a ${EXPECTED_URI_SHAPE} URI.`,
    });
  }

  return { url: fromEnv.value, label: DATABASE_URL_ENV };
});

/**
 * Returns `url` when given, otherwise the value of `DATABASE_URL`. An unset `DATABASE_URL` fails
 * with `PgConfigError` naming the variable; there is no fallback URI. The value is not validated
 * here: `makePgLayer` and `runMigrations` do that before connecting.
 */
export const resolveDatabaseUrl = (
  url?: Redacted.Redacted,
): Effect.Effect<Redacted.Redacted, PgConfigError | Config.ConfigError> =>
  resolveConnectionUri(url).pipe(Effect.map((resolved) => resolved.url));

/**
 * Resolves the first non-empty user from the URI, `USER` and `USERNAME`, in that order. Returns
 * `undefined` when none has a value.
 */
const resolveUser = (
  uriUser: string,
  env: Readonly<Record<string, string | undefined>>,
): string | undefined => {
  if (uriUser.length > 0) {
    return uriUser;
  }

  return OS_USER_ENVS.map((name) => env[name]).find(
    (value) => value !== undefined && value.length > 0,
  );
};

/**
 * Decodes the URI host the way `@effect/sql-pg` does: an IPv6 literal loses its brackets, anything
 * else is percent-decoded. Returns `undefined` for malformed percent-encoding.
 */
const decodeHost = (hostname: string): string | undefined => {
  const ipv6 = IPV6_BRACKETS_PATTERN.exec(hostname);
  return ipv6?.[1] ?? decodeUriComponent(hostname);
};

/**
 * Parses an explicit URI port, or returns `DEFAULT_POSTGRES_PORT` when there is none. Returns
 * `undefined` for a port outside 1..65535, which `@effect/sql-pg` rejects and node-postgres would
 * replace with `PGPORT`.
 */
const resolvePort = (port: string): number | undefined => {
  const value = Number(port.length > 0 ? port : DEFAULT_POSTGRES_PORT);
  return Number.isInteger(value) && value >= 1 && value <= MAX_PORT ? value : undefined;
};

/**
 * Parses a redacted postgres URI into the target both drivers connect to and the decoded password.
 * This is the only place the URI is interpreted: `makePgLayer` and `runMigrations` pass the
 * resulting host, port, user, database and password to their drivers as explicit fields, so neither
 * driver parses the URI or reads `PGHOST`, `PGPORT`, `PGUSER` and the like. Fails with
 * `PgConfigError`, whose message starts with `uri.label` and never includes the URI or password,
 * for:
 *
 * - An empty value, a URI `URL` cannot parse, or a non-postgres protocol;
 * - A URI without `//` or a host (such as `postgres:user:pw@host/db`);
 * - A raw `@` in the path, query or fragment, which can mean a `/`, `?` or `#` in the user or
 *   password was not percent-encoded and part of the credentials would be read as the database
 *   (`postgres://app:/pw@host/db` has host `app` and database `pw@host/db`);
 * - A fragment (`#...`), which neither driver uses;
 * - Malformed percent-encoding in the host, user, password or database;
 * - A NUL character (`%00`) in the host, user, password or database, which node-postgres would send
 *   as the end of that startup parameter and the start of another;
 * - A port of 0 (or otherwise outside 1..65535), or a missing database name;
 * - No user in the URI and no non-empty `USER` or `USERNAME` in `env`;
 * - A query parameter rejected by `assertConnectionUriPolicy`.
 */
export const parseConnectionUri = (
  uri: ConnectionUri,
  env: Readonly<Record<string, string | undefined>> = process.env,
): Effect.Effect<ParsedConnectionUri, PgConfigError> => {
  const { label } = uri;
  const shape = parseUriShape(Redacted.value(uri.url), label);
  if (Result.isFailure(shape)) {
    return Effect.fail(shape.failure);
  }

  const parsed = shape.success;
  const components = decodeComponents(parsed);
  if (components === undefined) {
    return Effect.fail(
      new PgConfigError({
        message: `${label} has malformed percent-encoding in the host, user, password or database.`,
      }),
    );
  }

  if (Object.values(components).some((component) => component.includes('\0'))) {
    return Effect.fail(
      new PgConfigError({
        message: `${label} has a NUL character in the host, user, password or database.`,
      }),
    );
  }

  const port = resolvePort(parsed.port);
  if (port === undefined) {
    return Effect.fail(
      new PgConfigError({ message: `${label} has a port outside 1-${String(MAX_PORT)}.` }),
    );
  }

  const { host, database, password } = components;
  if (database.length === 0) {
    return Effect.fail(new PgConfigError({ message: `${label} is missing a database name.` }));
  }

  const user = resolveUser(components.user, env);
  if (user === undefined) {
    return Effect.fail(
      new PgConfigError({
        message: `${label} has no user, and neither USER nor USERNAME has a value. Put the user in the URI.`,
      }),
    );
  }

  return assertConnectionUriPolicy(parsed, label).pipe(
    Effect.as({
      target: { host, port: String(port), database, user },
      port,
      password: password.length > 0 ? Redacted.make(password) : undefined,
    }),
  );
};

/**
 * Parses `rawUrl` with `URL` and checks its shape: non-empty, a postgres protocol, a host, no raw
 * `@` after the host and no fragment. Returns the `PgConfigError` for the first problem instead of
 * the URL, as a `Result`. Messages start with `label` and never include the URI.
 */
const parseUriShape = (rawUrl: string, label: string): Result.Result<URL, PgConfigError> => {
  if (rawUrl.trim().length === 0) {
    return Result.fail(new PgConfigError({ message: `${label} is empty.` }));
  }

  let parsed: URL;
  try {
    parsed = new URL(rawUrl);
  } catch {
    return Result.fail(
      new PgConfigError({ message: `${label} is not a parseable postgres connection URI.` }),
    );
  }

  if (!POSTGRES_PROTOCOLS.has(parsed.protocol)) {
    return Result.fail(
      new PgConfigError({ message: `${label} must use the postgres or postgresql protocol.` }),
    );
  }

  if (parsed.hostname.length === 0) {
    return Result.fail(
      new PgConfigError({ message: `${label} has no host; expected ${EXPECTED_URI_SHAPE}.` }),
    );
  }

  if (`${parsed.pathname}${parsed.search}${parsed.hash}`.includes('@')) {
    return Result.fail(new PgConfigError({ message: ambiguousAuthorityMessage(label) }));
  }

  if (parsed.hash.length > 0) {
    return Result.fail(
      new PgConfigError({ message: `${label} must not have a fragment ('#...').` }),
    );
  }

  return Result.succeed(parsed);
};

/**
 * Percent-decoded host, database, user and password of `parsed`, or `undefined` when any of them
 * has malformed percent-encoding.
 */
const decodeComponents = (
  parsed: URL,
):
  | {
      readonly host: string;
      readonly database: string;
      readonly user: string;
      readonly password: string;
    }
  | undefined => {
  const host = decodeHost(parsed.hostname);
  const database = decodeUriComponent(parsed.pathname.replace(LEADING_SLASH_PATTERN, ''));
  const user = decodeUriComponent(parsed.username);
  const password = decodeUriComponent(parsed.password);
  return host === undefined ||
    database === undefined ||
    user === undefined ||
    password === undefined
    ? undefined
    : { host, database, user, password };
};

/**
 * Parses a redacted postgres URI into log fields, with the same checks and failures as
 * `parseConnectionUri`. Error messages refer to the URI as `label`, which defaults to
 * `CODE_URI_LABEL`. The password is discarded.
 */
export const describeDatabaseTarget = (
  url: Redacted.Redacted,
  label: string = CODE_URI_LABEL,
): Effect.Effect<DatabaseTarget, PgConfigError> =>
  parseConnectionUri({ url, label }).pipe(Effect.map((parsed) => parsed.target));

/**
 * The `application_name` both the pool and the migration session send: `applicationName`, or
 * `DEFAULT_APPLICATION_NAME` (`@effect/sql-pg`) when omitted. Neither driver can send none. An
 * empty name or one containing NUL fails with `PgConfigError`, because node-postgres would replace
 * an empty one with `PGAPPNAME` while the pool sent it empty.
 */
export const resolveApplicationName = (
  applicationName: string | undefined,
): Effect.Effect<string, PgConfigError> => {
  if (applicationName === undefined) {
    return Effect.succeed(DEFAULT_APPLICATION_NAME);
  }

  if (applicationName.length === 0 || applicationName.includes('\0')) {
    return Effect.fail(
      new PgConfigError({ message: 'applicationName must be non-empty and must not contain NUL.' }),
    );
  }

  return Effect.succeed(applicationName);
};

/**
 * Log fields for a database target plus the `application_name` the sessions report.
 */
export const databaseTargetFields = (
  target: DatabaseTarget,
  applicationName: string,
): LogFields => ({
  applicationName,
  host: target.host,
  port: target.port,
  database: target.database,
  user: target.user,
});
