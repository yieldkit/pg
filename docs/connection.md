# Connection

`makePgLive` and `makePgLayer` build the same logged pool and provide `PgClient | SqlClient`.

```ts
import { makePgLayer, makePgLive } from '@yieldkit/pg';
import { Redacted } from 'effect';

// Reads DATABASE_URL, PG_SSL_MODE and PG_SLOW_QUERY_LOG.
const PgLive = makePgLive({ applicationName: 'api' });

// Takes the URI, TLS mode and slow-query settings from code.
const PgFromCode = makePgLayer({
  url: Redacted.make('postgres://app:s3cret@db.internal:5432/app'),
  tls: 'verify-full',
});
```

`makePgLive` takes every `makePgLayer` option except `url` (see [Pool and timeouts](pool-and-timeouts.md)). Which environment variables each one reads is in [TLS](tls.md#choosing-the-mode) and [Slow-query logging](slow-query-logging.md).

There is no default URI. An unset `DATABASE_URL` fails with `PgConfigError: DATABASE_URL is not set. Set it to a postgres://user:password@host:port/database URI.` An invalid `PG_SSL_MODE`, `PG_SLOW_QUERY_LOG` or `PG_SLOW_QUERY_THRESHOLD_MS` fails with `ConfigError`.

`resolveDatabaseUrl(url?)` returns `url`, or else `DATABASE_URL` (exported as `DATABASE_URL_ENV`) as a `Redacted`. It fails the same way and does not validate the URI. `resolvePgTlsMode(tls)` returns `tls`, or else `PG_SSL_MODE`, or else `disable`. Together they build a layer that reads the environment only for what the caller leaves out:

```ts
import { makePgLayer, resolveDatabaseUrl, resolvePgTlsMode, type PgTlsMode } from '@yieldkit/pg';
import { Effect, Layer, Redacted } from 'effect';

// An explicit value wins, else DATABASE_URL / PG_SSL_MODE, as makePgLive does.
const makePg = (url?: Redacted.Redacted, tls?: PgTlsMode) =>
  Layer.unwrap(
    Effect.gen(function* () {
      return makePgLayer({
        url: yield* resolveDatabaseUrl(url),
        tls: yield* resolvePgTlsMode(tls),
      });
    }),
  );
```

## The connection URI

The shape is `postgres://user:password@host:port/database`. `postgresql:` works too, and the port defaults to `5432`.

Percent-encode `/`, `?`, `#`, `@` and `:` in the user and password. `encodeURIComponent` does that:

```ts
import { makePgLayer } from '@yieldkit/pg';
import { Redacted } from 'effect';

const password = encodeURIComponent('p@ss/word#1'); // 'p%40ss%2Fword%231'
const PgLive = makePgLayer({ url: Redacted.make(`postgres://app:${password}@db:5432/app`) });
```

The package parses the URI once and hands the pool and the migration session the same fields. Neither driver parses the URI or reads `PGHOST`, `PGPORT`, `PGUSER` and the like.

- The host is percent-decoded. A Unix socket directory is written as `postgres://app:pw@%2Fcloudsql%2Fproj%3Aregion%3Adb/app`.
- An `@` within the password is accepted. For example, `postgres://app:p@ss@db/app` connects to `db` with the password `p@ss`.
- An IPv6 host is written in brackets (`[::1]`) and used without them.
- Without a user in the URI, the session uses the first non-empty value of `USER` and `USERNAME`, in that order. `PGUSER` is never read.

## Reserved query parameters

The URI may not set what the layer sets. Names match in any case:

```ts
import { makePgLayer } from '@yieldkit/pg';
import { Redacted } from 'effect';

makePgLayer({ url: Redacted.make('postgres://app:pw@db/app?sslmode=require') });
// PgConfigError: The connection URI must not set TLS parameters (sslmode). Use PG_SSL_MODE or the tls option instead.
```

| Group    | Parameters                                                                                                                                                                                             | Use instead                  |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------- |
| TLS      | `ssl`, `sslmode`, `sslcert`, `sslkey`, `sslrootcert`, `sslcrl`, `sslpassword`, `sslnegotiation`, `sslcompression`, `sslsni`, `requiressl`, `gssencmode`, `uselibpqcompat`                              | `PG_SSL_MODE` or `tls`       |
| Pool     | `options`, `application_name`, `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout`, `query_timeout`, `connect_timeout`, `keepalives`, `keepalives_idle`, `target_session_attrs` | The layer options            |
| Target   | `host`, `hostaddr`, `port`, `user`, `password`, `dbname`                                                                                                                                               | The URI's authority and path |
| Protocol | `replication`, `client_encoding`, `binary`                                                                                                                                                             | Not supported                |

The drivers disagree on these: `@effect/sql-pg` falls back to plaintext on `sslmode=prefer`, node-postgres lets `?ssl=no-verify` override a verifying config, and a `host` parameter would connect somewhere other than the logged host.

## Startup

1. Validate every option. A bad one fails with `PgConfigError` before a socket is opened.
2. Log `pg.client.opening` with the resolved host and user, port, database, `applicationName`, `maxConnections`, `slowQueryLog` and `tls`.
3. Open one session and run `SELECT pg_backend_pid()` on it.
4. Log `pg.connection.created` (`backendPid`) and `pg.client.ready`. These prove that one session; the rest of the `minConnections` sessions open in the background.

Step 3 retries a connection error 3 times, with jittered backoff from 250 ms capped at about 2 seconds, logging `pg.client.reconnecting` before each retry. Retried:

- what `@effect/sql-pg` classifies as a connection error: a refused, reset or closed socket, a connect timeout, a failed TLS negotiation, or SQLSTATE class `08`;
- `57P03`, from a server that is starting up, shutting down or in recovery.

Everything else fails at once: an authentication error, `53300` (too many connections), any other SQLSTATE, and a pooler's `unsupported startup parameter` refusal. A failure logs `pg.client.failed` or `pg.connection.failed` with the server's reason and fails the layer with the driver's `SqlError`, both without the URI and password.

Closing the layer logs `pg.connection.disconnected` (the probe's `backendPid`) and `pg.client.closed` as its scope starts to close, before the pool ends its sessions.

## Redacting driver errors

The package masks the URI and password in its log lines, its own errors and the startup `SqlError` a pool layer fails with. `redactDatabaseSecrets(message, rawUrl)` does the same for your own logs:

```ts
import { redactDatabaseSecrets } from '@yieldkit/pg';

const url = 'postgres://app:s3cret@db:5432/app';
redactDatabaseSecrets(`connect ECONNREFUSED ${url} (password s3cret)`, url);
// 'connect ECONNREFUSED [redacted-database-url] (password ***)'
```

Masking matches the exact URI and password, decoded and percent-encoded. A `SqlError` from a query after startup, and the spans `@effect/sql-pg` ends itself, carry the server's text unchanged: PostgreSQL never echoes the password, but the user name is not masked anywhere.

## Limits

These fail with `PgConfigError` before a socket opens. The message starts with `DATABASE_URL` or `The connection URI` and never contains the URI or password.

- an empty or unparseable value, or another protocol;
- no host (`postgres:user:pw@host/db`), no database, or no user anywhere;
- a raw `@` in the path, query or fragment, the sign of an unencoded `/`, `?` or `#` in the credentials. Write `@` there as `%40`;
- a fragment, malformed percent-encoding, a NUL (`%00`) or port `0`;
- a [reserved query parameter](#reserved-query-parameters).
