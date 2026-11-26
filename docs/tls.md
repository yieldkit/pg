# TLS

TLS is off by default, so a local database or a test container works without certificates. Once on, it never falls back to plaintext.

```ts
import { makePgLayer, makePgLive } from '@yieldkit/pg';
import { Redacted } from 'effect';

const PgLive = makePgLive({ tls: 'verify-full' }); // or PG_SSL_MODE=verify-full

const PgFromCode = makePgLayer({
  url: Redacted.make('postgres://app:s3cret@db.example.com/app'),
  tls: 'verify-full',
});
```

| Mode          | Encrypted | Certificate checked                                     |
| ------------- | --------- | ------------------------------------------------------- |
| `disable`     | no        | no                                                      |
| `require`     | yes       | no                                                      |
| `verify-full` | yes       | the chain against Node's CAs (or `tlsCa`), and the host |

**Production should use `verify-full`.** With `disable` or `require`, whoever answers the connection can ask for the password in clear text.

## Choosing the mode

| Caller          | Order                                     |
| --------------- | ----------------------------------------- |
| `makePgLayer`   | `tls`, then `disable`                     |
| `makePgLive`    | `tls`, then `PG_SSL_MODE`, then `disable` |
| `runMigrations` | `tls`, then `PG_SSL_MODE`, then `disable` |
| `migrateTo`     | `PG_SSL_MODE`, then `disable`             |

`PG_SSL_MODE` is trimmed and matched in any case. `resolvePgTlsMode(tls)` returns the option when given, otherwise the variable ([example](connection.md)).

The package never turns TLS on by itself. With `disable` while `NODE_ENV` is `production` or `prod`, the pool warns with `pg.client.tls_disabled` and a migration with `pg.migration.tls_disabled`, then both carry on.

## Private CAs

For a server certificate from a private CA, as with most managed databases, pass the CA's PEM bundle as `tlsCa` instead of dropping to `require`, which accepts any certificate.

```ts
import { makePgLive } from '@yieldkit/pg';
import { readFileSync } from 'node:fs';

const PgLive = makePgLive({
  tls: 'verify-full',
  tlsCa: readFileSync('/etc/ssl/db-ca.pem', 'utf8'), // replaces Node's trusted CAs
});
```

`makePgLayer`, `makePgLive` and `runMigrations` accept `tlsCa`; `migrateTo` does not.

## Limits

- Any other `PG_SSL_MODE`, including libpq's `prefer`, `allow` and `verify-ca`, fails with `ConfigError`.
- With `require` or `verify-full`, a server without TLS fails the pool with `SqlError` (after the connect retries) and a migration with `MigrationError`.
- `servername` is set only when the URI's host is a DNS name. For an IP address both drivers check the certificate against the connect host.
- `tlsCa` works only with `verify-full`. Any other mode, or an empty string, fails with `PgConfigError` (`MigrationError` from `runMigrations`) before connecting.
- The PEM text is not parsed up front. A bundle the TLS stack can't use fails the connection.
