# Security policy

## Supported versions

Security fixes go into the latest published version of `@yieldkit/pg`. Upgrade to the newest release before reporting an issue that may already be fixed.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Use [GitHub private vulnerability reporting](https://github.com/yieldkit/pg/security/advisories/new) to share the details with the maintainer.

Include the affected version, the `effect` and `@effect/sql-pg` versions, the PostgreSQL version, reproduction steps and the expected impact. Do not include real connection strings, passwords or data; use made-up values.

## Scope

Most useful are reports where:

- the connection URI or the password reaches one of this package's log lines, one of its own errors, or the startup `SqlError` a pool layer fails with (its message or rendered `Cause`), from the pool startup, `LISTEN`, readiness or the migration runner. A `SqlError` from a query after startup, and the spans `@effect/sql-pg` ends itself, carry the server's text unchanged and are not covered; PostgreSQL never echoes the password, only the user name (see [Redacting driver errors](docs/connection.md#redacting-driver-errors)). The user name is logged as is in the `user` field and is not covered, so keep the password out of the user name;
- a TLS mode connects with weaker protection than [documented](docs/tls.md): `require` or `verify-full` falling back to plaintext, or `verify-full` accepting a certificate that does not match the CA or the host name;
- a URI the [connection policy](docs/connection.md) rejects is accepted, or the pool or migrations connect to a different host, database or user than the one logged;
- a channel name, payload, schema, search path or lock key that passed validation ends up interpreted as SQL.

Out of scope: secrets you write into your own SQL, migration files or log fields, and PostgreSQL server settings such as `log_min_duration_statement` writing statements to the server's own log.
