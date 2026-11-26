# Contributing

Thanks for helping improve `@yieldkit/pg`.

## Setup

Use Node.js 22.18+, 24.11+ or 26+ and the pnpm version declared in `package.json` (`devEngines`).

```bash
pnpm install
pnpm check
pnpm test
```

`pnpm check` formats, lints and type-checks through `effect-tsgo`, which adds Effect's type-aware rules to `vp check`. The lint preset is [`@yieldkit/oxlint-config`](https://github.com/yieldkit/oxlint-config).

## Tests

- `pnpm test` runs the unit tests (`src/**/*.test.ts`). They need no database and no Docker.
- `pnpm test:integration` runs `src/**/*.integration.test.ts` against PostgreSQL started with Testcontainers, one file at a time. It needs a running Docker daemon and `openssl` on `PATH` (the TLS suite issues a self-signed certificate). The PgBouncer suite also runs a pinned `edoburu/pgbouncer` image and reaches PostgreSQL through `host.docker.internal`. A cold run pulls the images first. It uses `POSTGRES_TEST_IMAGE` (PostgreSQL 19); set `PG_TEST_IMAGE` (for example `postgres:15-trixie`) to run it against another release, as CI does for PostgreSQL 15.

CI runs the unit tests on every Node version and operating system in the matrix, the integration tests on Ubuntu only, since macOS runners have no Docker, and the installed-package test once more on Bun.

## Layout

- `src/index.ts`: the public API. Anything not exported here is internal.
- `src/testing/`: the `@yieldkit/pg/testing` entry point. Only this entry may load `@testcontainers/postgresql`.
- `src/policy.ts`: every default, limit and timeout in one place.
- `src/<module>.ts`: one concern per file, with `<module>.test.ts` and, when it needs a database, `<module>.integration.test.ts` next to it.
- `docs/`: one page per topic, linked from `README.md`.

## Changing what reaches logs and errors

The connection URI and password must never reach this package's log lines, its own errors, or the startup `SqlError` a pool layer fails with (its message or `Cause.pretty`). A `SqlError` from a query after startup, and the spans `@effect/sql-pg` ends itself, carry the server's text unchanged; PostgreSQL never echoes the password, only the user name. Tests check this with `src/secret-scan.fixture.ts`, which collects every string reachable from an exit and its spans. A change to connection handling, startup errors or the migration runner needs a test that fails when the password leaks.

## Before opening a pull request

```bash
pnpm check
pnpm test
pnpm test:integration
pnpm pack --out /tmp/pg.tgz
pnpm test:package /tmp/pg.tgz
```

`test:package` installs the packed tarball into a temporary project without `@testcontainers/postgresql`, type-checks a consumer against both entry points, runs it, and checks that the main entry never loads the Testcontainers package.

## Commits and releases

Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci`, `chore`).

To release, bump `version` in `package.json` on `main`, wait for CI to pass, then publish a GitHub release tagged `v<version>`. The publish workflow checks the tag matches `package.json` and CI passed for that commit, runs the unit and integration tests, then publishes to npm with trusted publishing and provenance.
