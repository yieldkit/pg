/**
 * Maximum pooled sessions opened by one process.
 */
export const DEFAULT_MAX_CONNECTIONS = 10;

/**
 * `application_name` the pool and the migration session report when `applicationName` is omitted.
 * `@effect/sql-pg` always sends one and falls back to this name itself, so the migration session
 * uses it too instead of reading `PGAPPNAME`.
 */
export const DEFAULT_APPLICATION_NAME = '@effect/sql-pg';

/**
 * Sessions opened during pool construction so a refused connection fails at startup instead of on
 * the first query. Effect's own default is `0` (lazy).
 */
export const DEFAULT_MIN_CONNECTIONS = 1;

/**
 * How long a new TCP session may take to authenticate before the pool reports a connection failure.
 */
export const CONNECT_TIMEOUT = '5 seconds';

/**
 * How long a statement waits for a free pooled connection before failing.
 */
export const ACQUIRE_TIMEOUT = '5 seconds';

/**
 * How long an unused pooled connection stays open.
 */
export const IDLE_TIMEOUT = '30 seconds';

/**
 * Delay before TCP keepalive probes start on an idle session.
 */
export const KEEP_ALIVE = '10 seconds';

/**
 * Extra attempts after a refused connect or a server that is not ready yet (`57P03`).
 * Authentication failures are not retried.
 */
export const CONNECT_RETRY_TIMES = 3;

/**
 * Initial delay before the first connect retry.
 */
export const CONNECT_RETRY_BASE = '250 millis';

/**
 * Upper bound of the connect retry delay.
 */
export const CONNECT_RETRY_CAP = '2 seconds';

/**
 * Default server-side `statement_timeout` applied to every pooled session.
 */
export const STATEMENT_TIMEOUT = '15 seconds';

/**
 * Default server-side `lock_timeout` applied to every pooled session.
 */
export const LOCK_TIMEOUT = '5 seconds';

/**
 * Default server-side `idle_in_transaction_session_timeout` applied to every pooled session.
 */
export const IDLE_IN_TRANSACTION_TIMEOUT = '30 seconds';

/**
 * Default history table `node-pg-migrate` reads and writes. Columns are `id`, `name`, and `run_on`.
 * Override it with `RunMigrationsOptions.migrationsTable`.
 */
export const MIGRATIONS_TABLE = 'schema_migrations';

/**
 * Default schema of the migration history table. Override it with
 * `RunMigrationsOptions.migrationsSchema`.
 */
export const MIGRATIONS_SCHEMA = 'public';

/**
 * Default session `lock_timeout` for migration statements, set only after the migration advisory
 * lock is held. A migration that waits longer than this for a table lock (for example behind a
 * long-running transaction holding `widget`) fails instead of queueing every later query on that
 * table behind its `ACCESS EXCLUSIVE` request.
 */
export const MIGRATION_LOCK_TIMEOUT = '10 seconds';

/**
 * Default session `statement_timeout` for migration statements, set only after the migration
 * advisory lock is held. Bounds each statement, so a runaway backfill or index build fails instead
 * of holding the migration lock indefinitely.
 */
export const MIGRATION_STATEMENT_TIMEOUT = '10 minutes';

/**
 * Most lines of `node-pg-migrate` output `runMigrations` keeps per run. Later lines are counted in
 * `droppedLines`, not stored.
 */
export const MIGRATION_LOG_BUFFER_LIMIT = 500;

/**
 * Most characters of one `node-pg-migrate` output line `runMigrations` keeps. A longer line, such
 * as a failed statement that is a whole migration file, keeps its first and last half and loses the
 * middle, so the error message at its end survives.
 */
export const MIGRATION_LOG_LINE_MAX_CHARS = 4000;

/**
 * Session `idle_in_transaction_session_timeout` for migrations. Ends a session that left a
 * migration transaction open without running anything, which releases its locks.
 */
export const MIGRATION_IDLE_IN_TRANSACTION_TIMEOUT = '60s';

/**
 * How long the migration client may take to open and authenticate its session, in milliseconds.
 */
export const MIGRATION_CONNECT_TIMEOUT_MS = 5000;

/**
 * How long ending the migration session may take before it is abandoned.
 */
export const MIGRATION_CLIENT_END_TIMEOUT = '5 seconds';

/**
 * PostgreSQL rejects a `NOTIFY` payload of 8000 bytes or more, so payloads must be shorter.
 */
export const MAX_NOTIFY_PAYLOAD_BYTES = 8000;

/**
 * PostgreSQL identifier grammar for a `LISTEN` / `NOTIFY` channel, capped at 63 characters.
 */
export const NOTIFY_CHANNEL_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

/**
 * Initial delay of the `LISTEN` reconnect backoff, before jitter.
 */
export const LISTEN_RETRY_BASE = '500 millis';

/**
 * Upper bound of the `LISTEN` reconnect backoff, before jitter.
 */
export const LISTEN_RETRY_CAP = '30 seconds';

/**
 * How long a confirmed `LISTEN` session must stay up before its end resets the reconnect backoff to
 * `LISTEN_RETRY_BASE`. A session that confirms and then drops sooner counts as a failed attempt, so
 * a server that accepts `LISTEN` and immediately drops the session is retried with growing delays.
 */
export const LISTEN_STABLE_AFTER = '5 seconds';

/**
 * Retries after the first failed readiness probe, so `checkReadiness` makes up to three attempts.
 */
export const READINESS_RETRIES = 2;

/**
 * Delay before the first readiness retry. Each further retry doubles it, with jitter.
 */
export const READINESS_BASE_DELAY = '100 millis';

/**
 * How long one readiness `SELECT 1` may take, including waiting for a pooled connection.
 */
export const READINESS_ATTEMPT_TIMEOUT = '2 seconds';

/**
 * Default image `startPostgresContainer` runs: the official PostgreSQL 19 image on Debian, which
 * also ships `openssl`. Pass `image` to use another one, such as one with extensions installed.
 */
export const POSTGRES_TEST_IMAGE = 'postgres:19.0-trixie';

/**
 * Default database, user and password of `startPostgresContainer`.
 */
export const POSTGRES_TEST_CREDENTIAL = 'test';

/**
 * Default size cap of the tmpfs that holds the test container's data directory.
 */
export const POSTGRES_TEST_TMPFS_SIZE = '512m';

/**
 * Default number of notifications `listenWithRetry` holds for a slow handler. When the buffer is
 * full, newer notifications are dropped and counted.
 */
export const LISTEN_BUFFER_CAPACITY = 1024;

/**
 * Default `log_min_duration_statement` in milliseconds when `PG_SLOW_QUERY_LOG` is true.
 */
export const DEFAULT_SLOW_QUERY_THRESHOLD_MS = 500;
