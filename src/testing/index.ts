import type * as Testcontainers from '@testcontainers/postgresql';
import type { Config } from 'effect';
import type { ClientConfig } from 'pg';

import { describeError } from '@yieldkit/logger';
import { Effect, Redacted } from 'effect';
import { Client } from 'pg';

import { MigrationError, PgContainerError } from '../errors.ts';
import { runMigrations } from '../migrate.ts';
import {
  endMigrationClient,
  isUndefinedTable,
  migrationClientConfig,
} from '../migration-client.ts';
import {
  DEFAULT_APPLICATION_NAME,
  MIGRATIONS_SCHEMA,
  MIGRATIONS_TABLE,
  POSTGRES_TEST_CREDENTIAL,
  POSTGRES_TEST_IMAGE,
  POSTGRES_TEST_TMPFS_SIZE,
} from '../policy.ts';
import { validatePgSchemaName } from '../slow-query.ts';
import { CODE_URI_LABEL, parseConnectionUri, redactDatabaseSecrets } from '../target.ts';
import { pgTlsOptions, resolvePgTlsMode } from '../tls.ts';

/**
 * A started PostgreSQL container. `connectionUri` includes the password and must stay redacted.
 */
export type PostgresContainer = {
  /**
   * Redacted connection URI, including the password. Do not log `Redacted.value` of this field.
   */
  readonly connectionUri: Redacted.Redacted;

  /**
   * Stops and removes the container, or rejects with `PgContainerError` if Docker cannot stop it.
   * The URI is invalid after this resolves.
   */
  readonly stop: () => Promise<void>;
};

/**
 * Options for `startPostgresContainer`. Every field is optional.
 */
export type StartPostgresContainerOptions = {
  /**
   * Image to run. Defaults to `POSTGRES_TEST_IMAGE` (`postgres:19.0-trixie`). It must accept the
   * official image's `POSTGRES_*` variables, as the `pgvector/pgvector` images do.
   */
  readonly image?: string | undefined;

  /**
   * Database to create. Defaults to `test`.
   */
  readonly database?: string | undefined;

  /**
   * User to create. Defaults to `test`.
   */
  readonly username?: string | undefined;

  /**
   * Password of `username`. Defaults to `test`. It is part of `connectionUri`, which stays
   * redacted.
   */
  readonly password?: string | undefined;

  /**
   * Size cap of the tmpfs that holds the data directory, in Docker's syntax (`512m`, `2g`).
   * Defaults to `512m`.
   */
  readonly tmpfsSize?: string | undefined;
};

/**
 * How long Testcontainers may wait for Postgres to accept connections, including the first image
 * pull.
 */
const CONTAINER_START_TIMEOUT_MS = 180_000;

/**
 * Tmpfs mount point; `PGDATA` is set beneath it.
 */
const DATA_MOUNT_PATH = '/var/lib/postgresql';

/**
 * Package `startPostgresContainer` loads on first use. It is an optional peer dependency, so the
 * rest of this module works without it.
 */
const TESTCONTAINERS_PACKAGE = '@testcontainers/postgresql';

/**
 * How long `migrateTo` may take to open its counting session, in milliseconds.
 */
const COUNT_CONNECT_TIMEOUT_MS = 5000;

/**
 * Server-side `statement_timeout` of the counting query, in milliseconds.
 */
const COUNT_STATEMENT_TIMEOUT_MS = 10_000;

/**
 * Characters `startPostgresContainer` accepts in the database, user name and password. The image
 * passes them through a shell and the returned URI does not percent-encode them, so anything else
 * could hang the start or produce a URI that cannot log in.
 */
const CONTAINER_CREDENTIAL_PATTERN = /^[A-Za-z0-9_.-]+$/;

/**
 * Rejects with `PgContainerError` naming the first of `database`, `username` and `password` that
 * uses a character outside `CONTAINER_CREDENTIAL_PATTERN`. The value itself is not included.
 */
const assertContainerCredentials = (options: StartPostgresContainerOptions): void => {
  for (const field of ['database', 'username', 'password'] as const) {
    const value = options[field];
    if (value !== undefined && !CONTAINER_CREDENTIAL_PATTERN.test(value)) {
      throw new PgContainerError({
        message: `startPostgresContainer ${field} must use only letters, digits, '_', '.' and '-'.`,
      });
    }
  }
};

/**
 * Message strings captured by `makeTestLogger`, ignoring non-string log messages.
 */
export const logMessages = (
  records: ReadonlyArray<{ readonly message: unknown }>,
): ReadonlyArray<string> =>
  records.flatMap((record) => (typeof record.message === 'string' ? [record.message] : []));

/**
 * Starts a PostgreSQL container, by default `postgres:19.0-trixie` with database, user and password
 * `test`, setting `PGDATA` beneath a tmpfs mount (512 MB by default) so tests skip disk writes and
 * fsync waits. Loads `@testcontainers/postgresql` (an optional peer dependency) on first use.
 * Rejects with `PgContainerError`, before Docker is used, when `database`, `username` or `password`
 * has a character other than letters, digits, `_`, `.` and `-`; and when that package is not
 * installed or Docker cannot pull or start the image. Call `stop` when the suite finishes; the URI
 * is invalid after that.
 */
export const startPostgresContainer = async (
  options: StartPostgresContainerOptions = {},
): Promise<PostgresContainer> => {
  assertContainerCredentials(options);
  const { PostgreSqlContainer } = await loadTestcontainers();
  try {
    const container = await new PostgreSqlContainer(options.image ?? POSTGRES_TEST_IMAGE)
      .withDatabase(options.database ?? POSTGRES_TEST_CREDENTIAL)
      .withUsername(options.username ?? POSTGRES_TEST_CREDENTIAL)
      .withPassword(options.password ?? POSTGRES_TEST_CREDENTIAL)
      // Older images can mount an anonymous volume at the default data directory.
      .withEnvironment({ PGDATA: `${DATA_MOUNT_PATH}/pgdata` })
      .withTmpFs({ [DATA_MOUNT_PATH]: `rw,size=${options.tmpfsSize ?? POSTGRES_TEST_TMPFS_SIZE}` })
      .withStartupTimeout(CONTAINER_START_TIMEOUT_MS)
      .start();
    return {
      connectionUri: Redacted.make(container.getConnectionUri()),
      stop: async () => {
        try {
          await container.stop();
        } catch (cause) {
          const message =
            cause instanceof Error ? cause.message : 'Failed to stop PostgreSQL container';
          throw new PgContainerError({ message });
        }
      },
    };
  } catch (cause) {
    const message = cause instanceof Error ? cause.message : 'Failed to start PostgreSQL container';
    throw new PgContainerError({ message });
  }
};

/**
 * Imports `@testcontainers/postgresql`, rejecting with `PgContainerError` that names the package
 * when it is not installed.
 */
const loadTestcontainers = async (): Promise<typeof Testcontainers> => {
  try {
    return await import('@testcontainers/postgresql');
  } catch {
    throw new PgContainerError({
      message: `startPostgresContainer needs ${TESTCONTAINERS_PACKAGE}; add it to your dev dependencies.`,
    });
  }
};

/**
 * Options for `migrateTo`.
 */
export type MigrateToOptions = {
  /**
   * Redacted connection URI of the database to migrate, such as `PostgresContainer.connectionUri`.
   */
  readonly databaseUrl: Redacted.Redacted;

  /**
   * Directory of `.sql` migration files. Pass an absolute path.
   */
  readonly migrationsDir: string;

  /**
   * Number of migrations that should be applied afterwards. Zero rolls everything back. Must be a
   * non-negative integer no larger than the number of migration files.
   */
  readonly count: number;

  /**
   * History table, as in `runMigrations`. Defaults to `schema_migrations`.
   */
  readonly migrationsTable?: string | undefined;

  /**
   * Schema of the history table, as in `runMigrations`. Defaults to `public`.
   */
  readonly migrationsSchema?: string | undefined;
};

/**
 * Moves a test database up or down until exactly `count` migrations are applied. Reads how many
 * rows the history table has (zero when the table does not exist), applies `count - applied` files
 * when fewer are applied, rolls back `applied - count` files with `force: true` when more are, and
 * does nothing when equal. Returns the names applied or rolled back, in runner order. Rejects with
 * `MigrationError` for a `count` that is not a non-negative integer, when the directory has fewer
 * than `count` migrations (after applying all of them), or when the database cannot be read or
 * migrated; the URI and password are removed from the message. TLS follows `runMigrations`:
 * `PG_SSL_MODE`, then `disable`. The count is read on its own session before the run takes the
 * migration lock, so do not run it concurrently with other migrations. Test code only, because down
 * migrations drop data.
 */
export const migrateTo = async (options: MigrateToOptions): Promise<ReadonlyArray<string>> => {
  const { databaseUrl, migrationsDir, count } = options;
  if (!Number.isInteger(count) || count < 0) {
    throw new MigrationError({
      message: 'migrateTo count must be a non-negative integer.',
      direction: 'up',
    });
  }

  const history = {
    migrationsTable: options.migrationsTable ?? MIGRATIONS_TABLE,
    migrationsSchema: options.migrationsSchema ?? MIGRATIONS_SCHEMA,
  };
  const applied = await countAppliedMigrations(databaseUrl, history);
  if (applied === count) {
    return [];
  }

  if (applied > count) {
    return Effect.runPromise(
      runMigrations({
        databaseUrl,
        migrationsDir,
        ...history,
        direction: 'down',
        force: true,
        count: applied - count,
      }),
    );
  }

  const names = await Effect.runPromise(
    runMigrations({ databaseUrl, migrationsDir, ...history, count: count - applied }),
  );
  const reached = applied + names.length;
  if (reached !== count) {
    throw new MigrationError({
      message:
        `migrateTo was asked for ${String(count)} migrations but ${migrationsDir} ` +
        `has only ${String(reached)}; all of them are applied now.`,
      direction: 'up',
    });
  }

  return names;
};

/**
 * Names of the migration history table, already validated by `runMigrations`'s rules.
 */
type HistoryTable = {
  /**
   * Table name.
   */
  readonly migrationsTable: string;

  /**
   * Schema name.
   */
  readonly migrationsSchema: string;
};

/**
 * Listener for node-postgres `'error'` events on the count session. A socket reset would otherwise
 * be an unhandled `'error'` event; the query in flight still rejects.
 */
const ignoreSessionError = (): void => undefined;

/**
 * Number of rows in the history table, or zero when the table does not exist yet. Connects with the
 * same session settings and URI checks as `runMigrations`, plus a connect and statement timeout.
 */
const countAppliedMigrations = async (
  databaseUrl: Redacted.Redacted,
  history: HistoryTable,
): Promise<number> => {
  const rawUrl = Redacted.value(databaseUrl);
  const config = await Effect.runPromise(resolveCountClientConfig(databaseUrl, history));
  let client: Client | undefined;
  try {
    client = new Client(config);
    client.on('error', ignoreSessionError);
    await client.connect();
    const result = await client.query<{ applied: number }>(
      `SELECT count(*)::int AS applied FROM "${history.migrationsSchema}"."${history.migrationsTable}"`,
    );
    return result.rows[0]?.applied ?? 0;
  } catch (cause) {
    if (isUndefinedTable(cause)) {
      return 0;
    }

    throw new MigrationError({
      message: redactDatabaseSecrets(describeError(cause), rawUrl),
      direction: 'up',
    });
  } finally {
    if (client !== undefined) {
      await Effect.runPromise(endMigrationClient(client));
    }
  }
};

/**
 * Resolves the node-postgres config `runMigrations` would use for `databaseUrl` (the parsed URI and
 * the `PG_SSL_MODE` TLS mode), with a short connect and statement timeout, after validating the
 * history table names. A URI the connection policy rejects or an invalid name fails with
 * `MigrationError`, and an invalid `PG_SSL_MODE` with `ConfigError`, as they would in
 * `runMigrations`.
 */
const resolveCountClientConfig = (
  databaseUrl: Redacted.Redacted,
  history: HistoryTable,
): Effect.Effect<ClientConfig, MigrationError | Config.ConfigError> =>
  Effect.gen(function* () {
    yield* validatePgSchemaName(history.migrationsTable);
    yield* validatePgSchemaName(history.migrationsSchema);
    const parsed = yield* parseConnectionUri({ url: databaseUrl, label: CODE_URI_LABEL });
    const mode = yield* resolvePgTlsMode(undefined);
    const config = yield* migrationClientConfig(parsed, {
      applicationName: DEFAULT_APPLICATION_NAME,
      ssl: pgTlsOptions(mode, parsed.target.host),
    });
    return {
      ...config,
      connectionTimeoutMillis: COUNT_CONNECT_TIMEOUT_MS,
      statement_timeout: COUNT_STATEMENT_TIMEOUT_MS,
    };
  }).pipe(
    Effect.catchTag('PgConfigError', (error) =>
      Effect.fail(new MigrationError({ message: error.message, direction: 'up' })),
    ),
  );
