import { Effect, Redacted, Schema } from 'effect';
import { Client } from 'pg';
import { describe, expect, it } from 'vite-plus/test';

import type { MigrationSessionSettings } from './migration-client.ts';

import { migrationClientConfig } from './migration-client.ts';
import { parseConnectionUri } from './target.ts';

/**
 * Parses `url` as a URI passed in code, with `USER` set to `os-user`.
 */
const parse = (url: string) =>
  Effect.runPromise(
    parseConnectionUri(
      { url: Redacted.make(url), label: 'The connection URI' },
      { USER: 'os-user' },
    ),
  );

/**
 * Libpq variables node-postgres reads for any setting left empty, each pointing somewhere the pool
 * never goes.
 */
const POLLUTED_ENV = {
  PGHOST: 'env-host',
  PGPORT: '6432',
  PGUSER: 'env_user',
  PGDATABASE: 'env_db',
  PGPASSWORD: 'env-password',
  PGOPTIONS: '-c search_path=other',
  PGAPPNAME: 'env-app',
  PGCLIENT_ENCODING: 'LATIN1',
  PGBINARY: 'true',
  PGSSLMODE: 'require',
  PGCONNECT_TIMEOUT: '99',
} satisfies Record<string, string>;

/**
 * What a node-postgres `Client` resolved from its config and the environment when it was built: its
 * `binary` flag and its untyped `connectionParameters`.
 */
const ResolvedClient = Schema.Struct({
  binary: Schema.Boolean,
  connectionParameters: Schema.Struct({
    host: Schema.String,
    port: Schema.Finite,
    user: Schema.String,
    database: Schema.String,
    options: Schema.String,
    application_name: Schema.String,
    client_encoding: Schema.String,
    ssl: Schema.Boolean,
    connect_timeout: Schema.Finite,
  }),
});

/**
 * Runs `body` with `vars` set in `process.env`, which node-postgres reads directly, and restores
 * the previous values afterwards.
 */
const withProcessEnv = <A>(vars: Record<string, string>, body: () => A): A => {
  const saved: ReadonlyArray<readonly [string, string | undefined]> = Object.keys(vars).map(
    (key) => [key, process.env[key]],
  );
  Object.assign(process.env, vars);
  try {
    return body();
  } finally {
    for (const [key, value] of saved) {
      if (value === undefined) {
        delete process.env[key];
      } else {
        process.env[key] = value;
      }
    }
  }
};

/**
 * Builds the migration config for `url` with no TLS and an empty environment unless given.
 */
const configFor = async (url: string, env: Record<string, string> = {}) =>
  Effect.runPromise(
    migrationClientConfig(await parse(url), { applicationName: 'app-migrate', ssl: false }, env),
  );

describe('migrationClientConfig', () => {
  it('builds explicit fields from the URI instead of passing the connection string', async () => {
    const config = await configFor('postgres://my%20user:p%40ss@db.internal/my%2Ddb');
    expect(config.connectionString).toBeUndefined();
    expect(config).toMatchObject({
      host: 'db.internal',
      port: 5432,
      user: 'my user',
      password: 'p@ss',
      database: 'my-db',
      application_name: 'app-migrate',
      ssl: false,
    });
  });

  it('turns on TCP keepalive so a silently dropped session fails instead of hanging', async () => {
    const config = await configFor('postgres://app:pw@db/app');
    expect(config.keepAlive).toBe(true);
    expect(config.keepAliveInitialDelayMillis).toBe(10_000);
    expect(config.connectionTimeoutMillis).toBe(5000);
  });

  it('sends non-empty startup options so node-postgres ignores PGOPTIONS', async () => {
    const config = await configFor('postgres://app:pw@db/app');
    expect(config.options?.trim()).toBe('');
    expect(config.options?.length).toBeGreaterThan(0);
  });

  it('uses a password callback when the URI has none, so PGPASSWORD is not read', async () => {
    const config = await configFor('postgres://app@db/app');
    expect(typeof config.password).toBe('function');
  });

  it('strips the brackets around an IPv6 host', async () => {
    const config = await configFor('postgres://app:pw@[::1]:6543/app');
    expect(config.host).toBe('::1');
    expect(config.port).toBe(6543);
  });

  it('fails when PGREPLICATION is set, which would open a replication session', async () => {
    const parsed = await parse('postgres://app:pw@db/app');
    const error = await Effect.runPromise(
      Effect.flip(
        migrationClientConfig(
          parsed,
          { applicationName: 'app-migrate', ssl: false },
          { PGREPLICATION: 'database' },
        ),
      ),
    );
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('PGREPLICATION');
  });

  it('connects as the pool would when every libpq variable points elsewhere', async () => {
    const config = await configFor('postgres://db.internal:5433/app');
    const client = withProcessEnv(POLLUTED_ENV, () => new Client(config));
    const resolved = Schema.decodeUnknownSync(ResolvedClient)(client);
    expect(resolved.binary).toBe(false);
    expect(resolved.connectionParameters).toEqual({
      host: 'db.internal',
      port: 5433,
      user: 'os-user',
      database: 'app',
      application_name: 'app-migrate',
      options: ' ',
      client_encoding: 'utf8',
      ssl: false,
      connect_timeout: 5,
    });
  });

  it('decodes a Unix socket directory host the way the pool does', async () => {
    const config = await configFor('postgres://app:pw@%2Fcloudsql%2Fproj%3Aregion%3Adb/app');
    expect(config.host).toBe('/cloudsql/proj:region:db');
  });

  it('fails when PGSSLNEGOTIATION asks for a handshake the pool does not use', async () => {
    const parsed = await parse('postgres://app:pw@db/app');
    const settings: MigrationSessionSettings = { applicationName: 'app-migrate', ssl: false };
    const error = await Effect.runPromise(
      Effect.flip(migrationClientConfig(parsed, settings, { PGSSLNEGOTIATION: 'direct' })),
    );
    expect(error.message).toContain('PGSSLNEGOTIATION');
    const standard = await Effect.runPromise(
      migrationClientConfig(parsed, settings, { PGSSLNEGOTIATION: 'postgres' }),
    );
    expect(standard.host).toBe('db');
  });

  it('fails when NODE_PG_FORCE_NATIVE loads the client that ignores the TLS settings', async () => {
    const parsed = await parse('postgres://app:pw@db/app');
    const settings: MigrationSessionSettings = { applicationName: 'app-migrate', ssl: false };
    const error = await Effect.runPromise(
      Effect.flip(migrationClientConfig(parsed, settings, { NODE_PG_FORCE_NATIVE: '1' })),
    );
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('NODE_PG_FORCE_NATIVE');
    const unset = await Effect.runPromise(
      migrationClientConfig(parsed, settings, { NODE_PG_FORCE_NATIVE: '' }),
    );
    expect(unset.host).toBe('db');
  });
});
