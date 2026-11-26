import { makeTestLogger } from '@yieldkit/logger/testing';
import { ConfigProvider, Effect, Exit, Redacted } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { runMigrations } from './migrate.ts';
import { capRunnerLine } from './migration-log.ts';
import { MIGRATION_LOG_LINE_MAX_CHARS } from './policy.ts';
import { logMessages, migrateTo } from './testing/index.ts';

/**
 * A port nothing listens on, so a run that gets past validation fails fast.
 */
const REFUSED_URL = 'postgres://app:s3cret@127.0.0.1:1/app';

/**
 * A non-routable address, so a run that does not fail in validation would hang until the connect
 * timeout.
 */
const UNROUTABLE_URL = 'postgres://app:s3cret@10.255.255.1:5432/app';

/**
 * Upper bound for a validation failure, far below the 5 second connect timeout.
 */
const FAILS_FAST_WITHIN_MS = 1000;

describe('runMigrations', () => {
  it('rejects a blank migrations directory before connecting', async () => {
    const databaseUrl = Redacted.make(REFUSED_URL);
    const errors = await Promise.all(
      ['', ' \t'].map((migrationsDir) =>
        Effect.runPromise(Effect.flip(runMigrations({ databaseUrl, migrationsDir }))),
      ),
    );
    for (const error of errors) {
      expect(error).toMatchObject({
        _tag: 'MigrationError',
        message: 'migrationsDir must not be empty.',
      });
    }
  });

  it('refuses down without force before connecting', async () => {
    const startedAt = Date.now();
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make(UNROUTABLE_URL),
          migrationsDir: '/nonexistent',
          direction: 'down',
        }),
      ),
    );
    expect(Date.now() - startedAt).toBeLessThan(FAILS_FAST_WITHIN_MS);
    expect(error).toMatchObject({
      _tag: 'MigrationError',
      message: 'Refusing to run down migrations without force: true; they drop application data.',
      direction: 'down',
    });
  });

  it('refuses down with force false', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make(UNROUTABLE_URL),
          migrationsDir: '/nonexistent',
          direction: 'down',
          force: false,
        }),
      ),
    );
    expect(error.message).toContain('without force: true');
  });

  it('rejects a count that is not a positive integer or Infinity before connecting', async () => {
    const counts = [0, -1, 1.5, Number.NaN, Number.NEGATIVE_INFINITY];
    const startedAt = Date.now();
    const errors = await Promise.all(
      counts.map((count) =>
        Effect.runPromise(
          Effect.flip(
            runMigrations({
              databaseUrl: Redacted.make(UNROUTABLE_URL),
              migrationsDir: '/nonexistent',
              count,
            }),
          ),
        ),
      ),
    );
    expect(Date.now() - startedAt).toBeLessThan(FAILS_FAST_WITHIN_MS);
    expect(errors.map((error) => [error._tag, error.message])).toEqual(
      counts.map(() => [
        'MigrationError',
        'Migration count must be a positive integer or Infinity.',
      ]),
    );
  });

  it('rejects a URI TLS parameter before connecting', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make(`${REFUSED_URL}?sslmode=require`),
          migrationsDir: '/nonexistent',
        }),
      ),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain('(sslmode)');
    expect(error.message).not.toContain('s3cret');
  });

  it('fails with ConfigError for an invalid PG_SSL_MODE', async () => {
    const exit = await Effect.runPromiseExit(
      runMigrations({
        databaseUrl: Redacted.make(REFUSED_URL),
        migrationsDir: '/nonexistent',
      }).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { PG_SSL_MODE: 'prefer' } }),
        ),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toContain('PG_SSL_MODE');
  });

  it('warns about plaintext in production', async () => {
    const harness = makeTestLogger();
    await Effect.runPromiseExit(
      runMigrations({
        databaseUrl: Redacted.make(REFUSED_URL),
        migrationsDir: '/nonexistent',
      }).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { NODE_ENV: 'production' } }),
        ),
        Effect.provide(harness.layer),
      ),
    );
    expect(logMessages(harness.records)).toContain('pg.migration.tls_disabled');
    expect(JSON.stringify(harness.records)).not.toContain('s3cret');
  });

  it('rejects an invalid lock or statement timeout before connecting', async () => {
    const cases = [
      { lockTimeout: 0, name: 'lockTimeout' },
      { lockTimeout: -5, name: 'lockTimeout' },
      { lockTimeout: 'Infinity', name: 'lockTimeout' },
      { lockTimeout: '25 days', name: 'lockTimeout' },
      { statementTimeout: '0 millis', name: 'statementTimeout' },
      { statementTimeout: 'Infinity', name: 'statementTimeout' },
    ] as const;
    const errors = await Promise.all(
      cases.map(({ name: _name, ...timeouts }) =>
        Effect.runPromise(
          Effect.flip(
            runMigrations({
              databaseUrl: Redacted.make(REFUSED_URL),
              migrationsDir: '/nonexistent',
              ...timeouts,
            }),
          ),
        ),
      ),
    );
    expect(errors.map((error) => [error._tag, error.message])).toEqual(
      cases.map(({ name }) => [
        'MigrationError',
        `Migration ${name} must be a positive duration of at most 2147483647 ms.`,
      ]),
    );
  });
});

describe('runMigrations without a URI', () => {
  it('fails with MigrationError naming DATABASE_URL when it is unset, with no fallback', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({ migrationsDir: '/nonexistent' }).pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env: {} })),
        ),
      ),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain('DATABASE_URL is not set');
  });

  it('rejects an invalid history table name before connecting', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl: Redacted.make(UNROUTABLE_URL),
          migrationsDir: '/nonexistent',
          migrationsTable: 'bad-name',
        }),
      ),
    );
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain('bad-name');
  });
});

describe('capRunnerLine', () => {
  it('keeps a capped line within MIGRATION_LOG_LINE_MAX_CHARS, marker included', () => {
    const capped = capRunnerLine(`${'a'.repeat(3000)}${'b'.repeat(2000)}`);
    expect(capped.length).toBeLessThanOrEqual(MIGRATION_LOG_LINE_MAX_CHARS);
    expect(capped).toContain('characters omitted');
    expect(capped.startsWith('a')).toBe(true);
    expect(capped.endsWith('b')).toBe(true);
  });

  it('returns a line at the limit unchanged', () => {
    const line = 'x'.repeat(MIGRATION_LOG_LINE_MAX_CHARS);
    expect(capRunnerLine(line)).toBe(line);
  });
});

describe('migrateTo', () => {
  it('applies the URI policy before counting applied migrations', async () => {
    const startedAt = Date.now();
    const rejection = migrateTo({
      databaseUrl: Redacted.make(`${UNROUTABLE_URL}?sslmode=require`),
      migrationsDir: '/nonexistent',
      count: 1,
    });
    const error = await rejection.then(
      () => undefined,
      (cause: unknown) => cause,
    );
    expect(Date.now() - startedAt).toBeLessThan(FAILS_FAST_WITHIN_MS);
    expect(error).toMatchObject({ _tag: 'MigrationError' });
    expect(String(error)).toContain('sslmode');
  });
});
