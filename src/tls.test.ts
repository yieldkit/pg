import { makeTestLogger } from '@yieldkit/logger/testing';
import { ConfigProvider, Effect, Exit } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import { logMessages } from './testing/index.ts';
import {
  assertPgTlsMode,
  parsePgTlsMode,
  pgTlsOptions,
  resolvePgTlsMode,
  warnIfTlsDisabledInProduction,
} from './tls.ts';

/**
 * Runs `effect` with only `env` visible to `Config`.
 */
const withEnv = <A, E>(
  effect: Effect.Effect<A, E>,
  env: Readonly<Record<string, string>>,
): Effect.Effect<A, E> =>
  effect.pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
  );

describe('parsePgTlsMode', () => {
  it('accepts the three modes case-insensitively and rejects libpq-only modes', () => {
    expect(parsePgTlsMode('disable')).toBe('disable');
    expect(parsePgTlsMode(' REQUIRE ')).toBe('require');
    expect(parsePgTlsMode('Verify-Full')).toBe('verify-full');
    expect(parsePgTlsMode('prefer')).toBeUndefined();
    expect(parsePgTlsMode('verify-ca')).toBeUndefined();
    expect(parsePgTlsMode('')).toBeUndefined();
  });
});

describe('resolvePgTlsMode', () => {
  it('defaults to disable when PG_SSL_MODE is unset', async () => {
    const mode = await Effect.runPromise(withEnv(resolvePgTlsMode(undefined), {}));
    expect(mode).toBe('disable');
  });

  it('reads PG_SSL_MODE case-insensitively', async () => {
    const mode = await Effect.runPromise(
      withEnv(resolvePgTlsMode(undefined), { PG_SSL_MODE: 'Verify-Full' }),
    );
    expect(mode).toBe('verify-full');
  });

  it('fails with ConfigError for an invalid PG_SSL_MODE', async () => {
    const exit = await Effect.runPromiseExit(
      withEnv(resolvePgTlsMode(undefined), { PG_SSL_MODE: 'prefer' }),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toContain('PG_SSL_MODE');
  });

  it('prefers the explicit option over PG_SSL_MODE', async () => {
    const mode = await Effect.runPromise(
      withEnv(resolvePgTlsMode('require'), { PG_SSL_MODE: 'not-a-mode' }),
    );
    expect(mode).toBe('require');
  });
});

describe('assertPgTlsMode', () => {
  it('rejects a value outside PgTlsMode with PgConfigError', async () => {
    const error = await Effect.runPromise(Effect.flip(assertPgTlsMode('prefer')));
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('verify-full');
  });
});

describe('pgTlsOptions', () => {
  it('maps disable to literally false so node-postgres ignores PGSSLMODE', () => {
    expect(pgTlsOptions('disable', 'db.internal')).toBe(false);
  });

  it('maps require to TLS without certificate verification', () => {
    expect(pgTlsOptions('require', 'db.internal')).toEqual({ rejectUnauthorized: false });
  });

  it('maps verify-full to a verifying session with the host as servername', () => {
    expect(pgTlsOptions('verify-full', 'db.internal')).toEqual({
      rejectUnauthorized: true,
      servername: 'db.internal',
    });
  });

  it('omits servername for IPv4 and bracketed IPv6 hosts', () => {
    expect(pgTlsOptions('verify-full', '10.0.0.5')).toEqual({ rejectUnauthorized: true });
    expect(pgTlsOptions('verify-full', '[::1]')).toEqual({ rejectUnauthorized: true });
  });
});

/**
 * Runs the production warning for `mode` under `env` and returns the captured log records.
 */
const runWarning = async (mode: 'disable' | 'require', env: Readonly<Record<string, string>>) => {
  const harness = makeTestLogger();
  await Effect.runPromise(
    withEnv(warnIfTlsDisabledInProduction('pg.client.tls_disabled', mode, {}), env).pipe(
      Effect.provide(harness.layer),
    ),
  );
  return harness.records;
};

describe('warnIfTlsDisabledInProduction', () => {
  it('warns when TLS is disabled and NODE_ENV is production or prod', async () => {
    const production = await runWarning('disable', { NODE_ENV: 'production' });
    const prod = await runWarning('disable', { NODE_ENV: 'PROD' });
    expect(logMessages(production)).toEqual(['pg.client.tls_disabled']);
    expect(logMessages(prod)).toEqual(['pg.client.tls_disabled']);
    expect(production[0]?.level).toBe('WARN');
    expect(JSON.stringify(production)).toContain('PG_SSL_MODE=verify-full');
  });

  it('stays quiet outside production or when TLS is enabled', async () => {
    expect(await runWarning('disable', {})).toEqual([]);
    expect(await runWarning('disable', { NODE_ENV: 'development' })).toEqual([]);
    expect(await runWarning('require', { NODE_ENV: 'production' })).toEqual([]);
  });

  it('stays quiet when the config provider cannot read NODE_ENV', async () => {
    const harness = makeTestLogger();
    const unreadable = ConfigProvider.make(() =>
      Effect.fail(new ConfigProvider.SourceError({ message: 'source unavailable' })),
    );
    await Effect.runPromise(
      warnIfTlsDisabledInProduction('pg.client.tls_disabled', 'disable', {}).pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, unreadable),
        Effect.provide(harness.layer),
      ),
    );
    expect(harness.records).toEqual([]);
  });
});
