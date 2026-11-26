import { makeTestLogger } from '@yieldkit/logger/testing';
import { ConfigProvider, Effect, Exit, Layer, Redacted } from 'effect';
import { createServer } from 'node:net';
import { describe, expect, it, vi } from 'vite-plus/test';

import type { PgLayerOptions } from './client.ts';

import { makePgLayer, makePgLive } from './client.ts';
import { reachableStrings, renderExit, withSpanRecorder } from './secret-scan.fixture.ts';
import { describeDatabaseTarget } from './target.ts';

const messages = (records: ReadonlyArray<{ readonly message: unknown }>): ReadonlyArray<string> =>
  records.flatMap((record) => (typeof record.message === 'string' ? [record.message] : []));

/**
 * A URI on a port nothing listens on.
 */
const REFUSED_URL = 'postgres://app:s3cret@127.0.0.1:1/app';

/**
 * Password used by the leak tests. It differs from the user (`app`) and database (`appdb`), so
 * finding it anywhere means the password itself leaked.
 */
const SENTINEL_PASSWORD = 'Zq9-sentinel-pw';

/**
 * A local TCP server posing as PostgreSQL, and how to stop it.
 */
interface EchoingServer {
  /**
   * Port the server listens on, on 127.0.0.1.
   */
  readonly port: number;

  /**
   * Stops accepting connections and resolves once the server is closed.
   */
  readonly close: () => Promise<void>;
}

/**
 * Encodes a PostgreSQL `ErrorResponse` (FATAL, SQLSTATE 28P01) whose message is `message`. SQLSTATE
 * 28P01 is an authentication failure, which the client does not retry.
 */
const errorResponse = (message: string): Buffer => {
  const fields = Buffer.from(`SFATAL\0VFATAL\0C28P01\0M${message}\0\0`);
  const header = Buffer.alloc(5);
  header.write('E', 0);
  header.writeInt32BE(fields.length + 4, 1);
  return Buffer.concat([header, fields]);
};

/**
 * Starts a server that answers every startup packet with an authentication `ErrorResponse` whose
 * message is `messageFor(port)`, so a test controls exactly what the driver reports.
 */
const startEchoingServer = async (messageFor: (port: number) => string): Promise<EchoingServer> => {
  let port = 0;
  const server = createServer((socket) => {
    socket.once('data', () => {
      socket.end(errorResponse(messageFor(port)));
    });
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  port = typeof address === 'object' && address !== null ? address.port : 0;
  return {
    port,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => {
          resolve();
        });
      }),
  };
};

/**
 * Builds `makePgLayer(options)` with a test logger and returns the exit and the captured records.
 */
const buildLayer = async (options: PgLayerOptions) => {
  const harness = makeTestLogger();
  const exit = await Effect.runPromiseExit(
    Effect.scoped(Layer.build(makePgLayer(options))).pipe(Effect.provide(harness.layer)),
  );
  return { exit, records: harness.records };
};

describe('makePgLayer', () => {
  it('uses the environment user for a URI without credentials', async () => {
    const options = {
      url: Redacted.make('postgres://localhost/app'),
      slowQueryLog: { thresholdMs: -1 },
    };
    vi.stubEnv('USER', undefined);
    vi.stubEnv('USERNAME', undefined);
    try {
      const withoutUser = await buildLayer(options);
      expect(renderExit(withoutUser.exit)).toContain('has no user');
      vi.stubEnv('USER', 'service');
      const withUser = await buildLayer(options);
      expect(renderExit(withUser.exit)).toContain('thresholdMs');
    } finally {
      vi.unstubAllEnvs();
    }
  });

  it('rejects a non-postgres URI before opening a socket', async () => {
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: Redacted.make('mysql://app:s3cret@127.0.0.1/app'),
            applicationName: 'app-test',
          }),
        ),
      ).pipe(Effect.provide(harness.layer)),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(messages(harness.records)).not.toContain('pg.connection.created');
    expect(JSON.stringify(harness.records)).not.toContain('s3cret');
  });

  it('rejects a keepAlive under one second with PgConfigError before opening a socket', async () => {
    const { exit, records } = await buildLayer({
      url: Redacted.make(REFUSED_URL),
      keepAlive: '500 millis',
    });
    expect(JSON.stringify(exit)).toContain('keepAlive must be from 1');
    expect(messages(records)).not.toContain('pg.client.opening');
  });

  it('never logs a password whose unencoded slash would make it the database name', async () => {
    const results = await Promise.all(
      [
        'postgres://app:/Sekr3tPW@127.0.0.1:1/appdb',
        'postgres://app:8/Sekr3tPW@127.0.0.1:1/appdb',
        'postgres://us/er:Sekr3tPW@127.0.0.1:1/appdb',
      ].map((url) => buildLayer({ url: Redacted.make(url) })),
    );
    for (const { exit, records } of results) {
      expect(Exit.isFailure(exit)).toBe(true);
      expect(JSON.stringify(records)).not.toContain('Sekr3tPW');
      expect(JSON.stringify(exit)).not.toContain('Sekr3tPW');
    }
  });

  it('logs a refused connection without the password anywhere in the records or the exit', async () => {
    const url = `postgres://app:${SENTINEL_PASSWORD}@127.0.0.1:1/appdb`;
    const harness = makeTestLogger();
    const { exit, spans } = await Effect.runPromise(
      withSpanRecorder(
        Effect.scoped(
          Layer.build(makePgLayer({ url: Redacted.make(url), applicationName: 'app-test' })),
        ).pipe(Effect.provide(harness.layer)),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    const logged = messages(harness.records);
    expect(logged).toContain('pg.client.opening');
    expect(logged).toContain('pg.client.reconnecting');
    expect(logged).toContain('pg.connection.failed');
    expect(JSON.stringify(harness.records)).not.toContain(SENTINEL_PASSWORD);
    expect(renderExit(exit)).not.toContain(SENTINEL_PASSWORD);
    expect(spans.map((span) => span.renderedAttributes).join('\n')).not.toContain(
      SENTINEL_PASSWORD,
    );
    const target = await Effect.runPromise(describeDatabaseTarget(Redacted.make(url)));
    expect(target.port).toBe('1');
  });

  it('removes the URI and password a server echoes in its startup error from logs and the exit', async () => {
    const server = await startEchoingServer((port) => {
      const url = `postgres://app:${SENTINEL_PASSWORD}@127.0.0.1:${String(port)}/appdb`;
      return `login refused for ${url}, password ${SENTINEL_PASSWORD}`;
    });
    try {
      const url = `postgres://app:${SENTINEL_PASSWORD}@127.0.0.1:${String(server.port)}/appdb`;
      const harness = makeTestLogger();
      const { exit, spans } = await Effect.runPromise(
        withSpanRecorder(
          Effect.scoped(Layer.build(makePgLayer({ url: Redacted.make(url) }))).pipe(
            Effect.provide(harness.layer),
          ),
        ),
      );
      expect(Exit.isFailure(exit)).toBe(true);
      const failure = harness.records.find((record) => record.message === 'pg.connection.failed');
      expect(failure?.annotations.errorMessage).toBe(
        'PgConnection: Failed to connect: login refused for [redacted-database-url], password ***',
      );
      expect(JSON.stringify(harness.records)).not.toContain(SENTINEL_PASSWORD);
      expect(reachableStrings(harness.records).join('\n')).not.toContain(SENTINEL_PASSWORD);
      const rendered = renderExit(exit);
      expect(rendered).toContain('login refused for [redacted-database-url], password ***');
      expect(rendered).not.toContain(SENTINEL_PASSWORD);
      const probe = spans.find((span) => span.attributeKeys.includes('db.query.text'));
      expect(probe).toBeDefined();
      expect(spans.map((span) => span.renderedAttributes).join('\n')).not.toContain(
        SENTINEL_PASSWORD,
      );
    } finally {
      await server.close();
    }
  });

  it('warns when TLS is disabled in production and still tries to connect', async () => {
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: Redacted.make('postgres://app:s3cret@127.0.0.1:1/app'),
            applicationName: 'app-test',
          }),
        ),
      ).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { NODE_ENV: 'production' } }),
        ),
        Effect.provide(harness.layer),
      ),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    const logged = messages(harness.records);
    expect(logged).toContain('pg.client.tls_disabled');
    expect(logged).toContain('pg.connection.failed');
    expect(JSON.stringify(harness.records)).not.toContain('s3cret');
  });

  it('rejects a URI sslmode before opening a socket', async () => {
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(
        Layer.build(
          makePgLayer({
            url: Redacted.make('postgres://app:s3cret@127.0.0.1:1/app?sslmode=disable'),
            applicationName: 'app-test',
            tls: 'verify-full',
          }),
        ),
      ).pipe(Effect.provide(harness.layer)),
    );
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toContain('PgConfigError');
    expect(messages(harness.records)).not.toContain('pg.client.opening');
  });

  it('rejects a URI without // and never logs its password', async () => {
    const { exit, records } = await buildLayer({
      url: Redacted.make('postgres:app:SENTINEL-PW@127.0.0.1:1/app'),
    });
    expect(String(exit)).toContain('PgConfigError');
    expect(String(exit)).not.toContain('SENTINEL-PW');
    expect(messages(records)).not.toContain('pg.client.opening');
    expect(JSON.stringify(records)).not.toContain('SENTINEL-PW');
  });

  it('rejects malformed percent-encoding with PgConfigError instead of a defect', async () => {
    const { exit } = await buildLayer({ url: Redacted.make('postgres://a%zz:pw@127.0.0.1:1/app') });
    expect(String(exit)).toContain('PgConfigError');
    expect(String(exit)).not.toContain('URIError');
  });

  it('does not mention DATABASE_URL for a URI passed in code', async () => {
    const { exit } = await buildLayer({ url: Redacted.make(`${REFUSED_URL}?sslmode=disable`) });
    expect(String(exit)).toContain('The connection URI must not set TLS parameters');
    expect(String(exit)).not.toContain('DATABASE_URL');
  });

  it('rejects invalid pool options before opening a socket', async () => {
    const cases: ReadonlyArray<Partial<PgLayerOptions>> = [
      { tls: 'require', tlsCa: '-----BEGIN CERTIFICATE-----' },
      { tls: 'verify-full', tlsCa: ' ' },
      { minConnections: 3, maxConnections: 2 },
      { acquireTimeout: 'Infinity' },
      { connectTimeout: 0 },
      { statementTimeout: 0 },
    ];
    const results = await Promise.all(
      cases.map((options) => buildLayer({ url: Redacted.make(REFUSED_URL), ...options })),
    );
    for (const { exit, records } of results) {
      expect(String(exit)).toContain('PgConfigError');
      expect(messages(records)).not.toContain('pg.client.opening');
    }
  });

  it('logs slowQueryLog as server-default when no threshold is sent', async () => {
    const { records } = await buildLayer({ url: Redacted.make(REFUSED_URL) });
    const opening = records.find((record) => record.message === 'pg.client.opening');
    expect(opening?.annotations.slowQueryLog).toBe('server-default');
  });
});

describe('makePgLive', () => {
  it('fails with PgConfigError naming DATABASE_URL when it is unset, without connecting', async () => {
    const harness = makeTestLogger();
    const exit = await Effect.runPromiseExit(
      Effect.scoped(Layer.build(makePgLive({ applicationName: 'app-test' }))).pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env: {} })),
        Effect.provide(harness.layer),
      ),
    );
    expect(String(exit)).toContain('PgConfigError');
    expect(String(exit)).toContain('DATABASE_URL is not set');
    expect(messages(harness.records)).not.toContain('pg.client.opening');
  });

  it('names DATABASE_URL in URI errors when the URI came from it', async () => {
    const exit = await Effect.runPromiseExit(
      Effect.scoped(Layer.build(makePgLive())).pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromEnv({ env: { DATABASE_URL: `${REFUSED_URL}?sslmode=disable` } }),
        ),
        Effect.provide(makeTestLogger().layer),
      ),
    );
    expect(String(exit)).toContain('DATABASE_URL must not set TLS parameters');
  });

  for (const threshold of ['-5', '3000000000']) {
    it(`fails with ConfigError naming PG_SLOW_QUERY_THRESHOLD_MS=${threshold}`, async () => {
      const harness = makeTestLogger();
      const exit = await Effect.runPromiseExit(
        Effect.scoped(Layer.build(makePgLive())).pipe(
          Effect.provideService(
            ConfigProvider.ConfigProvider,
            ConfigProvider.fromEnv({
              env: {
                DATABASE_URL: REFUSED_URL,
                PG_SLOW_QUERY_LOG: 'true',
                PG_SLOW_QUERY_THRESHOLD_MS: threshold,
              },
            }),
          ),
          Effect.provide(harness.layer),
        ),
      );
      expect(String(exit)).toContain('ConfigError');
      expect(String(exit)).toContain('PG_SLOW_QUERY_THRESHOLD_MS');
      expect(String(exit)).not.toContain('PgConfigError');
      expect(messages(harness.records)).not.toContain('pg.client.opening');
    });
  }
});
