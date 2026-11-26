import type { StartedPostgreSqlContainer } from '@testcontainers/postgresql';

import { PostgreSqlContainer } from '@testcontainers/postgresql';
import { makeTestLogger } from '@yieldkit/logger/testing';
import { Effect, Exit, Layer, Redacted } from 'effect';
import { SqlClient } from 'effect/sql';
import { execFileSync } from 'node:child_process';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Client } from 'pg';
import { afterAll, beforeAll, describe, expect, it } from 'vite-plus/test';

import type { PgTlsMode } from './tls.ts';

import { makePgLayer } from './client.ts';
import { INTEGRATION_IMAGE } from './integration.fixture.ts';
import { runMigrations } from './migrate.ts';
import { omitUndefined } from './omit-undefined.ts';

/**
 * Host name the self-signed certificate is issued for, and the host the URI uses when the case
 * expects verification to pass.
 */
const CERTIFICATE_HOST = 'localhost';

/**
 * An address of the same server that is not in the certificate, for the host-name mismatch cases.
 */
const MISMATCHED_HOST = '127.0.0.1';

/**
 * How long the TLS container may take to start, including the first image pull.
 */
const CONTAINER_START_TIMEOUT_MS = 180_000;

/**
 * Migration that records whether its own session is encrypted.
 */
const RECORD_SSL_MIGRATION = `-- Up Migration
CREATE TABLE tls_probe AS SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid();
-- Down Migration
DROP TABLE tls_probe;
`;

/**
 * Writes a self-signed certificate for `CERTIFICATE_HOST` and its key into `dir` with the `openssl`
 * command line tool, and returns their paths.
 */
const writeSelfSignedCertificate = (dir: string) => {
  const certPath = path.join(dir, 'server.crt');
  const keyPath = path.join(dir, 'server.key');
  execFileSync('openssl', [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-days',
    '1',
    '-subj',
    `/CN=${CERTIFICATE_HOST}`,
    '-addext',
    `subjectAltName=DNS:${CERTIFICATE_HOST}`,
    '-keyout',
    keyPath,
    '-out',
    certPath,
  ]);
  return { certPath, keyPath };
};

/**
 * Opens a pool and returns whether its session is encrypted, or the failure.
 */
const poolSessionIsEncrypted = (url: Redacted.Redacted, tls: PgTlsMode, tlsCa?: string) =>
  Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          ssl: boolean;
        }>`SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()`;
        return rows[0]?.ssl;
      }),
    ).pipe(
      Effect.provide(
        Layer.provideMerge(
          makePgLayer({ url, tls, ...omitUndefined({ tlsCa }) }),
          makeTestLogger().layer,
        ),
      ),
    ),
  );

describe('TLS against a server with a self-signed certificate', () => {
  let container: StartedPostgreSqlContainer;
  let certificateDir: string;
  let migrationsDir: string;
  let ca: string;
  let databaseCount = 0;

  /**
   * The container URI with `host` in place of the container host.
   */
  const uriWithHost = (host: string, database?: string): Redacted.Redacted => {
    const parsed = new URL(container.getConnectionUri());
    parsed.hostname = host;
    if (database !== undefined) {
      parsed.pathname = `/${database}`;
    }

    return Redacted.make(parsed.toString());
  };

  /**
   * Creates a fresh database and returns its URI on `CERTIFICATE_HOST`.
   */
  const freshDatabase = async (): Promise<Redacted.Redacted> => {
    databaseCount += 1;
    const name = `tls_case_${String(databaseCount)}`;
    const admin = new Client({ connectionString: container.getConnectionUri(), ssl: false });
    await admin.connect();
    try {
      await admin.query(`CREATE DATABASE ${name}`);
    } finally {
      await admin.end();
    }

    return uriWithHost(CERTIFICATE_HOST, name);
  };

  /**
   * Runs the probe migration and returns whether the migration session was encrypted.
   */
  const migrationSessionIsEncrypted = async (
    databaseUrl: Redacted.Redacted,
    tls: PgTlsMode,
    tlsCa?: string,
  ) => {
    await Effect.runPromise(
      runMigrations({
        databaseUrl,
        migrationsDir,
        tls,
        ...omitUndefined({ tlsCa }),
      }),
    );
    const client = new Client({ connectionString: Redacted.value(databaseUrl), ssl: false });
    await client.connect();
    try {
      const result = await client.query<{ ssl: boolean }>('SELECT ssl FROM tls_probe');
      return result.rows[0]?.ssl;
    } finally {
      await client.end();
    }
  };

  /**
   * Runs the probe migration expecting it to fail, and returns the error.
   */
  const migrationFailure = async (databaseUrl: Redacted.Redacted, tls: PgTlsMode, tlsCa?: string) =>
    Effect.runPromise(
      Effect.flip(
        runMigrations({
          databaseUrl,
          migrationsDir,
          tls,
          ...omitUndefined({ tlsCa }),
        }),
      ),
    );

  beforeAll(async () => {
    certificateDir = await mkdtemp(path.join(tmpdir(), 'pg-tls-'));
    migrationsDir = await mkdtemp(path.join(tmpdir(), 'pg-tls-migrations-'));
    await writeFile(path.join(migrationsDir, '0001_record_ssl.sql'), RECORD_SSL_MIGRATION);
    const { certPath, keyPath } = writeSelfSignedCertificate(certificateDir);
    ca = await readFile(certPath, 'utf8');
    container = await new PostgreSqlContainer(INTEGRATION_IMAGE)
      .withSSL(certPath, keyPath)
      .withStartupTimeout(CONTAINER_START_TIMEOUT_MS)
      .start();
  });

  afterAll(async () => {
    await container.stop();
    await rm(certificateDir, { recursive: true, force: true });
    await rm(migrationsDir, { recursive: true, force: true });
  });

  it('encrypts a pool session with require without checking the certificate', async () => {
    const exit = await poolSessionIsEncrypted(uriWithHost(CERTIFICATE_HOST), 'require');
    expect(exit).toEqual(Exit.succeed(true));
  });

  it('rejects the self-signed certificate under verify-full without a CA', async () => {
    const exit = await poolSessionIsEncrypted(uriWithHost(CERTIFICATE_HOST), 'verify-full');
    expect(Exit.isFailure(exit)).toBe(true);
    expect(String(exit)).toContain('SqlError');
  });

  it('verifies the certificate against tlsCa and the host name under verify-full', async () => {
    const exit = await poolSessionIsEncrypted(uriWithHost(CERTIFICATE_HOST), 'verify-full', ca);
    expect(exit).toEqual(Exit.succeed(true));
  });

  it('rejects a host the certificate does not name under verify-full, even with tlsCa', async () => {
    const exit = await poolSessionIsEncrypted(uriWithHost(MISMATCHED_HOST), 'verify-full', ca);
    expect(Exit.isFailure(exit)).toBe(true);
  });

  it('encrypts the migration session with require', async () => {
    expect(await migrationSessionIsEncrypted(await freshDatabase(), 'require')).toBe(true);
  });

  it('verifies the migration session against tlsCa under verify-full', async () => {
    expect(await migrationSessionIsEncrypted(await freshDatabase(), 'verify-full', ca)).toBe(true);
  });

  it('fails the migration under verify-full without a CA', async () => {
    const error = await migrationFailure(await freshDatabase(), 'verify-full');
    expect(error._tag).toBe('MigrationError');
    // Node writes "self-signed certificate", Bun "self signed certificate".
    expect(error.message).toMatch(/self[- ]signed certificate/);
  });

  it('fails the migration under verify-full for a host the certificate does not name', async () => {
    await freshDatabase();
    const databaseUrl = uriWithHost(MISMATCHED_HOST, `tls_case_${String(databaseCount)}`);
    const error = await migrationFailure(databaseUrl, 'verify-full', ca);
    expect(error._tag).toBe('MigrationError');
    expect(error.message).toContain(MISMATCHED_HOST);
  });
});
