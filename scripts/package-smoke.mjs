import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';

/**
 * How long `npm install` of the tarball may take: it downloads every dependency on a cold cache.
 */
const INSTALL_TIMEOUT_MS = 300_000;

/**
 * How long one type-check or script run in the consumer may take.
 */
const COMMAND_TIMEOUT_MS = 120_000;

/**
 * TypeScript sources other than declarations, which must not ship: consumers load the built modules
 * from `node_modules`, where Node does not strip types.
 */
const TYPESCRIPT_SOURCE_PATTERN = /(?<!\.d)\.(ts|tsx)$/;

/**
 * The repository the tarball was packed from.
 */
const root = resolve(import.meta.dirname, '..');

/**
 * The tarball supplied by CI or a local `pnpm pack` command.
 */
const tarball = process.argv[2];
assert.ok(tarball, 'Usage: pnpm test:package /absolute/path/to/pg.tgz');

/**
 * The manifest of the checkout, which the packed tarball must match.
 */
const sourceManifest = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));

/**
 * Reads the version of a dependency installed in this checkout, so the consumer runs the same
 * versions CI tested against.
 */
const installedVersion = (name) =>
  JSON.parse(readFileSync(join(root, 'node_modules', name, 'package.json'), 'utf8')).version;

/**
 * An isolated consumer project outside the checkout and its development dependencies, removed when
 * the script exits.
 */
const consumer = mkdtempSync(join(tmpdir(), 'pg-package-'));
process.on('exit', () => rmSync(consumer, { recursive: true, force: true }));

writeFileSync(
  join(consumer, 'package.json'),
  JSON.stringify({ name: 'consumer', private: true, type: 'module' }, null, 2),
);

// Dependencies resolve outside the lockfile here, so install scripts stay off. The optional
// `@testcontainers/postgresql` peer is left out on purpose: both entry points must load without it.
execFileSync(
  'npm',
  [
    'install',
    '--no-audit',
    '--no-fund',
    '--ignore-scripts',
    resolve(tarball),
    `effect@${installedVersion('effect')}`,
    `@effect/sql-pg@${installedVersion('@effect/sql-pg')}`,
    `typescript@${installedVersion('typescript')}`,
    `@types/node@${installedVersion('@types/node')}`,
  ],
  { cwd: consumer, stdio: 'inherit', timeout: INSTALL_TIMEOUT_MS },
);

/**
 * Where npm installed the package under test inside the consumer.
 */
const installed = join(consumer, 'node_modules', sourceManifest.name);

/**
 * The manifest npm installed from the tarball.
 */
const manifest = JSON.parse(readFileSync(join(installed, 'package.json'), 'utf8'));
assert.equal(manifest.version, sourceManifest.version);

for (const section of ['dependencies', 'peerDependencies', 'optionalDependencies']) {
  for (const version of Object.values(manifest[section] ?? {})) {
    assert.ok(
      !version.startsWith('catalog:') && !version.startsWith('workspace:'),
      'pnpm must resolve catalog and workspace versions before publishing',
    );
  }
}

/**
 * Every path inside the installed package.
 */
const shipped = readdirSync(installed, { recursive: true }).map(String);

/**
 * Shipped TypeScript sources, which must be empty.
 */
const sources = shipped.filter((name) => TYPESCRIPT_SOURCE_PATTERN.test(name));
assert.deepEqual(sources, [], 'The package must not ship TypeScript sources');
assert.ok(!shipped.some((name) => name.includes('.test.')), 'The package must not ship tests');
assert.ok(
  !shipped.some((name) => name.includes('.fixture.')),
  'The package must not ship test fixtures',
);
assert.ok(
  !existsSync(join(consumer, 'node_modules', '@testcontainers', 'postgresql')),
  'The optional Testcontainers peer must not be installed for this test',
);

/**
 * The optional peer only `startPostgresContainer` in the `testing` entry point may load.
 */
const TESTCONTAINERS_PACKAGE = '@testcontainers/postgresql';

/**
 * Static and dynamic module specifiers in a built file: `from "x"`, `import "x"` and `import("x")`.
 */
const IMPORT_PATTERN = /(?:\bfrom|\bimport)\s*\(?\s*"([^"]+)"/g;

/**
 * Returns every built module `entry` loads, itself included, by following relative imports, with
 * the bare package specifiers each one imports.
 */
const moduleGraph = (entry) => {
  const modules = new Map();
  const pending = [entry];
  while (pending.length > 0) {
    const file = pending.pop();
    if (!modules.has(file)) {
      const specifiers = [...readFileSync(file, 'utf8').matchAll(IMPORT_PATTERN)].map(
        ([, specifier]) => specifier,
      );
      modules.set(file, specifiers);
      for (const specifier of specifiers) {
        if (specifier.startsWith('.')) {
          pending.push(resolve(dirname(file), specifier));
        }
      }
    }
  }

  return modules;
};

/**
 * Built modules reachable from `entry` that import the Testcontainers package in any form.
 */
const testcontainersImporters = (entry) =>
  [...moduleGraph(join(installed, entry))].flatMap(([file, specifiers]) =>
    specifiers.includes(TESTCONTAINERS_PACKAGE) ? [file] : [],
  );

assert.deepEqual(
  testcontainersImporters('dist/index.js'),
  [],
  `dist/index.js must not load ${TESTCONTAINERS_PACKAGE}`,
);
assert.deepEqual(
  testcontainersImporters('dist/testing/index.js'),
  [join(installed, 'dist/testing/index.js')],
  `Only dist/testing/index.js may import ${TESTCONTAINERS_PACKAGE}`,
);
assert.ok(
  !/^import[^;\n(]*"@testcontainers\/postgresql"/m.test(
    readFileSync(join(installed, 'dist/testing/index.js'), 'utf8'),
  ),
  `dist/testing/index.js must load ${TESTCONTAINERS_PACKAGE} lazily, with import()`,
);

// The README quick start must type-check against the built declarations. It reads DATABASE_URL, so
// it is not run.
writeFileSync(
  join(consumer, 'quick-start.ts'),
  `import { checkReadiness, makePgLive } from '${sourceManifest.name}';
import { Effect } from 'effect';
import { SqlClient } from 'effect/sql';

const PgLive = makePgLive({ applicationName: 'api' });

export const notes = Effect.gen(function* () {
  yield* checkReadiness();
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{ id: number; body: string }>\`SELECT id, body FROM note\`;
}).pipe(Effect.provide(PgLive));
`,
);

// Every entry point must resolve, type-check and run from the built output without a database or
// Docker: a URI the connection policy rejects and a refused port must both fail at startup, and
// neither the password nor the URI may reach the error, its rendering or a log line.
writeFileSync(
  join(consumer, 'smoke.ts'),
  `import {
  PgClient,
  PgContainerError,
  makePgLayer,
  redactDatabaseSecrets,
  validateNotifyChannel,
} from '${sourceManifest.name}';
import { startPostgresContainer } from '${sourceManifest.name}/testing';
import { Cause, Effect, Exit, Layer, Logger, Redacted } from 'effect';
import { SqlClient } from 'effect/sql';
import { createServer } from 'node:net';

const PASSWORD = 'smoke-s3cret';

const lines: Array<string> = [];
const CaptureLogs = Logger.layer([Logger.map(Logger.formatJson, (line) => lines.push(line))]);

const assertNoSecret = (label: string, text: string) => {
  if (text.includes(PASSWORD)) {
    throw new Error(\`The password reached \${label}: \${text}\`);
  }
};

const openPool = (url: string) =>
  Effect.runPromiseExit(
    Effect.scoped(
      Effect.gen(function* () {
        yield* PgClient.PgClient;
        const sql = yield* SqlClient.SqlClient;
        return yield* sql\`SELECT 1\`;
      }).pipe(Effect.provide(makePgLayer({ url: Redacted.make(url) }).pipe(Layer.provide(CaptureLogs)))),
    ),
  );

const render = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) ? \`\${String(exit)}\\n\${Cause.pretty(exit.cause)}\` : String(exit);

// A query parameter the connection policy reserves fails before a socket is opened.
const rejected = await openPool(\`postgres://app:\${PASSWORD}@127.0.0.1:5432/app?sslmode=require\`);
if (!Exit.isFailure(rejected) || !render(rejected).includes('PgConfigError')) {
  throw new Error(\`A URI with sslmode must fail with PgConfigError: \${render(rejected)}\`);
}
assertNoSecret('the PgConfigError', render(rejected));

// A refused port fails the layer at startup, after the connect retries.
const port = await new Promise<number>((done) => {
  const server = createServer();
  server.listen(0, '127.0.0.1', () => {
    const address = server.address();
    server.close(() => done(typeof address === 'object' && address !== null ? address.port : 0));
  });
});
const refused = await openPool(\`postgres://app:\${PASSWORD}@127.0.0.1:\${String(port)}/app\`);
if (!Exit.isFailure(refused) || !render(refused).includes('SqlError')) {
  throw new Error(\`A refused port must fail with SqlError: \${render(refused)}\`);
}
assertNoSecret('the startup SqlError', render(refused));
assertNoSecret('a log line', lines.join('\\n'));
if (!lines.some((line) => line.includes('pg.client.reconnecting'))) {
  throw new Error(\`A refused connection must be retried: \${lines.join('\\n')}\`);
}

const url = \`postgres://app:\${PASSWORD}@db:5432/app\`;
assertNoSecret('redactDatabaseSecrets', redactDatabaseSecrets(\`connect failed for \${url}\`, url));

const channel = await Effect.runPromise(Effect.flip(validateNotifyChannel('jobs; DROP TABLE x')));
if (channel._tag !== 'PgChannelError') {
  throw new Error('An invalid channel name must fail with PgChannelError');
}

// Without the optional peer, the testing entry still loads and reports what is missing.
const missing = await startPostgresContainer().then(
  () => undefined,
  (error: unknown) => error,
);
if (!(missing instanceof PgContainerError) || !missing.message.includes('@testcontainers/postgresql')) {
  throw new Error(\`startPostgresContainer must reject with PgContainerError: \${String(missing)}\`);
}
`,
);

/**
 * Process options shared by the consumer's type-check and run.
 */
const options = {
  /**
   * Run inside the consumer project, so the installed tarball resolves.
   */
  cwd: consumer,

  /**
   * Show compiler and runtime output in the CI log.
   */
  stdio: 'inherit',

  /**
   * Fail instead of hanging on a stuck process.
   */
  timeout: COMMAND_TIMEOUT_MS,
};

execFileSync(
  join(consumer, 'node_modules/.bin/tsc'),
  [
    '--ignoreConfig',
    '--noEmit',
    '--strict',
    '--exactOptionalPropertyTypes',
    '--skipLibCheck',
    '--target',
    'ES2025',
    '--module',
    'nodenext',
    '--moduleResolution',
    'nodenext',
    '--types',
    'node',
    'smoke.ts',
    'quick-start.ts',
  ],
  options,
);

// Node strips the types of a file outside `node_modules`, so the fixture runs as written.
execFileSync(process.execPath, ['smoke.ts'], options);

console.log(`Package smoke test passed for ${manifest.name}@${manifest.version}.`);
