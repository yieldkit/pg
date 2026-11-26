import { ConfigProvider, Effect, Redacted } from 'effect';
import { describe, expect, it } from 'vite-plus/test';

import {
  CODE_URI_LABEL,
  DATABASE_URL_ENV,
  describeDatabaseTarget,
  parseConnectionUri,
  redactDatabaseSecrets,
  resolveApplicationName,
  resolveConnectionUri,
} from './target.ts';

/**
 * A URI with only the local defaults: host `localhost`, default port, user and database `app`.
 */
const LOCAL_URL = 'postgres://app:app@localhost/app';

const SECRET_URL = 'postgres://app:s3cret@db.internal:5432/app';

/**
 * Query parameters that only `PG_SSL_MODE` or the `tls` option may set.
 */
const TLS_PARAMETERS = [
  'ssl',
  'sslmode',
  'sslcert',
  'sslkey',
  'sslrootcert',
  'sslcrl',
  'sslpassword',
  'sslnegotiation',
  'sslcompression',
  'sslsni',
  'requiressl',
  'gssencmode',
  'uselibpqcompat',
];

/**
 * Query parameters owned by the pool settings.
 */
const POOL_PARAMETERS = [
  'options',
  'application_name',
  'statement_timeout',
  'lock_timeout',
  'idle_in_transaction_session_timeout',
  'query_timeout',
  'connect_timeout',
  'keepalives',
  'keepalives_idle',
  'target_session_attrs',
];

/**
 * Query parameters that repeat the URI authority or path.
 */
const TARGET_PARAMETERS = ['host', 'hostaddr', 'port', 'user', 'password', 'dbname'];

/**
 * Query parameters that change the wire protocol.
 */
const PROTOCOL_PARAMETERS = ['replication', 'client_encoding', 'binary'];

/**
 * A query value that must never be echoed back in an error.
 */
const SECRET_QUERY_VALUE = 'q-s3cret-value';

/**
 * Describes `SECRET_URL` with one extra query parameter and returns the failure.
 */
const rejectParameter = (name: string) =>
  Effect.runPromise(
    Effect.flip(
      describeDatabaseTarget(Redacted.make(`${SECRET_URL}?${name}=${SECRET_QUERY_VALUE}`)),
    ),
  );

describe('describeDatabaseTarget', () => {
  it('reads host, port, database, and user without the password', async () => {
    const target = await Effect.runPromise(describeDatabaseTarget(Redacted.make(SECRET_URL)));
    expect(target).toEqual({
      host: 'db.internal',
      port: '5432',
      database: 'app',
      user: 'app',
    });
    expect(JSON.stringify(target)).not.toContain('s3cret');
  });

  it('rejects an empty URI and a non-postgres protocol without echoing the secret', async () => {
    const empty = await Effect.runPromise(Effect.flip(describeDatabaseTarget(Redacted.make('  '))));
    expect(empty.message).toBe('The connection URI is empty.');

    const mysql = await Effect.runPromise(
      Effect.flip(describeDatabaseTarget(Redacted.make('mysql://app:s3cret@db/app'))),
    );
    expect(mysql.message).toContain('postgres');
    expect(mysql.message).not.toContain('s3cret');
  });

  it('names DATABASE_URL only when the URI came from that variable', async () => {
    const fromCode = await Effect.runPromise(
      Effect.flip(describeDatabaseTarget(Redacted.make(`${SECRET_URL}?sslmode=require`))),
    );
    expect(fromCode.message).not.toContain(DATABASE_URL_ENV);

    const fromEnv = await Effect.runPromise(
      Effect.flip(
        describeDatabaseTarget(Redacted.make(`${SECRET_URL}?sslmode=require`), DATABASE_URL_ENV),
      ),
    );
    expect(fromEnv.message.startsWith('DATABASE_URL must not set TLS parameters')).toBe(true);
  });

  it('rejects a URI without // instead of logging its password as the database', async () => {
    const opaque = 'postgres:app:SENTINEL-PW@db.example:5432/app';
    const error = await Effect.runPromise(
      Effect.flip(describeDatabaseTarget(Redacted.make(opaque))),
    );
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('has no host');
    expect(error.message).not.toContain('SENTINEL-PW');
  });

  it('fails with PgConfigError for malformed percent-encoding in the user or database', async () => {
    const urls = [
      'postgres://a%zz:pw@127.0.0.1:1/app',
      'postgres://app:pw@127.0.0.1:1/app%zz',
      'postgres://app:50%zz@127.0.0.1:1/app',
    ];
    const errors = await Promise.all(
      urls.map((url) => Effect.runPromise(Effect.flip(describeDatabaseTarget(Redacted.make(url))))),
    );
    for (const error of errors) {
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain('percent-encoding');
      expect(error.message).not.toContain('%zz');
    }
  });

  it('fails with PgConfigError for a NUL in the host, user, password or database', async () => {
    const urls = [
      'postgres://app:pw@db%00x/app',
      'postgres://app%00options:pw@db/app',
      'postgres://app:pw%00x@db/app',
      'postgres://app:pw@db/app%00search_path%00evil',
    ];
    const errors = await Promise.all(
      urls.map((url) => Effect.runPromise(Effect.flip(describeDatabaseTarget(Redacted.make(url))))),
    );
    for (const error of errors) {
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain('NUL character');
      expect(error.message).not.toContain('postgres://');
    }
  });

  it('decodes a percent-encoded user and database', async () => {
    const target = await Effect.runPromise(
      describeDatabaseTarget(Redacted.make('postgres://my%20user:pw@db/my%2Ddb')),
    );
    expect(target.user).toBe('my user');
    expect(target.database).toBe('my-db');
  });
});

/**
 * Parses `url` as a URI passed in code against the environment `env`.
 */
const parseWith = (url: string, env: Record<string, string | undefined> = { USER: 'os-user' }) =>
  parseConnectionUri({ url: Redacted.make(url), label: CODE_URI_LABEL }, env);

/**
 * URIs whose user or password has a raw '/', '?', '#' or '@', each carrying `Sekr3tPW`. `URL` ends
 * the authority early, so without a check part of the password would become the database.
 */
const AMBIGUOUS_AUTHORITY_URIS = [
  'postgres://app:/Sekr3tPW@127.0.0.1:1/appdb',
  'postgres://app:8/Sekr3tPW@127.0.0.1:1/appdb',
  'postgres://us/er:Sekr3tPW@127.0.0.1:1/appdb',
  'postgres://user:9/x?Sekr3tPW@127.0.0.1:1/appdb',
  'postgres://user:9/Sekr3tPW#x@127.0.0.1:1/appdb',
];

describe('parseConnectionUri', () => {
  for (const url of AMBIGUOUS_AUTHORITY_URIS) {
    it(`rejects ${url.replace('Sekr3tPW', '<pw>')} instead of logging the password as the database`, async () => {
      const error = await Effect.runPromise(Effect.flip(parseWith(url)));
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain('Percent-encode');
      expect(error.message).not.toContain('Sekr3tPW');
    });
  }

  it('accepts the same credentials once they are percent-encoded', async () => {
    const parsed = await Effect.runPromise(
      parseWith('postgres://us%2Fer:%2FSekr3t%40PW%3F%23@db/appdb'),
    );
    expect(parsed.target).toEqual({ host: 'db', port: '5432', database: 'appdb', user: 'us/er' });
    expect(parsed.password === undefined ? '' : Redacted.value(parsed.password)).toBe(
      '/Sekr3t@PW?#',
    );
  });

  it('decodes a raw at sign within the password', async () => {
    const parsed = await Effect.runPromise(parseWith('postgres://app:p@ss@db/app'));
    expect(parsed.target.host).toBe('db');
    expect(parsed.password === undefined ? '' : Redacted.value(parsed.password)).toBe('p@ss');
  });

  it('decodes a percent-encoded host, such as a Unix socket directory', async () => {
    const socket = await Effect.runPromise(
      parseWith('postgres://app:pw@%2Fcloudsql%2Fproj%3Aregion%3Adb/app'),
    );
    expect(socket.target.host).toBe('/cloudsql/proj:region:db');
    const encoded = await Effect.runPromise(parseWith('postgres://app:pw@local%68ost/app'));
    expect(encoded.target.host).toBe('localhost');
    const malformed = await Effect.runPromise(
      Effect.flip(parseWith('postgres://app:pw@bad%zzhost/app')),
    );
    expect(malformed.message).toContain('malformed percent-encoding');
  });

  it('returns an IPv6 host without brackets and the port as a number', async () => {
    const parsed = await Effect.runPromise(parseWith('postgres://app:pw@[::1]:6543/app'));
    expect(parsed.target.host).toBe('::1');
    expect(parsed.target.port).toBe('6543');
    expect(parsed.port).toBe(6543);
  });

  it('rejects port 0, which node-postgres would replace with PGPORT', async () => {
    const error = await Effect.runPromise(
      Effect.flip(parseWith('postgres://app:pw@db:0/app', { USER: 'os-user', PGPORT: '6432' })),
    );
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('port');
  });

  it('rejects a fragment, which neither driver uses', async () => {
    const error = await Effect.runPromise(Effect.flip(parseWith('postgres://app:pw@db/app#x')));
    expect(error.message).toContain('fragment');
  });

  it('takes the user from USER, then USERNAME, never PGUSER, when the URI has none', async () => {
    const fromUser = await Effect.runPromise(
      parseWith('postgres://db/app', { USER: 'os-user', USERNAME: 'win-user', PGUSER: 'pg-user' }),
    );
    expect(fromUser.target.user).toBe('os-user');
    const fromUsername = await Effect.runPromise(
      parseWith('postgres://db/app', { USERNAME: 'win-user', PGUSER: 'pg-user' }),
    );
    expect(fromUsername.target.user).toBe('win-user');
    const fromUri = await Effect.runPromise(
      parseWith('postgres://uri-user@db/app', { USER: 'os-user' }),
    );
    expect(fromUri.target.user).toBe('uri-user');
  });

  it('fails when the URI has no user and neither USER nor USERNAME is set', async () => {
    const error = await Effect.runPromise(
      Effect.flip(parseWith('postgres://db/app', { PGUSER: 'pg-user' })),
    );
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('no user');
  });

  it('takes the user from USERNAME when USER is empty', async () => {
    const parsed = await Effect.runPromise(
      parseWith('postgres://db/app', { USER: '', USERNAME: 'service' }),
    );
    expect(parsed.target.user).toBe('service');
  });
});

describe('resolveApplicationName', () => {
  it('defaults to the name the pool driver sends and rejects an empty one', async () => {
    expect(await Effect.runPromise(resolveApplicationName(undefined))).toBe('@effect/sql-pg');
    expect(await Effect.runPromise(resolveApplicationName('api'))).toBe('api');
    const error = await Effect.runPromise(Effect.flip(resolveApplicationName('')));
    expect(error._tag).toBe('PgConfigError');
  });
});

/**
 * Resolves without a code URI against an environment of `env`.
 */
const resolveFromEnv = (env: Record<string, string>) =>
  resolveConnectionUri(undefined).pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, ConfigProvider.fromEnv({ env })),
  );

describe('resolveConnectionUri', () => {
  it('fails with PgConfigError naming DATABASE_URL when it is unset, with no fallback', async () => {
    const error = await Effect.runPromise(Effect.flip(resolveFromEnv({})));
    expect(error._tag).toBe('PgConfigError');
    expect(error.message).toContain('DATABASE_URL is not set');
  });

  it('labels a URI from the environment DATABASE_URL and one from code neutrally', async () => {
    const fromEnv = await Effect.runPromise(resolveFromEnv({ DATABASE_URL: LOCAL_URL }));
    expect(fromEnv.label).toBe('DATABASE_URL');
    expect(Redacted.value(fromEnv.url)).toBe(LOCAL_URL);

    const fromCode = await Effect.runPromise(resolveConnectionUri(Redacted.make(LOCAL_URL)));
    expect(fromCode.label).not.toContain('DATABASE_URL');
  });
});

describe('connection URI policy', () => {
  it('accepts a URI without query parameters or a port', async () => {
    const plain = await Effect.runPromise(describeDatabaseTarget(Redacted.make(SECRET_URL)));
    const local = await Effect.runPromise(describeDatabaseTarget(Redacted.make(LOCAL_URL)));
    expect(plain.host).toBe('db.internal');
    expect(local.host).toBe('localhost');
  });

  it('ignores unreserved URI query parameters', async () => {
    const plain = await Effect.runPromise(parseWith(SECRET_URL));
    const withQuery = await Effect.runPromise(
      parseWith(`${SECRET_URL}?fallback_application_name=x&schema=billing`),
    );
    expect(withQuery).toEqual(plain);
  });

  for (const name of TLS_PARAMETERS) {
    it(`rejects the TLS parameter ${name} and points to PG_SSL_MODE`, async () => {
      const error = await rejectParameter(name);
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain(`(${name})`);
      expect(error.message).toContain('PG_SSL_MODE');
      expect(error.message).not.toContain('s3cret');
      expect(error.message).not.toContain(SECRET_QUERY_VALUE);
    });
  }

  for (const name of POOL_PARAMETERS) {
    it(`rejects the pool parameter ${name}`, async () => {
      const error = await rejectParameter(name);
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain(`(${name})`);
      expect(error.message).toContain('pool-owned');
      expect(error.message).not.toContain('s3cret');
      expect(error.message).not.toContain(SECRET_QUERY_VALUE);
    });
  }

  for (const name of TARGET_PARAMETERS) {
    it(`rejects the connection target parameter ${name}`, async () => {
      const error = await rejectParameter(name);
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain(`(${name})`);
      expect(error.message).toContain('connection target');
      expect(error.message).not.toContain('s3cret');
      expect(error.message).not.toContain(SECRET_QUERY_VALUE);
    });
  }

  for (const name of PROTOCOL_PARAMETERS) {
    it(`rejects the protocol parameter ${name}`, async () => {
      const error = await rejectParameter(name);
      expect(error._tag).toBe('PgConfigError');
      expect(error.message).toContain(`(${name})`);
      expect(error.message).toContain('protocol parameters');
      expect(error.message).not.toContain('s3cret');
      expect(error.message).not.toContain(SECRET_QUERY_VALUE);
    });
  }

  it('matches parameter names case-insensitively and lists each one once', async () => {
    const error = await Effect.runPromise(
      Effect.flip(
        describeDatabaseTarget(
          Redacted.make(`${SECRET_URL}?SSLMode=require&sslmode=disable&SSLROOTCERT=/tmp/ca.pem`),
        ),
      ),
    );
    expect(error.message).toContain('(sslmode, sslrootcert)');
    expect(error.message).not.toContain('/tmp/ca.pem');
  });
});

describe('redactDatabaseSecrets', () => {
  it('removes the connection URI and the password from a driver message', () => {
    const message = `connect failed for ${SECRET_URL} password s3cret`;
    const redacted = redactDatabaseSecrets(message, SECRET_URL);
    expect(redacted).not.toContain('s3cret');
    expect(redacted).not.toContain(SECRET_URL);
    expect(redacted).toContain('[redacted-database-url]');
  });

  it('masks the password as written in the URI and decoded', () => {
    const url = 'postgres://app:p%40ss-word@db/app';
    const redacted = redactDatabaseSecrets('failed with p%40ss-word and p@ss-word', url);
    expect(redacted).toBe('failed with *** and ***');
  });

  it('masks a password with a bare % that cannot be decoded', () => {
    const url = 'postgres://app:50%off-deal@db/app';
    expect(redactDatabaseSecrets('bad password "50%off-deal"', url)).toBe('bad password "***"');
  });
});
