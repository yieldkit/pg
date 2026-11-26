import { describe, expect, it, vi } from 'vite-plus/test';

import { PgContainerError } from '../errors.ts';
import { startPostgresContainer } from './index.ts';

vi.mock('@testcontainers/postgresql', () => ({
  PostgreSqlContainer: class {
    withDatabase = () => this;
    withUsername = () => this;
    withPassword = () => this;
    withEnvironment = () => this;
    withTmpFs = () => this;
    withStartupTimeout = () => this;
    start = () =>
      Promise.resolve({
        getConnectionUri: () => 'postgres://test@localhost/test',
        stop: () => Promise.reject(new Error('Docker unavailable')),
      });
  },
}));

describe('startPostgresContainer', () => {
  it('wraps a container stop failure in PgContainerError', async () => {
    const container = await startPostgresContainer();
    await expect(container.stop()).rejects.toBeInstanceOf(PgContainerError);
  });

  for (const password of ['a b', 'a%40b']) {
    it(`rejects the password ${JSON.stringify(password)} before Docker is used`, async () => {
      const failure = await startPostgresContainer({ database: 'my-app.test', password }).catch(
        (error: unknown) => error,
      );
      expect(failure).toMatchObject({ _tag: 'PgContainerError' });
      expect(String(failure)).toContain('password');
      expect(String(failure)).not.toContain('database');
      expect(String(failure)).not.toContain(password);
    });
  }
});
