import { POSTGRES_TEST_IMAGE } from './policy.ts';

/**
 * Image the integration tests run: `PG_TEST_IMAGE` when set, so CI can run the suite against
 * several PostgreSQL versions, and the package default otherwise.
 */
export const INTEGRATION_IMAGE = process.env['PG_TEST_IMAGE'] ?? POSTGRES_TEST_IMAGE;
