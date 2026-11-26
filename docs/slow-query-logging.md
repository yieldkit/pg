# Slow-query logging

`slowQueryLog: { thresholdMs }` sends `log_min_duration_statement` with every session, so PostgreSQL logs statements at least that slow:

```ts
import { makePgLive } from '@yieldkit/pg';

const PgLive = makePgLive({ slowQueryLog: { thresholdMs: 250 } }); // 0 logs every statement
```

Or from the environment, with `makePgLive` and no `slowQueryLog` option:

```bash
PG_SLOW_QUERY_LOG=true PG_SLOW_QUERY_THRESHOLD_MS=250 node server.js
```

They go to PostgreSQL's server log, not through `@yieldkit/logger`. `pg.client.opening` logs the threshold as `slowQueryLog`, or `server-default` when none is sent.

| Caller        | Source                                                                                                                                          |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `makePgLayer` | `slowQueryLog`; omitted leaves the server or role setting in effect                                                                             |
| `makePgLive`  | `slowQueryLog`, else `PG_SLOW_QUERY_LOG` (default `false`) with `PG_SLOW_QUERY_THRESHOLD_MS` (default `500`, `DEFAULT_SLOW_QUERY_THRESHOLD_MS`) |

## The privilege

Only a superuser can set `log_min_duration_statement`, or, on PostgreSQL 15 and later, a role granted it:

```sql
GRANT SET ON PARAMETER log_min_duration_statement TO app;
```

For any other role PostgreSQL refuses every session, and the layer fails with a `PgConfigError` naming the privilege.

Without the privilege, leave `slowQueryLog` unset and have a superuser set it on the role or database:

```sql
ALTER ROLE app SET log_min_duration_statement = 500;
```

## Limits

- `thresholdMs` must be an integer from 0 to 2,147,483,647. An invalid value fails with `PgConfigError`.
- An invalid `PG_SLOW_QUERY_THRESHOLD_MS` fails with `ConfigError` naming the variable.
- `PG_SLOW_QUERY_LOG` accepts `true`, `yes`, `on`, `1`, `y` and `false`, `no`, `off`, `0`, `n`, in lowercase only.
- `log_min_duration_statement` is a startup parameter, so PgBouncer refuses it (see [Behind PgBouncer](pool-and-timeouts.md#behind-pgbouncer)).
