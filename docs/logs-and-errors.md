# Logs and errors

Every error is a tagged class, so you can handle one kind and let the rest through:

```ts
import { checkReadiness } from '@yieldkit/pg';
import { Effect } from 'effect';

const ready = checkReadiness().pipe(
  Effect.catchTag('PgReadinessError', (error) =>
    Effect.logWarning(`database not ready after ${error.attempts} attempts`),
  ),
);
```

## Errors

| Error              | Raised by                                                                                 | Fields                 |
| ------------------ | ----------------------------------------------------------------------------------------- | ---------------------- |
| `PgConfigError`    | `makePgLayer`, `makePgLive`, `resolveDatabaseUrl`, `withAdvisoryXactLock`, the validators | `message`              |
| `PgChannelError`   | `notifyAfterCommit`, `listenWithRetry`, `validateNotifyChannel`, `validateNotifyPayload`  | `channel`, `message`   |
| `MigrationError`   | `runMigrations`, `migrateTo`                                                              | `message`, `direction` |
| `PgReadinessError` | `checkReadiness`                                                                          | `message`, `attempts`  |
| `PgContainerError` | `startPostgresContainer`                                                                  | `message`              |

Connection and query failures are Effect's `SqlError`. Invalid environment values are Effect's `ConfigError`.

## Log events

Messages never include the connection URI or password. The target fields are `host`, `port`, `database`, `user` and, when set, `applicationName`.

| Event                              | Level                                        | Fields                                                                                         |
| ---------------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| `pg.client.opening`                | info                                         | `host`, `port`, `database`, `user`, `applicationName`, `maxConnections`, `slowQueryLog`, `tls` |
| `pg.client.tls_disabled`           | warn                                         | target fields, `tls`, `hint`; only when `NODE_ENV` is `production` or `prod`                   |
| `pg.client.reconnecting`           | info                                         | target fields; before each startup connect retry                                               |
| `pg.client.failed`                 | error                                        | target fields, `errorMessage`; building the pool failed                                        |
| `pg.connection.failed`             | error                                        | target fields, `errorMessage`; the startup probe failed                                        |
| `pg.connection.created`            | info                                         | target fields, `backendPid`                                                                    |
| `pg.client.ready`                  | info                                         | target fields, `maxConnections`                                                                |
| `pg.connection.disconnected`       | info                                         | target fields, `backendPid` (the startup probe's session); the layer scope started to close    |
| `pg.client.closed`                 | info                                         | target fields; the layer scope started to close, before the pool ended its sessions            |
| `pg.listen.connected`              | info                                         | `channel`                                                                                      |
| `pg.listen.reconnected`            | info                                         | `channel`                                                                                      |
| `pg.listen.disconnected`           | info                                         | `channel`, `reason: 'closed'` or `errorMessage`                                                |
| `pg.listen.failed`                 | error                                        | `channel`, `errorMessage`                                                                      |
| `pg.listen.retry_scheduled`        | debug                                        | `channel`, `attempt`, `delayMs`                                                                |
| `pg.listen.overflow`               | warn                                         | `channel`, `bufferCapacity`, `droppedTotal`                                                    |
| `pg.listen.backlog_discarded`      | warn                                         | `channel`, `discarded`                                                                         |
| `pg.listen.status_callback_failed` | error                                        | `channel`, `status`, `errorMessage`                                                            |
| `pg.notify.failed`                 | error                                        | `channel`, `payloadBytes`, `errorMessage`                                                      |
| `pg.readiness.retry`               | warn                                         | `attempt`, `delayMs`                                                                           |
| `pg.readiness.failed`              | error                                        | `attempts`, `reason`                                                                           |
| `pg.migration.tls_disabled`        | warn                                         | `host`, `port`, `database`, `tls`, `hint`                                                      |
| `pg.migration.lock_acquired`       | info                                         | `direction`, `waitedMs`                                                                        |
| `pg.migration.applied`             | info                                         | `direction`, `name`                                                                            |
| `pg.migration.up_to_date`          | info                                         | `direction`, `migrationsDir`                                                                   |
| `pg.migration.runner`              | debug after a success, error after a failure | `level`, `line`; or `droppedLines`                                                             |

## Spans

`ping`, `checkReadiness`, `runMigrations` and `withAdvisoryXactLock` run in the spans `pg.ping`, `pg.checkReadiness`, `pg.runMigrations` and `pg.withAdvisoryXactLock`. Query spans come from `@effect/sql-pg`, with `db.system.name`, `db.namespace` and `server.address` set from the URI.

What is masked where: [Redacting driver errors](connection.md#redacting-driver-errors).
