import type { Client } from 'pg';

import { logDebug, logError, logInfo } from '@yieldkit/logger';
import { Effect } from 'effect';

import type { MigrationDirection } from './migrate.ts';

import { isUndefinedTable } from './migration-client.ts';
import { MIGRATION_LOG_BUFFER_LIMIT, MIGRATION_LOG_LINE_MAX_CHARS } from './policy.ts';

/**
 * Severity `node-pg-migrate` gave a line of its own output.
 */
type RunnerLogLevel = 'debug' | 'info' | 'warn' | 'error';

/**
 * One line of runner output, already redacted.
 */
type RunnerLogLine = {
  /**
   * Which `node-pg-migrate` logger method produced the line.
   */
  readonly level: RunnerLogLevel;

  /**
   * Redacted text of the line.
   */
  readonly line: string;
};

/**
 * Logger handed to `node-pg-migrate`, with one method per severity.
 */
export type RunnerLogger = Readonly<Record<RunnerLogLevel, (line: string) => void>>;

/**
 * Bounded record of what `node-pg-migrate` printed during one run.
 */
export type RunnerLogBuffer = {
  /**
   * Logger to pass to the runner. Every line is redacted before it is stored.
   */
  readonly logger: RunnerLogger;

  /**
   * First `MIGRATION_LOG_BUFFER_LIMIT` lines, in the order the runner wrote them.
   */
  readonly lines: ReadonlyArray<RunnerLogLine>;

  /**
   * Number of lines dropped because the buffer was full.
   */
  readonly dropped: () => number;
};

/**
 * Event name for the runner output lines and the dropped-line count.
 */
const RUNNER_LOG_EVENT = 'pg.migration.runner';

/**
 * Emits an event for each committed migration, in execution order.
 */
export const logAppliedMigrations = (
  names: ReadonlyArray<string>,
  direction: MigrationDirection,
): Effect.Effect<void> =>
  Effect.forEach(names, (name) => logInfo('pg.migration.applied', { direction, name }), {
    discard: true,
  });

/**
 * Recovers committed names after a failed run while the client still holds the migration lock.
 * `table` is a validated, quoted identifier. If history cannot be read, only the run error is
 * kept.
 */
export const logCommittedMigrationsOnFailure = <A, E>(
  client: Client,
  table: string,
  direction: MigrationDirection,
  run: Effect.Effect<A, E>,
): Effect.Effect<A, E> => {
  /**
   * Rolls back an aborted transaction if the run failed, so the snapshot only sees committed rows.
   */
  const history = (rollback: boolean) =>
    Effect.promise(async () => {
      try {
        if (rollback) {
          await client.query('ROLLBACK');
        }

        const result = await client.query<{ name: string }>(
          `SELECT name FROM ${table} ORDER BY run_on, id`,
        );
        return result.rows.map(({ name }) => name);
      } catch (cause) {
        return !rollback && isUndefinedTable(cause) ? [] : undefined;
      }
    });
  return Effect.gen(function* () {
    const before = yield* history(false);
    if (before === undefined) {
      return yield* run;
    }

    const logCommitted = Effect.gen(function* () {
      const after = yield* history(true);
      if (after === undefined) {
        return;
      }

      const [candidates, unchanged] =
        direction === 'up' ? [after, before] : [before.toReversed(), after];
      const unchangedNames = new Set(unchanged);
      yield* logAppliedMigrations(
        candidates.filter((name) => !unchangedNames.has(name)),
        direction,
      );
    });
    return yield* run.pipe(Effect.tapError(() => logCommitted));
  });
};

/**
 * Collects runner output in memory, redacting each line. Keeps at most `MIGRATION_LOG_BUFFER_LIMIT`
 * lines and counts the rest.
 */
export const makeRunnerLogBuffer = (redact: (line: string) => string): RunnerLogBuffer => {
  const lines: Array<RunnerLogLine> = [];
  let dropped = 0;
  /**
   * Builds the logger method for `level`: stores the redacted line, or counts it once full.
   */
  const record =
    (level: RunnerLogLevel) =>
    (line: string): void => {
      if (lines.length >= MIGRATION_LOG_BUFFER_LIMIT) {
        dropped += 1;
        return;
      }

      lines.push({ level, line: capRunnerLine(redact(line)) });
    };
  return {
    logger: {
      debug: record('debug'),
      info: record('info'),
      warn: record('warn'),
      error: record('error'),
    },
    lines,
    dropped: () => dropped,
  };
};

/**
 * Text that replaces the middle of a capped line, naming how many characters it stands for. Sized
 * for the line's full length, so it is never shorter than the marker actually used.
 */
const omissionMarker = (omitted: number): string => ` …[${String(omitted)} characters omitted]… `;

/**
 * Cuts `line` to at most {@link MIGRATION_LOG_LINE_MAX_CHARS} characters, marker included, by
 * keeping its first and last part and naming how many characters were left out in between. Shorter
 * lines are returned unchanged.
 */
export const capRunnerLine = (line: string): string => {
  if (line.length <= MIGRATION_LOG_LINE_MAX_CHARS) {
    return line;
  }

  const half = Math.floor((MIGRATION_LOG_LINE_MAX_CHARS - omissionMarker(line.length).length) / 2);
  return `${line.slice(0, half)}${omissionMarker(line.length - half * 2)}${line.slice(-half)}`;
};

/**
 * Logs the buffered runner output as `pg.migration.runner`, at debug after a successful run and at
 * error otherwise, followed by `droppedLines` when the buffer overflowed.
 */
export const emitRunnerLog = Effect.fnUntraced(function* (
  buffer: RunnerLogBuffer,
  succeeded: boolean,
): Effect.fn.Return<void> {
  const emit = succeeded ? logDebug : logError;
  for (const { level, line } of buffer.lines) {
    yield* emit(RUNNER_LOG_EVENT, { level, line });
  }

  const droppedLines = buffer.dropped();
  if (droppedLines > 0) {
    yield* emit(RUNNER_LOG_EVENT, { droppedLines });
  }
});
