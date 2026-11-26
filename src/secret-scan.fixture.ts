import { Cause, Effect, Exit, Tracer } from 'effect';

/**
 * How deep `reachableStrings` follows nested objects. Error chains in these tests are three levels
 * deep (`SqlError` → reason → driver `Error`).
 */
const MAX_DEPTH = 8;

/**
 * Every string reachable from `value` through own enumerable properties, plus the non-enumerable
 * `message`, `stack` and `cause` of every error on the way, which `JSON.stringify` drops. Use it to
 * check that a secret is absent from fields a renderer might not print.
 */
export const reachableStrings = (value: unknown): ReadonlyArray<string> => {
  const strings: Array<string> = [];
  const seen = new Set<object>();
  const visit = (current: unknown, depth: number): void => {
    if (typeof current === 'string') {
      strings.push(current);
      return;
    }

    if (typeof current !== 'object' || current === null || depth > MAX_DEPTH) {
      return;
    }

    if (seen.has(current)) {
      return;
    }

    seen.add(current);
    if (current instanceof Error) {
      strings.push(current.message, current.stack ?? '');
      visit(current.cause, depth + 1);
    }

    for (const nested of Object.values(current)) {
      visit(nested, depth + 1);
    }
  };
  visit(value, 0);
  return strings;
};

/**
 * Every rendering of `exit` a caller could log or print: `String(exit)`, `Cause.pretty` of a
 * failure, and the strings reachable from each error in it, joined into one searchable text.
 */
export const renderExit = (exit: Exit.Exit<unknown, unknown>): string => {
  if (Exit.isSuccess(exit)) {
    return String(exit);
  }

  return [
    String(exit),
    Cause.pretty(exit.cause),
    ...reachableStrings(Cause.squash(exit.cause)),
    ...exit.cause.reasons.flatMap((reason) =>
      reachableStrings(Cause.isFailReason(reason) ? reason.error : reason),
    ),
  ].join('\n');
};

/**
 * A span recorded by `withSpanRecorder`: its name, attributes, and how its exit rendered at the
 * moment it ended, which is what an exporter would have sent.
 */
export interface RecordedSpan {
  /**
   * Span name.
   */
  readonly name: string;

  /**
   * Names of the attributes set on the span.
   */
  readonly attributeKeys: ReadonlyArray<string>;

  /**
   * The span's attributes as JSON, for searching.
   */
  readonly renderedAttributes: string;

  /**
   * `renderExit` of the exit the span ended with, captured when it ended. Empty while the span is
   * still open.
   */
  readonly renderedExit: string;
}

/**
 * Provides a tracer to `effect` that records every span it starts, and returns them with the
 * effect's exit. Each span's exit is rendered when the span ends.
 */
export const withSpanRecorder = <A, E>(
  effect: Effect.Effect<A, E>,
): Effect.Effect<{
  readonly exit: Exit.Exit<A, E>;
  readonly spans: ReadonlyArray<RecordedSpan>;
}> =>
  Effect.suspend(() => {
    const recorded: Array<{ span: Tracer.NativeSpan; renderedExit: string }> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        const entry = { span, renderedExit: '' };
        const end = span.end.bind(span);
        span.end = (endTime, exit) => {
          end(endTime, exit);
          entry.renderedExit = renderExit(exit);
        };
        recorded.push(entry);
        return span;
      },
    });
    return Effect.exit(effect).pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.map((exit) => ({
        exit,
        spans: recorded.map(({ span, renderedExit }) => ({
          name: span.name,
          attributeKeys: [...span.attributes.keys()],
          renderedAttributes: JSON.stringify(Object.fromEntries(span.attributes)),
          renderedExit,
        })),
      })),
    );
  });
