import { Duration, Option } from 'effect';

/**
 * Converts `input` to milliseconds when it is a finite duration greater than zero. Returns
 * `undefined` for zero, negative, infinite and unparseable input, including strings such as
 * `'100ms'` that `Duration.Input` does not accept, so callers can fail with their own typed error
 * instead of the defect `Duration.fromInputUnsafe` would raise.
 */
export const positiveFiniteMillis = (input: Duration.Input): number | undefined => {
  const millis = Option.match(Duration.fromInput(input), {
    onNone: () => Number.NaN,
    onSome: Duration.toMillis,
  });
  return Number.isFinite(millis) && millis > 0 ? millis : undefined;
};
