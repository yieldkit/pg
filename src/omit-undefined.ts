/**
 * Object type produced by `omitUndefined`: keys whose value type can be `undefined` become optional
 * keys without `undefined`, which is what `exactOptionalPropertyTypes` expects.
 */
export type OmitUndefined<T> = {
  readonly [K in keyof T as undefined extends T[K] ? never : K]: T[K];
} & {
  readonly [K in keyof T as undefined extends T[K] ? K : never]?: Exclude<T[K], undefined>;
};

/**
 * Returns a copy of `value` without the keys whose value is `undefined`, so optional fields can be
 * passed as `{ limit: params.limit }` instead of conditional spreads.
 */
export function omitUndefined<T extends object>(value: T): OmitUndefined<T>;
/**
 * Implementation signature: copies the own entries whose value is not `undefined`.
 */
export function omitUndefined<T extends object>(value: T): Partial<T> {
  const result: Partial<T> = {};
  for (const key in value) {
    if (Object.hasOwn(value, key) && value[key] !== undefined) {
      result[key] = value[key];
    }
  }

  return result;
}
