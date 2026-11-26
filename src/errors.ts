import { Schema } from 'effect';

/**
 * Raised when a connection URI or pool option cannot be used. The message never includes the
 * password or the raw URI.
 */
export class PgConfigError extends Schema.TaggedError<
  PgConfigError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('PgConfigError', {
  /**
   * What was rejected, naming the field and the accepted shape.
   */
  message: Schema.String,
}) {}

/**
 * Raised when `node-pg-migrate` cannot load, lock, or apply a migration directory. The message has
 * the connection URI and password removed.
 */
export class MigrationError extends Schema.TaggedError<
  MigrationError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('MigrationError', {
  /**
   * Runner or filesystem failure with secrets removed.
   */
  message: Schema.String,

  /**
   * Requested direction. Invalid directions are rejected before the runner starts.
   */
  direction: Schema.Literals(['up', 'down']),
}) {}

/**
 * Raised when a `LISTEN` / `NOTIFY` channel, payload or listener option is rejected before anything
 * is sent. Nothing is registered with PostgreSQL.
 */
export class PgChannelError extends Schema.TaggedError<
  PgChannelError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('PgChannelError', {
  /**
   * Channel argument as supplied by the caller. Payloads are not stored here.
   */
  channel: Schema.String,

  /**
   * Which limit failed: identifier grammar, the payload size or content, or a listener option.
   */
  message: Schema.String,
}) {}

/**
 * Raised when Testcontainers cannot start or stop the PostgreSQL image.
 */
export class PgContainerError extends Schema.TaggedError<
  PgContainerError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('PgContainerError', {
  /**
   * Docker or image failure. Does not include database credentials.
   */
  message: Schema.String,
}) {}

/**
 * Raised when `checkReadiness` cannot run `SELECT 1` within its attempts. The message names the
 * failure kind only and never includes the connection URI or password.
 */
export class PgReadinessError extends Schema.TaggedError<
  PgReadinessError,
  {
    /**
     * Nominal marker: only values built by this class's constructor satisfy the type, never a
     * structurally similar object.
     */
    readonly brand: unique symbol;
  }
>()('PgReadinessError', {
  /**
   * Which failure ended the check and how many attempts were made.
   */
  message: Schema.String,

  /**
   * Probes that ran before giving up, including the first. Zero when the options were rejected.
   */
  attempts: Schema.Int,
}) {}
