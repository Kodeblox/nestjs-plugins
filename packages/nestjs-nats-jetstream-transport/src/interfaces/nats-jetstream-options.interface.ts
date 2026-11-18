/**
 * Driver-neutral JetStream options (spec 001, section 4.2.3). Named fields
 * cover the common knobs; anything else rides the index signature and is
 * forwarded to `jetstream()` / `jetstreamManager()` untouched.
 */
export interface NatsJetStreamOptions {
  apiPrefix?: string;
  timeout?: number;
  /** Pass-through for any other JetStream option. */
  [option: string]: unknown;
}
