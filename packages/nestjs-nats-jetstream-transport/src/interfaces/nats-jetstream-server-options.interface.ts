import { ServerConsumerOptions } from './server-consumer-options.interface';
import { NatsConnectionOptions } from './nats-connection-options.interface';
import { NatsJetStreamOptions } from './nats-jetstream-options.interface';
import { NatsStreamConfig } from './nats-stream-config.interface';
import type { NatsDriverOption } from '../driver/resolve-driver';

/**
 * Startup policy mode (spec 002, Part B).
 *
 * - `fail-fast`: attempt once; on failure tear down partial state and route
 *   the error to the `listen()` callback (Part A behavior, no loop).
 * - `retry`: attempt up to `maxAttempts` total; wait `delayMs` between
 *   attempts; after the last failure route the last error to the callback.
 *   A non-recoverable error aborts immediately (see the classifier).
 * - `retry-forever`: same as `retry` with no attempt cap.
 */
export type StartupPolicyMode = 'fail-fast' | 'retry' | 'retry-forever';

export interface StartupPolicyAttemptInfo {
  /** 1-based number of the attempt that just failed. */
  attempt: number;
  /** Total attempt cap, or `undefined` in `retry-forever` mode. */
  maxAttempts?: number;
  /** The error that failed the attempt. */
  err: unknown;
  /** Classifier verdict: `false` aborts the loop immediately. */
  recoverable: boolean;
}

export interface StartupPolicyOptions {
  /** Default `'fail-fast'`: success path is byte-identical to today. */
  mode?: StartupPolicyMode;
  /** Total attempts in `retry` mode. Default 5. Ignored in `retry-forever`. */
  maxAttempts?: number;
  /**
   * Delay between attempts in ms. Default: the connection options'
   * `reconnectDelayHandler()` when present (called per gap), else
   * `reconnectTimeWait`, else 2000. Never `maxReconnectAttempts`, which
   * governs steady-state reconnects, not startup attempts.
   */
  delayMs?: number;
  /** Called after every failed attempt, before the retry/abort decision. */
  onAttemptFailed?: (info: StartupPolicyAttemptInfo) => void;
}

export interface NatsJetStreamServerOptions {
  connectionOptions: Partial<NatsConnectionOptions> &
    Pick<NatsConnectionOptions, 'name'>;
  consumerOptions: Partial<ServerConsumerOptions>;
  jetStreamOptions?: NatsJetStreamOptions;
  streamConfig?: NatsStreamConfig | NatsStreamConfig[];
  /**
   * Startup policy (spec 002, Part B). Omitted means `fail-fast`: a single
   * attempt whose failure routes to the `listen()` callback (Part A).
   */
  startupPolicy?: StartupPolicyOptions;
  /**
   * Name of the stream each JetStream handler consumes from. Required by
   * the modern driver (spec 001, section 7.2); may be omitted when exactly
   * one stream is configured via `streamConfig`, which is then reused.
   * Ignored by the legacy driver, which infers the stream server-side.
   */
  stream?: string;
  /**
   * Pin the driver generation. When omitted the driver is resolved lazily
   * at `listen()` time (modern wins if both are installed). Part of the
   * dual-driver support (spec 001).
   */
  driver?: NatsDriverOption;
}
