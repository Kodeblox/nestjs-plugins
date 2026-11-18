import type { NatsDriverOption, NatsDriverResolution } from './resolve-driver';
import { resolveNatsDriver } from './resolve-driver';
import type { NatsConnectionOptions } from '../interfaces/nats-connection-options.interface';
import type { NatsStreamConfig } from '../interfaces/nats-stream-config.interface';
import type { ServerConsumerOptions } from '../interfaces/server-consumer-options.interface';
import { createLegacyDriver } from './legacy-driver';

/**
 * Internal driver adapter (spec 001, section 4.2). Every NATS access in
 * `client.ts` / `server.ts` goes through this interface, which speaks only
 * this plugin's types (plus `unknown` for driver handles and passthrough
 * option bags). No driver package is referenced here — not even as a type —
 * so this module loads cleanly on either driver generation. The legacy
 * implementation lives in `legacy-driver.ts`; the modern one lands with the
 * Phase 3 spike and shares this exact surface.
 *
 * Scope note: `client.ts` / `server.ts` go through this interface. The
 * builder modules (`server-consumer-options-builder`, the two record
 * builders) are a documented legacy-path call-time seam — they obtain the
 * driver runtime at call time (function scope) rather than through this
 * interface, because they run synchronously in user code. Phase 3 must port
 * the `headers()` / `consumerOpts()` / `createInbox()` shapes alongside the
 * driver, or inject them via this interface.
 */

export interface JetStreamPublishResult {
  readonly stream: string;
  readonly seq: number;
  readonly duplicate: boolean;
}

export interface StreamSetupResult {
  readonly name: string;
  readonly created: boolean;
}

export interface ConsumedMessage {
  readonly data: Uint8Array;
  /** Opaque driver message; the server adapts it once via the context views. */
  readonly raw: unknown;
}

export interface EventSubscription {
  readonly messages: AsyncIterable<ConsumedMessage>;
  destroy(): Promise<void>;
}

export type MessageCallback = (
  err: Error | null,
  message: ConsumedMessage | null,
) => void | Promise<void>;

export interface NatsDriver {
  readonly kind: NatsDriverOption;
  connect(options: Partial<NatsConnectionOptions>): Promise<unknown>;
  close(connection: unknown): Promise<void>;
  isClosed(connection: unknown): boolean;
  isDraining(connection: unknown): boolean;
  publish(
    connection: unknown,
    subject: string,
    payload: Uint8Array,
    jetStreamOptions?: unknown,
    publishOptions?: unknown,
  ): Promise<JetStreamPublishResult>;
  request(
    connection: unknown,
    subject: string,
    payload: Uint8Array,
    options?: unknown,
  ): Promise<Uint8Array>;
  ensureStreams(
    connection: unknown,
    jetStreamOptions: unknown,
    configs: NatsStreamConfig[],
  ): Promise<StreamSetupResult[]>;
  subscribeEvents(
    connection: unknown,
    subject: string,
    consumerOptions: ServerConsumerOptions,
    jetStreamOptions: unknown,
  ): Promise<EventSubscription>;
  subscribeMessages(
    connection: unknown,
    subject: string,
    queue: string | undefined,
    callback: MessageCallback,
  ): void;
}

/**
 * JSON payload codec matching the legacy driver's JSON codec on the wire
 * and in error shape: `undefined` encodes as `null`, and codec failures
 * throw an error with the same name/message/code (`NatsError` / `Bad JSON` /
 * `BAD_JSON`) carrying the original error as `chainedError`. The prototype
 * chain is intentionally not the driver's `NatsError` class — this module
 * never imports the driver — so match on `code === 'BAD_JSON'` instead of
 * `instanceof` (this is also the check that survives the Phase 3 modern
 * driver, where `NatsError` no longer exists). Owned here so the
 * client/server never touch a driver codec; per spec 001 section 4.2
 * rule 2, encoding lives in the client/server, not the driver.
 */
function badJsonError(chainedError: unknown): Error {
  const err = new Error('Bad JSON');
  err.name = 'NatsError';
  (err as Error & { code: string }).code = 'BAD_JSON';
  (err as Error & { chainedError: unknown }).chainedError = chainedError;
  return err;
}

export const jsonPayloadCodec = {
  encode(value: unknown): Uint8Array {
    try {
      if (value === undefined) {
        value = null;
      }
      return new TextEncoder().encode(JSON.stringify(value));
    } catch (err) {
      throw badJsonError(err);
    }
  },
  decode<T = unknown>(data: Uint8Array): T {
    try {
      return JSON.parse(new TextDecoder().decode(data)) as T;
    } catch (err) {
      throw badJsonError(err);
    }
  },
};

/**
 * Resolve the configured driver and return its implementation. Only the
 * legacy driver exists in this round; a resolution naming the modern driver
 * fails loudly here until the Phase 3 spike lands (issue #7).
 */
export function resolveDriver(explicit?: NatsDriverOption): {
  driver: NatsDriver;
  resolution: NatsDriverResolution;
} {
  const resolution = resolveNatsDriver(explicit);
  if (resolution.driver !== 'legacy') {
    throw new Error(
      `NATS driver '${resolution.driver}' is not implemented yet ` +
        `(Phase 3, issue #7). Install the legacy driver with ` +
        `'npm install nats@^2' and set options.driver to 'legacy'.`,
    );
  }
  return { driver: createLegacyDriver(), resolution };
}
