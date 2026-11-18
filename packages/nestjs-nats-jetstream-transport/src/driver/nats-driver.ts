import type { NatsDriverOption, NatsDriverResolution } from './resolve-driver';
import { resolveNatsDriver } from './resolve-driver';
import type { NatsConnectionOptions } from '../interfaces/nats-connection-options.interface';
import type { NatsJetStreamServerOptions } from '../interfaces/nats-jetstream-server-options.interface';
import type { NatsStreamConfig } from '../interfaces/nats-stream-config.interface';
import type { ServerConsumerOptions } from '../interfaces/server-consumer-options.interface';
import { createLegacyDriver } from './legacy-driver';
import { createModernDriver } from './modern-driver';

/**
 * Internal driver adapter (spec 001, section 4.2). Every NATS access in
 * `client.ts` / `server.ts` goes through this interface, which speaks only
 * this plugin's types (plus `unknown` for driver handles and passthrough
 * option bags). No driver package is referenced here — not even as a type —
 * so this module loads cleanly on either driver generation. The legacy
 * implementation lives in `legacy-driver.ts` and the modern one in
 * `modern-driver.ts`; both share this exact surface.
 *
 * Scope note: `client.ts` / `server.ts` go through this interface. The
 * `server-consumer-options-builder` module is a documented legacy-path
 * call-time seam — it obtains the legacy runtime at call time (function
 * scope) rather than through this interface, because it runs synchronously;
 * it is used only by the legacy driver. The record builders resolve their
 * `headers()` factory per driver package (`driver-headers.ts`) and stay
 * interoperable because both drivers translate any `keys()`/`values()`
 * carrier at publish/request time.
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
    stream?: string,
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
 * Resolve the stream a JetStream handler consumes from. The modern driver
 * must name the stream at consumer-creation time (the legacy push subscribe
 * inferred it), so exactly one source must identify it: the explicit
 * `stream` server option, else a single configured stream. Anything else
 * fails loudly at `listen()` time.
 */
export function resolveStreamName(
  options: NatsJetStreamServerOptions,
): string {
  if (options.stream) {
    return options.stream;
  }
  const configs = options.streamConfig;
  const list = configs === undefined ? [] : Array.isArray(configs) ? configs : [configs];
  if (list.length === 1 && list[0]?.name) {
    return list[0].name;
  }
  throw new Error(
    'The modern NATS driver needs a stream name for each JetStream ' +
      'consumer. Set the `stream` server option or configure exactly one ' +
      'stream via `streamConfig`.',
  );
}

/**
 * Translate a header carrier entry-for-entry into the calling driver's own
 * headers via its factory. Only data headers transfer: a carrier that
 * reports status metadata (`hasError`) is rejected loudly instead of being
 * silently stripped. Accepts any `keys()`/`values()` carrier, so builders
 * (which resolve the factory per driver package) stay interoperable with
 * either driver, including an explicit legacy pin with both installed.
 */
export function withDriverHeaders<
  T extends { headers?: unknown },
  H extends { append(key: string, value: string): void },
>(
  options: T | undefined,
  makeHeaders: () => H,
): (Omit<T, 'headers'> & { headers?: H }) | undefined {
  if (!options || !options.headers) {
    // No carrier to translate: pass through untouched.
    return options as unknown as
      | (Omit<T, 'headers'> & { headers?: H })
      | undefined;
  }
  const carrier = options.headers as {
    keys(): string[];
    values(key: string): string[];
    hasError?: unknown;
  };
  if (carrier.hasError === true) {
    throw new Error(
      'NatsHeaders with status metadata cannot be translated across drivers: ' +
        'pass driver-native status headers via the raw-message escape hatch.',
    );
  }
  const out = makeHeaders();
  for (const key of carrier.keys()) {
    for (const value of carrier.values(key)) {
      out.append(key, value);
    }
  }
  return { ...options, headers: out };
}

/**
 * Resolve the configured driver and return its implementation.
 */
export function resolveDriver(explicit?: NatsDriverOption): {
  driver: NatsDriver;
  resolution: NatsDriverResolution;
} {
  const resolution = resolveNatsDriver(explicit);
  if (resolution.driver === 'modern') {
    return { driver: createModernDriver(), resolution };
  }
  return { driver: createLegacyDriver(), resolution };
}
