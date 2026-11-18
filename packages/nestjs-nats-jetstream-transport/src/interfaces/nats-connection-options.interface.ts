import type { ConnectionOptions } from 'nats';

/**
 * Minimal structural surface of a driver connection passed to
 * `connectedHook`. Satisfied by both the legacy `nats` `NatsConnection` and
 * the modern `@nats-io/transport-node` connection, so the hook type no
 * longer leaks a driver package into the public `.d.ts`.
 *
 * The value received IS the raw driver connection object — cast it to your
 * generation's connection type for full API access beyond this surface.
 */
export interface NatsConnectionInfo {
  getServer(): string;
  status(): AsyncIterable<unknown>;
  drain(): Promise<void>;
  isClosed(): boolean;
  isDraining(): boolean;
}

export type NatsConnectedHook = (
  connection: NatsConnectionInfo,
) => void | Promise<void>;

// Full decoupling of the options heritage (`ConnectionOptions` above) waits
// on the adapter phase; `import type` keeps it runtime-clean and elided.
export interface NatsConnectionOptions extends ConnectionOptions {
  name: string;
  connectedHook?: NatsConnectedHook;
}
