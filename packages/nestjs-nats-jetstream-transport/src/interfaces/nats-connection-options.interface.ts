/**
 * Driver-neutral connection options (spec 001, section 4.2.3). This is the
 * plugin's own surface: the published `.d.ts` never references a driver
 * package. Named fields cover what the plugin and its demos use; anything
 * else rides the index signature and is forwarded to `connect()` untouched,
 * so exotic driver options keep working without a type change here.
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

export interface NatsConnectionOptions {
  name: string;
  connectedHook?: NatsConnectedHook;
  servers?: string | string[];
  timeout?: number;
  reconnect?: boolean;
  maxReconnectAttempts?: number;
  reconnectTimeWait?: number;
  user?: string;
  password?: string;
  token?: string;
  verbose?: boolean;
  pedantic?: boolean;
  /**
   * Backoff between steady-state reconnects. Also supplies the default
   * gap for the startup retry policy (spec 002): it is called once per
   * startup gap when `startupPolicy.delayMs` is unset.
   */
  reconnectDelayHandler?: () => number;
  /** Pass-through for any other driver connection option. */
  [option: string]: unknown;
}
