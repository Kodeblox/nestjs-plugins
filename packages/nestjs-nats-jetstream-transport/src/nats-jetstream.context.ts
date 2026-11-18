import { BaseRpcContext } from '@nestjs/microservices/ctx-host/base-rpc.context';

/**
 * Driver-neutral message surface (spec 001, section 4.2.3). This module
 * never imports `nats` or `@nats-io/*`, so the published `.d.ts` stays free
 * of driver types while handlers keep working unchanged across both driver
 * generations. Anything driver-specific remains reachable through
 * `rawMessage` (messages) — for the connection itself, the hook already
 * receives the raw driver object (see `NatsConnectionOptions`), so a cast is
 * the escape hatch there.
 */

export interface NatsMessageHeaders {
  get(key: string): string;
}

/**
 * Driver-neutral mutable header carrier. Satisfied structurally by both
 * drivers' header objects; builders create them, handlers read them.
 */
export interface NatsHeaders extends NatsMessageHeaders {
  set(key: string, value: string): void;
  append(key: string, value: string): void;
  has(key: string): boolean;
  keys(): string[];
  values(key: string): string[];
  delete(key: string): void;
  /**
   * Optional status metadata (protocol-level, e.g. created via a status
   * `headers()` factory call). Drivers translate only data headers: a
   * carrier that reports status metadata is rejected loudly instead of
   * being silently stripped.
   */
  readonly hasError?: boolean;
}

export interface NatsJetStreamMessageInfo {
  readonly stream: string;
  readonly consumer: string;
  /** 1-based delivery count, reported by both drivers; defaults to 1. */
  readonly deliveryCount: number;
  readonly streamSequence: number;
  readonly deliverySequence: number;
}

export interface NatsJetStreamMessage {
  readonly subject: string;
  readonly data: Uint8Array;
  readonly headers?: NatsMessageHeaders | undefined;
  ack(): void;
  nak(millis?: number): void;
  working(): void;
  term(reason?: string): void;
  readonly info: NatsJetStreamMessageInfo;
  /** Escape hatch to the underlying driver message (`JsMsg` in both
   * generations) for driver-specific access. */
  readonly rawMessage: unknown;
}

export interface NatsCoreMessage {
  readonly subject: string;
  readonly data: Uint8Array;
  readonly headers?: NatsMessageHeaders | undefined;
  respond(payload: Uint8Array): void;
  /** Escape hatch to the underlying driver message (`Msg` in both
   * generations) for driver-specific access. */
  readonly rawMessage: unknown;
}

function normalizeDeliveryCount(info: any): number {
  if (typeof info?.deliveryCount === 'number') {
    return info.deliveryCount;
  }
  return 1;
}

class JetStreamMessageView implements NatsJetStreamMessage {
  constructor(private readonly raw: any) {}

  get subject(): string {
    return this.raw?.subject;
  }

  get data(): Uint8Array {
    return this.raw?.data;
  }

  get headers(): NatsMessageHeaders | undefined {
    return this.raw?.headers ?? undefined;
  }

  ack(): void {
    this.raw?.ack();
  }

  nak(millis?: number): void {
    this.raw?.nak(millis);
  }

  working(): void {
    this.raw?.working();
  }

  term(reason?: string): void {
    this.raw?.term(reason);
  }

  get info(): NatsJetStreamMessageInfo {
    const info = this.raw?.info;
    return {
      stream: info?.stream ?? '',
      consumer: info?.consumer ?? '',
      deliveryCount: normalizeDeliveryCount(info),
      streamSequence: info?.streamSequence ?? 0,
      deliverySequence: info?.deliverySequence ?? 0,
    };
  }

  get rawMessage(): unknown {
    return this.raw;
  }
}

class CoreMessageView implements NatsCoreMessage {
  constructor(private readonly raw: any) {}

  get subject(): string {
    return this.raw?.subject;
  }

  get data(): Uint8Array {
    return this.raw?.data;
  }

  get headers(): NatsMessageHeaders | undefined {
    return this.raw?.headers ?? undefined;
  }

  respond(payload: Uint8Array): void {
    this.raw?.respond(payload);
  }

  get rawMessage(): unknown {
    return this.raw;
  }
}

/** Adapt any driver-generation JetStream message to the structural surface. */
export function toJetStreamMessage(raw: unknown): NatsJetStreamMessage {
  return new JetStreamMessageView(raw);
}

/** Adapt any driver-generation core message to the structural surface. */
export function toCoreMessage(raw: unknown): NatsCoreMessage {
  return new CoreMessageView(raw);
}

export type NatsJetStreamContextArgs = [NatsJetStreamMessage];
export type NatsContextArgs = [NatsCoreMessage];

export class NatsJetStreamContext extends BaseRpcContext<NatsJetStreamContextArgs> {
  constructor(args: [unknown]) {
    super([toJetStreamMessage(args[0])]);
  }

  get message(): NatsJetStreamMessage {
    return this.args[0];
  }

  get rawMessage(): unknown {
    return this.args[0].rawMessage;
  }
}

export class NatsContext extends BaseRpcContext<NatsContextArgs> {
  constructor(args: [unknown]) {
    super([toCoreMessage(args[0])]);
  }

  get message(): NatsCoreMessage {
    return this.args[0];
  }

  get rawMessage(): unknown {
    return this.args[0].rawMessage;
  }
}
