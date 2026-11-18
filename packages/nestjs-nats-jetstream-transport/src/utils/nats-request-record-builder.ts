import type { NatsHeaders } from '../nats-jetstream.context';
import { createHeaders } from '../driver/driver-headers';

/** Driver-neutral core request options (spec 001, section 4.2.3). */
export interface NatsRequestOptions {
  timeout?: number;
  headers?: NatsHeaders;
  reply?: string;
  noMux?: boolean;
  /** Pass-through for any other request option. */
  [option: string]: unknown;
}

export class NatsRequestRecord<TData = any> {
  constructor(
    public readonly payload: TData,
    public readonly options: NatsRequestOptions,
  ) {}
}

export class NatsRequestOptionsBuilder<TData = any> {
  private options: NatsRequestOptions = {
    timeout: 1000,
  };
  constructor(private payload?: TData) {}

  appendHeader(key: string, value: string) {
    // Headers factory resolved per driver package at call time (modern
    // first, legacy fallback), so importing the package never evaluates a
    // driver and a modern-only install never touches `nats` (spec 001,
    // sections 4.2 rule 1 and 4.4). Either carrier shape stays interoperable:
    // both drivers translate `keys()`/`values()` carriers at request time.
    this.options.headers = this.options.headers || createHeaders();
    this.options.headers.append(key, value);
  }

  build(): NatsRequestRecord<TData> {
    return new NatsRequestRecord<TData>(this.payload, this.options);
  }

  setPayload(payload: TData) {
    this.payload = payload;
  }

  setReplyToSubject(replyTo: string) {
    this.options.reply = replyTo;
    this.options.noMux = true;
  }

  setTimeout(value: number) {
    this.options.timeout = value;
  }

  setNoMux(value: boolean) {
    this.options.noMux = value;
  }
}
