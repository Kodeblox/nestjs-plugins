import type { NatsHeaders } from '../nats-jetstream.context';
import { createHeaders } from '../driver/driver-headers';

/** Driver-neutral JetStream publish options (spec 001, section 4.2.3). */
export interface NatsJetStreamPublishOptions {
  msgID?: string;
  headers?: NatsHeaders;
  timeout?: number;
  expect?: {
    lastMsgID?: string;
    streamName?: string;
    lastSequence?: number;
    lastSubjectSequence?: number;
  };
  /** Pass-through for any other publish option. */
  [option: string]: unknown;
}

export class NatsJetStreamRecord<TData = any> {
  constructor(
    public readonly payload: TData,
    public readonly options: Partial<NatsJetStreamPublishOptions>,
  ) {}
}

export class NatsJetStreamRecordBuilder<TData = any> {
  private options: Partial<NatsJetStreamPublishOptions> = {};
  constructor(private payload?: TData) {}

  setMsgId(msgId: string) {
    this.options.msgID = msgId;
  }

  getMsgId(): string | undefined {
    return this.options.msgID;
  }

  appendHeader(key: string, value: string) {
    // Headers factory resolved per driver package at call time (modern
    // first, legacy fallback), so importing the package never evaluates a
    // driver and a modern-only install never touches `nats` (spec 001,
    // sections 4.2 rule 1 and 4.4). Either carrier shape stays interoperable:
    // both drivers translate `keys()`/`values()` carriers at publish time.
    this.options.headers = this.options.headers || createHeaders();
    this.options.headers.append(key, value);
  }

  build(): NatsJetStreamRecord<TData> {
    return new NatsJetStreamRecord<TData>(this.payload, this.options);
  }

  setExpectations(expect: Partial<NatsJetStreamPublishOptions['expect']>) {
    this.options.expect = expect;
  }

  setPayload(payload: TData) {
    this.payload = payload;
  }
}
