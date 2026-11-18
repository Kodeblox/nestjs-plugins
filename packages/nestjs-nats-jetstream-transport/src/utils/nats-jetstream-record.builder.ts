import type { JetStreamPublishOptions } from 'nats';

export class NatsJetStreamRecord<TData = any> {
  constructor(
    public readonly payload: TData,
    public readonly options: Partial<JetStreamPublishOptions>,
  ) {}
}

export class NatsJetStreamRecordBuilder<TData = any> {
  private options: Partial<JetStreamPublishOptions> = {};
  constructor(private payload?: TData) {}

  setMsgId(msgId: string) {
    this.options.msgID = msgId;
  }

  getMsgId(): string {
    return this.options.msgID;
  }

  appendHeader(key: string, value: string) {
    // Driver headers factory, loaded at call time so importing the package
    // never evaluates the driver (spec 001, section 4.2 rule 1). Legacy-path
    // call-time seam (see the scope note in `driver/nats-driver.ts`): Phase 3
    // must port the `headers()` shape alongside the driver, or inject it via
    // the driver interface.
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const { headers } = require('nats') as typeof import('nats');
    this.options.headers = this.options.headers || headers();
    this.options.headers.append(key, value);
  }

  build(): NatsJetStreamRecord<TData> {
    return new NatsJetStreamRecord<TData>(this.payload, this.options);
  }

  setExpectations(expect: Partial<JetStreamPublishOptions['expect']>) {
    this.options.expect = expect;
  }

  setPayload(payload: TData) {
    this.payload = payload;
  }
}
