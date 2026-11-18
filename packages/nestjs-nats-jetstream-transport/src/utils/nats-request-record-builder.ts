import type { RequestOptions } from 'nats';

export class NatsRequestRecord<TData = any> {
  constructor(
    public readonly payload: TData,
    public readonly options: RequestOptions,
  ) {}
}

export class NatsRequestOptionsBuilder<TData = any> {
  private options: RequestOptions = {
    timeout: 1000,
  };
  constructor(private payload?: TData) {}

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
