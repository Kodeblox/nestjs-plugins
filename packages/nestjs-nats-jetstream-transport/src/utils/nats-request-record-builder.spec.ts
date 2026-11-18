/** Unit (no server): request-options outcomes a core NATS request would use. */
import { NatsRequestOptionsBuilder, NatsRequestRecord } from './nats-request-record-builder';

describe('NatsRequestOptionsBuilder', () => {
  it('defaults timeout to 1000ms and carries payload', () => {
    const rec = new NatsRequestOptionsBuilder([1, 2]).build();
    expect(rec).toBeInstanceOf(NatsRequestRecord);
    expect(rec.payload).toEqual([1, 2]);
    expect(rec.options.timeout).toBe(1000);
    expect(rec.options.noMux).toBeUndefined();
    expect(rec.options.reply).toBeUndefined();
  });

  it('setTimeout overrides the default', () => {
    const b = new NatsRequestOptionsBuilder('p');
    b.setTimeout(250);
    expect(b.build().options.timeout).toBe(250);
  });

  it('setReplyToSubject sets reply and forces noMux', () => {
    const b = new NatsRequestOptionsBuilder('p');
    b.setReplyToSubject('reply.here');
    const opts = b.build().options;
    expect(opts.reply).toBe('reply.here');
    expect(opts.noMux).toBe(true);
  });

  it('setNoMux sets noMux independently', () => {
    const b = new NatsRequestOptionsBuilder('p');
    b.setNoMux(true);
    expect(b.build().options.noMux).toBe(true);
    b.setNoMux(false);
    expect(b.build().options.noMux).toBe(false);
  });

  it('appendHeader accumulates headers; setPayload replaces payload', () => {
    const b = new NatsRequestOptionsBuilder<string>('a');
    b.appendHeader('x-k', 'v1');
    b.appendHeader('x-k', 'v2');
    b.setPayload('b');
    const rec = b.build();
    expect(rec.payload).toBe('b');
    expect(rec.options.headers.values('x-k')).toEqual(['v1', 'v2']);
  });
});
