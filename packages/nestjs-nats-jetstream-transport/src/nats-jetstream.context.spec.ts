/** Unit (no server): context exposes the raw message surface handlers use. */
import { headers, JSONCodec } from 'nats';
import { NatsContext, NatsJetStreamContext } from './nats-jetstream.context';

function fakeJsMsg() {
  const hdrs = headers();
  hdrs.append('x-trace', 't-9');
  return {
    subject: 'order.created',
    data: JSONCodec().encode({ id: 7 }),
    headers: hdrs,
    ack: jest.fn(),
    nak: jest.fn(),
    term: jest.fn(),
    working: jest.fn(),
  } as any;
}

describe('NatsJetStreamContext / NatsContext', () => {
  it('exposes subject, headers and payload bytes through .message', () => {
    const msg = fakeJsMsg();
    const ctx = new NatsJetStreamContext([msg]);
    expect(ctx.message).toBe(msg);
    expect(ctx.message.subject).toBe('order.created');
    expect(ctx.message.headers.get('x-trace')).toBe('t-9');
    expect(JSONCodec().decode(ctx.message.data)).toEqual({ id: 7 });
  });

  it('ack/term/nak/working on context.message are the message operations', () => {
    const msg = fakeJsMsg();
    const ctx = new NatsJetStreamContext([msg]);
    ctx.message.ack();
    ctx.message.term();
    expect(msg.ack).toHaveBeenCalledTimes(1);
    expect(msg.term).toHaveBeenCalledTimes(1);
  });

  it('NatsContext exposes a core Msg the same way', () => {
    const msg = { subject: 'sum', data: JSONCodec().encode(3), respond: jest.fn() } as any;
    const ctx = new NatsContext([msg]);
    expect(ctx.message).toBe(msg);
    expect(ctx.message.subject).toBe('sum');
  });
});
