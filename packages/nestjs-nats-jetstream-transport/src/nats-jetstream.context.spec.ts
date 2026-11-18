/** Unit (no server, no driver): the context exposes a structural message
 * surface with a raw-message escape hatch. No `nats` import: the fakes below
 * are plain objects shaped like driver messages from either generation. */
import {
  NatsContext,
  NatsJetStreamContext,
  toCoreMessage,
  toJetStreamMessage,
} from './nats-jetstream.context';

function fakeHeaders() {
  const store = new Map<string, string>([['x-trace', 't-9']]);
  return {
    get: (k: string) => store.get(k) as string,
    append: (k: string, v: string) => void store.set(k, v),
  };
}

function fakeJsMsg() {
  return {
    subject: 'order.created',
    data: new TextEncoder().encode(JSON.stringify({ id: 7 })),
    headers: fakeHeaders(),
    info: {
      stream: 'mystream',
      consumer: 'durable',
      deliveryCount: 2,
      streamSequence: 41,
      deliverySequence: 7,
    },
    ack: jest.fn(),
    nak: jest.fn(),
    term: jest.fn(),
    working: jest.fn(),
  };
}

describe('NatsJetStreamContext / NatsContext (structural surface)', () => {
  it('exposes subject, headers and payload bytes through .message', () => {
    const ctx = new NatsJetStreamContext([fakeJsMsg()]);
    expect(ctx.message.subject).toBe('order.created');
    expect(ctx.message.headers?.get('x-trace')).toBe('t-9');
    expect(JSON.parse(new TextDecoder().decode(ctx.message.data))).toEqual({
      id: 7,
    });
  });

  it('ack/nak/term/working on context.message delegate to the raw message', () => {
    const msg = fakeJsMsg();
    const ctx = new NatsJetStreamContext([msg]);
    ctx.message.ack();
    ctx.message.nak(1000);
    ctx.message.working();
    ctx.message.term();
    expect(msg.ack).toHaveBeenCalledTimes(1);
    expect(msg.nak).toHaveBeenCalledWith(1000);
    expect(msg.working).toHaveBeenCalledTimes(1);
    expect(msg.term).toHaveBeenCalledTimes(1);
  });

  it('normalizes info with a single deliveryCount', () => {
    const ctx = new NatsJetStreamContext([fakeJsMsg()]);
    expect(ctx.message.info).toEqual({
      stream: 'mystream',
      consumer: 'durable',
      deliveryCount: 2,
      streamSequence: 41,
      deliverySequence: 7,
    });
  });

  it('defaults deliveryCount to 1 when info is absent', () => {
    const bare = toJetStreamMessage({ subject: 's' } as any);
    expect(bare.info.deliveryCount).toBe(1);
    expect(bare.headers).toBeUndefined();
  });

  it('exposes the raw driver message through the escape hatch', () => {
    const msg = fakeJsMsg();
    const ctx = new NatsJetStreamContext([msg]);
    expect(ctx.message.rawMessage).toBe(msg);
    expect(ctx.rawMessage).toBe(msg);
  });

  it('NatsContext exposes a core Msg the same way', () => {
    const respond = jest.fn();
    const msg = { subject: 'sum', data: new Uint8Array([1, 2, 3]), respond };
    const ctx = new NatsContext([msg]);
    expect(ctx.message.subject).toBe('sum');
    expect(ctx.message.data).toEqual(new Uint8Array([1, 2, 3]));
    ctx.message.respond(new Uint8Array([9]));
    expect(respond).toHaveBeenCalledWith(new Uint8Array([9]));
    expect(ctx.message.rawMessage).toBe(msg);
    expect(ctx.rawMessage).toBe(msg);
    expect(toCoreMessage(msg).subject).toBe('sum');
  });
});
