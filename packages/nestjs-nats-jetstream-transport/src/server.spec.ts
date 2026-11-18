/**
 * Unit (no server): faithful fake at the `nats` driver boundary.
 * Only `connect` is replaced (real JSONCodec/headers kept). The fake
 * JetStreamManager keeps an in-memory stream registry, the fake
 * JetStream subscription is an async iterable fed by the test, and the
 * fake core subscription captures its callback - so assertions are on
 * observable outcomes: streams created/updated in the registry,
 * payloads contexts handlers receive, bytes responded, and term() on
 * handler error. Real delivery is covered by the integration suite.
 */
import { connect, JSONCodec } from 'nats';
import { NatsJetStreamServer } from './server';

jest.mock('nats', () => {
  const actual = jest.requireActual('nats');
  return { ...actual, connect: jest.fn() };
});

jest.mock('./driver/resolve-driver', () => {
  const actual = jest.requireActual('./driver/resolve-driver');
  return { ...actual, resolveNatsDriver: jest.fn(actual.resolveNatsDriver) };
});

import { resolveNatsDriver } from './driver/resolve-driver';

const resolveMock = resolveNatsDriver as unknown as jest.Mock;

const codec = JSONCodec();
const waitFor = async (cond: () => boolean, ms = 5000) => {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > ms) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 10));
  }
};

class FakeSubscription {
  private queue: any[] = [];
  private waiter: ((v: IteratorResult<any>) => void) | null = null;
  private ended = false;
  push(msg: any) {
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: msg, done: false });
    } else this.queue.push(msg);
  }
  end() {
    this.ended = true;
    if (this.waiter) {
      const w = this.waiter;
      this.waiter = null;
      w({ value: undefined, done: true });
    }
  }
  destroy = jest.fn();
  [Symbol.asyncIterator]() {
    return {
      next: (): Promise<IteratorResult<any>> => {
        if (this.queue.length)
          return Promise.resolve({ value: this.queue.shift(), done: false });
        if (this.ended) return Promise.resolve({ value: undefined, done: true });
        return new Promise((res) => (this.waiter = res));
      },
    };
  }
}

function fakeMsg(subject: string, payload: unknown) {
  return {
    subject,
    data: codec.encode(payload as any),
    headers: undefined,
    ack: jest.fn(),
    nak: jest.fn(),
    term: jest.fn(),
    working: jest.fn(),
    respond: jest.fn(),
  } as any;
}

describe('NatsJetStreamServer (fake driver boundary)', () => {
  let registry: Map<string, any>;
  let streamsAdd: jest.Mock;
  let streamsUpdate: jest.Mock;
  let jsSubscribe: jest.Mock;
  let sub: FakeSubscription;
  let coreSubscribe: jest.Mock;
  let fakeNc: any;

  beforeEach(() => {
    registry = new Map();
    streamsAdd = jest.fn(async (cfg: any) => {
      const info = { config: cfg };
      registry.set(cfg.name, info);
      return info;
    });
    streamsUpdate = jest.fn(async (name: string, cfg: any) => {
      const info = { config: cfg };
      registry.set(name, info);
      return info;
    });
    sub = new FakeSubscription();
    jsSubscribe = jest.fn(async () => sub);
    coreSubscribe = jest.fn();
    const jsm = {
      streams: {
        list: () => ({
          next: async () => [...registry.values()],
        }),
        add: streamsAdd,
        update: streamsUpdate,
      },
    };
    fakeNc = {
      jetstreamManager: jest.fn(async () => jsm),
      jetstream: jest.fn(() => ({ subscribe: jsSubscribe })),
      subscribe: coreSubscribe,
      drain: jest.fn(async () => undefined),
      isClosed: () => false,
      isDraining: () => false,
    };
    (connect as jest.Mock).mockReset().mockResolvedValue(fakeNc);
  });

  function makeServer(extra: any = {}) {
    return new NatsJetStreamServer({
      connectionOptions: { name: 'unit-server' },
      consumerOptions: { durable: 'd1', manualAck: true },
      ...extra,
    } as any);
  }

  it('auto-creates a missing stream from a single streamConfig', async () => {
    const server = makeServer({
      streamConfig: { name: 'mystream', subjects: ['order.*'] },
    });
    await new Promise<void>((res) => server.listen(res));
    expect(registry.get('mystream')?.config.subjects).toEqual(['order.*']);
    expect(streamsUpdate).not.toHaveBeenCalled();
  });

  it('auto-creates every stream from an array streamConfig', async () => {
    const server = makeServer({
      streamConfig: [
        { name: 'a-stream', subjects: ['a.*'] },
        { name: 'b-stream', subjects: ['b.*'] },
      ],
    });
    await new Promise<void>((res) => server.listen(res));
    expect([...registry.keys()].sort()).toEqual(['a-stream', 'b-stream']);
  });

  it('updates an existing stream, merging (not replacing) subjects', async () => {
    registry.set('mystream', {
      config: { name: 'mystream', subjects: ['order.*'], retention: 'limits' },
    });
    const server = makeServer({
      streamConfig: { name: 'mystream', subjects: ['order.*', 'invoice.*'] },
    });
    await new Promise<void>((res) => server.listen(res));
    expect(streamsAdd).not.toHaveBeenCalled();
    const cfg = registry.get('mystream').config;
    expect(new Set(cfg.subjects)).toEqual(new Set(['order.*', 'invoice.*']));
  });

  it('delivers event payload + context to the handler; handler can ack via context', async () => {
    const received: any[] = [];
    const server = makeServer();
    server.addHandler(
      'order.created',
      (async (data: any, ctx: any) => {
        received.push({ data, subject: ctx.message.subject });
        ctx.message.ack();
      }) as any,
      true,
    );
    await new Promise<void>((res) => server.listen(res));
    // Assert the subscribed options' outcome at the subscribe boundary,
    // not merely that *something* was passed: the real builder's
    // getOpts().config is what the server would create the consumer with.
    expect(jsSubscribe).toHaveBeenCalledTimes(1);
    const [subscribedSubject, subscribedOpts] = jsSubscribe.mock.calls[0];
    expect(subscribedSubject).toBe('order.created');
    const subscribedConfig = subscribedOpts.getOpts().config;
    expect(subscribedConfig.durable_name).toBe('d1-order_created');
    expect(subscribedConfig.ack_policy).toBe('explicit');
    expect(subscribedOpts.getOpts().mack).toBe(true);
    const msg = fakeMsg('order.created', { id: 11 });
    sub.push(msg);
    await waitFor(
      () => received.length === 1 && msg.ack.mock.calls.length === 1,
    );
    expect(received).toEqual([{ data: { id: 11 }, subject: 'order.created' }]);
    expect(msg.ack).toHaveBeenCalledTimes(1);
    expect(msg.term).not.toHaveBeenCalled();
    sub.end();
  });

  it('terms (does not ack) a message whose event handler throws', async () => {
    const server = makeServer();
    server.addHandler(
      'order.poison',
      (async () => {
        throw new Error('boom');
      }) as any,
      true,
    );
    await new Promise<void>((res) => server.listen(res));
    const msg = fakeMsg('order.poison', { id: 12 });
    sub.push(msg);
    await waitFor(() => msg.term.mock.calls.length === 1);
    expect(msg.term).toHaveBeenCalledTimes(1);
    expect(msg.ack).not.toHaveBeenCalled();
    sub.end();
  });

  it('answers a message handler by responding with the encoded result', async () => {
    const server = makeServer();
    server.addHandler(
      { cmd: 'sum' },
      (async (data: number[]) =>
        (data || []).reduce((a, b) => a + b, 0)) as any,
    );
    await new Promise<void>((res) => server.listen(res));
    expect(coreSubscribe).toHaveBeenCalledTimes(1);
    const [, opts] = coreSubscribe.mock.calls[0];
    const msg = fakeMsg('sum-subject', [1, 2, 3]);
    await opts.callback(null, msg);
    await waitFor(() => msg.respond.mock.calls.length === 1);
    expect(msg.respond).toHaveBeenCalledTimes(1);
    const responded = codec.decode(msg.respond.mock.calls[0][0]);
    expect(responded).toEqual({ response: 6, isDisposed: true });
  });

  it('close() drains the connection', async () => {
    const server = makeServer();
    await new Promise<void>((res) => server.listen(res));
    await server.close();
    expect(fakeNc.drain).toHaveBeenCalledTimes(1);
  });

  it('awaits an async connectedHook before finishing listen()', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const server = makeServer({
      connectionOptions: {
        name: 'unit-server',
        connectedHook: async () => {
          await gate;
        },
      },
    });
    let called = false;
    const listening = server.listen(() => {
      called = true;
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(called).toBe(false);
    release();
    await listening;
    expect(called).toBe(true);
  });

  it('propagates a rejecting connectedHook from listen()', async () => {
    const server = makeServer({
      connectionOptions: {
        name: 'unit-server',
        connectedHook: async () => {
          throw new Error('hook boom');
        },
      },
    });
    await expect(server.listen(() => undefined)).rejects.toThrow('hook boom');
  });

  it('rejects listen() when driver resolution throws', async () => {
    resolveMock.mockImplementationOnce(() => {
      throw new Error('NATS driver mismatch (test)');
    });
    const server = makeServer({ driver: 'modern' });
    await expect(server.listen(() => undefined)).rejects.toThrow(
      'NATS driver mismatch (test)',
    );
  });

  it('logs the both-drivers notice once from listen()', async () => {
    resolveMock.mockReturnValueOnce({
      driver: 'legacy',
      legacyAvailable: true,
      modernAvailable: true,
      notice: 'Both drivers installed (test)',
    });
    const server = makeServer();
    const logSpy = jest
      .spyOn((server as any).logger, 'log')
      .mockImplementation(() => undefined);
    await new Promise<void>((res) => server.listen(res));
    expect(logSpy).toHaveBeenCalledWith('Both drivers installed (test)');
    logSpy.mockRestore();
  });
});
