/**
 * Spec 002, Part B — startup policy (unit, no server).
 *
 * Same faithful fake at the `nats` driver boundary as `server.spec.ts`:
 * only `connect` is replaced. Part A (callback-once routing) is asserted
 * in `server.spec.ts`; these tests cover the retry policy (attempt counts,
 * fatal abort, unknown modes, hook failures, single attach, delay
 * defaults). The file-level unhandled-rejection guard applies throughout.
 */
import { connect, JSONCodec } from 'nats';
import { NatsJetStreamServer } from './server';

jest.mock('nats', () => {
  const actual = jest.requireActual('nats');
  return { ...actual, connect: jest.fn() };
});

const codec = JSONCodec();

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
  destroy = jest.fn(async () => undefined);
  [Symbol.asyncIterator]() {
    return {
      next: (): Promise<IteratorResult<any>> => {
        if (this.queue.length)
          return Promise.resolve({ value: this.queue.shift(), done: false });
        if (this.ended)
          return Promise.resolve({ value: undefined, done: true });
        return new Promise((res) => (this.waiter = res));
      },
    };
  }
}

function refusedError() {
  return Object.assign(new Error('connection refused'), {
    code: 'CONNECTION_REFUSED',
  });
}

function authError() {
  return Object.assign(new Error('Authorization Violation'), {
    code: 'AUTHORIZATION_VIOLATION',
  });
}

describe('NATS startup policy (spec 002)', () => {
  let registry: Map<string, any>;
  let streamsAdd: jest.Mock;
  let jsSubscribe: jest.Mock;
  let coreSubscribe: jest.Mock;
  let fakeNc: any;
  let unhandled: unknown[];
  let onUnhandled: (err: unknown) => void;

  const listenResult = (server: NatsJetStreamServer) =>
    new Promise<{ calls: unknown[][] } & { err: unknown }>((resolve) => {
      const calls: unknown[][] = [];
      void server.listen((err?: unknown) => {
        calls.push([err]);
        resolve({ calls, err });
      });
    });

  const flush = async () => new Promise((r) => setTimeout(r, 20));

  beforeEach(() => {
    registry = new Map();
    streamsAdd = jest.fn(async (cfg: any) => {
      const info = { config: cfg };
      registry.set(cfg.name, info);
      return info;
    });
    jsSubscribe = jest.fn(async () => new FakeSubscription());
    coreSubscribe = jest.fn();
    const jsm = {
      streams: {
        list: () => {
          const items: any[] = [...registry.values()];
          return {
            next: async () => items,
            [Symbol.asyncIterator]: async function* () {
              yield* items;
            },
          };
        },
        add: streamsAdd,
        update: jest.fn(),
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
    unhandled = [];
    onUnhandled = (err: unknown) => {
      unhandled.push(err);
    };
    process.on('unhandledRejection', onUnhandled);
  });

  afterEach(async () => {
    await flush();
    process.removeListener('unhandledRejection', onUnhandled);
    expect(unhandled).toEqual([]);
  });

  function makeServer(extra: any = {}) {
    const server = new NatsJetStreamServer({
      connectionOptions: { name: 'unit-startup' },
      consumerOptions: { durable: 'd1', manualAck: true },
      driver: 'legacy',
      ...extra,
    } as any);
    jest
      .spyOn((server as any).logger, 'log')
      .mockImplementation(() => undefined);
    jest
      .spyOn((server as any).logger, 'error')
      .mockImplementation(() => undefined);
    return server;
  }

  it('Part B default is fail-fast: one attempt, callback(err) once', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: { onAttemptFailed: (info: any) => attempts.push(info) },
    });
    const { err } = await listenResult(server);
    expect(err).toBeDefined();
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({
      attempt: 1,
      maxAttempts: 1,
      recoverable: true,
    });
  });

  it('an async rejecting onAttemptFailed hook is swallowed: callback once, no unhandled rejection', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const server = makeServer({
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 2,
        delayMs: 1,
        onAttemptFailed: async () => {
          await new Promise((r) => setTimeout(r, 1));
          throw new Error('hook boom');
        },
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(2);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback.mock.calls[0][0]).toBeDefined();
    await flush();
  });

  it('an unknown startupPolicy.mode fails fast with a clear error, never retries silently', async () => {
    const server = makeServer({
      startupPolicy: {
        mode: 'retr',
        maxAttempts: 5,
        delayMs: 1,
      } as any,
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    expect((callback.mock.calls[0][0] as Error).message).toMatch(
      /Unknown NATS startupPolicy\.mode 'retr'/,
    );
    expect(connect as jest.Mock).not.toHaveBeenCalled();
    await flush();
  });

  it('Part B retry: exactly maxAttempts attempts, then callback(err) once', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 3,
        delayMs: 5,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(3);
    expect(attempts).toHaveLength(3);
    expect(attempts.map((a) => a.attempt)).toEqual([1, 2, 3]);
    expect(attempts.every((a) => a.recoverable)).toBe(true);
    expect(attempts.every((a) => a.maxAttempts === 3)).toBe(true);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback.mock.calls[0][0]).toBeDefined();
  });

  it('Part B: fatal auth error aborts a retry loop after the first attempt', async () => {
    (connect as jest.Mock).mockRejectedValue(authError());
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 5,
        delayMs: 1,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0]).toMatchObject({ attempt: 1, recoverable: false });
    expect(callback).toHaveBeenCalledTimes(1);
    expect((callback.mock.calls[0][0] as Error).message).toMatch(
      /Authorization Violation/,
    );
  });

  it('Part B: fatal bad stream config aborts without retrying', async () => {
    // Realistic JetStream validation failure (observed message shape
    // against a real broker; no numeric code on the legacy error).
    streamsAdd.mockRejectedValue(
      new Error("invalid stream name - stream name cannot contain ' '"),
    );
    const attempts: any[] = [];
    const server = makeServer({
      streamConfig: { name: 's', subjects: ['x'] },
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 5,
        delayMs: 1,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    expect((callback.mock.calls[0][0] as Error).message).toMatch(/invalid/i);
    // Connected once (the dial worked), but no second startup attempt ran.
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0].recoverable).toBe(false);
  });

  it('Part B: deterministic consumer misconfiguration is fatal, not retried', async () => {
    jsSubscribe.mockRejectedValue(
      new Error('push consumer requires deliver_subject'),
    );
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 5,
        delayMs: 1,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    server.addHandler('order.created', (async () => undefined) as any, true);
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0].recoverable).toBe(false);
  });

  it('Part B: broker returns mid-retry ends in a single attach, callback once', async () => {
    let calls = 0;
    (connect as jest.Mock).mockImplementation(async () => {
      calls += 1;
      if (calls < 3) throw refusedError();
      return fakeNc;
    });
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry',
        maxAttempts: 5,
        delayMs: 5,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    server.addHandler('order.created', (async () => undefined) as any, true);
    server.addHandler({ cmd: 'sum' }, (async (data: number[]) =>
      (data || []).reduce((a, b) => a + b, 0)) as any);
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback.mock.calls[0][0]).toBeUndefined();
    expect(calls).toBe(3);
    expect(attempts).toHaveLength(2);
    // Single attach: one subscription per handler, no duplicates across
    // the failed attempts.
    expect(jsSubscribe).toHaveBeenCalledTimes(1);
    expect(coreSubscribe).toHaveBeenCalledTimes(1);
  });

  it('Part B retry-forever: reports no cap and keeps trying until success', async () => {
    let calls = 0;
    (connect as jest.Mock).mockImplementation(async () => {
      calls += 1;
      if (calls < 4) throw refusedError();
      return fakeNc;
    });
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry-forever',
        delayMs: 5,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(calls).toBe(4);
    expect(attempts).toHaveLength(3);
    expect(attempts.every((a) => a.maxAttempts === undefined)).toBe(true);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(callback.mock.calls[0][0]).toBeUndefined();
  });

  it('Part B retry-forever still aborts at once on a fatal error', async () => {
    (connect as jest.Mock).mockRejectedValue(
      Object.assign(new Error('address already in use'), {
        code: 'EADDRINUSE',
      }),
    );
    const attempts: any[] = [];
    const server = makeServer({
      startupPolicy: {
        mode: 'retry-forever',
        delayMs: 1,
        onAttemptFailed: (info: any) => attempts.push(info),
      },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(attempts).toHaveLength(1);
    expect(attempts[0].recoverable).toBe(false);
    expect(callback).toHaveBeenCalledTimes(1);
  });

  it('Part B delay default calls reconnectDelayHandler once per gap', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const backoff = jest.fn(() => 5);
    const server = makeServer({
      connectionOptions: {
        name: 'unit-startup',
        reconnectDelayHandler: backoff,
      },
      startupPolicy: { mode: 'retry', maxAttempts: 3 },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    // Two gaps between three attempts.
    expect(backoff).toHaveBeenCalledTimes(2);
  });

  it('Part B explicit delayMs wins over reconnectDelayHandler', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const backoff = jest.fn(() => 5);
    const server = makeServer({
      connectionOptions: {
        name: 'unit-startup',
        reconnectDelayHandler: backoff,
      },
      startupPolicy: { mode: 'retry', maxAttempts: 2, delayMs: 1 },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback).toHaveBeenCalledTimes(1);
    expect(backoff).not.toHaveBeenCalled();
  });

  it('Part B ignores maxReconnectAttempts for the startup cap', async () => {
    (connect as jest.Mock).mockRejectedValue(refusedError());
    const server = makeServer({
      connectionOptions: { name: 'unit-startup', maxReconnectAttempts: 100 },
      startupPolicy: { mode: 'retry', maxAttempts: 2, delayMs: 1 },
    });
    const callback = jest.fn();
    await server.listen(callback);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(2);
    expect(callback).toHaveBeenCalledTimes(1);
  });

  it('delivers through a mid-retry attach exactly once', async () => {
    let calls = 0;
    (connect as jest.Mock).mockImplementation(async () => {
      calls += 1;
      if (calls < 2) throw refusedError();
      return fakeNc;
    });
    const received: unknown[] = [];
    const server = makeServer({
      startupPolicy: { mode: 'retry', maxAttempts: 3, delayMs: 1 },
    });
    server.addHandler(
      'order.created',
      (async (data: unknown) => {
        received.push(data);
      }) as any,
      true,
    );
    const callback = jest.fn();
    await server.listen(callback);
    expect(callback.mock.calls[0][0]).toBeUndefined();
    const sub: FakeSubscription = await jsSubscribe.mock.results[0].value;
    sub.push({ subject: 'order.created', data: codec.encode({ id: 7 }) });
    await new Promise((r) => setTimeout(r, 50));
    expect(received).toEqual([{ id: 7 }]);
    sub.end();
  });
});
