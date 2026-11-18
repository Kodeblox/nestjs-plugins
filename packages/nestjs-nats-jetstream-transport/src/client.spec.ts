/**
 * Unit (no server): faithful fake at the `nats` driver boundary.
 * Only `connect` is replaced (real JSONCodec/headers kept); the fake
 * JetStream/core connection captures the exact bytes + options that
 * would go on the wire and returns PubAck / response messages, so
 * assertions are on observable publish/request outcomes, not internals.
 * The real round-trip is covered by the integration suite.
 */
import { connect, JSONCodec } from 'nats';
import { firstValueFrom } from 'rxjs';
import { NatsJetStreamClientProxy } from './client';
import { NatsJetStreamRecordBuilder } from './utils/nats-jetstream-record.builder';
import { NatsRequestOptionsBuilder } from './utils/nats-request-record-builder';

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

describe('NatsJetStreamClientProxy (fake driver boundary)', () => {
  let publish: jest.Mock;
  let request: jest.Mock;
  let drain: jest.Mock;
  let fakeNc: any;
  let client: NatsJetStreamClientProxy;

  beforeEach(() => {
    publish = jest.fn(async () => ({ stream: 'mystream', seq: 42, duplicate: false }));
    request = jest.fn(async () => ({
      data: codec.encode({ response: 6, isDisposed: true } as any),
    }));
    drain = jest.fn(async () => undefined);
    fakeNc = {
      jetstream: jest.fn(() => ({ publish })),
      request,
      drain,
      isClosed: () => false,
      isDraining: () => false,
    };
    (connect as jest.Mock).mockReset().mockResolvedValue(fakeNc);
    client = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client', servers: 'nats://fake:4222' },
      // These suites pin the legacy path (fake `nats` boundary); the modern
      // path has its own driver-boundary coverage.
      driver: 'legacy',
    } as any);
  });

  it('connect() resolves the connection and invokes connectedHook with it', async () => {
    const hook = jest.fn();
    const c = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client', connectedHook: hook },
      driver: 'legacy',
    } as any);
    await expect(c.connect()).resolves.toBe(fakeNc);
    expect(hook).toHaveBeenCalledWith(fakeNc);
  });

  it('awaits an async connectedHook before resolving connect()', async () => {
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const hook = jest.fn(async () => {
      await gate;
    });
    const c = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client', connectedHook: hook },
      driver: 'legacy',
    } as any);
    let settled = false;
    const pending = c.connect().then((nc) => {
      settled = true;
      return nc;
    });
    await new Promise((r) => setTimeout(r, 20));
    expect(hook).toHaveBeenCalledTimes(1);
    expect(settled).toBe(false);
    release();
    await expect(pending).resolves.toBe(fakeNc);
    expect(settled).toBe(true);
  });

  it('propagates a rejecting connectedHook from connect()', async () => {
    const c = new NatsJetStreamClientProxy({
      connectionOptions: {
        name: 'unit-client',
        connectedHook: async () => {
          throw new Error('hook boom');
        },
      },
      driver: 'legacy',
    } as any);
    await expect(c.connect()).rejects.toThrow('hook boom');
  });

  it('drains and clears the connection when connectedHook throws', async () => {
    const hook: jest.Mock = jest.fn(async () => {
      throw new Error('hook boom');
    });
    const c = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client', connectedHook: hook },
      driver: 'legacy',
    } as any);
    await expect(c.connect()).rejects.toThrow('hook boom');
    // Drained, not orphaned: the dialed connection is released...
    expect(drain).toHaveBeenCalledTimes(1);
    // ...and the next connect() dials anew instead of reusing an orphan.
    hook.mockImplementation(async () => undefined);
    (connect as jest.Mock).mockClear();
    await expect(c.connect()).resolves.toBe(fakeNc);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
    expect(hook).toHaveBeenCalledTimes(2);
  });

  it('shares one connection for concurrent connect() calls', async () => {
    const c = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client' },
      driver: 'legacy',
    } as any);
    (connect as jest.Mock).mockClear();
    const [a, b] = await Promise.all([c.connect(), c.connect()]);
    expect(a).toBe(b);
    expect(connect as jest.Mock).toHaveBeenCalledTimes(1);
  });

  it('rejects connect() when driver resolution throws', async () => {
    resolveMock.mockImplementationOnce(() => {
      throw new Error('NATS driver mismatch (test)');
    });
    const c = new NatsJetStreamClientProxy({
      connectionOptions: { name: 'unit-client' },
      driver: 'modern',
    } as any);
    await expect(c.connect()).rejects.toThrow('NATS driver mismatch (test)');
  });

  it('logs the both-drivers notice once from connect()', async () => {
    resolveMock.mockReturnValueOnce({
      driver: 'legacy',
      legacyAvailable: true,
      modernAvailable: true,
      notice: 'Both drivers installed (test)',
    });
    const logSpy = jest
      .spyOn((client as any).logger, 'log')
      .mockImplementation(() => undefined);
    await client.connect();
    expect(logSpy).toHaveBeenCalledTimes(1);
    expect(logSpy).toHaveBeenCalledWith('Both drivers installed (test)');
    logSpy.mockRestore();
  });

  it('emit() publishes JSON bytes to the subject and resolves with the PubAck', async () => {
    const ack = await firstValueFrom(
      client.emit('order.created', { id: 1, product: 'Socks' }),
    );
    expect(ack).toEqual({ stream: 'mystream', seq: 42, duplicate: false });
    expect(publish).toHaveBeenCalledTimes(1);
    const [subject, bytes, opts] = publish.mock.calls[0];
    expect(subject).toBe('order.created');
    expect(codec.decode(bytes)).toEqual({ id: 1, product: 'Socks' });
    expect(opts).toBeUndefined();
  });

  it('emit() with a record carries msgID, headers and expectations to the publish', async () => {
    const builder = new NatsJetStreamRecordBuilder({ id: 2 });
    builder.setMsgId('msg-1');
    builder.appendHeader('x-trace', 't-1');
    builder.setExpectations({ streamName: 'mystream' });
    await firstValueFrom(client.emit('order.created', builder.build()));
    const [, bytes, opts] = publish.mock.calls[0];
    expect(codec.decode(bytes)).toEqual({ id: 2 });
    expect(opts.msgID).toBe('msg-1');
    expect(opts.headers.get('x-trace')).toBe('t-1');
    expect(opts.expect).toEqual({ streamName: 'mystream' });
  });

  it('emit() surfaces a JetStream publish failure as an observable error', async () => {
    publish.mockRejectedValue(new Error('no stream'));
    await expect(
      firstValueFrom(client.emit('order.created', { id: 3 })),
    ).rejects.toThrow('no stream');
  });

  it('send() performs a core request and returns the decoded response', async () => {
    const result = await firstValueFrom(client.send({ cmd: 'sum' }, [1, 2, 3]));
    expect(result).toBe(6);
    const [subject, bytes, opts] = request.mock.calls[0];
    expect(subject).toBe(JSON.stringify({ cmd: 'sum' }));
    expect(codec.decode(bytes)).toEqual([1, 2, 3]);
    expect(opts).toBeUndefined();
  });

  it('send() with a request record carries timeout / reply / headers to the request', async () => {
    const builder = new NatsRequestOptionsBuilder([4, 5]);
    builder.setTimeout(250);
    builder.setReplyToSubject('reply.here');
    builder.appendHeader('x-k', 'v');
    const result = await firstValueFrom(
      client.send({ cmd: 'sum' }, builder.build()),
    );
    expect(result).toBe(6);
    const [, bytes, opts] = request.mock.calls[0];
    expect(codec.decode(bytes)).toEqual([4, 5]);
    expect(opts.timeout).toBe(250);
    expect(opts.reply).toBe('reply.here');
    expect(opts.noMux).toBe(true);
    expect(opts.headers.get('x-k')).toBe('v');
  });

  it('send() surfaces a request failure (e.g. timeout) as an observable error', async () => {
    request.mockRejectedValue(new Error('timeout'));
    await expect(
      firstValueFrom(client.send({ cmd: 'sum' }, [1])),
    ).rejects.toThrow('timeout');
  });

  it('close() drains the connection', async () => {
    await client.connect();
    await client.close();
    expect(drain).toHaveBeenCalledTimes(1);
  });
});
