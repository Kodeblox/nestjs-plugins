/**
 * Unit (no server): modern driver at the driver boundary. The three
 * `@nats-io/*` packages are mocked (connect/jetstream/manager fakes), the
 * headers factories stay real so header translation is exercised
 * end-to-end, and legacy `headers()` objects serve as the input carriers.
 */
import { headers as legacyHeaders } from 'nats';

jest.mock('@nats-io/transport-node', () => ({
  connect: jest.fn(),
}));

jest.mock('@nats-io/jetstream', () => ({
  jetstream: jest.fn(),
  jetstreamManager: jest.fn(),
}));

jest.mock('@nats-io/nats-core', () => {
  const actual = jest.requireActual('@nats-io/nats-core');
  return { ...actual };
});

import { connect as modernConnect } from '@nats-io/transport-node';
import {
  jetstream as modernJetstream,
  jetstreamManager as modernJetstreamManager,
} from '@nats-io/jetstream';
import { createModernDriver } from './modern-driver';

const connectMock = modernConnect as jest.Mock;
const jetstreamMock = modernJetstream as jest.Mock;
const jetstreamManagerMock = modernJetstreamManager as jest.Mock;

function fakeJsMsg(partial: any = {}) {
  return {
    subject: 's',
    data: new TextEncoder().encode('x'),
    headers: undefined,
    ack: jest.fn(),
    nak: jest.fn(),
    term: jest.fn(),
    working: jest.fn(),
    info: {
      stream: 'st',
      consumer: 'c',
      deliveryCount: 1,
      streamSequence: 1,
      deliverySequence: 1,
    },
    redelivered: false,
    ...partial,
  };
}

function fakeConsumerMessages(items: any[]) {
  let stopped = false;
  return {
    [Symbol.asyncIterator]() {
      let i = 0;
      return {
        next: async () =>
          i < items.length && !stopped
            ? { value: items[i++], done: false }
            : { value: undefined, done: true },
      };
    },
    stop: jest.fn(() => {
      stopped = true;
    }),
  };
}

describe('modern driver (fake driver boundary, no server)', () => {
  let nc: any;
  let js: any;
  let jsm: any;
  let driver: ReturnType<typeof createModernDriver>;

  beforeEach(() => {
    nc = {
      drain: jest.fn(async () => undefined),
      isClosed: () => false,
      isDraining: () => false,
      subscribe: jest.fn(),
      request: jest.fn(),
    };
    js = {
      publish: jest.fn(async () => ({
        stream: 'st',
        seq: 7,
        duplicate: false,
      })),
      consumers: {
        get: jest.fn(),
        getPushConsumer: jest.fn(),
      },
    };
    jsm = {
      streams: {
        list: () => ({
          next: async () => [] as any[],
          [Symbol.asyncIterator]: async function* () {},
        }),
        add: jest.fn(async (cfg: any) => ({ config: { name: cfg.name } })),
        update: jest.fn(async (name: string) => ({ config: { name } })),
      },
      consumers: {
        add: jest.fn(async (_s: string, cfg: any) => ({
          name: cfg.durable_name ?? 'ephemeral-1',
        })),
        info: jest.fn(),
      },
    };
    connectMock.mockReset().mockResolvedValue(nc);
    jetstreamMock.mockReset().mockReturnValue(js);
    jetstreamManagerMock.mockReset().mockResolvedValue(jsm);
    driver = createModernDriver();
  });

  it('connects through the modern transport and delegates close/state', async () => {
    await expect(
      driver.connect({ name: 'm', servers: 'nats://x:4222' }),
    ).resolves.toBe(nc);
    expect(connectMock).toHaveBeenCalledWith({
      name: 'm',
      servers: 'nats://x:4222',
    });
    await driver.close(nc);
    expect(nc.drain).toHaveBeenCalledTimes(1);
    expect(driver.isClosed(nc)).toBe(false);
    expect(driver.isDraining(nc)).toBe(false);
  });

  it('publishes bytes and returns the neutral PubAck', async () => {
    const payload = new TextEncoder().encode('{"a":1}');
    await expect(
      driver.publish(nc, 's', payload, { timeout: 1000 }, undefined),
    ).resolves.toEqual({ stream: 'st', seq: 7, duplicate: false });
    expect(jetstreamMock).toHaveBeenCalledWith(nc, { timeout: 1000 });
    expect(js.publish).toHaveBeenCalledWith('s', payload, undefined);
  });

  it('translates legacy header carriers to modern headers on publish', async () => {
    const carrier = legacyHeaders();
    carrier.append('x-trace', 't-1');
    const payload = new TextEncoder().encode('{}');
    await driver.publish(nc, 's', payload, undefined, {
      msgID: 'm1',
      headers: carrier,
    });
    const [, , opts] = js.publish.mock.calls[0];
    expect(opts.msgID).toBe('m1');
    expect(opts.headers.get('x-trace')).toBe('t-1');
    // The input carrier is untouched.
    expect(carrier.get('x-trace')).toBe('t-1');
  });

  it('translates headers on request and resolves response bytes', async () => {
    const carrier = legacyHeaders();
    carrier.append('x-k', 'v');
    nc.request.mockResolvedValue({ data: new TextEncoder().encode('ok') });
    const out = await driver.request(nc, 's', new Uint8Array([1]), {
      timeout: 250,
      headers: carrier,
    });
    expect(new TextDecoder().decode(out)).toBe('ok');
    const [, , opts] = nc.request.mock.calls[0];
    expect(opts.timeout).toBe(250);
    expect(opts.headers.get('x-k')).toBe('v');
  });

  it('creates a missing stream, stream name included', async () => {
    const results = await driver.ensureStreams(nc, undefined, [
      { name: 'st', subjects: ['s.*'] } as any,
    ]);
    expect(results).toEqual([{ name: 'st', created: true }]);
    expect(jsm.streams.add).toHaveBeenCalledWith({
      name: 'st',
      subjects: ['s.*'],
    });
  });

  it('updates an existing stream, merging subjects', async () => {
    const existing: any[] = [
      { config: { name: 'st', subjects: ['a.*'], retention: 'limits' } },
    ];
    jsm.streams.list = () => ({
      next: async () => existing,
      [Symbol.asyncIterator]: async function* () {
        yield* existing;
      },
    });
    const results = await driver.ensureStreams(nc, undefined, [
      { name: 'st', subjects: ['a.*', 'b.*'] } as any,
    ]);
    expect(results).toEqual([{ name: 'st', created: false }]);
    const [, cfg] = jsm.streams.update.mock.calls[0];
    expect(new Set(cfg.subjects)).toEqual(new Set(['a.*', 'b.*']));
    expect(cfg.name).toBe('st');
  });

  it('creates a durable push consumer from the neutral config mapping', async () => {
    const msgs = fakeConsumerMessages([fakeJsMsg()]);
    js.consumers.getPushConsumer.mockResolvedValue({
      consume: async () => msgs,
    });
    const sub = await driver.subscribeEvents(
      nc,
      'order.created',
      { durable: 'd', deliverGroup: 'g', ackWait: 123 } as any,
      undefined,
      'st',
    );
    const [, cfg] = jsm.consumers.add.mock.calls[0];
    expect(cfg.durable_name).toBe('d-order_created');
    expect(cfg.deliver_group).toBe('g');
    expect(cfg.ack_wait).toBe(123 * 1_000_000);
    expect(cfg.ack_policy).toBe('explicit');
    // Handler subject is the default filter (legacy subscribe parity).
    expect(cfg.filter_subject).toBe('order.created');
    expect(typeof cfg.deliver_subject).toBe('string');
    expect(js.consumers.getPushConsumer).toHaveBeenCalledWith(
      'st',
      'd-order_created',
    );
    const seen: any[] = [];
    for await (const m of sub.messages) {
      seen.push(m);
      break;
    }
    expect(seen).toHaveLength(1);
    expect(seen[0].data).toEqual(new TextEncoder().encode('x'));
    expect(seen[0].raw.subject).toBe('s');
    await sub.destroy();
    expect(msgs.stop).toHaveBeenCalledTimes(1);
  });

  it('honours an explicit deliverToSubject instead of minting an inbox', async () => {
    const msgs = fakeConsumerMessages([]);
    js.consumers.getPushConsumer.mockResolvedValue({
      consume: async () => msgs,
    });
    await driver.subscribeEvents(
      nc,
      's',
      { durable: 'd', deliverToSubject: 'stable.here' } as any,
      undefined,
      'st',
    );
    const [, cfg] = jsm.consumers.add.mock.calls[0];
    expect(cfg.deliver_subject).toBe('stable.here');
  });

  it('creates an ephemeral consumer when no durable is set', async () => {
    const msgs = fakeConsumerMessages([]);
    js.consumers.getPushConsumer.mockResolvedValue({
      consume: async () => msgs,
    });
    jsm.consumers.add.mockResolvedValueOnce({ name: 'ephemeral-9' });
    await driver.subscribeEvents(nc, 's', {} as any, undefined, 'st');
    const [, cfg] = jsm.consumers.add.mock.calls[0];
    expect(cfg.durable_name).toBeUndefined();
    expect(typeof cfg.deliver_subject).toBe('string');
    expect(js.consumers.getPushConsumer).toHaveBeenCalledWith(
      'st',
      'ephemeral-9',
    );
  });

  it('reuses an existing durable instead of recreating it', async () => {
    const msgs = fakeConsumerMessages([]);
    js.consumers.getPushConsumer.mockResolvedValue({
      consume: async () => msgs,
    });
    jsm.consumers.add.mockRejectedValueOnce(
      Object.assign(new Error('consumer already exists'), {
        api_error: { err_code: 10148 },
      }),
    );
    jsm.consumers.info.mockResolvedValue({ name: 'd-s' });
    await driver.subscribeEvents(
      nc,
      's',
      { durable: 'd' } as any,
      undefined,
      'st',
    );
    // Same suffixed durable name the legacy builder produces.
    expect(jsm.consumers.info).toHaveBeenCalledWith('st', 'd-s');
    expect(js.consumers.getPushConsumer).toHaveBeenCalledWith('st', 'd-s');
  });

  it('throws when no stream is available for a modern subscribe', async () => {
    await expect(
      driver.subscribeEvents(nc, 's', {} as any, undefined, undefined),
    ).rejects.toThrow(/needs a stream name/);
  });

  it('maps ordered consumers to a filtered nameless get() with legacy validations', async () => {
    const msgs = fakeConsumerMessages([fakeJsMsg()]);
    js.consumers.get.mockResolvedValue({ consume: async () => msgs });
    const sub = await driver.subscribeEvents(
      nc,
      's',
      { orderedConsumer: true } as any,
      undefined,
      'st',
    );
    // Nameless, but filtered to the handler's subject (plural key: the
    // lib maps a string `filter_subjects` to the server's singular field).
    expect(js.consumers.get).toHaveBeenCalledWith('st', {
      filter_subjects: 's',
    });
    expect(jsm.consumers.add).not.toHaveBeenCalled();
    for await (const _ of sub.messages) {
      break;
    }
    await expect(
      driver.subscribeEvents(
        nc,
        's',
        { orderedConsumer: true, durable: 'd' } as any,
        undefined,
        'st',
      ),
    ).rejects.toThrow(/durable_name cannot be set/);
    await expect(
      driver.subscribeEvents(
        nc,
        's',
        { orderedConsumer: true, deliverGroup: 'g' } as any,
        undefined,
        'st',
      ),
    ).rejects.toThrow(/deliver_group cannot be set/);
  });

  it('rejects status-metadata carriers instead of stripping them silently', async () => {
    const statusCarrier = {
      keys: () => ['a'],
      values: () => ['b'],
      get: () => 'b',
      hasError: true,
    };
    await expect(
      driver.publish(nc, 's', new Uint8Array([1]), undefined, {
        headers: statusCarrier,
      } as any),
    ).rejects.toThrow(/status metadata/);
  });

  it('subscribes core messages and adapts the callback to views', async () => {
    const seen: any[] = [];
    await driver.subscribeMessages(nc, 's', 'q', (err, msg) => {
      seen.push([err, msg]);
    });
    expect(nc.subscribe).toHaveBeenCalledTimes(1);
    const [, opts] = nc.subscribe.mock.calls[0];
    expect(opts.queue).toBe('q');
    const raw = fakeJsMsg({ data: new TextEncoder().encode('hi') });
    opts.callback(null, raw);
    expect(seen).toHaveLength(1);
    expect(seen[0][0]).toBeNull();
    expect(seen[0][1].data).toEqual(new TextEncoder().encode('hi'));
    expect(seen[0][1].raw).toBe(raw);
    opts.callback(new Error('boom'), null);
    expect(seen[1][0].message).toBe('boom');
    expect(seen[1][1]).toBeNull();
  });
});
