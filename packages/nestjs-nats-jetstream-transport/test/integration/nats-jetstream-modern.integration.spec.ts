/**
 * Integration, modern driver (real NATS + JetStream server required).
 *
 * Mirrors nats-jetstream.integration.spec.ts (legacy cell) on the modern
 * stack: same plugin API, `driver: 'modern'`, raw assertions through
 * `@nats-io/*`. No `nats` import anywhere in this file.
 *
 * Run with:
 *   docker run -d --name nats-main -p 4222:4222 nats -js
 *   NATS_URL=nats://localhost:4222 npm run test:integration
 *
 * When NATS_URL is not set this suite is SKIPPED with the reason below -
 * a skip is not a pass. In CI (spec 001 section 6) each matrix cell must
 * set NATS_URL so these tests actually run against a real server.
 */
import { execFileSync } from 'child_process';
import { connect } from '@nats-io/transport-node';
import { jetstreamManager } from '@nats-io/jetstream';
import type { NatsConnection } from '@nats-io/nats-core';
import { firstValueFrom } from 'rxjs';
import { NatsJetStreamClientProxy } from '../../src/client';
import { NatsJetStreamServer } from '../../src/server';
import { NatsJetStreamRecordBuilder } from '../../src/utils/nats-jetstream-record.builder';
import { NatsRequestOptionsBuilder } from '../../src/utils/nats-request-record-builder';

const NATS_URL = process.env.NATS_URL;
if (!NATS_URL) {
  // eslint-disable-next-line no-console
  console.warn(
    '[integration:modern] SKIPPED: NATS_URL is not set. Start a JetStream server ' +
      '(docker run -p 4222:4222 nats -js) and run ' +
      'NATS_URL=nats://localhost:4222 npm run test:integration',
  );
}
const describeIntegration = NATS_URL ? describe : describe.skip;

const waitFor = async (cond: () => boolean, ms = 8000) => {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > ms) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
};

// Broker-restart coverage needs docker control of the server container.
// The documented local command names it `nats-main`; CI service containers
// are not reachable this way, so the test skips honestly there instead of
// failing. Override with NATS_CONTAINER when the name differs.
const NATS_CONTAINER = process.env.NATS_CONTAINER ?? 'nats-main';

const hasBrokerControl = (() => {
  if (!NATS_URL) return false;
  try {
    execFileSync('docker', ['inspect', NATS_CONTAINER], {
      stdio: 'ignore',
      timeout: 15000,
    });
    return true;
  } catch {
    return false;
  }
})();

const itWithBrokerControl = hasBrokerControl ? it : it.skip;

const waitForServer = async (ms = 30000) => {
  const start = Date.now();
  for (;;) {
    try {
      const nc = await connect({ servers: NATS_URL });
      await nc.close();
      return;
    } catch {
      if (Date.now() - start > ms) {
        throw new Error('broker did not come back after restart');
      }
      await new Promise((r) => setTimeout(r, 500));
    }
  }
};

const decode = (data: Uint8Array) =>
  JSON.parse(new TextDecoder().decode(data));

describeIntegration('NATS JetStream transport, modern driver (real server)', () => {
  const runId = Date.now().toString(36);
  const streamName = `modern${runId}`;
  const createdSubject = `modern.${runId}.order.created`;
  const poisonSubject = `modern.${runId}.order.poison`;
  let server: NatsJetStreamServer;
  let client: NatsJetStreamClientProxy;
  let raw: NatsConnection;
  const received: Array<{ data: any; subject: string; trace?: string }> = [];
  let poisonCalls = 0;

  beforeAll(async () => {
    server = new NatsJetStreamServer({
      connectionOptions: { name: `modern-server-${runId}`, servers: NATS_URL },
      consumerOptions: {
        durable: `d${runId}`,
        deliverGroup: `g${runId}`,
        deliverTo: `deliver${runId}`,
        manualAck: true,
        ackWait: 1000,
      },
      streamConfig: {
        name: streamName,
        subjects: [`modern.${runId}.order.*`],
      },
      driver: 'modern',
    } as any);
    server.addHandler(
      createdSubject,
      (async (data: any, ctx: any) => {
        received.push({
          data,
          subject: ctx.message.subject,
          trace: ctx.message.headers?.get('x-trace'),
        });
        ctx.message.ack();
      }) as any,
      true,
    );
    server.addHandler(
      poisonSubject,
      (async () => {
        poisonCalls++;
        throw new Error('poison handler');
      }) as any,
      true,
    );
    server.addHandler(
      { cmd: `sum-${runId}` },
      (async (data: number[]) =>
        (data || []).reduce((a, b) => a + b, 0)) as any,
    );
    await new Promise<void>((res) => server.listen(res));

    client = new NatsJetStreamClientProxy({
      connectionOptions: { name: `modern-client-${runId}`, servers: NATS_URL },
      driver: 'modern',
    } as any);
    raw = await connect({ servers: NATS_URL, name: `modern-raw-${runId}` });
  }, 30000);

  afterAll(async () => {
    await client.close().catch(() => undefined);
    await server.close().catch(() => undefined);
    if (raw) {
      const jsm = await jetstreamManager(raw);
      await jsm.streams.delete(streamName).catch(() => undefined);
      await raw.close();
    }
  }, 30000);

  it('auto-creates the stream via streamConfig', async () => {
    const jsm = await jetstreamManager(raw);
    const info = await jsm.streams.info(streamName);
    expect(info.config.name).toBe(streamName);
    expect(info.config.subjects).toEqual([`modern.${runId}.order.*`]);
  });

  it('updates an existing stream, merging subjects', async () => {
    const extra = new NatsJetStreamServer({
      connectionOptions: { name: `modern-updater-${runId}`, servers: NATS_URL },
      consumerOptions: {},
      streamConfig: {
        name: streamName,
        subjects: [`modern.${runId}.invoice.*`],
      },
      driver: 'modern',
    } as any);
    await new Promise<void>((res) => extra.listen(res));
    await extra.close();
    const jsm = await jetstreamManager(raw);
    const info = await jsm.streams.info(streamName);
    expect(new Set(info.config.subjects)).toEqual(
      new Set([`modern.${runId}.order.*`, `modern.${runId}.invoice.*`]),
    );
  });

  it('emit() -> @EventPattern-style handler round trip, incl. headers and PubAck', async () => {
    const builder = new NatsJetStreamRecordBuilder({ id: 1, product: 'Socks' });
    builder.setMsgId(`msg-${runId}-1`);
    builder.appendHeader('x-trace', 'trace-123');
    const ack: any = await firstValueFrom(client.emit(createdSubject, builder.build()));
    expect(ack.stream).toBe(streamName);
    expect(ack.seq).toBeGreaterThanOrEqual(1);
    await waitFor(() => received.length >= 1);
    expect(received[0]).toEqual({
      data: { id: 1, product: 'Socks' },
      subject: createdSubject,
      trace: 'trace-123',
    });
  });

  it('emit() with a duplicate msgID is deduplicated by the stream', async () => {
    const mk = () => {
      const b = new NatsJetStreamRecordBuilder({ id: 99 });
      b.setMsgId(`dup-${runId}`);
      return b.build();
    };
    const first: any = await firstValueFrom(client.emit(createdSubject, mk()));
    const second: any = await firstValueFrom(client.emit(createdSubject, mk()));
    expect(first.duplicate ?? false).toBe(false);
    expect(second.duplicate).toBe(true);
    expect(second.seq).toBe(first.seq);
  });

  it('plain emit() (no record) also publishes to the stream', async () => {
    const before = received.length;
    const ack: any = await firstValueFrom(
      client.emit(createdSubject, { id: 2, product: 'Plain' }),
    );
    expect(ack.stream).toBe(streamName);
    await waitFor(() => received.length > before);
    expect(received[received.length - 1].data).toEqual({ id: 2, product: 'Plain' });
  });

  it('send() -> @MessagePattern-style handler request/response', async () => {
    await expect(
      firstValueFrom(client.send({ cmd: `sum-${runId}` }, [1, 2, 3])),
    ).resolves.toBe(6);
  });

  it('send() with NatsRequestOptionsBuilder delivers the response to the configured reply subject', async () => {
    const replySubject = `modern.${runId}.reply.sum`;
    const replyReceived: any[] = [];
    const replySub = raw.subscribe(replySubject, {
      callback: (_err, msg) => {
        replyReceived.push(decode(msg.data));
      },
    });
    try {
      const builder = new NatsRequestOptionsBuilder([4, 5]);
      builder.setTimeout(5000);
      builder.setReplyToSubject(replySubject);
      await expect(
        firstValueFrom(client.send({ cmd: `sum-${runId}` }, builder.build())),
      ).resolves.toBe(9);
      await waitFor(() => replyReceived.length >= 1);
      expect(replyReceived[0]).toEqual({ response: 9, isDisposed: true });
    } finally {
      replySub.unsubscribe();
    }
  });

  it('send() to a subject with no handler errors (no responders)', async () => {
    const builder = new NatsRequestOptionsBuilder([1]);
    builder.setTimeout(300);
    await expect(
      firstValueFrom(client.send({ cmd: `missing-${runId}` }, builder.build())),
    ).rejects.toThrow(/503|no responders/i);
  });

  it('handler error terms the message: poison is not redelivered', async () => {
    await firstValueFrom(client.emit(poisonSubject, { id: 666 }));
    await waitFor(() => poisonCalls >= 1);
    // ackWait is 1s in this suite: an un-termed message would be
    // redelivered within this window, a termed one must not be.
    await new Promise((r) => setTimeout(r, 2500));
    expect(poisonCalls).toBe(1);
  });

  it('durable resume (in-suite resubscribe, no broker restart): acked messages are not redelivered after resubscribing with the same durable', async () => {
    const resumeSubject = `modern.${runId}.order.resume`;
    const seen: number[] = [];
    const makeResumeServer = (name: string) => {
      const s = new NatsJetStreamServer({
        connectionOptions: { name, servers: NATS_URL },
        consumerOptions: {
          durable: `resume${runId}`,
          // Stable deliver subject across both instances: a durable
          // push consumer can only be rebound on resubscribe when the
          // deliver subject matches the existing consumer's.
          deliverToSubject: `modern.${runId}.deliver.resume`,
          manualAck: true,
          ackWait: 1000,
        },
        streamConfig: {
          name: streamName,
          subjects: [`modern.${runId}.order.*`],
        },
        driver: 'modern',
      } as any);
      s.addHandler(
        resumeSubject,
        (async (data: any, ctx: any) => {
          seen.push(data.id);
          ctx.message.ack();
        }) as any,
        true,
      );
      return s;
    };

    const first = makeResumeServer(`modern-resume-a-${runId}`);
    await new Promise<void>((res) => first.listen(res));
    await firstValueFrom(client.emit(resumeSubject, { id: 1 }));
    await waitFor(() => seen.includes(1));
    await first.close();

    // Published while no subscriber is attached; stored in the stream.
    await firstValueFrom(client.emit(resumeSubject, { id: 2 }));

    const second = makeResumeServer(`modern-resume-b-${runId}`);
    try {
      await new Promise<void>((res) => second.listen(res));
      await waitFor(() => seen.includes(2));
      // Beyond ackWait (1s): an acked id 1 would be redelivered here
      // if the durable did not resume past it.
      await new Promise((r) => setTimeout(r, 2500));
      expect(seen).toEqual([1, 2]);
    } finally {
      await second.close().catch(() => undefined);
    }
  });

  itWithBrokerControl('durable resume across broker restart: acked messages are not redelivered', async () => {
    const resumeSubject = `modern.${runId}.order.brestart`;
    const seen: Array<{ id: number; dc: number }> = [];
    const makeServer = (name: string) => {
      const s = new NatsJetStreamServer({
        connectionOptions: { name, servers: NATS_URL },
        consumerOptions: {
          durable: `brestart${runId}`,
          deliverToSubject: `modern.${runId}.deliver.brestart`,
          manualAck: true,
          ackWait: 1000,
        },
        streamConfig: {
          name: streamName,
          subjects: [`modern.${runId}.order.*`],
        },
        driver: 'modern',
      } as any);
      s.addHandler(
        resumeSubject,
        (async (data: any, ctx: any) => {
          seen.push({ id: data.id, dc: ctx.message.info.deliveryCount });
          ctx.message.ack();
        }) as any,
        true,
      );
      return s;
    };

    const first = makeServer(`modern-brestart-a-${runId}`);
    await new Promise<void>((res) => first.listen(res));
    await firstValueFrom(client.emit(resumeSubject, { id: 1 }));
    await waitFor(() => seen.some((s) => s.id === 1));
    await first.close().catch(() => undefined);

    execFileSync('docker', ['restart', NATS_CONTAINER], {
      stdio: 'ignore',
      timeout: 60000,
    });
    await waitForServer();

    const second = makeServer(`modern-brestart-b-${runId}`);
    try {
      await new Promise<void>((res) => second.listen(res));
      await firstValueFrom(client.emit(resumeSubject, { id: 2 }));
      await waitFor(() => seen.some((s) => s.id === 2), 20000);
      // Beyond ackWait (1s): an acked id 1 would be redelivered here
      // if the durable did not survive the broker restart.
      await new Promise((r) => setTimeout(r, 2500));
      expect(seen.map((s) => s.id)).toEqual([1, 2]);
      expect(seen.filter((s) => s.id === 1).map((s) => s.dc)).toEqual([1]);
    } finally {
      await second.close().catch(() => undefined);
    }
  }, 90000);

  it('reports deliveryCount 1 then 2 across redelivery', async () => {
    const subject = `modern.${runId}.order.dcount`;
    const seen: number[] = [];
    const s = new NatsJetStreamServer({
      connectionOptions: { name: `modern-dcount-${runId}`, servers: NATS_URL },
      consumerOptions: {
        durable: `dc${runId}`,
        deliverToSubject: `modern.${runId}.deliver.dcount`,
        manualAck: true,
        ackWait: 1000,
      },
      streamConfig: {
        name: streamName,
        subjects: [`modern.${runId}.order.*`],
      },
      driver: 'modern',
    } as any);
    s.addHandler(
      subject,
      (async (_data: any, ctx: any) => {
        seen.push(ctx.message.info.deliveryCount);
        // Skip the ack on first delivery so the broker redelivers.
        if (ctx.message.info.deliveryCount >= 2) {
          ctx.message.ack();
        }
      }) as any,
      true,
    );
    try {
      await new Promise<void>((res) => s.listen(res));
      await firstValueFrom(client.emit(subject, { id: 1 }));
      await waitFor(() => seen.length >= 2);
      expect(seen).toEqual([1, 2]);
      // Beyond ackWait: the acked second delivery must not come back.
      await new Promise((r) => setTimeout(r, 1500));
      expect(seen).toEqual([1, 2]);
    } finally {
      await s.close().catch(() => undefined);
    }
  });

  it('two server instances sharing a deliverGroup process each event once', async () => {
    const queueSubject = `modern.${runId}.order.queue`;
    const seenA: number[] = [];
    const seenB: number[] = [];
    const makeQueueServer = (name: string, seen: number[]) => {
      const s = new NatsJetStreamServer({
        connectionOptions: { name, servers: NATS_URL },
        consumerOptions: {
          durable: `q${runId}`,
          deliverGroup: `qg${runId}`,
          deliverToSubject: `modern.${runId}.deliver.queue`,
          manualAck: true,
          ackWait: 1000,
        },
        streamConfig: {
          name: streamName,
          subjects: [`modern.${runId}.order.*`],
        },
        driver: 'modern',
      } as any);
      s.addHandler(
        queueSubject,
        (async (data: any, ctx: any) => {
          seen.push(data.id);
          ctx.message.ack();
        }) as any,
        true,
      );
      return s;
    };

    const qa = makeQueueServer(`modern-queue-a-${runId}`, seenA);
    const qb = makeQueueServer(`modern-queue-b-${runId}`, seenB);
    try {
      await new Promise<void>((res) => qa.listen(res));
      await new Promise<void>((res) => qb.listen(res));
      for (let id = 0; id < 6; id++) {
        await firstValueFrom(client.emit(queueSubject, { id }));
      }
      await waitFor(() => seenA.length + seenB.length >= 6);
      // Settle beyond ackWait so any duplicate/redelivery would show.
      await new Promise((r) => setTimeout(r, 1500));
      const combined = [...seenA, ...seenB].sort((a, b) => a - b);
      expect(combined).toEqual([0, 1, 2, 3, 4, 5]);
    } finally {
      await qa.close().catch(() => undefined);
      await qb.close().catch(() => undefined);
    }
  });
});
