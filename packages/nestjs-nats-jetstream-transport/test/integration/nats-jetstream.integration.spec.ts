/**
 * Integration (real NATS + JetStream server required).
 *
 * Run with:
 *   docker run -d --name nats-main -p 4222:4222 nats -js
 *   NATS_URL=nats://localhost:4222 npm run test:integration
 *
 * When NATS_URL is not set this suite is SKIPPED with the reason below -
 * a skip is not a pass. In CI (spec 001 section 6) each matrix cell must
 * set NATS_URL so these tests actually run against a real server.
 */
import { connect, JSONCodec, NatsConnection } from 'nats';
import { firstValueFrom } from 'rxjs';
import { NatsJetStreamClientProxy } from '../../src/client';
import { NatsJetStreamServer } from '../../src/server';
import { NatsJetStreamRecordBuilder } from '../../src/utils/nats-jetstream-record.builder';
import { NatsRequestOptionsBuilder } from '../../src/utils/nats-request-record-builder';

const NATS_URL = process.env.NATS_URL;
if (!NATS_URL) {
  // eslint-disable-next-line no-console
  console.warn(
    '[integration] SKIPPED: NATS_URL is not set. Start a JetStream server ' +
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

describeIntegration('NATS JetStream transport (real server)', () => {
  const runId = Date.now().toString(36);
  const streamName = `baseline${runId}`;
  const createdSubject = `baseline.${runId}.order.created`;
  const poisonSubject = `baseline.${runId}.order.poison`;
  let server: NatsJetStreamServer;
  let client: NatsJetStreamClientProxy;
  let raw: NatsConnection;
  const received: Array<{ data: any; subject: string; trace?: string }> = [];
  let poisonCalls = 0;

  beforeAll(async () => {
    server = new NatsJetStreamServer({
      connectionOptions: { name: `baseline-server-${runId}`, servers: NATS_URL },
      consumerOptions: {
        durable: `d${runId}`,
        deliverGroup: `g${runId}`,
        deliverTo: `deliver${runId}`,
        manualAck: true,
        ackWait: 1000,
      },
      streamConfig: {
        name: streamName,
        subjects: [`baseline.${runId}.order.*`],
      },
      // Legacy cell: pin the driver explicitly now that both generations
      // are installed (unpinned resolution would prefer modern).
      driver: 'legacy',
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
      connectionOptions: { name: `baseline-client-${runId}`, servers: NATS_URL },
      driver: 'legacy',
    } as any);
    raw = await connect({ servers: NATS_URL, name: `baseline-raw-${runId}` });
  }, 30000);

  afterAll(async () => {
    await client.close().catch(() => undefined);
    await server.close().catch(() => undefined);
    if (raw) {
      const jsm = await raw.jetstreamManager();
      await jsm.streams.delete(streamName).catch(() => undefined);
      await raw.close();
    }
  }, 30000);

  it('auto-created the stream via streamConfig', async () => {
    const jsm = await raw.jetstreamManager();
    const info = await jsm.streams.info(streamName);
    expect(info.config.name).toBe(streamName);
    expect(info.config.subjects).toEqual([`baseline.${runId}.order.*`]);
  });

  it('updates an existing stream, merging subjects', async () => {
    const extra = new NatsJetStreamServer({
      connectionOptions: { name: `baseline-updater-${runId}`, servers: NATS_URL },
      consumerOptions: {},
      streamConfig: {
        name: streamName,
        subjects: [`baseline.${runId}.invoice.*`],
      },
      driver: 'legacy',
    } as any);
    await new Promise<void>((res) => extra.listen(res));
    await extra.close();
    const jsm = await raw.jetstreamManager();
    const info = await jsm.streams.info(streamName);
    expect(new Set(info.config.subjects)).toEqual(
      new Set([`baseline.${runId}.order.*`, `baseline.${runId}.invoice.*`]),
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
    // Real reply-subject assertion: a raw subscription on the exact
    // reply subject must receive the handler's response bytes, while
    // send() itself still resolves with the decoded value (within the
    // configured 5s timeout, which is not otherwise observable on a
    // fast successful round trip).
    const replySubject = `baseline.${runId}.reply.sum`;
    const replyCodec = JSONCodec();
    const replyReceived: any[] = [];
    const replySub = raw.subscribe(replySubject, {
      callback: (_err, msg) => {
        replyReceived.push(replyCodec.decode(msg.data));
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
    // No subscription exists for this pattern subject, so the broker
    // reports no-responders immediately (nats surfaces this as a
    // "503" status error); this is not a timeout expiry.
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
    const resumeSubject = `baseline.${runId}.order.resume`;
    const seen: number[] = [];
    const makeResumeServer = (name: string) => {
      const s = new NatsJetStreamServer({
        connectionOptions: { name, servers: NATS_URL },
        consumerOptions: {
          durable: `resume${runId}`,
          // Stable deliver subject across both instances: a durable
          // push consumer can only be rebound on resubscribe when the
          // deliver subject matches the existing consumer's.
          deliverToSubject: `baseline.${runId}.deliver.resume`,
          manualAck: true,
          ackWait: 1000,
        },
        streamConfig: {
          name: streamName,
          subjects: [`baseline.${runId}.order.*`],
        },
        driver: 'legacy',
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

    const first = makeResumeServer(`baseline-resume-a-${runId}`);
    await new Promise<void>((res) => first.listen(res));
    await firstValueFrom(client.emit(resumeSubject, { id: 1 }));
    await waitFor(() => seen.includes(1));
    await first.close();

    // Published while no subscriber is attached; stored in the stream.
    await firstValueFrom(client.emit(resumeSubject, { id: 2 }));

    const second = makeResumeServer(`baseline-resume-b-${runId}`);
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

  it('two server instances sharing a deliverGroup process each event once', async () => {
    const queueSubject = `baseline.${runId}.order.queue`;
    const seenA: number[] = [];
    const seenB: number[] = [];
    const makeQueueServer = (name: string, seen: number[]) => {
      const s = new NatsJetStreamServer({
        connectionOptions: { name, servers: NATS_URL },
        consumerOptions: {
          durable: `q${runId}`,
          deliverGroup: `qg${runId}`,
          deliverToSubject: `baseline.${runId}.deliver.queue`,
          manualAck: true,
          ackWait: 1000,
        },
        streamConfig: {
          name: streamName,
          subjects: [`baseline.${runId}.order.*`],
        },
        driver: 'legacy',
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

    const qa = makeQueueServer(`baseline-queue-a-${runId}`, seenA);
    const qb = makeQueueServer(`baseline-queue-b-${runId}`, seenB);
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
