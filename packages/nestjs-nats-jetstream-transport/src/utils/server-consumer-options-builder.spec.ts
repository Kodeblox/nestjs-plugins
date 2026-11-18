/**
 * Unit (no server): pins the consumer configuration a JetStream consumer
 * would actually be created with, using the REAL `consumerOpts()` builder
 * from `nats` (not a mock). Assertions read `builder.getOpts()`, i.e. the
 * outcome handed to the server, not which chain methods were called.
 * A future v3 adapter must produce an equivalent ConsumerConfig mapping.
 *
 * Portability annotation (v2 -> v3):
 * - PORTABLE: everything under `getOpts().config.*` is a JetStream
 *   ConsumerConfig (snake_case) field. A v3 adapter building a plain
 *   ConsumerConfig must reproduce these values (noting v3 may express
 *   durations such as `ack_wait`/`idle_heartbeat` differently at the
 *   API boundary than the v2 builder's ms -> ns conversion pinned here).
 * - NON-PORTABLE (v2-internal): `getOpts()` itself and the wrapper
 *   fields beside `config` (`mack`, `max`, `queue`, `ordered`) are
 *   `consumerOpts()` builder internals with no ConsumerConfig/v3
 *   counterpart. Assertions on them are marked `v2-internal` inline;
 *   do not carry them into a v3 spec verbatim.
 */
import { serverConsumerOptionsBuilder } from './server-consumer-options-builder';

const build = (o: any, s: string): any => {
  const opts = (
    serverConsumerOptionsBuilder(o, s) as unknown as { getOpts: () => any }
  ).getOpts();
  return Object.assign(opts, { getOpts: () => opts });
};

describe('serverConsumerOptionsBuilder (real consumerOpts outcome)', () => {
  it('maps durable name with subject suffix, incl. wildcard normalisation', () => {
    const opts = build(
      { durable: 'my-durable' },
      'test.subject',
    ).getOpts();
    expect(opts.config.durable_name).toBe('my-durable-test_subject');

    const wild = build(
      { durable: 'd' },
      'order.*',
    ).getOpts();
    expect(wild.config.durable_name).toBe('d-order__ALL');

    const gt = build({ durable: 'd' }, 'a.>').getOpts();
    expect(gt.config.durable_name).toBe('d-a__ALL');
  });

  it('maps ack and deliver policies to server config values', () => {
    expect(
      build({ ackPolicy: 'Explicit' }, 's').getOpts()
        .config.ack_policy,
    ).toBe('explicit');
    expect(
      build({ ackPolicy: 'All' }, 's').getOpts().config
        .ack_policy,
    ).toBe('all');
    expect(
      build({ ackPolicy: 'None' }, 's').getOpts()
        .config.ack_policy,
    ).toBe('none');
    expect(
      build({ deliverPolicy: 'All' }, 's').getOpts()
        .config.deliver_policy,
    ).toBe('all');
    expect(
      build({ deliverPolicy: 'Last' }, 's').getOpts()
        .config.deliver_policy,
    ).toBe('last');
    expect(
      build({ deliverPolicy: 'New' }, 's').getOpts()
        .config.deliver_policy,
    ).toBe('new');
    expect(
      build(
        { deliverPolicy: 'last_per_subject' },
        's',
      ).getOpts().config.deliver_policy,
    ).toBe('last_per_subject');
  });

  it('maps replay policy', () => {
    expect(
      build({ replayPolicy: 'Instant' }, 's').getOpts()
        .config.replay_policy,
    ).toBe('instant');
    expect(
      build({ replayPolicy: 'Original' }, 's').getOpts()
        .config.replay_policy,
    ).toBe('original');
  });

  it('passes through backoff / filterSubjects / inactiveThreshold / memStorage (initial config)', () => {
    const cfg = build(
      {
        backoff: [1000, 5000],
        filterSubjects: ['events.A', 'events.B'],
        inactiveThreshold: 20000,
        memStorage: true,
      },
      'test.subject',
    ).getOpts().config;
    expect(cfg.backoff).toEqual([1000, 5000]);
    expect(cfg.filter_subjects).toEqual(['events.A', 'events.B']);
    expect(cfg.inactive_threshold).toBe(20000);
    expect(cfg.mem_storage).toBe(true);
  });

  it('maps numeric / boolean consumer limits (ackWait & heartbeat are ms -> ns)', () => {
    const opts = build(
      {
        ackWait: 123,
        maxDeliver: 5,
        maxAckPending: 9,
        maxMessages: 3,
        maxWaiting: 4,
        limit: 55,
        idleHeartbeat: 999,
        flowControl: true,
        headersOnly: true,
        manualAck: true,
        sample: 50,
        startSequence: 7,
        description: 'desc',
        filterSubject: 'a.b',
      },
      's',
    ).getOpts();
    expect(opts.config.ack_wait).toBe(123 * 1_000_000);
    expect(opts.config.max_deliver).toBe(5);
    expect(opts.config.max_ack_pending).toBe(9);
    expect(opts.max).toBe(3); // v2-internal: wrapper field, not ConsumerConfig
    expect(opts.config.max_waiting).toBe(4);
    expect(opts.config.rate_limit_bps).toBe(55);
    expect(opts.config.idle_heartbeat).toBe(999 * 1_000_000);
    expect(opts.config.flow_control).toBe(true);
    expect(opts.config.headers_only).toBe(true);
    expect(opts.mack).toBe(true); // v2-internal: wrapper field, not ConsumerConfig
    expect(opts.config.sample_freq).toBe('50%');
    expect(opts.config.opt_start_seq).toBe(7);
    expect(opts.config.description).toBe('desc');
    expect(opts.config.filter_subject).toBe('a.b');
  });

  it('maps deliverGroup, orderedConsumer, deliverToSubject and deliverTo inbox prefix', () => {
    const grouped = build(
      { deliverGroup: 'g1' },
      's',
    ).getOpts();
    expect(grouped.config.deliver_group).toBe('g1');
    expect(grouped.queue).toBe('g1'); // v2-internal: wrapper field, not ConsumerConfig

    expect(
      build({ orderedConsumer: true }, 's').getOpts()
        .ordered, // v2-internal: wrapper field, not ConsumerConfig
    ).toBe(true);

    expect(
      build(
        { deliverToSubject: 'deliver.here' },
        's',
      ).getOpts().config.deliver_subject,
    ).toBe('deliver.here');

    const inbox = build(
      { deliverTo: 'myservice-messages' },
      's',
    ).getOpts().config.deliver_subject;
    expect(inbox).toMatch(/^myservice-messages\..+/);
  });

  it('applies no durable / defaults when options are empty', () => {
    const opts = build({}, 'plain.subject').getOpts();
    expect(opts.config.durable_name).toBeUndefined();
    expect(opts.config.deliver_policy).toBe('all');
    expect(opts.config.ack_policy).toBe('explicit');
    expect(opts.mack).toBe(false); // v2-internal: wrapper field, not ConsumerConfig
  });
});
