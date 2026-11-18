/**
 * Unit (no server, no driver): pins the driver-neutral ConsumerConfig a
 * JetStream consumer would be created with. Ported from the PORTABLE
 * assertions in `server-consumer-options-builder.spec.ts`; the v2-internal
 * wrapper fields (`mack`, `max`, `queue`, `ordered`) have no ConsumerConfig
 * counterpart and are intentionally not asserted here.
 */
import { toConsumerConfig } from './consumer-config';

describe('toConsumerConfig (driver-neutral ConsumerConfig mapping)', () => {
  it('maps durable name with subject suffix, incl. wildcard normalisation', () => {
    expect(toConsumerConfig({ durable: 'my-durable' }, 'test.subject').durable_name).toBe(
      'my-durable-test_subject',
    );
    expect(toConsumerConfig({ durable: 'd' }, 'order.*').durable_name).toBe(
      'd-order__ALL',
    );
    expect(toConsumerConfig({ durable: 'd' }, 'a.>').durable_name).toBe(
      'd-a__ALL',
    );
  });

  it('maps ack and deliver policies to server config values', () => {
    expect(toConsumerConfig({ ackPolicy: 'Explicit' }, 's').ack_policy).toBe('explicit');
    expect(toConsumerConfig({ ackPolicy: 'All' }, 's').ack_policy).toBe('all');
    expect(toConsumerConfig({ ackPolicy: 'None' }, 's').ack_policy).toBe('none');
    expect(toConsumerConfig({ deliverPolicy: 'All' }, 's').deliver_policy).toBe('all');
    expect(toConsumerConfig({ deliverPolicy: 'Last' }, 's').deliver_policy).toBe('last');
    expect(toConsumerConfig({ deliverPolicy: 'New' }, 's').deliver_policy).toBe('new');
    expect(
      toConsumerConfig({ deliverPolicy: 'last_per_subject' }, 's').deliver_policy,
    ).toBe('last_per_subject');
  });

  it('maps replay policy', () => {
    expect(toConsumerConfig({ replayPolicy: 'Instant' }, 's').replay_policy).toBe(
      'instant',
    );
    expect(toConsumerConfig({ replayPolicy: 'Original' }, 's').replay_policy).toBe(
      'original',
    );
  });

  it('passes through backoff / filterSubjects / inactiveThreshold / memStorage', () => {
    const cfg = toConsumerConfig(
      {
        backoff: [1000, 5000],
        filterSubjects: ['events.A', 'events.B'],
        inactiveThreshold: 20000,
        memStorage: true,
      },
      'test.subject',
    );
    expect(cfg.backoff).toEqual([1000, 5000]);
    expect(cfg.filter_subjects).toEqual(['events.A', 'events.B']);
    expect(cfg.inactive_threshold).toBe(20000);
    expect(cfg.mem_storage).toBe(true);
  });

  it('maps numeric / boolean consumer limits (ackWait & heartbeat are ms -> ns)', () => {
    const cfg = toConsumerConfig(
      {
        ackWait: 123,
        maxDeliver: 5,
        maxAckPending: 9,
        maxWaiting: 4,
        limit: 55,
        idleHeartbeat: 999,
        flowControl: true,
        headersOnly: true,
        sample: 50,
        startSequence: 7,
        description: 'desc',
        filterSubject: 'a.b',
      },
      's',
    );
    expect(cfg.ack_wait).toBe(123 * 1_000_000);
    expect(cfg.max_deliver).toBe(5);
    expect(cfg.max_ack_pending).toBe(9);
    expect(cfg.max_waiting).toBe(4);
    expect(cfg.rate_limit_bps).toBe(55);
    expect(cfg.idle_heartbeat).toBe(999 * 1_000_000);
    expect(cfg.flow_control).toBe(true);
    expect(cfg.headers_only).toBe(true);
    expect(cfg.sample_freq).toBe('50%');
    expect(cfg.opt_start_seq).toBe(7);
    expect(cfg.deliver_policy).toBe('by_start_sequence');
    expect(cfg.description).toBe('desc');
    expect(cfg.filter_subject).toBe('a.b');
  });

  it('maps startTime / startAtTimeDelta to by_start_time with ISO time', () => {
    const at = new Date('2026-01-02T03:04:05.000Z');
    const timed = toConsumerConfig({ startTime: at }, 's');
    expect(timed.deliver_policy).toBe('by_start_time');
    expect(timed.opt_start_time).toBe('2026-01-02T03:04:05.000Z');

    const before = Date.now();
    const delta = toConsumerConfig({ startAtTimeDelta: 60_000 }, 's');
    expect(delta.deliver_policy).toBe('by_start_time');
    expect(new Date(delta.opt_start_time as string).getTime()).toBeLessThanOrEqual(
      before,
    );
  });

  it('maps deliverGroup, orderedConsumer, deliverToSubject and deliverTo inbox prefix', () => {
    expect(toConsumerConfig({ deliverGroup: 'g1' }, 's').deliver_group).toBe('g1');

    expect(toConsumerConfig({ orderedConsumer: true }, 's').ack_policy).toBe(
      'none',
    );

    expect(
      toConsumerConfig({ deliverToSubject: 'deliver.here' }, 's').deliver_subject,
    ).toBe('deliver.here');

    const inbox = toConsumerConfig({ deliverTo: 'myservice-messages' }, 's')
      .deliver_subject as string;
    expect(inbox).toMatch(/^myservice-messages\..+/);
  });

  it('collapses a single filterSubjects entry to filter_subject', () => {
    const cfg = toConsumerConfig({ filterSubjects: ['only.one'] }, 's');
    expect(cfg.filter_subject).toBe('only.one');
    expect(cfg.filter_subjects).toBeUndefined();
  });

  it('merges singular filterSubject with filterSubjects', () => {
    const cfg = toConsumerConfig(
      { filterSubject: 'a.b', filterSubjects: ['c.d'] },
      's',
    );
    expect(cfg.filter_subject).toBeUndefined();
    expect(cfg.filter_subjects).toEqual(['a.b', 'c.d']);
  });

  it('applies defaults when options are empty', () => {
    const cfg = toConsumerConfig({}, 'plain.subject');
    expect(cfg.durable_name).toBeUndefined();
    expect(cfg.deliver_policy).toBe('all');
    expect(cfg.ack_policy).toBe('explicit');
    expect(cfg.replay_policy).toBe('instant');
    expect(cfg.ack_wait).toBe(30_000 * 1_000_000);
  });

  it('rejects invalid durable names like the legacy builder', () => {
    expect(() => toConsumerConfig({ durable: 'bad name' }, 's')).toThrow(
      "invalid durable name - durable name cannot contain ' '",
    );
    expect(() => toConsumerConfig({ durable: 'has.dot' }, 's')).toThrow(
      /cannot contain '\.'/,
    );
  });

  it('omits options with no ConsumerConfig counterpart', () => {
    const cfg = toConsumerConfig({ manualAck: true, maxMessages: 3 }, 's');
    expect(cfg).not.toHaveProperty('mack');
    expect(cfg).not.toHaveProperty('max');
  });
});
