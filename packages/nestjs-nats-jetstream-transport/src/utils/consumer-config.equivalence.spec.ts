/**
 * Differential equivalence (handback finding 2): pins
 * `toConsumerConfig(opts, subject)` field-for-field against the legacy
 * `serverConsumerOptionsBuilder(opts, subject).getOpts().config` outcome.
 * The ported unit spec pins each new mapping individually; THIS spec fails
 * CI if a later builder change drifts the two paths apart.
 *
 * Two normalizations, both documented in `consumer-config.ts`:
 * - the legacy `defaultConsumer` `name: ''` artifact is stripped (the
 *   neutral config intentionally drops it);
 * - `deliverTo` inboxes are random per call in both paths, so they are
 *   compared by shape, not equality. `startAtTimeDelta` embeds `Date.now()`
 *   in both paths, so those timestamps are compared within tolerance.
 */
import { toConsumerConfig } from './consumer-config';
import { serverConsumerOptionsBuilder } from './server-consumer-options-builder';

const shapes: Array<[any, string]> = [
  [{ durable: 'd1' }, 'order.created'],
  [{ durable: 'd', ackPolicy: 'None', deliverPolicy: 'last_per_subject' }, 'a.>'],
  [
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
  ],
  [
    {
      backoff: [1000, 5000],
      filterSubjects: ['events.A', 'events.B'],
      inactiveThreshold: 20000,
      memStorage: true,
    },
    't.s',
  ],
  [{ deliverGroup: 'g1', deliverToSubject: 'x.y' }, 's'],
  [{ deliverTo: 'myservice-messages' }, 's'],
  [{ startTime: new Date('2026-01-02T03:04:05.000Z') }, 's'],
  [{ startAtTimeDelta: 60_000 }, 's'],
  [{ orderedConsumer: true, manualAck: true, maxMessages: 3 }, 's'],
  [{ filterSubject: 'a.b', filterSubjects: ['c.d'] }, 's'],
  [{ replayPolicy: 'Original', ackPolicy: 'All', deliverPolicy: 'New' }, 's'],
  [{}, 'plain.subject'],
];

function legacyConfig(opts: any, subject: string): any {
  const cfg = {
    ...(serverConsumerOptionsBuilder(opts, subject) as any).getOpts().config,
  };
  if (cfg.name === '') {
    delete cfg.name;
  }
  return cfg;
}

describe('toConsumerConfig == legacy builder outcome (differential)', () => {
  for (const [index, [opts, subject]] of shapes.entries()) {
    it(`shape ${index} (${subject}) matches the legacy config`, () => {
      const fresh: any = { ...toConsumerConfig(opts, subject) };
      const legacy: any = legacyConfig(opts, subject);

      if (opts.deliverTo) {
        const inbox = new RegExp(`^${opts.deliverTo}\\..+`);
        expect(fresh.deliver_subject).toMatch(inbox);
        expect(legacy.deliver_subject).toMatch(inbox);
        delete fresh.deliver_subject;
        delete legacy.deliver_subject;
      }

      if (opts.startAtTimeDelta) {
        expect(fresh.deliver_policy).toBe('by_start_time');
        expect(legacy.deliver_policy).toBe('by_start_time');
        expect(
          Math.abs(
            new Date(fresh.opt_start_time).getTime() -
              new Date(legacy.opt_start_time).getTime(),
          ),
        ).toBeLessThan(5000);
        delete fresh.opt_start_time;
        delete legacy.opt_start_time;
      }

      expect(fresh).toEqual(legacy);
    });
  }

  it('throws identically to the legacy builder on invalid input', () => {
    const bad: Array<[any, string]> = [
      [{ durable: 'bad name' }, 's'],
      [{ sample: 101 }, 's'],
      // Note: startSequence 0 is silently ignored by BOTH paths (falsy
      // guard), so only a negative value exercises the throw parity.
      [{ startSequence: -1 }, 's'],
      [{ deliverTo: 'bad.*' }, 's'],
    ];
    for (const [opts, subject] of bad) {
      let legacyMessage: string | null = null;
      try {
        serverConsumerOptionsBuilder(opts, subject);
      } catch (err) {
        legacyMessage = (err as Error).message;
      }
      expect(legacyMessage).not.toBeNull();
      expect(() => toConsumerConfig(opts, subject)).toThrow(
        legacyMessage as string,
      );
    }
  });
});
