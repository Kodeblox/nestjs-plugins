import { ServerConsumerOptions } from '../interfaces/server-consumer-options.interface';

/**
 * Driver-neutral JetStream consumer configuration (snake_case, as sent on
 * the wire to `consumers.add`). This is the v3 shape: it matches both the
 * legacy `nats` `ConsumerConfig` and the modern `@nats-io/jetstream`
 * `ConsumerConfig`, which are JSON-identical. No driver package is
 * imported here, so the modern driver (Phase 3) can build it without
 * loading `nats`.
 *
 * Value semantics replicate the legacy `serverConsumerOptionsBuilder`
 * outcome one-for-one (see `server-consumer-options-builder.ts`), so a
 * consumer created from this config behaves like the v2 push consumer:
 * - durations the user gives in milliseconds (`ackWait`, `idleHeartbeat`)
 *   are converted to nanoseconds, as the v2 builder did;
 * - `inactiveThreshold` and `backoff` are passed through verbatim (legacy
 *   quirk preserved so the two paths stay equivalent);
 * - the v2 `defaultConsumer` stamps a `name: ''` artifact into every config.
 *   It is intentionally dropped here: the server treats a missing name and
 *   an empty name identically on `consumers.add`, and the neutral config
 *   carries no user intent in that field.
 * - options with no `ConsumerConfig` counterpart (`manualAck`, `maxMessages`,
 *   which the v2 builder kept in wrapper fields, and `orderedConsumer`, which
 *   it kept as a subscribe-time flag) intentionally produce no field here.
 *   `orderedConsumer` still forces `ack_policy: 'none'`, mirroring the v2
 *   `getOpts()` outcome; ordered delivery itself stays a driver concern
 *   (Phase 3 spike, spec section 7).
 */
export interface NatsConsumerConfig {
  ack_policy: 'explicit' | 'all' | 'none';
  deliver_policy:
    | 'all'
    | 'last'
    | 'new'
    | 'by_start_sequence'
    | 'by_start_time'
    | 'last_per_subject';
  replay_policy: 'instant' | 'original';
  /** Nanoseconds. Defaults to 30s, mirroring the v2 builder default. */
  ack_wait: number;
  description?: string;
  max_deliver?: number;
  max_ack_pending?: number;
  max_waiting?: number;
  /** Bytes per second. */
  rate_limit_bps?: number;
  flow_control?: boolean;
  headers_only?: boolean;
  /** Nanoseconds. */
  idle_heartbeat?: number;
  opt_start_seq?: number;
  /** ISO-8601 date-time string. */
  opt_start_time?: string;
  sample_freq?: string;
  durable_name?: string;
  deliver_subject?: string;
  deliver_group?: string;
  filter_subject?: string;
  filter_subjects?: string[];
  backoff?: number[];
  /** Nanoseconds (legacy passthrough: value used verbatim). */
  inactive_threshold?: number;
  mem_storage?: boolean;
}

const MS_TO_NS = 1_000_000;
const DEFAULT_ACK_WAIT_NS = 30_000 * MS_TO_NS;

function durableName(durable: string, subject: string): string {
  return `${durable}-${subject
    .replaceAll('.', '_')
    .replaceAll('*', '_ALL')
    .replaceAll('>', '_ALL')}`;
}

// Same invalid-character set as the legacy `validateDurableName` (dots,
// `*`, `>`, slashes, backslash, whitespace): the composed durable name is
// validated so misuse still fails fast instead of surfacing later as a
// server-side consumer-creation error.
function assertValidDurableName(name: string): void {
  const bad = ['.', '*', '>', '/', '\\', ' ', '\t', '\n', '\r'] as const;
  for (const v of bad) {
    if (name.indexOf(v) !== -1) {
      const shown = v === '\n' ? '\\n' : v === '\r' ? '\\r' : v === '\t' ? '\\t' : v;
      throw new Error(`invalid durable name - durable name cannot contain '${shown}'`);
    }
  }
}

function inboxSubject(prefix: string): string {
  // Non-empty is enforced by the falsy guard at the call site
  // (`if (deliverTo)`), so only the wildcard rule needs checking here.
  for (const token of prefix.split('.')) {
    if (token === '*' || token === '>') {
      throw new Error(`inbox prefixes cannot have wildcards '${prefix}'`);
    }
  }
  // Same shape as the legacy `createInbox(prefix)`: prefix + '.' + random id.
  // The token only needs to be unique per consumer, not decodable.
  const token = Math.random().toString(36).slice(2, 10).padEnd(8, '0');
  return `${prefix}.${token}`;
}

export function toConsumerConfig(
  serverConsumerOptions: ServerConsumerOptions,
  subject: string,
): NatsConsumerConfig {
  const {
    deliverGroup,
    deliverToSubject,
    deliverTo,
    ackWait,
    ackPolicy,
    deliverPolicy,
    description,
    durable,
    filterSubject,
    filterSubjects,
    flowControl,
    headersOnly,
    idleHeartbeat,
    inactiveThreshold,
    limit,
    maxAckPending,
    maxDeliver,
    maxWaiting,
    orderedConsumer,
    replayPolicy,
    sample,
    startAtTimeDelta,
    startSequence,
    startTime,
    backoff,
    memStorage,
  } = serverConsumerOptions;

  const config: NatsConsumerConfig = {
    ack_policy: 'explicit',
    deliver_policy: 'all',
    replay_policy: 'instant',
    ack_wait: DEFAULT_ACK_WAIT_NS,
  };

  if (backoff) {
    config.backoff = backoff;
  }
  if (inactiveThreshold) {
    config.inactive_threshold = inactiveThreshold;
  }
  if (memStorage) {
    config.mem_storage = memStorage;
  }

  if (deliverGroup) {
    config.deliver_group = deliverGroup;
  }
  if (ackWait) {
    config.ack_wait = ackWait * MS_TO_NS;
  }
  if (ackPolicy === 'All') {
    config.ack_policy = 'all';
  } else if (ackPolicy === 'Explicit') {
    config.ack_policy = 'explicit';
  } else if (ackPolicy === 'None') {
    config.ack_policy = 'none';
  }
  if (deliverPolicy === 'All') {
    config.deliver_policy = 'all';
  } else if (deliverPolicy === 'Last') {
    config.deliver_policy = 'last';
  } else if (deliverPolicy === 'New') {
    config.deliver_policy = 'new';
  } else if (deliverPolicy === 'last_per_subject') {
    config.deliver_policy = 'last_per_subject';
  }
  // 'ByStartSequence' / 'ByStartTime' deliverPolicy strings have no v2
  // builder branch and are ignored here as well; use startSequence /
  // startTime / startAtTimeDelta instead (they override deliverPolicy,
  // matching the v2 method-call order).
  if (deliverToSubject) {
    config.deliver_subject = deliverToSubject;
  }
  if (deliverTo) {
    config.deliver_subject = inboxSubject(deliverTo);
  }
  if (description) {
    config.description = description;
  }
  if (durable) {
    const name = durableName(durable, subject);
    assertValidDurableName(name);
    config.durable_name = name;
  }
  // The v2 builder accumulates every filter into one list and collapses it:
  // a single filter becomes `filter_subject`, several become
  // `filter_subjects`. Reproduce that collapse (singular first: it is pushed
  // by the builder method at build time, while the plural list is
  // redistributed later inside `getOpts()`).
  const filters = [...(filterSubject ? [filterSubject] : []), ...(filterSubjects ?? [])];
  if (filters.length === 1) {
    config.filter_subject = filters[0];
  } else if (filters.length > 1) {
    config.filter_subjects = filters;
  }
  if (flowControl) {
    config.flow_control = true;
  }
  if (headersOnly) {
    config.headers_only = true;
  }
  if (idleHeartbeat) {
    config.idle_heartbeat = idleHeartbeat * MS_TO_NS;
  }
  if (limit) {
    config.rate_limit_bps = limit;
  }
  if (maxAckPending) {
    config.max_ack_pending = maxAckPending;
  }
  if (maxDeliver) {
    config.max_deliver = maxDeliver;
  }
  if (maxWaiting) {
    config.max_waiting = maxWaiting;
  }
  if (replayPolicy === 'Instant') {
    config.replay_policy = 'instant';
  } else if (replayPolicy === 'Original') {
    config.replay_policy = 'original';
  }
  if (sample) {
    const n = Math.trunc(sample);
    if (n < 0 || n > 100) {
      throw new Error('value must be between 0-100');
    }
    config.sample_freq = `${n}%`;
  }
  if (startSequence) {
    if (startSequence <= 0) {
      throw new Error('sequence must be greater than 0');
    }
    config.deliver_policy = 'by_start_sequence';
    config.opt_start_seq = startSequence;
  }
  if (startTime) {
    config.deliver_policy = 'by_start_time';
    config.opt_start_time = startTime.toISOString();
  }
  if (startAtTimeDelta) {
    config.deliver_policy = 'by_start_time';
    config.opt_start_time = new Date(Date.now() - startAtTimeDelta).toISOString();
  }
  if (orderedConsumer) {
    // Mirrors the v2 `getOpts()` outcome, which forces ack_policy none for
    // ordered consumers. Ordered delivery itself is a driver concern.
    config.ack_policy = 'none';
  }

  return config;
}
