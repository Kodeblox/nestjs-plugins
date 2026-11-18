import { ServerConsumerOptions } from '../interfaces/server-consumer-options.interface';
import { LEGACY_DRIVER_PACKAGE } from '../driver/resolve-driver';

/**
 * Legacy-path builder (spec 001, section 4.2): the `nats` v2
 * `consumerOpts()` chain, now loaded at call time instead of module load so
 * importing the package never evaluates the driver. Called only from the
 * legacy driver after `connect()`; direct standalone use works wherever the
 * driver package is installed. This is the legacy-path call-time seam (see
 * the scope note in `driver/nats-driver.ts`); it is used only by the legacy
 * driver, while the modern driver builds the equivalent config from the
 * neutral `toConsumerConfig` mapping.
 *
 * The return value is intentionally `unknown`: it is an opaque legacy
 * driver object, meaningful only to the legacy consume path. New code
 * should use the driver-neutral `toConsumerConfig` mapping instead.
 */
export function serverConsumerOptionsBuilder(
  serverConsumerOptions: ServerConsumerOptions,
  subject: string,
): unknown {
  // The specifier comes from the shared constant (not a literal) so the
  // published output contains no `require('nats')` string; the type
  // position still names the driver package for the legacy path.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const { consumerOpts, createInbox } = require(LEGACY_DRIVER_PACKAGE) as typeof import(
    'nats'
  );
  const {
    deliverGroup,
    deliverToSubject,
    deliverTo,
    ackWait,
    manualAck,
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
    maxMessages,
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

  const config = {
    ...(backoff && { backoff }),
    ...(inactiveThreshold && { inactive_threshold: inactiveThreshold }),
    ...(memStorage && { mem_storage: memStorage }),
    ...(filterSubjects && { filter_subjects: filterSubjects }),
  };

  const opts = consumerOpts(config);

  deliverGroup && opts.deliverGroup(deliverGroup);
  ackWait && opts.ackWait(ackWait);
  manualAck && opts.manualAck();
  ackPolicy === 'All' && opts.ackAll();
  ackPolicy === 'Explicit' && opts.ackExplicit();
  ackPolicy === 'None' && opts.ackNone();
  deliverPolicy === 'All' && opts.deliverAll();
  deliverPolicy === 'Last' && opts.deliverLast();
  deliverPolicy === 'last_per_subject' && opts.deliverLastPerSubject();
  deliverPolicy === 'New' && opts.deliverNew();
  deliverToSubject && opts.deliverTo(deliverToSubject);
  deliverTo && opts.deliverTo(createInbox(deliverTo));

  description && opts.description(description);
  durable &&
    opts.durable(
      `${durable}-${subject.replaceAll('.', '_').replaceAll('*', '_ALL').replaceAll('>', '_ALL')}`,
    );
  filterSubject && opts.filterSubject(filterSubject);
  flowControl && opts.flowControl();
  headersOnly && opts.headersOnly();
  idleHeartbeat && opts.idleHeartbeat(idleHeartbeat);
  limit && opts.limit(limit);
  maxAckPending && opts.maxAckPending(maxAckPending);
  maxDeliver && opts.maxDeliver(maxDeliver);
  maxMessages && opts.maxMessages(maxMessages);
  maxWaiting && opts.maxWaiting(maxWaiting);
  orderedConsumer && opts.orderedConsumer();
  replayPolicy === 'Instant' && opts.replayInstantly();
  replayPolicy === 'Original' && opts.replayOriginal();
  sample && opts.sample(sample);
  startAtTimeDelta && opts.startAtTimeDelta(startAtTimeDelta);
  startSequence && opts.startSequence(startSequence);
  startTime && opts.startTime(startTime);

  return opts;
}
