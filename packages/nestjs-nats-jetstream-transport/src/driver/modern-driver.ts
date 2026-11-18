import type {
  JetStreamManagerOptions,
  JetStreamPublishOptions,
  StreamConfig,
} from '@nats-io/jetstream';
import type { NatsConnection, RequestOptions } from '@nats-io/nats-core';
import type { NodeConnectionOptions } from '@nats-io/transport-node';
import type { NatsConnectionOptions } from '../interfaces/nats-connection-options.interface';
import type { NatsStreamConfig } from '../interfaces/nats-stream-config.interface';
import type { ServerConsumerOptions } from '../interfaces/server-consumer-options.interface';
import type {
  EventSubscription,
  MessageCallback,
  NatsDriver,
  StreamSetupResult,
} from './nats-driver';
import { withDriverHeaders } from './nats-driver';
import { toConsumerConfig } from '../utils/consumer-config';

/**
 * Modern driver implementation (spec 001, section 4.2): the `@nats-io/*` v3
 * API behind the internal `NatsDriver` surface, settled by the Phase 3
 * spike (push consumer + stream naming, see the report):
 *
 * - Consuming stays a durable *push* consumer (`consumers.add` once, then
 *   `getPushConsumer(...).consume()`), mirroring the legacy
 *   `js.subscribe` + `consumerOpts` behavior: delivery, ordering, and
 *   queue-group semantics are unchanged. Ordered consumers map to
 *   `consumers.get(stream)` with no name.
 * - The stream a handler consumes from comes from the caller: the server
 *   resolves it (`stream` option, else the single configured stream) and
 *   passes it in. It is required here and missing/ambiguous configuration
 *   fails loudly at `listen()` time, never silently.
 * - An existing durable is reused (bind), never recreated: `add` uses the
 *   create action, and an already-exists race falls back to `info`.
 * - Record-builder option bags carry the resolved driver package's
 *   `headers()` objects (builders run synchronously in user code), and any
 *   `keys()`/`values()` carrier is translated entry-for-entry at this
 *   boundary via the shared `withDriverHeaders` helper.
 *
 * Like the legacy driver, the runtime is loaded lazily via dynamic
 * `import()` on first use and cached — no top-level driver require.
 */

type TransportModule = typeof import('@nats-io/transport-node');
type JetstreamModule = typeof import('@nats-io/jetstream');
type CoreModule = typeof import('@nats-io/nats-core');

interface ModernRuntime {
  connect: TransportModule['connect'];
  jetstream: JetstreamModule['jetstream'];
  jetstreamManager: JetstreamModule['jetstreamManager'];
  headers: CoreModule['headers'];
  createInbox: CoreModule['createInbox'];
}

let cachedRuntimePromise: Promise<ModernRuntime> | null = null;

export function loadModernRuntime(): Promise<ModernRuntime> {
  // Same promise-cache shape as the legacy loader: concurrent first-use
  // callers share one dynamic import instead of evaluating it thrice.
  if (!cachedRuntimePromise) {
    const pending = (async (): Promise<ModernRuntime> => {
      const [transport, js, core] = await Promise.all([
        import('@nats-io/transport-node'),
        import('@nats-io/jetstream'),
        import('@nats-io/nats-core'),
      ]);
      return {
        connect: transport.connect,
        jetstream: js.jetstream,
        jetstreamManager: js.jetstreamManager,
        headers: core.headers,
        createInbox: core.createInbox,
      };
    })();
    pending.catch(() => {
      cachedRuntimePromise = null;
    });
    cachedRuntimePromise = pending;
  }
  return cachedRuntimePromise;
}

function isConsumerExistsError(err: unknown): boolean {
  const e = err as {
    api_error?: { err_code?: number };
    message?: string;
  };
  return (
    e?.api_error?.err_code === 10148 || /already exists/i.test(e?.message ?? '')
  );
}

export function createModernDriver(): NatsDriver {
  return {
    kind: 'modern',

    async connect(
      options: Partial<NatsConnectionOptions>,
    ): Promise<unknown> {
      const { connect } = await loadModernRuntime();
      return connect(options as NodeConnectionOptions);
    },

    async close(connection: unknown): Promise<void> {
      // Same contract as the legacy driver: drain, never cut abruptly.
      await (connection as NatsConnection).drain();
    },

    isClosed(connection: unknown): boolean {
      return (connection as NatsConnection).isClosed();
    },

    isDraining(connection: unknown): boolean {
      return (connection as NatsConnection).isDraining();
    },

    async publish(
      connection: unknown,
      subject: string,
      payload: Uint8Array,
      jetStreamOptions?: unknown,
      publishOptions?: unknown,
    ) {
      const { jetstream, headers } = await loadModernRuntime();
      const nc = connection as NatsConnection;
      const js = jetstream(
        nc,
        jetStreamOptions as unknown as JetStreamManagerOptions | undefined,
      );
      const ack = await js.publish(
        subject,
        payload,
        withDriverHeaders(
          publishOptions as JetStreamPublishOptions | undefined,
          headers,
        ),
      );
      return { stream: ack.stream, seq: ack.seq, duplicate: ack.duplicate };
    },

    async request(
      connection: unknown,
      subject: string,
      payload: Uint8Array,
      options?: unknown,
    ): Promise<Uint8Array> {
      const { headers } = await loadModernRuntime();
      const nc = connection as NatsConnection;
      const msg = await nc.request(
        subject,
        payload,
        withDriverHeaders(options as RequestOptions | undefined, headers),
      );
      return msg.data;
    },

    async ensureStreams(
      connection: unknown,
      jetStreamOptions: unknown,
      configs: NatsStreamConfig[],
    ): Promise<StreamSetupResult[]> {
      const { jetstreamManager } = await loadModernRuntime();
      const nc = connection as NatsConnection;
      const jsm = await jetstreamManager(
        nc,
        jetStreamOptions as unknown as JetStreamManagerOptions | undefined,
      );
      // Paginate: list().next() returns only the first page, so a server
      // with more streams than fit would miss an existing stream and fail
      // on add().
      const streams = [];
      for await (const stream of jsm.streams.list()) {
        streams.push(stream);
      }
      const results: StreamSetupResult[] = [];
      for (const requiredStreamConfig of configs) {
        const stream = streams.find(
          (entry) => entry.config.name === requiredStreamConfig.name,
        );
        if (stream) {
          const streamSubjects = new Set([
            ...stream.config.subjects,
            ...requiredStreamConfig.subjects,
          ]);
          const streamInfo = await jsm.streams.update(stream.config.name, {
            ...stream.config,
            ...requiredStreamConfig,
            subjects: [...streamSubjects.keys()],
          });
          results.push({ name: streamInfo.config.name, created: false });
        } else {
          // The neutral config keeps driver enums as plain strings (they
          // are identical on the wire); convert at this boundary the same
          // way jetStreamOptions is converted above.
          const streamInfo = await jsm.streams.add(
            requiredStreamConfig as unknown as StreamConfig,
          );
          results.push({ name: streamInfo.config.name, created: true });
        }
      }
      return results;
    },

    async subscribeEvents(
      connection: unknown,
      subject: string,
      consumerOptions: ServerConsumerOptions,
      jetStreamOptions: unknown,
      stream?: string,
    ): Promise<EventSubscription> {
      const { jetstream, jetstreamManager, createInbox } =
        await loadModernRuntime();
      if (!stream) {
        throw new Error(
          'The modern NATS driver needs a stream name for each JetStream ' +
            `consumer (subject '${subject}'). Set the 'stream' server option ` +
            'or configure exactly one stream via streamConfig.',
        );
      }
      const nc = connection as NatsConnection;
      const js = jetstream(
        nc,
        jetStreamOptions as unknown as JetStreamManagerOptions | undefined,
      );
      const jsm = await jetstreamManager(
        nc,
        jetStreamOptions as unknown as JetStreamManagerOptions | undefined,
      );

      if (consumerOptions.orderedConsumer) {
        // Mirrors the legacy builder's fail-fast validation for ordered
        // consumers, then uses an ordered consumer with no name.
        if (consumerOptions.durable) {
          throw new Error('ordered consumer: durable_name cannot be set');
        }
        if (consumerOptions.deliverGroup) {
          throw new Error('ordered consumer: deliver_group cannot be set');
        }
        if (consumerOptions.deliverTo || consumerOptions.deliverToSubject) {
          throw new Error('ordered consumer: deliver_subject cannot be set');
        }
        if (
          consumerOptions.maxDeliver !== undefined &&
          consumerOptions.maxDeliver > 1
        ) {
          throw new Error(
            'ordered consumer: max_deliver can only be set to 1',
          );
        }
        // Nameless ordered consumer, filtered to this handler's subject like
        // every other consume path (the legacy builder defaults the filter
        // to the subscribe subject). Note the plural key: the lib's
        // ordered() only reads `filter_subjects` (a string is mapped to the
        // singular server field); the singular key would be ignored.
        const consumer = await js.consumers.get(stream, {
          filter_subjects: subject,
        });
        const messages = await consumer.consume();
        return {
          messages: (async function* () {
            for await (const msg of messages) {
              yield { data: msg.data, raw: msg };
            }
          })(),
          destroy: () => {
            messages.stop();
            return Promise.resolve();
          },
        };
      }

      // Durable push consumer: reuse the existing one when present (mirrors
      // the legacy attach path, including its stale-deliver-subject
      // semantics), otherwise create it. `add` uses the create action, so a
      // simultaneous create from a peer instance falls back to `info`.
      const base = toConsumerConfig(consumerOptions, subject);
      if (!base.filter_subject && !base.filter_subjects) {
        // Mirror the legacy `js.subscribe(subject, ...)`: the handler's
        // subject is the default filter unless the user set their own.
        base.filter_subject = subject;
      }
      const durable = base.durable_name;
      let consumerName: string;
      if (!durable) {
        const deliverSubject = resolveDeliverSubject(
          consumerOptions,
          createInbox,
        );
        const created = await jsm.consumers.add(stream, {
          ...base,
          deliver_subject: deliverSubject,
        });
        consumerName = created.name;
      } else {
        try {
          const created = await jsm.consumers.add(stream, {
            ...base,
            name: durable,
            durable_name: durable,
            deliver_subject: resolveDeliverSubject(
              consumerOptions,
              createInbox,
            ),
          });
          consumerName = created.name;
        } catch (err) {
          if (!isConsumerExistsError(err)) {
            throw err;
          }
          consumerName = (await jsm.consumers.info(stream, durable)).name;
        }
      }
      const consumer = await js.consumers.getPushConsumer(
        stream,
        consumerName,
      );
      const messages = await consumer.consume();
      return {
        messages: (async function* () {
          for await (const msg of messages) {
            yield { data: msg.data, raw: msg };
          }
        })(),
        destroy: () => {
          // Client-side stop only: unlike the legacy `destroy()`, this must
          // not delete the server-side durable, or durable resume breaks.
          messages.stop();
          return Promise.resolve();
        },
      };
    },

    subscribeMessages(
      connection: unknown,
      subject: string,
      queue: string | undefined,
      callback: MessageCallback,
    ): void {
      const nc = connection as NatsConnection;
      nc.subscribe(subject, {
        queue,
        callback: (err, msg) => {
          if (err || !msg) {
            callback(err as unknown as Error, null);
            return;
          }
          callback(null, { data: msg.data, raw: msg });
        },
      });
    },
  };
}

function resolveDeliverSubject(
  consumerOptions: ServerConsumerOptions,
  createInbox: CoreModule['createInbox'],
): string {
  // Stable shared subject wins (required for resubscribe and queue-group
  // rendezvous); otherwise mint a prefixed inbox like the legacy path.
  if (consumerOptions.deliverToSubject) {
    return consumerOptions.deliverToSubject;
  }
  if (consumerOptions.deliverTo) {
    return createInbox(consumerOptions.deliverTo);
  }
  return createInbox();
}
