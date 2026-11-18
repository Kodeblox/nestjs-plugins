import type {
  ConsumerOptsBuilder,
  JetStreamOptions,
  JetStreamPublishOptions,
  NatsConnection,
  RequestOptions,
  StreamConfig,
  StreamUpdateConfig,
} from 'nats';
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
import { LEGACY_DRIVER_PACKAGE } from './resolve-driver';
import { serverConsumerOptionsBuilder } from '../utils/server-consumer-options-builder';

/**
 * Legacy driver implementation (spec 001, section 4.2): the `nats` v2 API
 * behind the internal `NatsDriver` surface. The driver package is loaded
 * lazily via dynamic `import()` on first use and cached — this module has no
 * top-level driver require, so importing the package never evaluates the
 * driver at load time. The builder modules it uses obtain the same runtime
 * at call time; see their headers. Publish/request header carriers are
 * translated into legacy headers via the shared `withDriverHeaders` helper,
 * so carriers built by either driver generation keep working here.
 */

type NatsModule = typeof import('nats');

let cachedNatsPromise: Promise<NatsModule> | null = null;

export function loadNats(): Promise<NatsModule> {
  // Cache the promise, not the resolved module, so concurrent first-use
  // callers share a single dynamic import instead of evaluating it twice.
  // The specifier comes from the shared constant (not a literal) so the
  // published output contains no `require('nats')` string for the tarball
  // gate to trip on; probing in resolve-driver.ts uses the same constant.
  if (!cachedNatsPromise) {
    const pending = import(LEGACY_DRIVER_PACKAGE) as Promise<NatsModule>;
    pending.catch(() => {
      cachedNatsPromise = null;
    });
    cachedNatsPromise = pending;
  }
  return cachedNatsPromise;
}

export function createLegacyDriver(): NatsDriver {
  return {
    kind: 'legacy',

    async connect(
      options: Partial<NatsConnectionOptions>,
    ): Promise<unknown> {
      const { connect } = await loadNats();
      return connect(options);
    },

    async close(connection: unknown): Promise<void> {
      // Historical behavior: drain, never cut the socket abruptly.
      await (connection as NatsConnection).drain();
    },

    isClosed(connection: unknown): boolean {
      return (connection as NatsConnection).isClosed();
    },

    isDraining(connection: unknown): boolean {
      return (connection as NatsConnection).isDraining();
    },

    status(connection: unknown): AsyncIterable<unknown> {
      return (connection as NatsConnection).status();
    },

    async publish(
      connection: unknown,
      subject: string,
      payload: Uint8Array,
      jetStreamOptions?: unknown,
      publishOptions?: unknown,
    ) {
      const { headers } = await loadNats();
      const nc = connection as NatsConnection;
      const js = nc.jetstream(jetStreamOptions as JetStreamOptions | undefined);
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
      const { headers } = await loadNats();
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
      const nc = connection as NatsConnection;
      const jsm = await nc.jetstreamManager(
        jetStreamOptions as JetStreamOptions | undefined,
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
          // Neutral config in, driver-typed config at the edge.
          const streamInfo = await jsm.streams.update(stream.config.name, {
            ...stream.config,
            ...requiredStreamConfig,
            subjects: [...streamSubjects.keys()],
          } as Partial<StreamUpdateConfig>);
          results.push({ name: streamInfo.config.name, created: false });
        } else {
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
    ): Promise<EventSubscription> {
      const nc = connection as NatsConnection;
      const js = nc.jetstream(jetStreamOptions as JetStreamOptions | undefined);
      const consumerOpts = serverConsumerOptionsBuilder(
        consumerOptions,
        subject,
      ) as unknown as ConsumerOptsBuilder;
      const subscription = await js.subscribe(subject, consumerOpts);
      return {
        messages: (async function* () {
          for await (const msg of subscription) {
            yield { data: msg.data, raw: msg };
          }
        })(),
        destroy: () => subscription.destroy(),
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
