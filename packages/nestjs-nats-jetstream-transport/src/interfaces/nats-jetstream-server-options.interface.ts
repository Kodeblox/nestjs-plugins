import { ServerConsumerOptions } from './server-consumer-options.interface';
import { NatsConnectionOptions } from './nats-connection-options.interface';
import { NatsJetStreamOptions } from './nats-jetstream-options.interface';
import { NatsStreamConfig } from './nats-stream-config.interface';
import type { NatsDriverOption } from '../driver/resolve-driver';

export interface NatsJetStreamServerOptions {
  connectionOptions: Partial<NatsConnectionOptions> &
    Pick<NatsConnectionOptions, 'name'>;
  consumerOptions: Partial<ServerConsumerOptions>;
  jetStreamOptions?: NatsJetStreamOptions;
  streamConfig?: NatsStreamConfig | NatsStreamConfig[];
  /**
   * Name of the stream each JetStream handler consumes from. Required by
   * the modern driver (spec 001, section 7.2); may be omitted when exactly
   * one stream is configured via `streamConfig`, which is then reused.
   * Ignored by the legacy driver, which infers the stream server-side.
   */
  stream?: string;
  /**
   * Pin the driver generation. When omitted the driver is resolved lazily
   * at `listen()` time (modern wins if both are installed). Part of the
   * dual-driver support (spec 001).
   */
  driver?: NatsDriverOption;
}
