import type { JetStreamOptions } from 'nats';
import { ServerConsumerOptions } from './server-consumer-options.interface';
import { NatsConnectionOptions } from './nats-connection-options.interface';
import { NatsStreamConfig } from './nats-stream-config.interface';
import type { NatsDriverOption } from '../driver/resolve-driver';

export interface NatsJetStreamServerOptions {
  connectionOptions: Partial<NatsConnectionOptions> &
    Pick<NatsConnectionOptions, 'name'>;
  consumerOptions: Partial<ServerConsumerOptions>;
  jetStreamOptions?: JetStreamOptions;
  streamConfig?: NatsStreamConfig | NatsStreamConfig[];
  /**
   * Pin the driver generation. When omitted the driver is resolved lazily
   * at `listen()` time (modern wins if both are installed). Part of the
   * dual-driver support (spec 001); the legacy path is used until the
   * Phase 3 modern driver lands.
   */
  driver?: NatsDriverOption;
}
