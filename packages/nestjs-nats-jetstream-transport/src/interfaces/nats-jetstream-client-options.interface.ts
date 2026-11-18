import { ModuleMetadata } from '@nestjs/common';
import { NatsConnectionOptions } from './nats-connection-options.interface';
import { NatsJetStreamOptions } from './nats-jetstream-options.interface';
import type { NatsDriverOption } from '../driver/resolve-driver';

export interface NatsJetStreamClientOptions {
  connectionOptions: Partial<NatsConnectionOptions> &
    Pick<NatsConnectionOptions, 'name'>;
  jetStreamOption?: NatsJetStreamOptions;
  /**
   * Pin the driver generation. When omitted the driver is resolved lazily
   * at `connect()` time (modern wins if both are installed). Part of the
   * dual-driver support (spec 001).
   */
  driver?: NatsDriverOption;
}

// noinspection JSUnusedGlobalSymbols
export interface NatsJetStreamClientAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  useFactory: (
    ...args: any[]
  ) => Promise<NatsJetStreamClientOptions> | NatsJetStreamClientOptions;
  inject?: any[];
}
