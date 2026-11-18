import { ModuleMetadata } from '@nestjs/common';
import type { ConnectionOptions, JetStreamOptions } from 'nats';
import { NatsConnectionOptions } from './nats-connection-options.interface';
import type { NatsDriverOption } from '../driver/resolve-driver';

export interface NatsJetStreamClientOptions {
  connectionOptions: Partial<NatsConnectionOptions> &
    Pick<ConnectionOptions, 'name'>;
  jetStreamOption?: JetStreamOptions;
  /**
   * Pin the driver generation. When omitted the driver is resolved lazily
   * at `connect()` time (modern wins if both are installed). Part of the
   * dual-driver support (spec 001); the legacy path is used until the
   * Phase 3 modern driver lands.
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
