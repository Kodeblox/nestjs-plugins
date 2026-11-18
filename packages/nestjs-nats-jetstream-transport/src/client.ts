import { EventEmitter } from 'events';
import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';
import { NATS_JETSTREAM_OPTIONS } from './constants';
import { NatsJetStreamClientOptions } from './interfaces/nats-jetstream-client-options.interface';
import type { NatsConnectionInfo } from './interfaces/nats-connection-options.interface';
import {
  jsonPayloadCodec,
  resolveDriver,
  type NatsDriver,
} from './driver/nats-driver';
import { NatsJetStreamRecord } from './utils/nats-jetstream-record.builder';
import { NatsRequestRecord } from './utils/nats-request-record-builder';

@Injectable()
export class NatsJetStreamClientProxy extends ClientProxy {
  private connection: unknown;
  private connecting: Promise<unknown> | null = null;
  private driver: NatsDriver;
  private readonly statusEventEmitter = new EventEmitter();
  private readonly logger = new Logger(NatsJetStreamClientProxy.name);

  constructor(
    @Inject(NATS_JETSTREAM_OPTIONS) private options: NatsJetStreamClientOptions,
  ) {
    super();
  }

  async connect(): Promise<NatsConnectionInfo> {
    const { driver, resolution } = resolveDriver(this.options.driver);
    if (resolution.notice) {
      this.logger.log(resolution.notice);
    }
    this.driver = driver;
    if (!this.connection) {
      // Share one in-flight connect so concurrent callers reuse a single
      // connection (and run the hook once) instead of leaking a duplicate.
      if (!this.connecting) {
        this.connecting = (async () => {
          const connection = await driver.connect(
            this.options.connectionOptions,
          );
          this.connection = connection;
          try {
            if (this.options.connectionOptions.connectedHook) {
              await this.options.connectionOptions.connectedHook(
                connection as NatsConnectionInfo,
              );
            }
          } catch (err) {
            // Best-effort cleanup: the hook failed, so drain the dialed
            // connection instead of orphaning it; the next connect() dials
            // anew. Drain errors are swallowed — the hook failure propagates.
            this.connection = undefined;
            try {
              await driver.close(connection);
            } catch {
              // ignore drain errors while propagating the hook failure
            }
            throw err;
          }
          return connection;
        })();
      }
      try {
        await this.connecting;
      } finally {
        this.connecting = null;
      }
    }

    return this.connection as NatsConnectionInfo;
  }

  async close() {
    await this.driver.close(this.connection);
    this.connection = undefined;
  }

  /**
   * Status-event subscription, mirroring Nest's built-in NATS client: the
   * listener attaches to this transport's own status emitter.
   */
  public on(event: string, callback: Function) {
    this.statusEventEmitter.on(
      event,
      callback as (...args: any[]) => void,
    );
  }

  /**
   * Returns the underlying driver connection (spec 001 section 4.2.5),
   * typed by the caller exactly like the built-in transport
   * (`client.unwrap<NatsConnection>()`). Throws before `connect()`.
   */
  public unwrap<T>(): T {
    if (!this.connection) {
      throw new Error(
        'Not initialized. Please call the "connect" method first.',
      );
    }
    return this.connection as T;
  }

  protected publish(
    packet: ReadPacket,
    callback: (packet: WritePacket) => void,
  ): () => void {
    const subject = this.normalizePattern(packet.pattern);

    if (packet.data instanceof NatsRequestRecord) {
      const options = packet.data.options;
      const payload = jsonPayloadCodec.encode(packet.data.payload);
      this.driver
        .request(this.connection, subject, payload, options)
        .then((data) => jsonPayloadCodec.decode(data) as WritePacket)
        .then((packet) => callback(packet))
        .catch((err) => {
          callback({ err });
        });
      return () => null;
    } else {
      const payload = jsonPayloadCodec.encode(packet.data);
      this.driver
        .request(this.connection, subject, payload)
        .then((data) => jsonPayloadCodec.decode(data) as WritePacket)
        .then((packet) => callback(packet))
        .catch((err) => {
          callback({ err });
        });
      return () => null;
    }
  }

  protected async dispatchEvent(
    packet: ReadPacket<NatsJetStreamRecord>,
  ): Promise<any> {
    const subject = this.normalizePattern(packet.pattern);
    const jetStreamOpts = this.options.jetStreamOption;

    if (packet.data instanceof NatsJetStreamRecord) {
      const payload = jsonPayloadCodec.encode(packet.data.payload);
      const options = packet.data.options;
      return this.driver.publish(
        this.connection,
        subject,
        payload,
        jetStreamOpts,
        options,
      );
    } else {
      const payload = jsonPayloadCodec.encode(packet.data);
      return this.driver.publish(
        this.connection,
        subject,
        payload,
        jetStreamOpts,
      );
    }
  }
}
