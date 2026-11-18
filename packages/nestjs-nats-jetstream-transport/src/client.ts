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
      // Only the creator starts status forwarding so concurrent callers
      // share one loop instead of splitting the status stream.
      let freshConnect = false;
      if (!this.connecting) {
        freshConnect = true;
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
      if (freshConnect) {
        // Detached: ends when the connection's status stream completes.
        void this.forwardStatusUpdates();
      }
    }

    return this.connection as NatsConnectionInfo;
  }

  async close() {
    await this.driver.close(this.connection);
    this.connection = undefined;
  }

  /**
   * Status-event subscription, mirroring Nest's built-in NATS client:
   * `connect()` forwards the driver's live connection status stream into
   * this transport's own emitter (`disconnect` / `reconnect` / `update`
   * carry the driver's status data).
   */
  public on(event: string, callback: Function) {
    this.statusEventEmitter.on(
      event,
      callback as (...args: any[]) => void,
    );
  }

  /**
   * Detached background loop forwarding the driver's connection status
   * stream into the status emitter. Ends when the connection's status
   * stream completes (close/drain); failures are logged, never thrown.
   */
  private async forwardStatusUpdates(): Promise<void> {
    try {
      for await (const event of this.driver.status(this.connection)) {
        const { type, data } = event as {
          type?: unknown;
          data?: unknown;
        };
        const detail =
          data !== null && typeof data === 'object'
            ? JSON.stringify(data)
            : String(data);
        switch (type) {
          case 'error':
            this.logger.error(`NatsStatus: type: "error", data: "${detail}".`);
            break;
          case 'disconnect':
            this.logger.error(
              `NatsStatus: type: "disconnect", data: "${detail}".`,
            );
            this.statusEventEmitter.emit('disconnect', data);
            break;
          case 'reconnect':
            this.logger.log(
              `NatsStatus: type: "reconnect", data: "${detail}".`,
            );
            this.statusEventEmitter.emit('reconnect', data);
            break;
          case 'update':
            this.logger.log(`NatsStatus: type: "update", data: "${detail}".`);
            this.statusEventEmitter.emit('update', data);
            break;
          default:
            this.logger.debug(
              `NatsStatus: type: "${String(type)}", data: "${detail}".`,
            );
            break;
        }
      }
    } catch (err) {
      this.logger.error(
        `NatsStatus forwarding ended: ${(err as Error)?.message}`,
      );
    }
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
