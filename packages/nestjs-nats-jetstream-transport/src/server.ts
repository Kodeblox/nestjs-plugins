import { CustomTransportStrategy, Server } from '@nestjs/microservices';

import { connectable, isObservable, Subject } from 'rxjs';
import { NATS_JETSTREAM_TRANSPORT } from './constants';
import { NatsJetStreamServerOptions } from './interfaces/nats-jetstream-server-options.interface';
import type { NatsConnectionInfo } from './interfaces/nats-connection-options.interface';
import {
  NatsContext,
  NatsJetStreamContext,
  toJetStreamMessage,
} from './nats-jetstream.context';
import {
  jsonPayloadCodec,
  resolveDriver,
  type NatsDriver,
} from './driver/nats-driver';

export class NatsJetStreamServer
  extends Server
  implements CustomTransportStrategy
{
  readonly transportId: symbol = NATS_JETSTREAM_TRANSPORT;
  private connection: unknown;
  private connecting: Promise<unknown> | null = null;
  private driver: NatsDriver;

  constructor(private options: NatsJetStreamServerOptions) {
    super();
  }

  async listen(callback: () => void) {
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
            // connection instead of orphaning it; the next listen() dials
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
    if (this.options.streamConfig) {
      await this.setupStream();
    }

    await this.bindEventHandlers();
    this.bindMessageHandlers();
    callback();
  }

  async close() {
    await this.driver.close(this.connection);
    this.connection = undefined;
  }

  private async bindEventHandlers() {
    const eventHandlers = [...this.messageHandlers.entries()].filter(
      ([, handler]) => handler.isEventHandler,
    );

    for (const [subject, eventHandler] of eventHandlers) {
      const subscription = await this.driver.subscribeEvents(
        this.connection,
        subject,
        this.options.consumerOptions,
        this.options.jetStreamOptions,
      );
      this.logger.log(`Subscribed to ${subject} events`);
      const done = (async () => {
        for await (const consumed of subscription.messages) {
          try {
            const data = jsonPayloadCodec.decode(consumed.data);
            const context = new NatsJetStreamContext([consumed.raw]);
            const resultOrStream = await eventHandler(data, context);
            if (isObservable(resultOrStream)) {
              const connectableSource = connectable(resultOrStream, {
                connector: () => new Subject(),
                resetOnDisconnect: false,
              });
              connectableSource.connect();
            }
          } catch (err) {
            this.logger.error(err.message, err.stack);
            // specifies that you failed to process the server and instructs
            // the server to not send it again (to any consumer)
            toJetStreamMessage(consumed.raw).term();
          }
        }
      })();
      done.then(() => {
        // if the connection is closed or draining, we don't need to unsubscribe as the subscription will be unsubscribed automatically
        if (
          this.driver.isDraining(this.connection) ||
          this.driver.isClosed(this.connection)
        ) {
          return;
        }
        subscription.destroy();

        this.logger.log(`Unsubscribed ${subject}`);
      });
    }
  }

  private bindMessageHandlers() {
    const messageHandlers = [...this.messageHandlers.entries()].filter(
      ([, handler]) => !handler.isEventHandler,
    );

    for (const [subject, messageHandler] of messageHandlers) {
      this.driver.subscribeMessages(
        this.connection,
        subject,
        this.options.consumerOptions.deliverTo,
        async (err, message) => {
          if (err) {
            return this.logger.error(err.message, err.stack);
          }
          const payload = jsonPayloadCodec.decode(message.data);
          const context = new NatsContext([message.raw]);
          const response$ = this.transformToObservable(
            messageHandler(payload, context),
          );
          this.send(response$, (response) =>
            context.message.respond(jsonPayloadCodec.encode(response)),
          );
        },
      );
      this.logger.log(`Subscribed to ${subject} messages`);
    }
  }

  private async setupStream() {
    const { streamConfig } = this.options;
    const reqStreamConfigs = !Array.isArray(streamConfig)
      ? [streamConfig]
      : streamConfig;

    const results = await this.driver.ensureStreams(
      this.connection,
      this.options.jetStreamOptions,
      reqStreamConfigs,
    );
    for (const result of results) {
      this.logger.log(
        `Stream ${result.name} ${result.created ? 'created' : 'updated'}`,
      );
    }
  }
}
