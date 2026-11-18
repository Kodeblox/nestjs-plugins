import { EventEmitter } from 'events';
import { CustomTransportStrategy, Server } from '@nestjs/microservices';

import { connectable, isObservable, Subject } from 'rxjs';
import { NATS_JETSTREAM_TRANSPORT } from './constants';
import {
  NatsJetStreamServerOptions,
  StartupPolicyAttemptInfo,
} from './interfaces/nats-jetstream-server-options.interface';
import type { NatsConnectionInfo } from './interfaces/nats-connection-options.interface';
import {
  NatsContext,
  NatsJetStreamContext,
  toJetStreamMessage,
} from './nats-jetstream.context';
import {
  jsonPayloadCodec,
  resolveDriver,
  resolveStreamName,
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
  private readonly statusEventEmitter = new EventEmitter();

  constructor(private options: NatsJetStreamServerOptions) {
    super();
  }

  /**
   * Spec 002, Part A: every startup failure — driver resolution,
   * `connect()`, `connectedHook`, `setupStream()`, handler binding — is
   * routed to `callback(err)`. The callback fires exactly once: with no
   * error on success, with the terminal error otherwise. This method never
   * rejects, so the Nest microservice wrapper (which settles only via the
   * callback and drops the returned promise) can no longer produce an
   * unhandled rejection plus a hung boot promise.
   */
  async listen(callback: (err?: unknown, info?: unknown) => void) {
    try {
      await this.startWithPolicy();
      callback();
    } catch (err) {
      callback(err);
    }
  }

  /**
   * Spec 002, Part B: run `attemptStartup()` under the configured startup
   * policy (default `fail-fast`: a single attempt). Retries reuse the
   * in-flight connect guard inside `attemptStartup()` — one connection per
   * attempt, never a leaked dial — and every attempt goes through the
   * driver interface. A non-recoverable error aborts the loop at once.
   */
  private async startWithPolicy(): Promise<void> {
    const policy = this.options.startupPolicy;
    const mode = policy?.mode ?? 'fail-fast';
    // Runtime guard for JS consumers and config-driven typos (TS covers
    // typed users): an unrecognized mode must not silently boot with
    // fail-fast semantics and no signal.
    if (mode !== 'fail-fast' && mode !== 'retry' && mode !== 'retry-forever') {
      const err = new Error(
        `Unknown NATS startupPolicy.mode '${String(mode)}'. ` +
          `Expected one of 'fail-fast' | 'retry' | 'retry-forever'.`,
      );
      this.logger.error(err.message);
      throw err;
    }
    const maxAttempts =
      mode === 'retry-forever'
        ? Number.POSITIVE_INFINITY
        : Math.max(
            1,
            Math.floor(policy?.maxAttempts ?? (mode === 'retry' ? 5 : 1)) || 1,
          );
    let attempt = 0;
    for (;;) {
      attempt += 1;
      try {
        await this.attemptStartup();
        return;
      } catch (err) {
        const recoverable = isRecoverableStartupError(err);
        const info: StartupPolicyAttemptInfo = {
          attempt,
          maxAttempts: mode === 'retry-forever' ? undefined : maxAttempts,
          err,
          recoverable,
        };
        try {
          // Awaited, not floated: the hook type admits async functions, and
          // a floating rejection would escape as an unhandled rejection —
          // the crash class Part A eliminates. Hook failures are logged and
          // swallowed; the attempt error below still drives retry/abort.
          await policy?.onAttemptFailed?.(info);
        } catch (hookErr) {
          this.logger.error(
            `NATS startup onAttemptFailed hook threw: ${startupErrorLabel(
              hookErr,
            )}`,
          );
        }
        if (!recoverable) {
          this.logger.error(
            `NATS startup attempt ${attempt} failed (fatal ${startupErrorLabel(
              err,
            )}), aborting`,
          );
          throw err;
        }
        if (attempt >= maxAttempts) {
          this.logger.error(
            `NATS startup failed after ${attempt} attempt(s) (last: ${startupErrorLabel(
              err,
            )})`,
          );
          throw err;
        }
        const delay = this.startupRetryDelay(attempt);
        const total = mode === 'retry-forever' ? '' : `/${maxAttempts}`;
        this.logger.log(
          `NATS startup attempt ${attempt}${total} failed (recoverable ${startupErrorLabel(
            err,
          )}), retrying in ${delay}ms`,
        );
        await sleep(delay);
      }
    }
  }

  /**
   * One startup attempt: the pre-spec-002 `listen()` body. On failure the
   * partial state is released cheaply (an opened connection is drained and
   * forgotten, so the next attempt dials anew and no dial leaks) and the
   * error propagates to the policy loop, which routes it to the callback.
   */
  private async attemptStartup(): Promise<void> {
    const { driver, resolution } = resolveDriver(this.options.driver);
    if (resolution.notice) {
      this.logger.log(resolution.notice);
    }
    this.driver = driver;
    try {
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
        if (freshConnect) {
          // Detached: ends when the connection's status stream completes.
          void this.forwardStatusUpdates();
        }
      }
      if (this.options.streamConfig) {
        await this.setupStream();
      }

      await this.bindEventHandlers();
      this.bindMessageHandlers();
    } catch (err) {
      await this.teardownPartialConnection();
      throw err;
    }
  }

  /** Drain and forget an opened connection after a later step failed. */
  private async teardownPartialConnection(): Promise<void> {
    const connection = this.connection;
    this.connection = undefined;
    if (connection !== undefined && this.driver !== undefined) {
      try {
        await this.driver.close(connection);
      } catch {
        // Best effort: the startup error propagates, not the drain error.
      }
    }
  }

  /**
   * Gap between startup attempts: explicit `delayMs`, else the existing
   * `reconnectDelayHandler()` called per gap, else `reconnectTimeWait`,
   * else 2000ms. `maxReconnectAttempts` is never consulted: it governs
   * steady-state reconnects inside the driver, not startup attempts.
   */
  private startupRetryDelay(attempt: number): number {
    const override = this.options.startupPolicy?.delayMs;
    if (typeof override === 'number' && Number.isFinite(override)) {
      return Math.max(0, override);
    }
    const handler = this.options.connectionOptions.reconnectDelayHandler;
    if (typeof handler === 'function') {
      try {
        const value = (handler as (attempt: number) => number)(attempt);
        if (typeof value === 'number' && Number.isFinite(value)) {
          return Math.max(0, value);
        }
      } catch {
        // A throwing backoff must not break the retry loop; fall through.
      }
    }
    const wait = this.options.connectionOptions.reconnectTimeWait;
    if (typeof wait === 'number' && Number.isFinite(wait)) {
      return Math.max(0, wait);
    }
    return 2000;
  }

  async close() {
    if (this.driver === undefined || this.connection === undefined) {
      return;
    }
    await this.driver.close(this.connection);
    this.connection = undefined;
  }

  /**
   * Status-event subscription, mirroring Nest's built-in NATS server:
   * `listen()` forwards the driver's live connection status stream into
   * this transport's own emitter (`disconnect` / `reconnect` / `update`
   * carry the driver's status data). Message handlers keep going through
   * `addHandler` (used by `listen()`).
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
   * typed by the caller exactly like the built-in transport. Throws
   * before `listen()`.
   */
  public unwrap<T>(): T {
    if (!this.connection) {
      throw new Error(
        'Not initialized. Please call the "listen"/"startAllMicroservices" method before accessing the server.',
      );
    }
    return this.connection as T;
  }

  private async bindEventHandlers() {
    const eventHandlers = [...this.messageHandlers.entries()].filter(
      ([, handler]) => handler.isEventHandler,
    );

    for (const [subject, eventHandler] of eventHandlers) {
      // Only the modern driver needs the stream name; the legacy push
      // subscribe infers the stream server-side.
      const stream =
        this.driver.kind === 'modern'
          ? resolveStreamName(this.options)
          : undefined;
      const subscription = await this.driver.subscribeEvents(
        this.connection,
        subject,
        this.options.consumerOptions,
        this.options.jetStreamOptions,
        stream,
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Compact `code message` label for per-attempt log lines. */
function startupErrorLabel(err: unknown): string {
  if (err instanceof Error) {
    const code = (err as Error & { code?: unknown }).code;
    const prefix =
      typeof code === 'string' || typeof code === 'number' ? `[${code}] ` : '';
    return `${prefix}${err.message}`;
  }
  return String(err);
}

/**
 * Startup error classifier (spec 002, section 3), seeded from the app-side
 * taxonomy: dial-level failures (refused/timeout/DNS/unavailable) are
 * recoverable, auth/bad-config/held-port failures are fatal. Anything
 * unrecognized is recoverable — in `retry` modes an unknown error is
 * retried, while `fail-fast` (the default) is unaffected either way.
 */
const RECOVERABLE_CODES = new Set([
  'CONNECTION_REFUSED',
  'CONN_REFUSED',
  'CONNECTION_TIMEOUT',
  'CONNECTION_TIMED_OUT',
  'CONN_TIMEOUT',
  'TIMEOUT',
  'ETIMEDOUT',
  'ECONNREFUSED',
  'ECONNRESET',
  'ENOTFOUND',
  'EAI_AGAIN',
  'EHOSTUNREACH',
  'EHOSTDOWN',
  'ENETUNREACH',
  'ENETDOWN',
  'EPIPE',
  'SERVER_UNAVAILABLE',
  'NO_SERVERS',
  'CONNECTION_CLOSED',
]);

const FATAL_CODES = new Set([
  'AUTHORIZATION_VIOLATION',
  'AUTHENTICATION_EXPIRED',
  'AUTH_TIMEOUT',
  'PERMISSIONS_VIOLATION',
  'EADDRINUSE',
]);

const FATAL_MESSAGE_PATTERNS = [
  /EADDRINUSE/,
  /address already in use/i,
  /authorization violation/i,
  /permissions? violation/i,
  /unauthorized/i,
  /\bforbidden\b/i,
  /bad credentials/i,
  /\bauth(?:entication|orization)? (?:failed|failure|error|timeout|violation)\b/i,
  /\bordered consumer:/i,
  /invalid (?:stream|subject|consumer|config)/i,
  // JetStream validation phrasing observed against a real broker, e.g.
  // "stream name ('bad cfg stream') cannot contain ' '" (modern driver)
  // and 'subject "x" is invalid' variants.
  /cannot contain/i,
  /is invalid/i,
  // Deterministic client-side config validation (nats.js jsclient):
  // retrying a consumer that can never be built is pointless.
  /requires deliver_subject/i,
  /unknown field/i,
  /\bbad request\b/i,
  /driver mismatch/i,
  /no nats driver found/i,
];

const RECOVERABLE_MESSAGE_PATTERNS = [
  /refused/i,
  /timed? ?out/i,
  /ENOTFOUND/i,
  /EAI_AGAIN/i,
  /ECONNRESET/i,
  /ECONNREFUSED/i,
  /ETIMEDOUT/i,
  /EHOSTUNREACH/i,
  /ENETUNREACH/i,
  /server.*unavailable/i,
  /no.*servers? (?:available|found)/i,
];

function errorCodes(err: unknown): Array<string | number> {
  const seen: Array<string | number> = [];
  const push = (value: unknown) => {
    if (typeof value === 'string' || typeof value === 'number') {
      seen.push(value);
    }
  };
  if (err !== null && typeof err === 'object') {
    const record = err as Record<string, unknown>;
    push(record.code);
    push(record.statusCode);
    const apiError = record.api_error as Record<string, unknown> | undefined;
    if (apiError !== null && typeof apiError === 'object') {
      push(apiError.err_code);
    }
  }
  return seen;
}

export function isRecoverableStartupError(err: unknown): boolean {
  for (const code of errorCodes(err)) {
    if (typeof code === 'number') {
      // JetStream API status: auth/bad-config are fatal, the rest defer
      // to the message patterns below.
      if (code === 401 || code === 403 || code === 400 || code === 422) {
        return false;
      }
      continue;
    }
    if (FATAL_CODES.has(code)) {
      return false;
    }
    if (RECOVERABLE_CODES.has(code)) {
      return true;
    }
  }
  const message = err instanceof Error ? err.message : String(err);
  if (FATAL_MESSAGE_PATTERNS.some((pattern) => pattern.test(message))) {
    return false;
  }
  if (RECOVERABLE_MESSAGE_PATTERNS.some((pattern) => pattern.test(message))) {
    return true;
  }
  return true;
}
