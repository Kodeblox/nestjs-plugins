/**
 * Spec 002, Part B — startup policy integration (real NATS + JetStream).
 *
 * Runs against a live broker on NATS_URL in both driver cells (legacy and
 * modern). Broker-down boots use a dead port (no docker control needed);
 * mid-retry broker return uses docker stop/start of NATS_CONTAINER and is
 * honestly skipped when the container is not controllable here.
 *
 * Run with:
 *   docker run -d --name nats-main -p 4222:4222 nats -js
 *   NATS_URL=nats://localhost:4222 npm run test:integration
 */
import { execFileSync } from 'child_process';
import { NatsJetStreamServer } from '../../src/server';
import { NatsJetStreamClientProxy } from '../../src/client';

const NATS_URL = process.env.NATS_URL;
if (!NATS_URL) {
  // eslint-disable-next-line no-console
  console.warn(
    '[integration:startup-policy] SKIPPED: NATS_URL is not set. Start a JetStream server ' +
      '(docker run -p 4222:4222 nats -js) and run ' +
      'NATS_URL=nats://localhost:4222 npm run test:integration',
  );
}
const describeIntegration = NATS_URL ? describe : describe.skip;

// Dead port: nothing listens here, so dials fail fast with
// CONNECTION_REFUSED (legacy) / connection refused (modern).
const DEAD_URL = 'nats://127.0.0.1:4299';

const NATS_CONTAINER = process.env.NATS_CONTAINER ?? 'nats-main';

const hasBrokerControl = (() => {
  if (!NATS_URL) return false;
  try {
    execFileSync('docker', ['inspect', NATS_CONTAINER], {
      stdio: 'ignore',
      timeout: 15000,
    });
    return true;
  } catch {
    return false;
  }
})();
const itWithBrokerControl = hasBrokerControl ? it : it.skip;
if (!hasBrokerControl && NATS_URL) {
  // eslint-disable-next-line no-console
  console.warn(
    `[integration:startup-policy] broker-restart tests SKIPPED: container '${NATS_CONTAINER}' not controllable`,
  );
}

const waitFor = async (cond: () => boolean, ms = 15000) => {
  const start = Date.now();
  while (!cond()) {
    if (Date.now() - start > ms) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, 50));
  }
};

const waitForServer = async (ms = 30000) => {
  const start = Date.now();
  const { connect } = await import('nats');
  for (;;) {
    try {
      const nc = await connect({ servers: NATS_URL });
      await nc.close();
      return;
    } catch {
      if (Date.now() - start > ms) {
        throw new Error('broker did not come back after restart');
      }
      await new Promise((r) => setTimeout(r, 500));
    }
  }
};

const docker = (verb: 'stop' | 'start') =>
  execFileSync('docker', [verb, NATS_CONTAINER], {
    stdio: 'ignore',
    timeout: 60000,
  });

function listenOutcome(server: NatsJetStreamServer): {
  promise: Promise<unknown>;
  callback: jest.Mock;
} {
  const callback = jest.fn();
  const promise = server.listen(callback);
  return { promise, callback };
}

describeIntegration('startup policy (real server)', () => {
  const runId = Date.now().toString(36);
  let unhandled: unknown[] = [];
  let onUnhandled: (err: unknown) => void;

  beforeEach(() => {
    unhandled = [];
    onUnhandled = (err: unknown) => {
      unhandled.push(err);
    };
    process.on('unhandledRejection', onUnhandled);
  });

  afterEach(async () => {
    await new Promise((r) => setTimeout(r, 50));
    process.removeListener('unhandledRejection', onUnhandled);
    expect(unhandled).toEqual([]);
    if (hasBrokerControl) {
      try {
        docker('start');
      } catch {
        // Already running; ignore.
      }
      await waitForServer().catch(() => undefined);
    }
  });

  describe.each([
    ['legacy', 'legacy'],
    ['modern', 'modern'],
  ])('driver cell: %s', (driver) => {
    const streamName = `startup${runId}${driver}`;
    const subject = `startup.${runId}.${driver}.order.created`;

    function makeServer(extra: any = {}) {
      return new NatsJetStreamServer({
        connectionOptions: {
          name: `startup-${driver}-${runId}`,
          servers: NATS_URL,
        },
        consumerOptions: {
          durable: `sp${runId}${driver}`,
          deliverGroup: `spq${runId}${driver}`,
          deliverTo: `spdlv${runId}${driver}`,
          manualAck: true,
        },
        streamConfig: { name: streamName, subjects: [subject] },
        driver,
        ...extra,
      } as any);
    }

    afterEach(async () => {
      // Best-effort stream cleanup so reruns do not collide.
      try {
        const { connect } = await import('nats');
        const nc = await connect({ servers: NATS_URL });
        const jsm = await nc.jetstreamManager();
        await jsm.streams.delete(streamName).catch(() => undefined);
        await nc.close();
      } catch {
        // Broker may be down in restart tests; ignore.
      }
    });

    it('fail-fast broker-down: callback(err) once, one attempt, no hang', async () => {
      const attempts: any[] = [];
      const server = makeServer({
        connectionOptions: {
          name: `startup-${driver}-ff-${runId}`,
          servers: DEAD_URL,
        },
        startupPolicy: {
          onAttemptFailed: (info: any) => attempts.push(info),
        },
      });
      const { promise, callback } = listenOutcome(server);
      // Resolves (never rejects): the failure travels via the callback.
      await promise;
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback.mock.calls[0][0]).toBeDefined();
      expect(attempts).toHaveLength(1);
      expect(attempts[0]).toMatchObject({ attempt: 1, recoverable: true });
    }, 30000);

    it('retry broker-down: exactly maxAttempts attempts, then callback(err)', async () => {
      const attempts: any[] = [];
      const server = makeServer({
        connectionOptions: {
          name: `startup-${driver}-retry-${runId}`,
          servers: DEAD_URL,
        },
        startupPolicy: {
          mode: 'retry',
          maxAttempts: 3,
          delayMs: 50,
          onAttemptFailed: (info: any) => attempts.push(info),
        },
      });
      const { promise, callback } = listenOutcome(server);
      await promise;
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback.mock.calls[0][0]).toBeDefined();
      expect(attempts).toHaveLength(3);
      expect(attempts.map((a) => a.attempt)).toEqual([1, 2, 3]);
      expect(attempts.every((a) => a.recoverable)).toBe(true);
      expect(attempts.every((a) => a.maxAttempts === 3)).toBe(true);
    }, 30000);

    it('fatal bad stream config aborts a retry loop after one attempt', async () => {
      const attempts: any[] = [];
      const server = makeServer({
        streamConfig: { name: 'bad cfg stream', subjects: [subject] },
        startupPolicy: {
          mode: 'retry',
          maxAttempts: 5,
          delayMs: 20,
          onAttemptFailed: (info: any) => attempts.push(info),
        },
      });
      const { promise, callback } = listenOutcome(server);
      await promise;
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback.mock.calls[0][0]).toBeDefined();
      expect(attempts).toHaveLength(1);
      expect(attempts[0].recoverable).toBe(false);
      await server.close().catch(() => undefined);
    }, 30000);

    itWithBrokerControl(
      'mid-retry broker return: single attach, callback once',
      async () => {
        const attempts: any[] = [];
        docker('stop');
        const server = makeServer({
          connectionOptions: {
            name: `startup-${driver}-return-${runId}`,
            servers: NATS_URL,
          },
          startupPolicy: {
            mode: 'retry',
            maxAttempts: 30,
            delayMs: 500,
            onAttemptFailed: (info: any) => attempts.push(info),
          },
        });
        const received: unknown[] = [];
        server.addHandler(
          subject,
          (async (data: any, ctx: any) => {
            received.push(data);
            ctx.message.ack();
          }) as any,
          true,
        );
        const { promise, callback } = listenOutcome(server);
        try {
          await waitFor(() => attempts.length >= 2);
          // Still down: no success callback, process responsive.
          expect(callback).not.toHaveBeenCalled();
          docker('start');
          await waitForServer();
          await promise;
          expect(callback).toHaveBeenCalledTimes(1);
          expect(callback.mock.calls[0][0]).toBeUndefined();
          expect(attempts.length).toBeGreaterThanOrEqual(2);
          expect(attempts.every((a) => a.recoverable)).toBe(true);
          // Single attach: one live subscription delivers the event once.
          const client = new NatsJetStreamClientProxy({
            connectionOptions: {
              name: `startup-${driver}-client-${runId}`,
              servers: NATS_URL,
            },
            driver,
          } as any);
          const { firstValueFrom } = await import('rxjs');
          await firstValueFrom(client.emit(subject, { id: 1 }));
          await waitFor(() => received.length >= 1);
          await new Promise((r) => setTimeout(r, 1500));
          expect(received).toEqual([{ id: 1 }]);
          await client.close().catch(() => undefined);
        } finally {
          try {
            docker('start');
          } catch {
            // ignore
          }
          await waitForServer().catch(() => undefined);
          await server.close().catch(() => undefined);
        }
      },
      60000,
    );

    itWithBrokerControl(
      'retry-forever stays alive while down, succeeds on return',
      async () => {
        const attempts: any[] = [];
        docker('stop');
        const server = makeServer({
          connectionOptions: {
            name: `startup-${driver}-forever-${runId}`,
            servers: NATS_URL,
          },
          startupPolicy: {
            mode: 'retry-forever',
            delayMs: 300,
            onAttemptFailed: (info: any) => attempts.push(info),
          },
        });
        const { promise, callback } = listenOutcome(server);
        try {
          await waitFor(() => attempts.length >= 3);
          // Never rejects, never calls back while the broker is down.
          expect(callback).not.toHaveBeenCalled();
          expect(attempts.every((a) => a.maxAttempts === undefined)).toBe(true);
          docker('start');
          await waitForServer();
          await promise;
          expect(callback).toHaveBeenCalledTimes(1);
          expect(callback.mock.calls[0][0]).toBeUndefined();
        } finally {
          try {
            docker('start');
          } catch {
            // ignore
          }
          await waitForServer().catch(() => undefined);
          await server.close().catch(() => undefined);
        }
      },
      60000,
    );
  });
});
