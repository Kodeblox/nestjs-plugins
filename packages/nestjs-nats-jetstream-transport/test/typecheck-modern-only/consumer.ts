/**
 * Strict modern-only consumer fixture (spec 001, section 6, type-level smoke
 * check). Compiled with strict + skipLibCheck:false while the legacy `nats`
 * package is unresolvable (see scripts/check-decoupled-dts.js), so ANY
 * driver import leaking into the published `.d.ts` fails this check.
 * Uses only the neutral public surface.
 */
import {
  NatsConnectionInfo,
  NatsConnectionOptions,
  NatsHeaders,
  NatsJetStreamClientOptions,
  NatsJetStreamContext,
  NatsJetStreamRecordBuilder,
  NatsJetStreamServer,
  NatsJetStreamServerOptions,
  NatsRequestOptionsBuilder,
  NatsStreamConfig,
} from '../../dist/index';

const connectionOptions: NatsConnectionOptions = {
  name: 'modern-app',
  servers: 'nats://localhost:4222',
  // Exotic driver passthrough still typechecks via the index signature.
  token: 's3cr3t',
  connectedHook: async (nc: NatsConnectionInfo) => {
    // eslint-disable-next-line no-console
    console.log(nc.getServer(), nc.isClosed());
    for await (const _status of nc.status()) {
      break;
    }
    await nc.drain();
  },
};

const streamConfig: NatsStreamConfig = {
  name: 'orders',
  subjects: ['orders.*'],
  retention: 'limits',
  num_replicas: 1,
  // Unknown future stream options still typecheck.
  custom_future_option: true,
};

const serverOptions: NatsJetStreamServerOptions = {
  connectionOptions,
  consumerOptions: {
    durable: 'd',
    deliverGroup: 'g',
    deliverToSubject: 'deliver.here',
    manualAck: true,
  },
  streamConfig,
  stream: 'orders',
  driver: 'modern',
};

const clientOptions: NatsJetStreamClientOptions = {
  connectionOptions: { name: 'modern-client', servers: 'nats://x' },
  driver: 'modern',
};

export function buildAll() {
  const server = new NatsJetStreamServer(serverOptions);
  const record = new NatsJetStreamRecordBuilder({ id: 1 });
  record.setMsgId('m-1');
  record.appendHeader('x-trace', 't');
  record.setExpectations({ streamName: 'orders' });
  const built = record.build();
  const req = new NatsRequestOptionsBuilder([1, 2]);
  req.setTimeout(500);
  req.setReplyToSubject('reply.here');
  req.setNoMux(true);
  const reqBuilt = req.build();
  return { server, clientOptions, built, reqBuilt };
}

export function useContext(ctx: NatsJetStreamContext) {
  const subject: string = ctx.message.subject;
  const data: Uint8Array = ctx.message.data;
  const trace: string | undefined = ctx.message.headers?.get('x-trace');
  const count: number = ctx.message.info.deliveryCount;
  ctx.message.ack();
  ctx.message.nak(1000);
  ctx.message.working();
  ctx.message.term('done');
  const raw: unknown = ctx.message.rawMessage;
  const alsoRaw: unknown = ctx.rawMessage;
  return { subject, data, trace, count, raw, alsoRaw };
}

export function useHeaders(h: NatsHeaders) {
  h.append('a', 'b');
  h.set('a', 'c');
  const v: string = h.get('a');
  const present: boolean = h.has('a');
  const keys: string[] = h.keys();
  const values: string[] = h.values('a');
  h.delete('a');
  return { v, present, keys, values };
}

export async function useConnect(
  client: { connect(): Promise<NatsConnectionInfo> },
): Promise<string> {
  const nc = await client.connect();
  return nc.getServer();
}
