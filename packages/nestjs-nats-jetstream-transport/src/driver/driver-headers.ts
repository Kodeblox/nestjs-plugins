import type { NatsHeaders } from '../nats-jetstream.context';

/**
 * Driver headers factory (spec 001, section 4.2.3). Builders run
 * synchronously in user code with no driver instance in scope, so the
 * factory is resolved per driver package instead of through the `NatsDriver`
 * interface: the modern package first (matching the auto-resolution
 * default), falling back to the legacy package.
 *
 * Either shape is safe on either path: both drivers translate any
 * `keys()`/`values()` carrier into their own headers at publish/request
 * time, so an explicit `driver: 'legacy'` pin with both packages installed
 * still works. The factory is cached after the first successful load; like
 * every other driver access in this package, nothing is evaluated at module
 * load time — the requires below run only on first use.
 */

type HeadersFactory = () => NatsHeaders;

let cachedFactory: HeadersFactory | null = null;

function resolveHeadersFactory(): HeadersFactory {
  if (!cachedFactory) {
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      cachedFactory = require('@nats-io/nats-core').headers as HeadersFactory;
    } catch {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      cachedFactory = require('nats').headers as HeadersFactory;
    }
  }
  return cachedFactory;
}

/** Create an empty header carrier from the resolved driver package. */
export function createHeaders(): NatsHeaders {
  return resolveHeadersFactory()();
}
