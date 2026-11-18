/**
 * Unit (no server, no driver): lazy driver resolution and its actionable
 * startup errors. Availability is injected, so no driver package is loaded.
 */
import { resolveNatsDriver } from './resolve-driver';

function probe(installed: string[]) {
  return (pkg: string) => installed.includes(pkg);
}

const LEGACY = ['nats'];
const MODERN = ['@nats-io/transport-node', '@nats-io/jetstream'];
const BOTH = [...LEGACY, ...MODERN];

describe('resolveNatsDriver (lazy, no server)', () => {
  it('resolves legacy when only nats is installed', () => {
    const r = resolveNatsDriver(undefined, probe(LEGACY));
    expect(r.driver).toBe('legacy');
    expect(r.notice).toBeUndefined();
  });

  it('resolves modern when only @nats-io/* is installed', () => {
    const r = resolveNatsDriver(undefined, probe(MODERN));
    expect(r.driver).toBe('modern');
    expect(r.notice).toBeUndefined();
  });

  it('prefers modern with a notice when both are installed', () => {
    const r = resolveNatsDriver(undefined, probe(BOTH));
    expect(r.driver).toBe('modern');
    expect(r.legacyAvailable).toBe(true);
    expect(r.modernAvailable).toBe(true);
    expect(r.notice).toMatch(/both/i);
    expect(r.notice).toMatch(/options\.driver/);
  });

  it('throws an actionable error when neither driver is installed', () => {
    expect(() => resolveNatsDriver(undefined, probe([]))).toThrow(/no nats driver/i);
    expect(() => resolveNatsDriver(undefined, probe([]))).toThrow(/nats@\^2/);
    expect(() => resolveNatsDriver(undefined, probe([]))).toThrow(
      /@nats-io\/transport-node/,
    );
  });

  it('honours an explicit legacy option when nats is present', () => {
    expect(resolveNatsDriver('legacy', probe(LEGACY))).toMatchObject({
      driver: 'legacy',
    });
  });

  it('honours an explicit modern option when @nats-io/* is present', () => {
    expect(resolveNatsDriver('modern', probe(MODERN))).toMatchObject({
      driver: 'modern',
    });
  });

  it('throws a mismatch error for explicit legacy without nats', () => {
    expect(() => resolveNatsDriver('legacy', probe(MODERN))).toThrow(
      /driver mismatch.*'legacy'.*'nats' is not installed/,
    );
  });

  it('throws a mismatch error for explicit modern without @nats-io/*', () => {
    expect(() => resolveNatsDriver('modern', probe(LEGACY))).toThrow(
      /driver mismatch.*'modern'.*@nats-io\/transport-node/,
    );
  });

  it('explicit choice wins when both drivers are installed', () => {
    expect(resolveNatsDriver('legacy', probe(BOTH)).driver).toBe('legacy');
    expect(
      resolveNatsDriver('legacy', probe(BOTH)).notice,
    ).toBeUndefined();
  });

  it('does not misresolve to modern for jetstream-without-transport', () => {
    // Only the connection package counts as a usable modern driver: with
    // nats plus a lone @nats-io/jetstream (no transport), resolve legacy
    // with no notice rather than modern.
    const r = resolveNatsDriver(
      undefined,
      probe(['nats', '@nats-io/jetstream']),
    );
    expect(r.driver).toBe('legacy');
    expect(r.modernAvailable).toBe(false);
    expect(r.notice).toBeUndefined();
  });

  it('treats a lone @nats-io/transport-node install as modern', () => {
    const r = resolveNatsDriver(
      undefined,
      probe(['@nats-io/transport-node']),
    );
    expect(r.driver).toBe('modern');
  });

  it('throws a mismatch error for an explicit option with neither installed', () => {
    expect(() => resolveNatsDriver('legacy', probe([]))).toThrow(/driver mismatch/);
    expect(() => resolveNatsDriver('modern', probe([]))).toThrow(/driver mismatch/);
  });

  it('default probe runs self-consistently in this repo', () => {
    // No injected probe: exercises the real require.resolve path used in
    // production. Pinned loosely so installing the modern driver later does
    // not break it — the resolution must simply agree with availability.
    const r = resolveNatsDriver();
    expect(r.legacyAvailable).toBe(true);
    expect(r.driver).toBe(r.modernAvailable ? 'modern' : 'legacy');
  });
});
