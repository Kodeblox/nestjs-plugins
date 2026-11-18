/**
 * Unit (no server): the legacy driver's lazy loading. The `nats` module is
 * mocked so `connect` never dials. Import sharing is pinned via the shared
 * loader-promise identity — not via mock-factory evaluation counts, which
 * jest's require cache dedupes however many times require() is invoked and
 * therefore cannot discriminate the promise-cache fix.
 */
let mockImportShouldFail = false;

jest.mock('nats', () => {
  if (mockImportShouldFail) {
    throw new Error('mock driver load failure');
  }
  const actual = jest.requireActual('nats');
  return {
    ...actual,
    connect: jest.fn(async () => ({ fake: 'connection' })),
  };
});

describe('legacy driver loading (no server)', () => {
  it('shares one loader promise across concurrent first-use callers', async () => {
    jest.resetModules();
    const { loadNats } = require('./legacy-driver');
    const [first, second] = [loadNats(), loadNats()];
    // Promise identity (not mock evaluations): the pre-fix loader returned
    // a fresh promise per call even when both resolved to the same module.
    expect(first).toBe(second);
    await expect(first).resolves.toBeDefined();
  });

  it('resets the loader cache after a failed import so the next call retries', async () => {
    jest.resetModules();
    mockImportShouldFail = true;
    const { loadNats } = require('./legacy-driver');
    await expect(loadNats()).rejects.toThrow('mock driver load failure');
    // Without the failure reset, this would return the same rejected
    // promise; instead the loader retries and now succeeds.
    mockImportShouldFail = false;
    await expect(loadNats()).resolves.toBeDefined();
  });

  it('dials once per driver.connect while sharing the loaded runtime', async () => {
    jest.resetModules();
    const { createLegacyDriver } = require('./legacy-driver');
    const natsMock = require('nats');
    const driver = createLegacyDriver();
    expect(driver.kind).toBe('legacy');
    const [a, b] = await Promise.all([
      driver.connect({ name: 'x' }),
      driver.connect({ name: 'x' }),
    ]);
    // Connection dedupe lives in the client/server in-flight guard (pinned
    // by their own specs); at this layer each connect dials, sharing one
    // loaded runtime.
    expect(natsMock.connect).toHaveBeenCalledTimes(2);
    expect(a).toEqual(b);
  });

  it('delegates close/isClosed/isDraining to the connection', async () => {
    const { createLegacyDriver } = require('./legacy-driver');
    const driver = createLegacyDriver();
    expect(driver.kind).toBe('legacy');
    const nc = {
      drain: jest.fn(async () => undefined),
      isClosed: () => true,
      isDraining: () => false,
    };
    await driver.close(nc);
    expect(nc.drain).toHaveBeenCalledTimes(1);
    expect(driver.isClosed(nc)).toBe(true);
    expect(driver.isDraining(nc)).toBe(false);
  });
});
