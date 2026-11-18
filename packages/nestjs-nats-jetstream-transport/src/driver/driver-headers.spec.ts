/**
 * Unit (no server): the per-driver headers factory resolves the modern
 * package first and falls back to the legacy package only when the modern
 * one cannot be loaded. `jest.doMock` + a fresh registry per test keep the
 * module-level cache from leaking between cases.
 */
describe('resolveHeadersFactory (no server)', () => {
  it('prefers the modern headers factory when both are installed', () => {
    jest.resetModules();
    const modernHeaders = jest.fn(() => ({ __flavor: 'modern' }));
    jest.doMock('@nats-io/nats-core', () => ({ headers: modernHeaders }));
    const { createHeaders } = require('./driver-headers');
    expect(createHeaders()).toEqual({ __flavor: 'modern' });
    expect(modernHeaders).toHaveBeenCalledTimes(1);
  });

  it('falls back to the legacy factory when the modern package is missing', () => {
    jest.resetModules();
    jest.doMock('@nats-io/nats-core', () => {
      throw new Error('no modern driver');
    });
    const legacyHeaders = jest.fn(() => ({ __flavor: 'legacy' }));
    jest.doMock('nats', () => ({ headers: legacyHeaders }));
    const { createHeaders } = require('./driver-headers');
    expect(createHeaders()).toEqual({ __flavor: 'legacy' });
    expect(legacyHeaders).toHaveBeenCalledTimes(1);
  });

  it('caches the resolved factory across calls', () => {
    jest.resetModules();
    const modernHeaders = jest.fn(() => ({ __flavor: 'modern' }));
    jest.doMock('@nats-io/nats-core', () => ({ headers: modernHeaders }));
    const { createHeaders } = require('./driver-headers');
    createHeaders();
    createHeaders();
    expect(modernHeaders).toHaveBeenCalledTimes(2);
  });
});
