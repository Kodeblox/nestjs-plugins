/**
 * Unit (no server): `resolveDriver` branch coverage plus the JSON payload
 * codec's legacy parity. The legacy `nats` package is installed but never
 * connected here.
 */
import { JSONCodec } from 'nats';
import { jsonPayloadCodec, resolveDriver, resolveStreamName } from './nats-driver';
import { resolveNatsDriver } from './resolve-driver';

jest.mock('./resolve-driver', () => {
  const actual = jest.requireActual('./resolve-driver');
  return { ...actual, resolveNatsDriver: jest.fn(actual.resolveNatsDriver) };
});

const resolveMock = resolveNatsDriver as unknown as jest.Mock;
const legacyCodec = JSONCodec();
const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

describe('resolveDriver (no server)', () => {
  it('returns the legacy driver for legacy resolution', () => {
    resolveMock.mockReturnValueOnce({
      driver: 'legacy',
      legacyAvailable: true,
      modernAvailable: false,
    });
    const { driver, resolution } = resolveDriver(undefined);
    expect(driver.kind).toBe('legacy');
    expect(resolution.driver).toBe('legacy');
  });

  it('returns the modern driver for modern resolution', () => {
    resolveMock.mockReturnValueOnce({
      driver: 'modern',
      legacyAvailable: false,
      modernAvailable: true,
    });
    const { driver, resolution } = resolveDriver(undefined);
    expect(driver.kind).toBe('modern');
    expect(resolution.driver).toBe('modern');
  });

  it('resolves the stream name explicitly, from a single streamConfig, or throws', () => {
    expect(
      resolveStreamName({ stream: 's', streamConfig: { name: 'other', subjects: [] } } as any),
    ).toBe('s');
    expect(
      resolveStreamName({ streamConfig: { name: 'only', subjects: [] } } as any),
    ).toBe('only');
    expect(
      resolveStreamName({ streamConfig: [{ name: 'only', subjects: [] }] } as any),
    ).toBe('only');
    expect(() =>
      resolveStreamName({ streamConfig: [
        { name: 'a', subjects: [] },
        { name: 'b', subjects: [] },
      ] } as any),
    ).toThrow(/needs a stream name/);
    expect(() => resolveStreamName({} as any)).toThrow(/needs a stream name/);
  });
});

describe('jsonPayloadCodec (legacy parity)', () => {
  it('encodes undefined as null bytes, matching the legacy codec', () => {
    expect(textDecoder.decode(jsonPayloadCodec.encode(undefined))).toBe(
      'null',
    );
    expect(jsonPayloadCodec.encode(undefined)).toEqual(
      legacyCodec.encode(undefined),
    );
    expect(jsonPayloadCodec.decode(jsonPayloadCodec.encode(undefined))).toBe(
      null,
    );
  });

  it('round-trips JSON payloads byte-identically to the legacy codec', () => {
    const payload = { id: 7, tags: ['a', 'b'], nested: { n: 1 } };
    expect(jsonPayloadCodec.encode(payload)).toEqual(
      legacyCodec.encode(payload as any),
    );
    expect(jsonPayloadCodec.decode(jsonPayloadCodec.encode(payload))).toEqual(
      payload,
    );
  });

  it('wraps circular encode failures with the BadJson shape', () => {
    const circular: any = {};
    circular.self = circular;
    let ours: any = null;
    let theirs: any = null;
    try {
      jsonPayloadCodec.encode(circular);
    } catch (err) {
      ours = err;
    }
    try {
      legacyCodec.encode(circular);
    } catch (err) {
      theirs = err;
    }
    expect(ours).not.toBeNull();
    for (const err of [ours, theirs]) {
      expect(err.name).toBe('NatsError');
      expect(err.message).toBe('Bad JSON');
      expect(err.code).toBe('BAD_JSON');
      expect(err.chainedError).toBeInstanceOf(TypeError);
    }
  });

  it('wraps decode failures with the BadJson shape', () => {
    const empty = textEncoder.encode('');
    let ours: any = null;
    let theirs: any = null;
    try {
      jsonPayloadCodec.decode(empty);
    } catch (err) {
      ours = err;
    }
    try {
      legacyCodec.decode(empty);
    } catch (err) {
      theirs = err;
    }
    expect(ours).not.toBeNull();
    for (const err of [ours, theirs]) {
      expect(err.name).toBe('NatsError');
      expect(err.message).toBe('Bad JSON');
      expect(err.code).toBe('BAD_JSON');
      expect(err.chainedError).toBeInstanceOf(SyntaxError);
    }
  });
});
