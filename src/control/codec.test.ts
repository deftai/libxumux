import { describe, it, expect } from 'vitest';
import {
  decodeHello,
  decodeWelcome,
  decodeOpenChannel,
  decodeCloseChannel,
  decodeClose,
  decodeError,
  decodeWindowUpdate,
  decodeProbeToken,
  windowUpdateFrame,
  pingFrame,
} from './codec.js';

const enc = (v: unknown) => new TextEncoder().encode(typeof v === 'string' ? v : JSON.stringify(v));
const code = (fn: () => unknown): number | string => {
  try {
    fn();
    return 'ok';
  } catch (e) {
    return (e as { code: number }).code;
  }
};

describe('control decoders', () => {
  it('accept valid messages and ignore unknown fields', () => {
    const h = decodeHello(
      enc({
        version: [0, 2, 0],
        future: true,
        channels: [{ id: 1, name: 'a', reliable: true, ordered: false, metadata: { x: 1 } }],
      }),
    );
    expect(h.channels![0]).toMatchObject({ id: 1, name: 'a', ordered: false, metadata: { x: 1 } });
    expect(
      decodeWelcome(enc({ version: [0, 2, 0], rejected: [{ id: 3, code: 4010 }] })).rejected,
    ).toEqual([{ id: 3, code: 4010, reason: undefined }]);
    expect(decodeClose(enc({ code: 1000, reason: 'bye' }))).toEqual({ code: 1000, reason: 'bye' });
    expect(decodeError(enc({ code: 4003, channel: 2 }))).toMatchObject({ code: 4003, channel: 2 });
    expect(decodeCloseChannel(enc({ id: 2 }))).toMatchObject({ id: 2 });
  });

  it.each([
    ['not JSON', () => decodeHello(enc('{nope'))],
    ['bad UTF-8', () => decodeHello(new Uint8Array([0xff, 0xfe]))],
    ['array payload', () => decodeHello(enc([1]))],
    ['missing version', () => decodeHello(enc({}))],
    ['short version', () => decodeHello(enc({ version: [0, 2] }))],
    ['extensions not strings', () => decodeHello(enc({ version: [0, 2, 0], extensions: [1] }))],
    ['channels not array', () => decodeHello(enc({ version: [0, 2, 0], channels: {} }))],
    ['channel not object', () => decodeHello(enc({ version: [0, 2, 0], channels: [1] }))],
    [
      'channel name missing',
      () => decodeOpenChannel(enc({ id: 1, reliable: true, ordered: true })),
    ],
    [
      'reliable not boolean',
      () => decodeOpenChannel(enc({ id: 1, name: 'a', reliable: 1, ordered: true })),
    ],
    [
      'metadata not object',
      () =>
        decodeOpenChannel(enc({ id: 1, name: 'a', reliable: true, ordered: true, metadata: [] })),
    ],
    ['non-integer id', () => decodeCloseChannel(enc({ id: 1.5 }))],
    ['id out of range', () => decodeCloseChannel(enc({ id: 70000 }))],
    ['negative pingInterval', () => decodeHello(enc({ version: [0, 2, 0], pingInterval: -1 }))],
    ['reason not string', () => decodeClose(enc({ code: 1000, reason: 5 }))],
    ['auth not object', () => decodeHello(enc({ version: [0, 2, 0], auth: 'x' }))],
  ])('rejects %s with 4001', (_n, fn) => {
    expect(code(fn)).toBe(4001);
  });
});

describe('binary payloads', () => {
  it('WINDOW_UPDATE round-trips and validates', () => {
    const f = windowUpdateFrame([
      { channel: 1, increment: 10 },
      { channel: 2, increment: 20 },
    ]);
    expect(decodeWindowUpdate(f.payload)).toEqual([
      { channel: 1, increment: 10 },
      { channel: 2, increment: 20 },
    ]);
    expect(code(() => decodeWindowUpdate(new Uint8Array(5)))).toBe(4001);
    expect(code(() => decodeWindowUpdate(new Uint8Array(0)))).toBe(4001);
    expect(code(() => decodeWindowUpdate(new Uint8Array(6)))).toBe(4001);
  });
  it('probe tokens are exactly 4 bytes', () => {
    expect(decodeProbeToken(pingFrame(3, 0xdeadbeef).payload)).toBe(0xdeadbeef);
    expect(decodeProbeToken(new Uint8Array(5))).toBeNull();
  });
});
