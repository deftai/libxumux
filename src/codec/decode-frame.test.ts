import { describe, it, expect } from 'vitest';
import { decodeFrame, StreamFramer, validateFrame, payloadLimit } from './decode-frame.js';
import { encodeFrame } from './encode-frame.js';
import { FrameFlags, XMUX_MAGIC } from './constants.js';
import { XumuxProtocolError } from '../errors.js';

const f = (channel: number, len: number, flags = 0) =>
  encodeFrame({ channel, type: 1, flags, payload: new Uint8Array(len).fill(len & 0xff) });
const code = (fn: () => unknown): number | string => {
  try {
    fn();
    return 'no error';
  } catch (e) {
    return (e as XumuxProtocolError).code;
  }
};

describe('decodeFrame', () => {
  it('decodes an exact frame', () => {
    expect(decodeFrame(f(3, 5))).toMatchObject({ channel: 3, type: 1, flags: 0 });
  });
  it('rejects short, mismatched and oversized input', () => {
    expect(code(() => decodeFrame(new Uint8Array(3)))).toBe(4001);
    expect(code(() => decodeFrame(f(1, 5).subarray(0, 10)))).toBe(4001);
    expect(code(() => decodeFrame(f(1, 5000), 4096))).toBe(4005);
    expect(code(() => decodeFrame(f(0, 5000), 4096))).toBe('no error'); // channel 0 uses 65536
  });
  it('payloadLimit uses 65536 for channel 0', () => {
    expect(payloadLimit(0, 4096)).toBe(65536);
    expect(payloadLimit(1, 4096)).toBe(4096);
  });
});

describe('validateFrame', () => {
  const v = (channel: number, flags: number) =>
    code(() => validateFrame({ channel, type: 1, flags, payload: new Uint8Array(0) }));
  it('applies core §2.3 structural rules', () => {
    expect(v(1, 0)).toBe('no error');
    expect(v(0xffff, 0)).toBe(1002);
    expect(v(1, 0x04)).toBe(1002);
    expect(v(0, FrameFlags.MORE)).toBe(1002);
    expect(v(1, FrameFlags.MORE | FrameFlags.PROBE)).toBe(1002);
    expect(
      code(() =>
        validateFrame({ channel: 1, type: 1, flags: 0x04, payload: new Uint8Array(0) }, 0x04),
      ),
    ).toBe('no error');
  });
});

describe('StreamFramer', () => {
  const stream = new Uint8Array([...XMUX_MAGIC, ...f(1, 3), ...f(2, 0), ...f(3, 70)]);

  it('parses frames fed one byte at a time', () => {
    const fr = new StreamFramer(65536);
    const out = [];
    for (const b of stream) out.push(...fr.push(new Uint8Array([b])));
    expect(out.map((x) => [x.channel, x.payload.length])).toEqual([
      [1, 3],
      [2, 0],
      [3, 70],
    ]);
    expect(fr.bufferedBytes).toBe(0);
  });
  it('parses everything from one chunk, and from uneven chunks', () => {
    expect(new StreamFramer(65536).push(stream)).toHaveLength(3);
    const fr = new StreamFramer(65536);
    const a = fr.push(stream.subarray(0, 7));
    const b = fr.push(stream.subarray(7, 30));
    const c = fr.push(stream.subarray(30));
    expect(a.length + b.length + c.length).toBe(3);
    expect(c.at(-1)!.payload).toEqual(new Uint8Array(70).fill(70));
  });
  it('rejects bad magic with transport scope', () => {
    const fr = new StreamFramer(65536);
    try {
      fr.push(new TextEncoder().encode('OMUX'));
      expect.unreachable();
    } catch (e) {
      expect((e as XumuxProtocolError).scope).toBe('transport');
    }
  });
  it('rejects an oversized frame from its header alone', () => {
    const fr = new StreamFramer(4096, false);
    expect(code(() => fr.push(f(1, 5000).subarray(0, 8)))).toBe(4005);
  });
  it('can skip magic', () => {
    expect(new StreamFramer(65536, false).push(f(1, 1))).toHaveLength(1);
  });
});
