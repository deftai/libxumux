import { describe, it, expect } from 'vitest';
import { encodeFrame } from './encode-frame.js';
import { FrameFlags } from './constants.js';

describe('encodeFrame', () => {
  it('encodes an empty-payload standard frame', () => {
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array(0) });
    expect(bytes).toHaveLength(6);
    expect(bytes[0]).toBe(0x00); // channel
    expect(bytes[1]).toBe(0x01); // type
    expect(bytes[2]).toBe(0x00); // flags
    expect(bytes[3]).toBe(0x00); // reserved
    expect(bytes[4]).toBe(0x00); // length hi
    expect(bytes[5]).toBe(0x00); // length lo
  });

  it('encodes a frame with payload', () => {
    const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const bytes = encodeFrame({ channel: 1, type: 0x10, flags: 0, payload });
    expect(bytes).toHaveLength(10);
    expect(bytes[0]).toBe(1);
    expect(bytes[1]).toBe(0x10);
    expect(bytes[4]).toBe(0x00);
    expect(bytes[5]).toBe(0x04); // length = 4
    expect(bytes.slice(6)).toEqual(payload);
  });

  it('sets length bytes correctly (big-endian)', () => {
    const payload = new Uint8Array(300);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    expect(bytes[4]).toBe(0x01); // 300 = 0x012C → hi = 0x01
    expect(bytes[5]).toBe(0x2c); // lo = 0x2C
  });

  it('uses standard 6-byte header for payload <= 65535 bytes', () => {
    const payload = new Uint8Array(65535);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    expect(bytes).toHaveLength(6 + 65535);
    expect(bytes[2] & FrameFlags.EXTENDED_LENGTH).toBe(0); // no EXT flag
  });

  it('uses extended 8-byte header for payload > 65535 bytes', () => {
    const payload = new Uint8Array(65536);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    expect(bytes).toHaveLength(8 + 65536);
    expect(bytes[2] & FrameFlags.EXTENDED_LENGTH).toBe(FrameFlags.EXTENDED_LENGTH);
    // Length field: 65536 = 0x00010000
    expect(bytes[4]).toBe(0x00);
    expect(bytes[5]).toBe(0x01);
    expect(bytes[6]).toBe(0x00);
    expect(bytes[7]).toBe(0x00);
  });

  it('forces extended header when EXTENDED_LENGTH flag is set', () => {
    const payload = new Uint8Array(10);
    const bytes = encodeFrame({
      channel: 0, type: 1, flags: FrameFlags.EXTENDED_LENGTH, payload,
    });
    expect(bytes).toHaveLength(8 + 10);
    expect(bytes[2] & FrameFlags.EXTENDED_LENGTH).toBe(FrameFlags.EXTENDED_LENGTH);
  });

  it('preserves FRAGMENT and FRAGMENT_END flags', () => {
    const payload = new Uint8Array(8);
    const flags = FrameFlags.FRAGMENT | FrameFlags.FRAGMENT_END;
    const bytes = encodeFrame({ channel: 3, type: 2, flags, payload });
    expect(bytes[2] & FrameFlags.FRAGMENT).toBeTruthy();
    expect(bytes[2] & FrameFlags.FRAGMENT_END).toBeTruthy();
  });

  it('PING test vector: channel=0, type=0x10, 4-byte payload 0x000003E8', () => {
    // From spec: 00 10 00 00 00 04 00 00 03 E8
    const payload = new Uint8Array([0x00, 0x00, 0x03, 0xe8]);
    const bytes = encodeFrame({ channel: 0x00, type: 0x10, flags: 0, payload });
    const expected = new Uint8Array([0x00, 0x10, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x03, 0xe8]);
    expect(bytes).toEqual(expected);
  });
});
