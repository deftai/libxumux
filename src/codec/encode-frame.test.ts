import { describe, it, expect } from 'vitest';
import { encodeFrame } from './encode-frame.js';
import { FrameFlags } from './constants.js';

describe('encodeFrame', () => {
  it('encodes an empty-payload frame (8-byte header)', () => {
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array(0) });
    expect(bytes).toHaveLength(8);
    expect(bytes[0]).toBe(0x00); // channel hi
    expect(bytes[1]).toBe(0x00); // channel lo
    expect(bytes[2]).toBe(0x01); // type
    expect(bytes[3]).toBe(0x00); // flags
    expect(bytes[4]).toBe(0x00); // length byte 0
    expect(bytes[5]).toBe(0x00); // length byte 1
    expect(bytes[6]).toBe(0x00); // length byte 2
    expect(bytes[7]).toBe(0x00); // length byte 3
  });

  it('encodes a frame with payload', () => {
    const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const bytes = encodeFrame({ channel: 1, type: 0x10, flags: 0, payload });
    expect(bytes).toHaveLength(12);
    expect(bytes[0]).toBe(0x00); // channel hi
    expect(bytes[1]).toBe(0x01); // channel lo
    expect(bytes[2]).toBe(0x10); // type
    expect(bytes[6]).toBe(0x00);
    expect(bytes[7]).toBe(0x04); // length = 4
    expect(bytes.slice(8)).toEqual(payload);
  });

  it('sets length bytes correctly (big-endian, 4 bytes)', () => {
    const payload = new Uint8Array(300);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    // 300 = 0x0000012C
    expect(bytes[4]).toBe(0x00);
    expect(bytes[5]).toBe(0x00);
    expect(bytes[6]).toBe(0x01);
    expect(bytes[7]).toBe(0x2c);
  });

  it('always uses 8-byte header regardless of payload size', () => {
    const small = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array(10) });
    expect(small).toHaveLength(8 + 10);

    const large = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array(65536) });
    expect(large).toHaveLength(8 + 65536);
  });

  it('encodes 2-byte channel ID correctly (big-endian)', () => {
    const bytes = encodeFrame({ channel: 0x1234, type: 1, flags: 0, payload: new Uint8Array(0) });
    expect(bytes[0]).toBe(0x12);
    expect(bytes[1]).toBe(0x34);
  });

  it('preserves MORE and PROBE flags', () => {
    const payload = new Uint8Array(8);
    const flags = FrameFlags.MORE | FrameFlags.PROBE;
    const bytes = encodeFrame({ channel: 3, type: 2, flags, payload });
    expect(bytes[3] & FrameFlags.MORE).toBeTruthy();
    expect(bytes[3] & FrameFlags.PROBE).toBeTruthy();
  });

  it('PING test vector: channel=0, type=0x10, 4-byte payload 0x000003E8', () => {
    // From spec: 00 00 10 00 00 00 00 04 00 00 03 E8
    const payload = new Uint8Array([0x00, 0x00, 0x03, 0xe8]);
    const bytes = encodeFrame({ channel: 0x00, type: 0x10, flags: 0, payload });
    const expected = new Uint8Array([
      0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x03, 0xe8,
    ]);
    expect(bytes).toEqual(expected);
  });
});
