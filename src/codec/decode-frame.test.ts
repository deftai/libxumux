import { describe, it, expect } from 'vitest';
import { decodeFrame, FrameDecoder } from './decode-frame.js';
import { encodeFrame } from './encode-frame.js';
import { FrameFlags } from './constants.js';

describe('decodeFrame', () => {
  it('decodes a frame with no payload', () => {
    // [ch:0x0000][type:0x01][flags:0x00][len:0x00000000]
    const bytes = new Uint8Array([0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]);
    const frame = decodeFrame(bytes);
    expect(frame.channel).toBe(0);
    expect(frame.type).toBe(1);
    expect(frame.flags).toBe(0);
    expect(frame.payload).toHaveLength(0);
  });

  it('decodes a frame with payload', () => {
    const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef]);
    const bytes = encodeFrame({ channel: 2, type: 0x20, flags: 0, payload });
    const frame = decodeFrame(bytes);
    expect(frame.channel).toBe(2);
    expect(frame.type).toBe(0x20);
    expect(frame.payload).toEqual(payload);
  });

  it('decodes a large payload (>65535 bytes)', () => {
    const payload = new Uint8Array(65536);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    const frame = decodeFrame(bytes);
    expect(frame.payload).toHaveLength(65536);
  });

  it('decodes 2-byte channel ID correctly', () => {
    const bytes = encodeFrame({ channel: 0x1234, type: 0x05, flags: 0, payload: new Uint8Array(0) });
    const frame = decodeFrame(bytes);
    expect(frame.channel).toBe(0x1234);
  });

  it('round-trips encode→decode', () => {
    const original = {
      channel: 5,
      type: 0x03,
      flags: FrameFlags.FRAGMENT,
      payload: new Uint8Array([1, 2, 3, 4, 5]),
    };
    const decoded = decodeFrame(encodeFrame(original));
    expect(decoded.channel).toBe(original.channel);
    expect(decoded.type).toBe(original.type);
    expect(decoded.flags).toBe(original.flags);
    expect(decoded.payload).toEqual(original.payload);
  });

  it('throws RangeError on data shorter than 8 bytes', () => {
    expect(() => decodeFrame(new Uint8Array([0, 1, 2, 3, 4, 5, 6]))).toThrow(RangeError);
  });

  it('PING test vector (from spec)', () => {
    // 00 00 10 00 00 00 00 04 00 00 03 E8
    const vec = new Uint8Array([0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x03, 0xe8]);
    const frame = decodeFrame(vec);
    expect(frame.channel).toBe(0x0000);
    expect(frame.type).toBe(0x10);
    expect(frame.payload).toEqual(new Uint8Array([0x00, 0x00, 0x03, 0xe8]));
  });

  it('HELLO test vector (from spec)', () => {
    // {"version":[0,1,0],"channels":[]} → 34 bytes JSON
    const json = '{"version":[0,1,0],"channels":[]}';
    const payload = new TextEncoder().encode(json);
    // [ch:0x0000][type:0x01][flags:0x00][len:0x00000022]
    const header = new Uint8Array([0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x22]);
    const vec = new Uint8Array(header.length + payload.length);
    vec.set(header);
    vec.set(payload, header.length);
    const frame = decodeFrame(vec);
    expect(frame.channel).toBe(0);
    expect(frame.type).toBe(0x01);
    expect(new TextDecoder().decode(frame.payload)).toBe(json);
  });
});

describe('FrameDecoder', () => {
  it('parses a single frame from one push', () => {
    const bytes = encodeFrame({ channel: 1, type: 2, flags: 0, payload: new Uint8Array([10, 20]) });
    const decoder = new FrameDecoder();
    const frames = decoder.push(bytes);
    expect(frames).toHaveLength(1);
    const frame = decodeFrame(frames[0]!);
    expect(frame.channel).toBe(1);
    expect(frame.payload).toEqual(new Uint8Array([10, 20]));
  });

  it('handles header split across two pushes', () => {
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array([7, 8]) });
    const decoder = new FrameDecoder();
    expect(decoder.push(bytes.slice(0, 4))).toHaveLength(0); // partial header
    expect(decoder.push(bytes.slice(4))).toHaveLength(1);    // rest
  });

  it('handles payload split across multiple pushes', () => {
    const payload = new Uint8Array(100).fill(0xaa);
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload });
    const decoder = new FrameDecoder();
    for (let i = 0; i < bytes.length - 1; i++) {
      expect(decoder.push(bytes.slice(i, i + 1))).toHaveLength(0);
    }
    const frames = decoder.push(bytes.slice(bytes.length - 1));
    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).payload).toEqual(payload);
  });

  it('extracts multiple frames from one push', () => {
    const f1 = encodeFrame({ channel: 1, type: 1, flags: 0, payload: new Uint8Array([1]) });
    const f2 = encodeFrame({ channel: 2, type: 2, flags: 0, payload: new Uint8Array([2]) });
    const combined = new Uint8Array(f1.length + f2.length);
    combined.set(f1);
    combined.set(f2, f1.length);
    const decoder = new FrameDecoder();
    const frames = decoder.push(combined);
    expect(frames).toHaveLength(2);
    expect(decodeFrame(frames[0]!).channel).toBe(1);
    expect(decodeFrame(frames[1]!).channel).toBe(2);
  });

  it('reports bufferedBytes', () => {
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array([1]) });
    const decoder = new FrameDecoder();
    decoder.push(bytes.slice(0, 3));
    expect(decoder.bufferedBytes).toBe(3);
  });

  it('parses a large frame from a stream', () => {
    const payload = new Uint8Array(65536).fill(0xab);
    const bytes = encodeFrame({ channel: 1, type: 2, flags: 0, payload });

    // Feed into FrameDecoder in chunks
    const decoder = new FrameDecoder();
    const chunkSize = 1024;
    let frames: Uint8Array[] = [];
    for (let i = 0; i < bytes.length; i += chunkSize) {
      frames = [...frames, ...decoder.push(bytes.slice(i, i + chunkSize))];
    }
    expect(frames).toHaveLength(1);
    const decoded = decodeFrame(frames[0]!);
    expect(decoded.payload).toHaveLength(65536);
    expect(decoder.bufferedBytes).toBe(0);
  });

  it('reset() clears the buffer', () => {
    const bytes = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array([1]) });
    const decoder = new FrameDecoder();
    decoder.push(bytes.slice(0, 3));
    decoder.reset();
    expect(decoder.bufferedBytes).toBe(0);
  });
});
