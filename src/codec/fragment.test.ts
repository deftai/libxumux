import { describe, it, expect } from 'vitest';
import { fragment, Reassembler } from './fragment.js';
import { FrameFlags } from './constants.js';

describe('fragment', () => {
  it('returns a single frame when payload fits in maxPayloadSize', () => {
    const frame = { channel: 1, type: 1, flags: 0, payload: new Uint8Array(100) };
    const result = fragment(frame, 1000);
    expect(result).toHaveLength(1);
    expect(result[0]).toBe(frame); // same reference
  });

  it('fragments a payload into exactly 2 pieces', () => {
    const payload = new Uint8Array(200);
    const frames = fragment({ channel: 1, type: 2, flags: 0, payload }, 100);
    expect(frames).toHaveLength(2);
    expect(frames[0]!.flags & FrameFlags.FRAGMENT).toBeTruthy();
    expect(frames[0]!.flags & FrameFlags.FRAGMENT_END).toBeFalsy(); // not last
    expect(frames[1]!.flags & FrameFlags.FRAGMENT).toBeTruthy();
    expect(frames[1]!.flags & FrameFlags.FRAGMENT_END).toBeTruthy(); // last
  });

  it('all fragments share channel and type with the original', () => {
    const payload = new Uint8Array(300);
    const frames = fragment({ channel: 5, type: 0x42, flags: 0, payload }, 100);
    for (const f of frames) {
      expect(f.channel).toBe(5);
      expect(f.type).toBe(0x42);
    }
  });

  it('fragment payloads concatenate to the original payload', () => {
    const payload = new Uint8Array(250).map((_, i) => i % 256);
    const frames = fragment({ channel: 1, type: 1, flags: 0, payload }, 100);
    const combined = new Uint8Array(payload.length);
    let offset = 0;
    for (const f of frames) {
      combined.set(f.payload, offset);
      offset += f.payload.length;
    }
    expect(combined).toEqual(payload);
  });
});

describe('Reassembler', () => {
  it('passes through a non-fragment frame immediately', () => {
    const payload = new Uint8Array([1, 2, 3]);
    const reassembler = new Reassembler();
    const result = reassembler.push({ channel: 1, type: 1, flags: 0, payload });
    expect(result).toEqual(payload);
    expect(reassembler.isAssembling).toBe(false);
  });

  it('reassembles two fragments into the original payload', () => {
    const payload = new Uint8Array(200).map((_, i) => i % 256);
    const frames = fragment({ channel: 1, type: 1, flags: 0, payload }, 100);
    const reassembler = new Reassembler();
    expect(reassembler.push(frames[0]!)).toBeNull();
    expect(reassembler.isAssembling).toBe(true);
    const result = reassembler.push(frames[1]!);
    expect(result).toEqual(payload);
    expect(reassembler.isAssembling).toBe(false);
  });

  it('reassembles many fragments', () => {
    const payload = new Uint8Array(1000).map((_, i) => i % 256);
    const frames = fragment({ channel: 2, type: 3, flags: 0, payload }, 100);
    expect(frames.length).toBeGreaterThan(2);
    const reassembler = new Reassembler();
    let result: Uint8Array | null = null;
    for (const f of frames) {
      result = reassembler.push(f);
    }
    expect(result).toEqual(payload);
  });

  it('throws on non-fragment interleaved with in-progress sequence', () => {
    const payload = new Uint8Array(200);
    const frames = fragment({ channel: 1, type: 1, flags: 0, payload }, 100);
    const reassembler = new Reassembler();
    reassembler.push(frames[0]!); // start fragment sequence
    // Push a non-fragment frame before FRAGMENT_END
    expect(() =>
      reassembler.push({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(5) }),
    ).toThrow(/interleaved/);
  });

  it('throws on FRAGMENT_END without preceding FRAGMENT', () => {
    const reassembler = new Reassembler();
    expect(() =>
      reassembler.push({
        channel: 1,
        type: 1,
        flags: FrameFlags.FRAGMENT_END,
        payload: new Uint8Array(5),
      }),
    ).toThrow(/preceding FRAGMENT/);
  });

  it('throws on fragment channel/type mismatch', () => {
    const payload = new Uint8Array(200);
    const frames = fragment({ channel: 1, type: 1, flags: 0, payload }, 100);
    const reassembler = new Reassembler();
    reassembler.push(frames[0]!); // start with ch=1, type=1
    // Push a fragment with different channel
    expect(() =>
      reassembler.push({
        channel: 2, // wrong channel
        type: 1,
        flags: FrameFlags.FRAGMENT,
        payload: new Uint8Array(5),
      }),
    ).toThrow(/mismatch/);
  });

  it('abort() resets state and allows a new sequence', () => {
    const payload = new Uint8Array(200);
    const frames = fragment({ channel: 1, type: 1, flags: 0, payload }, 100);
    const reassembler = new Reassembler();
    reassembler.push(frames[0]!);
    expect(reassembler.isAssembling).toBe(true);
    reassembler.abort();
    expect(reassembler.isAssembling).toBe(false);
    // Can now push a normal frame without error
    const result = reassembler.push({ channel: 1, type: 1, flags: 0, payload: new Uint8Array([99]) });
    expect(result).toEqual(new Uint8Array([99]));
  });
});
