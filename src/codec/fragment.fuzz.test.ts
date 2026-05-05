/**
 * Property-based fuzz tests for xumux fragmentation and reassembly.
 */
import { describe, it } from 'vitest';
import * as fc from 'fast-check';
import { fragment, Reassembler } from './fragment.js';
import { FrameFlags } from './constants.js';

const channelArb = fc.integer({ min: 1, max: 254 });
const typeArb = fc.integer({ min: 1, max: 255 });
const payloadArb = fc.uint8Array({ minLength: 1, maxLength: 200_000 });
const maxSizeArb = fc.integer({ min: 64, max: 65535 });

describe('Fragment round-trip', () => {
  it('fragment + reassemble reconstructs the original payload exactly', () => {
    fc.assert(
      fc.property(channelArb, typeArb, payloadArb, maxSizeArb, (channel, type, payload, maxSize) => {
        const frames = fragment({ channel, type, flags: 0, payload }, maxSize);
        const reassembler = new Reassembler();
        let result: Uint8Array | null = null;
        for (const frame of frames) {
          result = reassembler.push(frame);
        }
        if (!result) return false;
        if (result.length !== payload.length) return false;
        return result.every((b, i) => b === payload[i]);
      }),
      { numRuns: 500 },
    );
  });

  it('fragment count equals ceil(payload / maxSize) when payload > maxSize', () => {
    fc.assert(
      fc.property(
        channelArb, typeArb,
        fc.uint8Array({ minLength: 65, maxLength: 10000 }),
        fc.integer({ min: 1, max: 64 }),
        (channel, type, payload, maxSize) => {
          const frames = fragment({ channel, type, flags: 0, payload }, maxSize);
          const expected = Math.ceil(payload.length / maxSize);
          return frames.length === expected;
        },
      ),
      { numRuns: 500 },
    );
  });

  it('all fragments share the same channel and type as the original', () => {
    fc.assert(
      fc.property(channelArb, typeArb, payloadArb, maxSizeArb, (channel, type, payload, maxSize) => {
        const frames = fragment({ channel, type, flags: 0, payload }, maxSize);
        return frames.every((f) => f.channel === channel && f.type === type);
      }),
      { numRuns: 500 },
    );
  });

  it('only the last fragment has FRAGMENT_END set', () => {
    fc.assert(
      fc.property(
        channelArb, typeArb,
        fc.uint8Array({ minLength: 2, maxLength: 200_000 }),
        fc.integer({ min: 1, max: 64 }),
        (channel, type, payload, maxSize) => {
          const frames = fragment({ channel, type, flags: 0, payload }, maxSize);
          if (frames.length <= 1) return true; // no fragmentation
          const lastHasFin = (frames[frames.length - 1]!.flags & FrameFlags.FRAGMENT_END) !== 0;
          const noneBeforeHasFin = frames.slice(0, -1).every(
            (f) => (f.flags & FrameFlags.FRAGMENT_END) === 0,
          );
          return lastHasFin && noneBeforeHasFin;
        },
      ),
      { numRuns: 300 },
    );
  });

  it('passes through single frame unchanged when payload fits in maxSize', () => {
    fc.assert(
      fc.property(
        channelArb, typeArb,
        fc.uint8Array({ minLength: 0, maxLength: 100 }),
        fc.integer({ min: 100, max: 65535 }),
        (channel, type, payload, maxSize) => {
          const frame = { channel, type, flags: 0, payload };
          const frames = fragment(frame, maxSize);
          return frames.length === 1 && frames[0] === frame; // same reference
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe('Reassembler error invariants', () => {
  it('pushing FRAGMENT_END without preceding FRAGMENT always throws', () => {
    fc.assert(
      fc.property(channelArb, typeArb, fc.uint8Array({ minLength: 1, maxLength: 100 }), (ch, type, payload) => {
        const r = new Reassembler();
        let threw = false;
        try {
          r.push({ channel: ch, type, flags: FrameFlags.FRAGMENT_END, payload });
        } catch {
          threw = true;
        }
        return threw;
      }),
      { numRuns: 300 },
    );
  });

  it('pushing a non-fragment frame while assembling always throws', () => {
    fc.assert(
      fc.property(
        channelArb, typeArb,
        fc.uint8Array({ minLength: 1, maxLength: 50 }),
        fc.uint8Array({ minLength: 1, maxLength: 50 }),
        (ch, type, frag1, plain) => {
          const r = new Reassembler();
          r.push({ channel: ch, type, flags: FrameFlags.FRAGMENT, payload: frag1 });
          let threw = false;
          try {
            r.push({ channel: ch, type, flags: 0, payload: plain });
          } catch {
            threw = true;
          }
          return threw;
        },
      ),
      { numRuns: 300 },
    );
  });
});
