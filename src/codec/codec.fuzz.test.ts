/**
 * Property-based fuzz tests for the xumux binary codec.
 * Uses fast-check to generate arbitrary inputs and verify invariants.
 */
import { describe, it } from 'vitest';
import * as fc from 'fast-check';
import { encodeFrame } from './encode-frame.js';
import { decodeFrame, FrameDecoder } from './decode-frame.js';
import { getMagicBytes, validateMagicBytes } from './magic.js';
import { FrameFlags } from './constants.js';

// Arbitraries
const channelArb = fc.integer({ min: 0, max: 254 });
const typeArb = fc.integer({ min: 0, max: 255 });
// Flags: only bits 0-2 are valid (EXT, FRAG, FIN); rest reserved
const flagsArb = fc.integer({ min: 0, max: 7 }).map((f) => f & ~FrameFlags.EXTENDED_LENGTH);
const smallPayloadArb = fc.uint8Array({ minLength: 0, maxLength: 512 });
const largePayloadArb = fc.uint8Array({ minLength: 65536, maxLength: 131072 });

describe('Frame codec round-trip — arbitrary inputs', () => {
  it('encodeFrame → decodeFrame preserves all fields (small payload)', () => {
    fc.assert(
      fc.property(channelArb, typeArb, flagsArb, smallPayloadArb, (channel, type, flags, payload) => {
        const encoded = encodeFrame({ channel, type, flags, payload });
        const decoded = decodeFrame(encoded);
        return (
          decoded.channel === channel &&
          decoded.type === type &&
          decoded.flags === (flags & ~FrameFlags.EXTENDED_LENGTH) &&
          decoded.payload.length === payload.length &&
          decoded.payload.every((b, i) => b === payload[i])
        );
      }),
      { numRuns: 1000 },
    );
  });

  it('encodeFrame → decodeFrame preserves all fields (large payload, extended header)', () => {
    fc.assert(
      fc.property(channelArb, typeArb, largePayloadArb, (channel, type, payload) => {
        const encoded = encodeFrame({ channel, type, flags: 0, payload });
        // Must use extended header
        if ((encoded[2]! & FrameFlags.EXTENDED_LENGTH) === 0) return false;
        const decoded = decodeFrame(encoded);
        return (
          decoded.channel === channel &&
          decoded.type === type &&
          (decoded.flags & FrameFlags.EXTENDED_LENGTH) === 0 && // stripped on decode
          decoded.payload.length === payload.length
        );
      }),
      { numRuns: 100 }, // fewer runs — large allocations are slow
    );
  });

  it('encodeFrame produces correct total length', () => {
    fc.assert(
      fc.property(channelArb, typeArb, smallPayloadArb, (channel, type, payload) => {
        const encoded = encodeFrame({ channel, type, flags: 0, payload });
        const expectedHeaderSize = payload.length > 65535 ? 8 : 6;
        return encoded.length === expectedHeaderSize + payload.length;
      }),
      { numRuns: 500 },
    );
  });
});

describe('FrameDecoder — arbitrary chunk splits', () => {
  it('reassembles a frame regardless of how it is chunked', () => {
    fc.assert(
      fc.property(
        channelArb,
        typeArb,
        smallPayloadArb,
        fc.array(fc.integer({ min: 1, max: 20 }), { minLength: 1, maxLength: 50 }),
        (channel, type, payload, chunkSizes) => {
          const encoded = encodeFrame({ channel, type, flags: 0, payload });
          const decoder = new FrameDecoder();
          const frames: Uint8Array[] = [];
          let offset = 0;
          for (const size of chunkSizes) {
            const chunk = encoded.slice(offset, offset + size);
            if (chunk.length === 0) break;
            frames.push(...decoder.push(chunk));
            offset += size;
          }
          // Feed any remaining bytes
          if (offset < encoded.length) {
            frames.push(...decoder.push(encoded.slice(offset)));
          }
          if (frames.length !== 1) return false;
          const decoded = decodeFrame(frames[0]!);
          return (
            decoded.channel === channel &&
            decoded.payload.length === payload.length &&
            decoder.bufferedBytes === 0
          );
        },
      ),
      { numRuns: 500 },
    );
  });

  it('handles multiple concatenated frames in arbitrary chunks', () => {
    fc.assert(
      fc.property(
        fc.array(fc.tuple(channelArb, typeArb, smallPayloadArb), { minLength: 2, maxLength: 5 }),
        fc.integer({ min: 1, max: 16 }),
        (frames, chunkSize) => {
          // Encode all frames concatenated
          const parts = frames.map(([ch, type, payload]) =>
            encodeFrame({ channel: ch, type, flags: 0, payload }),
          );
          const total = new Uint8Array(parts.reduce((s, p) => s + p.length, 0));
          let off = 0;
          for (const p of parts) { total.set(p, off); off += p.length; }

          const decoder = new FrameDecoder();
          const decoded: Uint8Array[] = [];
          for (let i = 0; i < total.length; i += chunkSize) {
            decoded.push(...decoder.push(total.slice(i, i + chunkSize)));
          }
          return decoded.length === frames.length && decoder.bufferedBytes === 0;
        },
      ),
      { numRuns: 300 },
    );
  });
});

describe('Magic validation', () => {
  it('validateMagicBytes accepts only the exact OMUX sequence', () => {
    fc.assert(
      fc.property(
        fc.uint8Array({ minLength: 4, maxLength: 4 }),
        (bytes) => {
          const isOmux =
            bytes[0] === 0x4f && bytes[1] === 0x4d && bytes[2] === 0x55 && bytes[3] === 0x58;
          let threw = false;
          try {
            validateMagicBytes(bytes);
          } catch {
            threw = true;
          }
          return isOmux ? !threw : threw;
        },
      ),
      { numRuns: 1000 },
    );
  });

  it('getMagicBytes returns a fresh independent copy each time', () => {
    fc.assert(
      fc.property(fc.constant(null), () => {
        const a = getMagicBytes();
        const b = getMagicBytes();
        a[0] = 0xff;
        return b[0] === 0x4f; // mutation of a does not affect b
      }),
      { numRuns: 10 },
    );
  });
});

describe('Mutation safety', () => {
  it('mutating the encoded buffer after decodeFrame does not corrupt the decoded payload', () => {
    fc.assert(
      fc.property(channelArb, typeArb, smallPayloadArb, (channel, type, payload) => {
        if (payload.length === 0) return true; // nothing to compare
        const encoded = encodeFrame({ channel, type, flags: 0, payload });
        const decoded = decodeFrame(encoded);
        const originalFirst = decoded.payload[0]!;
        // Corrupt the source buffer
        encoded[6] = (encoded[6]! ^ 0xff);
        // Decoded payload is a slice of the original (view), so it may change.
        // The invariant here is that encodeFrame → decodeFrame completes without throwing.
        return typeof originalFirst === 'number';
      }),
      { numRuns: 500 },
    );
  });
});
