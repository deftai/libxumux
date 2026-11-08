import { describe, it, expect } from 'vitest';
import fc from 'fast-check';
import { decodeFrame } from './codec/decode-frame.js';
import { encodeFrame } from './codec/encode-frame.js';
import {
  decodeHello,
  decodeWelcome,
  decodeCloseChannel,
  decodeClose,
  decodeError,
  decodeWindowUpdate,
  decodeProbeToken,
} from './control/codec.js';
import { XumuxProtocolError } from './errors.js';

const decoders = {
  decodeHello,
  decodeWelcome,
  decodeCloseChannel,
  decodeClose,
  decodeError,
  decodeWindowUpdate,
  decodeProbeToken,
};

const enc = new TextEncoder();

/** Arbitrary bytes, arbitrary JSON values, and JSON shaped like real messages. */
const payloadArb = fc.oneof(
  fc.uint8Array({ maxLength: 200 }),
  fc.jsonValue().map((v) => enc.encode(JSON.stringify(v))),
  fc
    .record(
      {
        version: fc.oneof(fc.array(fc.integer(), { maxLength: 4 }), fc.jsonValue()),
        maxFramePayload: fc.oneof(fc.integer(), fc.double(), fc.string()),
        maxMessageSize: fc.oneof(fc.integer(), fc.double(), fc.constant(null)),
        initialWindow: fc.oneof(fc.integer(), fc.double()),
        channels: fc.oneof(fc.array(fc.jsonValue(), { maxLength: 3 }), fc.jsonValue()),
        id: fc.oneof(fc.integer(), fc.double(), fc.string()),
        code: fc.oneof(fc.integer(), fc.string()),
        reason: fc.oneof(fc.string(), fc.jsonValue()),
      },
      { requiredKeys: [] },
    )
    .map((v) => enc.encode(JSON.stringify(v))),
);

describe('fuzz: decoders', () => {
  for (const [name, decode] of Object.entries(decoders)) {
    it(`${name} only ever throws XumuxProtocolError`, () => {
      fc.assert(
        fc.property(payloadArb, (p) => {
          try {
            decode(p);
          } catch (e) {
            expect(e, `${name} threw ${String(e)}`).toBeInstanceOf(XumuxProtocolError);
          }
        }),
        { numRuns: 3000 },
      );
    });
  }

  it('decodeFrame accepts only frames that re-encode to the same bytes', () => {
    fc.assert(
      fc.property(fc.uint8Array({ maxLength: 64 }), (b) => {
        let f;
        try {
          f = decodeFrame(b, 4096);
        } catch (e) {
          expect(e).toBeInstanceOf(XumuxProtocolError);
          return;
        }
        expect(Array.from(encodeFrame(f))).toEqual(Array.from(b));
      }),
      { numRuns: 5000 },
    );
  });
});
