import { describe, it, expect } from 'vitest';
import fc from 'fast-check';
import { decodeFrame } from './codec/decode-frame.js';
import { encodeFrame } from './codec/encode-frame.js';
import {
  decodeHello,
  decodeWelcome,
  decodeOpenChannel,
  decodeChannelAck,
  decodeChannelReject,
  decodeCloseChannel,
  decodeClose,
  decodeError,
  decodeWindowUpdate,
  decodeProbeToken,
} from './control/codec.js';
import { XumuxProtocolError } from './errors.js';

const decoders: Record<string, (p: Uint8Array) => unknown> = {
  decodeHello,
  decodeWelcome,
  decodeOpenChannel,
  decodeChannelAck,
  decodeChannelReject,
  decodeCloseChannel,
  decodeClose,
  decodeError,
  decodeWindowUpdate,
  decodeProbeToken,
};

const enc = new TextEncoder();
const json = (v: unknown) => enc.encode(JSON.stringify(v));

/** Fails the property unless `decode(p)` succeeds or throws XumuxProtocolError. */
function onlyProtocolErrors(name: string, decode: (p: Uint8Array) => unknown, p: Uint8Array) {
  try {
    decode(p);
  } catch (e) {
    expect(e, `${name} threw ${String(e)}`).toBeInstanceOf(XumuxProtocolError);
  }
}

const limits = { maxFramePayload: 65536, maxMessageSize: 262144, initialWindow: 262144 };
const request = {
  id: 1,
  name: 'a',
  reliable: true,
  ordered: true,
  metadata: { profile: 'pipe/1' },
};
const accept = { id: 1, reliable: true, ordered: true };

/** A valid body for each JSON decoder, to mutate one field at a time. */
const valid: Record<string, Record<string, unknown>> = {
  decodeHello: { version: [0, 2, 0], ...limits, extensions: ['x'], channels: [request] },
  decodeWelcome: {
    version: [0, 2, 0],
    ...limits,
    channels: [accept],
    rejected: [{ id: 3, code: 4010, reason: 'r' }],
    pingInterval: 30,
    pingTimeout: 10,
  },
  decodeOpenChannel: request,
  decodeChannelAck: accept,
  decodeChannelReject: { id: 1, code: 4010, reason: 'r' },
  decodeCloseChannel: { id: 1, code: 1000, reason: 'r' },
  decodeClose: { code: 1000, reason: 'r' },
  decodeError: { code: 1002, channel: 1, reason: 'r' },
};

/** Every path to a field in `v`, including inside arrays (e.g. channels.0.reliable). */
function paths(v: unknown, prefix: (string | number)[] = []): (string | number)[][] {
  if (v === null || typeof v !== 'object') return [prefix];
  const out: (string | number)[][] = prefix.length ? [prefix] : [];
  for (const [k, x] of Object.entries(v))
    out.push(...paths(x, [...prefix, Array.isArray(v) ? Number(k) : k]));
  return out;
}

function withField(v: unknown, path: (string | number)[], x: unknown): unknown {
  const copy = structuredClone(v) as Record<string | number, unknown>;
  let o = copy;
  for (const k of path.slice(0, -1)) o = o[k] as Record<string | number, unknown>;
  const last = path[path.length - 1]!;
  if (x === undefined) delete o[last];
  else o[last] = x;
  return copy;
}

const wrongValue = fc.oneof(
  fc.constant(undefined), // field missing
  fc.constant(null),
  fc.boolean(),
  fc.integer(),
  fc.double(),
  fc.constantFrom(-1, 0x1_0000, 2 ** 32, 2 ** 53, Number.MAX_SAFE_INTEGER + 2),
  fc.string(),
  fc.array(fc.jsonValue(), { maxLength: 3 }),
  fc.dictionary(fc.string(), fc.jsonValue(), { maxKeys: 3 }),
);

describe('fuzz: decoders', () => {
  for (const [name, decode] of Object.entries(decoders)) {
    it(`${name} only ever throws XumuxProtocolError on arbitrary input`, () => {
      fc.assert(
        fc.property(
          fc.oneof(
            fc.uint8Array({ maxLength: 200 }),
            fc.jsonValue().map(json),
            fc.dictionary(fc.string(), fc.jsonValue(), { maxKeys: 8 }).map(json),
          ),
          (p) => onlyProtocolErrors(name, decode, p),
        ),
        { numRuns: 2000 },
      );
    });
  }

  for (const [name, body] of Object.entries(valid)) {
    const decode = decoders[name]!;
    it(`${name} only ever throws XumuxProtocolError when one field is wrong`, () => {
      expect(() => decode(json(body)), `${name} rejects its own valid body`).not.toThrow();
      fc.assert(
        fc.property(fc.constantFrom(...paths(body)), wrongValue, (path, x) =>
          onlyProtocolErrors(name, decode, json(withField(body, path, x))),
        ),
        { numRuns: 3000 },
      );
    });
  }

  const frameArb = fc.record({
    channel: fc.integer({ min: 0, max: 0xfffe }),
    type: fc.integer({ min: 0, max: 255 }),
    flags: fc.integer({ min: 0, max: 255 }),
    payload: fc.uint8Array({ maxLength: 64 }),
  });

  /** Valid encodings, encodings with a nearby wrong length field, and raw bytes. */
  const frameBytes = fc.oneof(
    frameArb.map((f) => encodeFrame(f)),
    fc.tuple(frameArb, fc.integer({ min: -3, max: 3 })).map(([f, d]) => {
      const b = encodeFrame(f);
      const v = new DataView(b.buffer, b.byteOffset, b.byteLength);
      v.setUint32(4, (v.getUint32(4) + d) >>> 0);
      return b;
    }),
    fc.uint8Array({ maxLength: 64 }),
  );

  it('decodeFrame accepts exactly the frames that re-encode to the same bytes', () => {
    let accepted = 0;
    fc.assert(
      fc.property(frameBytes, (b) => {
        let f;
        try {
          f = decodeFrame(b, 4096);
        } catch (e) {
          expect(e).toBeInstanceOf(XumuxProtocolError);
          return;
        }
        accepted++;
        expect(Array.from(encodeFrame(f))).toEqual(Array.from(b));
      }),
      { numRuns: 5000 },
    );
    // The generator must actually reach the accepting path.
    expect(accepted).toBeGreaterThan(1000);
  });
});
