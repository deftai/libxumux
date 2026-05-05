/**
 * Property-based fuzz tests for the xumux control channel codec.
 * Verifies PING/PONG binary round-trips and JSON message codec round-trips.
 */
import { describe, it } from 'vitest';
import * as fc from 'fast-check';
import {
  encodePing, decodePing,
  encodePong, decodePong,
  encodeHello, decodeHello,
  encodeWelcome, decodeWelcome,
  encodeOpenChannel, decodeOpenChannel,
  encodeClose, decodeClose,
  encodeError, decodeError,
} from './codec.js';
import { decodeFrame } from '../codec/decode-frame.js';

const uint32Arb = fc.integer({ min: 0, max: 0xffffffff });
const stringArb = fc.string({ minLength: 0, maxLength: 100 });
const versionArb: fc.Arbitrary<[number, number, number]> = fc.tuple(
  fc.integer({ min: 0, max: 9 }),
  fc.integer({ min: 0, max: 99 }),
  fc.integer({ min: 0, max: 999 }),
);

describe('PING binary codec', () => {
  it('encodePing → decodePing round-trip for any uint32 timestamp', () => {
    fc.assert(
      fc.property(uint32Arb, (ts) => {
        const frame = decodeFrame(encodePing(ts));
        const { timestamp } = decodePing(frame.payload);
        return timestamp === (ts >>> 0);
      }),
      { numRuns: 1000 },
    );
  });

  it('PING payload is always exactly 4 bytes', () => {
    fc.assert(
      fc.property(uint32Arb, (ts) => {
        const frame = decodeFrame(encodePing(ts));
        return frame.payload.length === 4;
      }),
      { numRuns: 500 },
    );
  });
});

describe('PONG binary codec', () => {
  it('encodePong → decodePong round-trip for any (echo, receiver) pair', () => {
    fc.assert(
      fc.property(uint32Arb, uint32Arb, (echo, recv) => {
        const frame = decodeFrame(encodePong(echo, recv));
        const { echoTimestamp, receiverTimestamp } = decodePong(frame.payload);
        return echoTimestamp === (echo >>> 0) && receiverTimestamp === (recv >>> 0);
      }),
      { numRuns: 1000 },
    );
  });

  it('PONG payload is always exactly 8 bytes', () => {
    fc.assert(
      fc.property(uint32Arb, uint32Arb, (echo, recv) => {
        const frame = decodeFrame(encodePong(echo, recv));
        return frame.payload.length === 8;
      }),
      { numRuns: 500 },
    );
  });
});

describe('HELLO JSON codec', () => {
  it('encodeHello → decodeHello preserves version and channels', () => {
    fc.assert(
      fc.property(
        versionArb,
        fc.array(
          fc.record({
            name: fc.string({ minLength: 1, maxLength: 20 }),
            reliable: fc.boolean(),
            ordered: fc.boolean(),
          }),
          { minLength: 0, maxLength: 5 },
        ),
        (version, channels) => {
          const msg = { version, channels };
          const decoded = decodeHello(decodeFrame(encodeHello(msg)));
          return (
            decoded.version[0] === version[0] &&
            decoded.version[1] === version[1] &&
            decoded.version[2] === version[2] &&
            decoded.channels?.length === channels.length
          );
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe('WELCOME JSON codec', () => {
  it('encodeWelcome → decodeWelcome preserves version and assigned channels', () => {
    fc.assert(
      fc.property(
        versionArb,
        fc.array(
          fc.record({
            name: fc.string({ minLength: 1, maxLength: 20 }),
            id: fc.integer({ min: 1, max: 254 }),
          }),
          { minLength: 0, maxLength: 5 },
        ),
        (version, channels) => {
          const msg = { version, channels };
          const decoded = decodeWelcome(decodeFrame(encodeWelcome(msg)));
          return (
            decoded.channels.length === channels.length &&
            decoded.channels.every((ch, i) => ch.id === channels[i]!.id && ch.name === channels[i]!.name)
          );
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe('OPEN_CHANNEL JSON codec', () => {
  it('encodeOpenChannel → decodeOpenChannel preserves requestId, name, reliable, ordered', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1, max: 10000 }),
        fc.string({ minLength: 1, maxLength: 50 }),
        fc.boolean(),
        fc.boolean(),
        (requestId, name, reliable, ordered) => {
          const msg = { requestId, name, reliable, ordered };
          const decoded = decodeOpenChannel(decodeFrame(encodeOpenChannel(msg)));
          return (
            decoded.requestId === requestId &&
            decoded.name === name &&
            decoded.reliable === reliable &&
            decoded.ordered === ordered
          );
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe('CLOSE JSON codec', () => {
  it('encodeClose → decodeClose preserves code and optional reason', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1000, max: 4999 }),
        fc.option(stringArb, { nil: undefined }),
        (code, reason) => {
          const msg = reason !== undefined ? { code, reason } : { code };
          const decoded = decodeClose(decodeFrame(encodeClose(msg)));
          return decoded.code === code && decoded.reason === reason;
        },
      ),
      { numRuns: 500 },
    );
  });
});

describe('ERROR JSON codec', () => {
  it('encodeError → decodeError preserves code, optional channel, optional reason', () => {
    fc.assert(
      fc.property(
        fc.integer({ min: 1000, max: 4999 }),
        fc.option(fc.integer({ min: 1, max: 254 }), { nil: undefined }),
        fc.option(stringArb, { nil: undefined }),
        (code, channel, reason) => {
          const msg = { code, ...(channel !== undefined ? { channel } : {}), ...(reason !== undefined ? { reason } : {}) };
          const decoded = decodeError(decodeFrame(encodeError(msg)));
          return (
            decoded.code === code &&
            decoded.channel === channel &&
            decoded.reason === reason
          );
        },
      ),
      { numRuns: 500 },
    );
  });
});
