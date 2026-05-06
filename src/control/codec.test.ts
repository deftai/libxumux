import { describe, it, expect } from 'vitest';
import {
  encodePing, decodePing,
  encodePong, decodePong,
  encodeHello, decodeHello,
  encodeWelcome, decodeWelcome,
  encodeOpenChannel, decodeOpenChannel,
  encodeChannelAck, decodeChannelAck,
  encodeClose, decodeClose,
  encodeError, decodeError,
} from './codec.js';
import { decodeFrame } from '../codec/decode-frame.js';
import { ControlType, CONTROL_CHANNEL } from '../codec/constants.js';

describe('PING encoding', () => {
  it('produces a 12-byte frame (8 header + 4 payload)', () => {
    const bytes = encodePing(1000);
    expect(bytes).toHaveLength(12);
  });

  it('matches spec test vector for ts=1000', () => {
    // 00 00 10 00 00 00 00 04 00 00 03 E8
    const expected = new Uint8Array([0x00, 0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x03, 0xe8]);
    expect(encodePing(1000)).toEqual(expected);
  });

  it('round-trips through decodePing', () => {
    const frame = decodeFrame(encodePing(42000));
    expect(frame.channel).toBe(CONTROL_CHANNEL);
    expect(frame.type).toBe(ControlType.PING);
    const { timestamp } = decodePing(frame.payload);
    expect(timestamp).toBe(42000);
  });
});

describe('PONG encoding', () => {
  it('matches spec test vector for echo=1000, recv=500', () => {
    // 00 00 11 00 00 00 00 08 00 00 03 E8 00 00 01 F4
    const expected = new Uint8Array([
      0x00, 0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x00, 0x01, 0xf4,
    ]);
    expect(encodePong(1000, 500)).toEqual(expected);
  });

  it('round-trips through decodePong', () => {
    const frame = decodeFrame(encodePong(9999, 1234));
    const { echoTimestamp, receiverTimestamp } = decodePong(frame.payload);
    expect(echoTimestamp).toBe(9999);
    expect(receiverTimestamp).toBe(1234);
  });
});

describe('HELLO / WELCOME round-trips', () => {
  it('round-trips a minimal HELLO', () => {
    const msg = { version: [0, 1, 0] as [number, number, number], channels: [] };
    const frame = decodeFrame(encodeHello(msg));
    expect(frame.type).toBe(ControlType.HELLO);
    const decoded = decodeHello(frame);
    expect(decoded.version).toEqual([0, 1, 0]);
    expect(decoded.channels).toEqual([]);
  });

  it('round-trips a HELLO with channels', () => {
    const msg = {
      version: [0, 1, 0] as [number, number, number],
      channels: [{ name: 'pointer', reliable: false, ordered: false }],
      maxMessageSize: 32768,
    };
    const decoded = decodeHello(decodeFrame(encodeHello(msg)));
    expect(decoded.channels?.[0]?.name).toBe('pointer');
    expect(decoded.maxMessageSize).toBe(32768);
  });

  it('round-trips a WELCOME', () => {
    const msg = {
      version: [0, 1, 0] as [number, number, number],
      channels: [{ name: 'pointer', id: 1 }],
    };
    const decoded = decodeWelcome(decodeFrame(encodeWelcome(msg)));
    expect(decoded.channels[0]?.id).toBe(1);
    expect(decoded.channels[0]?.name).toBe('pointer');
  });
});

describe('OPEN_CHANNEL / CHANNEL_ACK round-trips', () => {
  it('round-trips OPEN_CHANNEL', () => {
    const msg = { requestId: 7, name: 'data', reliable: true, ordered: true };
    const decoded = decodeOpenChannel(decodeFrame(encodeOpenChannel(msg)));
    expect(decoded.requestId).toBe(7);
    expect(decoded.name).toBe('data');
  });

  it('round-trips CHANNEL_ACK', () => {
    const msg = { requestId: 7, id: 3, name: 'data' };
    const decoded = decodeChannelAck(decodeFrame(encodeChannelAck(msg)));
    expect(decoded.id).toBe(3);
    expect(decoded.requestId).toBe(7);
  });
});

describe('decodePing / decodePong error paths', () => {
  it('decodePing throws RangeError on payload shorter than 4 bytes', () => {
    expect(() => decodePing(new Uint8Array([0, 0, 3]))).toThrow(RangeError);
  });

  it('decodePong throws RangeError on payload shorter than 8 bytes', () => {
    expect(() => decodePong(new Uint8Array([0, 0, 3, 0xe8, 0, 0]))).toThrow(RangeError);
  });
});

describe('CLOSE / ERROR round-trips', () => {
  it('round-trips CLOSE', () => {
    const msg = { code: 1000, reason: 'done' };
    const decoded = decodeClose(decodeFrame(encodeClose(msg)));
    expect(decoded.code).toBe(1000);
    expect(decoded.reason).toBe('done');
  });

  it('round-trips ERROR', () => {
    const msg = { code: 4001, channel: 3, reason: 'bad message' };
    const decoded = decodeError(decodeFrame(encodeError(msg)));
    expect(decoded.code).toBe(4001);
    expect(decoded.channel).toBe(3);
  });
});
