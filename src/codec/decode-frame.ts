/**
 * Frame decoding, validation (core §2.3) and stream framing.
 * @module codec/decode-frame
 */

import {
  CloseCode,
  CONTROL_CHANNEL,
  CONTROL_MAX_PAYLOAD,
  FrameFlags,
  HEADER_SIZE,
  KNOWN_FLAGS,
  RESERVED_CHANNEL,
  XMUX_MAGIC,
} from './constants.js';
import type { Frame } from './frame.js';
import { XumuxProtocolError } from '../errors.js';

function readU32(b: Uint8Array, o: number): number {
  return (
    (((b[o] as number) << 24) |
      ((b[o + 1] as number) << 16) |
      ((b[o + 2] as number) << 8) |
      (b[o + 3] as number)) >>>
    0
  );
}

/** Payload limit for a frame on `channel`, given the receiver's own maxFramePayload. */
export function payloadLimit(channel: number, maxFramePayload: number): number {
  return channel === CONTROL_CHANNEL ? CONTROL_MAX_PAYLOAD : maxFramePayload;
}

/**
 * Decode exactly one frame. `data` must be exactly header + payload (message bindings).
 * @throws {XumuxProtocolError} 4001 if the length does not match, 4005 if over `maxFramePayload`.
 */
export function decodeFrame(data: Uint8Array, maxFramePayload = Infinity): Frame {
  if (data.length < HEADER_SIZE) {
    throw new XumuxProtocolError(
      CloseCode.INVALID_MESSAGE,
      `frame too short: ${data.length} bytes`,
    );
  }
  const channel = ((data[0] as number) << 8) | (data[1] as number);
  const length = readU32(data, 4);
  if (length > payloadLimit(channel, maxFramePayload)) {
    throw new XumuxProtocolError(
      CloseCode.MESSAGE_TOO_LARGE,
      `frame payload ${length} exceeds limit`,
    );
  }
  if (data.length !== HEADER_SIZE + length) {
    throw new XumuxProtocolError(
      CloseCode.INVALID_MESSAGE,
      `frame length mismatch: header says ${length}, message carries ${data.length - HEADER_SIZE}`,
    );
  }
  return {
    channel,
    type: data[2] as number,
    flags: data[3] as number,
    payload: data.subarray(HEADER_SIZE),
  };
}

/**
 * Structural checks from core §2.3 step 3.
 * @param allowedFlags - extra flag bits enabled by negotiated extensions.
 */
export function validateFrame(frame: Frame, allowedFlags = 0): void {
  const bad = (msg: string): never => {
    throw new XumuxProtocolError(CloseCode.PROTOCOL_ERROR, msg);
  };
  if (frame.channel === RESERVED_CHANNEL) bad('frame on reserved channel 0xFFFF');
  if (frame.flags & ~(KNOWN_FLAGS | allowedFlags) & 0xff)
    bad(`reserved flag bits set: 0x${frame.flags.toString(16)}`);
  const more = (frame.flags & FrameFlags.MORE) !== 0;
  if (more && frame.channel === CONTROL_CHANNEL) bad('MORE set on control channel');
  if (more && (frame.flags & FrameFlags.PROBE) !== 0) bad('MORE set on probe frame');
}

/**
 * Parses frames out of a byte stream (TCP, Unix, stdio, QUIC streams).
 *
 * Checks the 4 magic bytes first, rejects oversized frames from the header alone,
 * and keeps a list of chunks rather than re-concatenating on every push.
 */
export class StreamFramer {
  private chunks: Uint8Array[] = [];
  private buffered = 0;
  private magicOk: boolean;

  /** @param maxFramePayload - the receiver's own limit for non-control channels. */
  constructor(
    public maxFramePayload: number,
    expectMagic = true,
  ) {
    this.magicOk = !expectMagic;
  }

  /** Bytes currently held. */
  get bufferedBytes(): number {
    return this.buffered;
  }

  /**
   * Feed bytes; returns complete frames.
   * @throws {XumuxProtocolError} scope 'transport' on bad magic; 4005 on oversized frames.
   */
  push(data: Uint8Array): Frame[] {
    if (data.length > 0) {
      this.chunks.push(data);
      this.buffered += data.length;
    }
    if (!this.magicOk) {
      if (this.buffered < 4) return [];
      const m = this.take(4);
      for (let i = 0; i < 4; i++) {
        if (m[i] !== XMUX_MAGIC[i]) {
          throw new XumuxProtocolError(
            CloseCode.PROTOCOL_ERROR,
            `bad magic ${Array.from(m, (b) => b.toString(16).padStart(2, '0')).join(' ')}`,
            'transport',
          );
        }
      }
      this.magicOk = true;
    }
    const frames: Frame[] = [];
    while (this.buffered >= HEADER_SIZE) {
      const header = this.peek(HEADER_SIZE);
      const channel = ((header[0] as number) << 8) | (header[1] as number);
      const length = readU32(header, 4);
      if (length > payloadLimit(channel, this.maxFramePayload)) {
        throw new XumuxProtocolError(
          CloseCode.MESSAGE_TOO_LARGE,
          `frame payload ${length} exceeds limit`,
        );
      }
      if (this.buffered < HEADER_SIZE + length) break;
      this.take(HEADER_SIZE);
      frames.push({
        channel,
        type: header[2] as number,
        flags: header[3] as number,
        payload: this.take(length),
      });
    }
    return frames;
  }

  /** Copy the first n buffered bytes without consuming them. */
  private peek(n: number): Uint8Array {
    const first = this.chunks[0] as Uint8Array;
    if (first.length >= n) return first.subarray(0, n);
    const out = new Uint8Array(n);
    let off = 0;
    for (const c of this.chunks) {
      const k = Math.min(c.length, n - off);
      out.set(c.subarray(0, k), off);
      off += k;
      if (off === n) break;
    }
    return out;
  }

  /** Consume and return the first n buffered bytes. */
  private take(n: number): Uint8Array {
    this.buffered -= n;
    if (n === 0) return new Uint8Array(0);
    const first = this.chunks[0] as Uint8Array;
    if (first.length === n) {
      this.chunks.shift();
      return first;
    }
    if (first.length > n) {
      this.chunks[0] = first.subarray(n);
      return first.subarray(0, n);
    }
    const out = new Uint8Array(n);
    let off = 0;
    while (off < n) {
      const c = this.chunks[0] as Uint8Array;
      const k = Math.min(c.length, n - off);
      out.set(c.subarray(0, k), off);
      off += k;
      if (k === c.length) this.chunks.shift();
      else this.chunks[0] = c.subarray(k);
    }
    return out;
  }
}
