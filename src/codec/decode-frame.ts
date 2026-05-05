/**
 * xumux binary frame decoder — single-frame and streaming variants.
 * @module codec/decode-frame
 */

import {
  FrameFlags,
  STANDARD_HEADER_SIZE,
  EXTENDED_HEADER_SIZE,
} from './constants.js';
import type { Frame } from './frame.js';

/**
 * Parse a single complete frame from its binary wire representation.
 *
 * The EXTENDED_LENGTH flag is stripped from the returned frame's flags field —
 * it is a wire-level encoding detail, not an application-visible flag.
 *
 * @param data - Bytes containing exactly one frame.
 * @throws {RangeError} If the data is too short for a valid frame header.
 */
export function decodeFrame(data: Uint8Array): Frame {
  if (data.length < STANDARD_HEADER_SIZE) {
    throw new RangeError(
      `Frame too short: need at least ${STANDARD_HEADER_SIZE} bytes, got ${data.length}`,
    );
  }

  // Safe accesses: bounds checked above
  const channel = data[0] as number;
  const type = data[1] as number;
  const wireFlags = data[2] as number;
  const isExtended = (wireFlags & FrameFlags.EXTENDED_LENGTH) !== 0;
  const headerSize = isExtended ? EXTENDED_HEADER_SIZE : STANDARD_HEADER_SIZE;

  if (data.length < headerSize) {
    throw new RangeError(
      `Extended frame header incomplete: need ${headerSize} bytes, got ${data.length}`,
    );
  }

  let payloadLength: number;
  if (isExtended) {
    // 4-byte big-endian unsigned integer; >>> 0 ensures unsigned interpretation
    payloadLength =
      (((data[4] as number) << 24) |
        ((data[5] as number) << 16) |
        ((data[6] as number) << 8) |
        (data[7] as number)) >>>
      0;
  } else {
    payloadLength = ((data[4] as number) << 8) | (data[5] as number);
  }

  return {
    channel,
    type,
    flags: wireFlags & ~FrameFlags.EXTENDED_LENGTH, // strip wire-only flag
    payload: data.slice(headerSize, headerSize + payloadLength),
  };
}

/**
 * Streaming frame decoder for stream-oriented transports (TCP, stdio).
 *
 * Feed incoming byte chunks via `push(chunk)`; each call returns zero or more
 * complete frame byte arrays, ready to be passed to `decodeFrame()`.
 *
 * The decoder maintains an internal buffer for incomplete frames that span
 * multiple incoming chunks.
 */
export class FrameDecoder {
  private buffer = new Uint8Array(0);

  /**
   * Feed incoming bytes to the decoder.
   * @param data - A chunk of raw bytes from the transport.
   * @returns Zero or more complete frame byte arrays.
   */
  push(data: Uint8Array): Uint8Array[] {
    // Append data to the internal buffer
    const combined = new Uint8Array(this.buffer.length + data.length);
    combined.set(this.buffer);
    combined.set(data, this.buffer.length);
    this.buffer = combined;

    const frames: Uint8Array[] = [];

    for (;;) {
      if (this.buffer.length < STANDARD_HEADER_SIZE) break;

      const wireFlags = this.buffer[2] as number;
      const isExtended = (wireFlags & FrameFlags.EXTENDED_LENGTH) !== 0;
      const headerSize = isExtended ? EXTENDED_HEADER_SIZE : STANDARD_HEADER_SIZE;

      if (this.buffer.length < headerSize) break;

      let payloadLength: number;
      if (isExtended) {
        payloadLength =
          (((this.buffer[4] as number) << 24) |
            ((this.buffer[5] as number) << 16) |
            ((this.buffer[6] as number) << 8) |
            (this.buffer[7] as number)) >>>
          0;
      } else {
        payloadLength = ((this.buffer[4] as number) << 8) | (this.buffer[5] as number);
      }

      const totalSize = headerSize + payloadLength;
      if (this.buffer.length < totalSize) break;

      frames.push(this.buffer.slice(0, totalSize));
      this.buffer = this.buffer.slice(totalSize);
    }

    return frames;
  }

  /** Number of bytes currently held in the internal buffer. */
  get bufferedBytes(): number {
    return this.buffer.length;
  }

  /** Reset the internal buffer, discarding any partial frame data. */
  reset(): void {
    this.buffer = new Uint8Array(0);
  }
}
