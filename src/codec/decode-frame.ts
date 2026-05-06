/**
 * xumux binary frame decoder — single-frame and streaming variants.
 * @module codec/decode-frame
 */

import { HEADER_SIZE } from './constants.js';
import type { Frame } from './frame.js';

/**
 * Parse a single complete frame from its binary wire representation.
 *
 * @param data - Bytes containing exactly one frame.
 * @throws {RangeError} If the data is too short for a valid frame header.
 */
export function decodeFrame(data: Uint8Array): Frame {
  if (data.length < HEADER_SIZE) {
    throw new RangeError(
      `Frame too short: need at least ${HEADER_SIZE} bytes, got ${data.length}`,
    );
  }

  // Channel: 2 bytes, big-endian
  const channel = ((data[0] as number) << 8) | (data[1] as number);
  const type = data[2] as number;
  const flags = data[3] as number;

  // Payload Length: 4 bytes, big-endian unsigned
  const payloadLength =
    (((data[4] as number) << 24) |
      ((data[5] as number) << 16) |
      ((data[6] as number) << 8) |
      (data[7] as number)) >>>
    0;

  return {
    channel,
    type,
    flags,
    payload: data.slice(HEADER_SIZE, HEADER_SIZE + payloadLength),
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
      if (this.buffer.length < HEADER_SIZE) break;

      // Payload Length: 4 bytes, big-endian unsigned (offset 4–7)
      const payloadLength =
        (((this.buffer[4] as number) << 24) |
          ((this.buffer[5] as number) << 16) |
          ((this.buffer[6] as number) << 8) |
          (this.buffer[7] as number)) >>>
        0;

      const totalSize = HEADER_SIZE + payloadLength;
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
