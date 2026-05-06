/**
 * xumux binary frame encoder.
 * @module codec/encode-frame
 */

import { HEADER_SIZE } from './constants.js';
import type { Frame } from './frame.js';

/**
 * Encode a Frame into its binary wire representation.
 *
 * Uses the fixed 8-byte header: [Channel:2][Type:1][Flags:1][Length:4].
 *
 * @param frame - The frame to encode.
 * @returns A Uint8Array containing the complete frame (header + payload).
 */
export function encodeFrame(frame: Frame): Uint8Array {
  const buf = new Uint8Array(HEADER_SIZE + frame.payload.length);

  // Channel: 2 bytes, big-endian
  buf[0] = (frame.channel >>> 8) & 0xff;
  buf[1] = frame.channel & 0xff;

  // Type: 1 byte
  buf[2] = frame.type & 0xff;

  // Flags: 1 byte
  buf[3] = frame.flags & 0xff;

  // Payload Length: 4 bytes, big-endian
  const len = frame.payload.length;
  buf[4] = (len >>> 24) & 0xff;
  buf[5] = (len >>> 16) & 0xff;
  buf[6] = (len >>> 8) & 0xff;
  buf[7] = len & 0xff;

  buf.set(frame.payload, HEADER_SIZE);
  return buf;
}
