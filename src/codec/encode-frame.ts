/**
 * xumux binary frame encoder.
 * @module codec/encode-frame
 */

import {
  FrameFlags,
  STANDARD_HEADER_SIZE,
  EXTENDED_HEADER_SIZE,
  HEADER_RESERVED_BYTE,
} from './constants.js';
import type { Frame } from './frame.js';

/**
 * Encode a Frame into its binary wire representation.
 *
 * Automatically uses an 8-byte extended header when the payload exceeds 65535 bytes
 * or when the caller explicitly sets the EXTENDED_LENGTH flag.
 *
 * @param frame - The frame to encode.
 * @returns A Uint8Array containing the complete frame (header + payload).
 */
export function encodeFrame(frame: Frame): Uint8Array {
  const useExtended =
    frame.payload.length > 65535 || (frame.flags & FrameFlags.EXTENDED_LENGTH) !== 0;
  const headerSize = useExtended ? EXTENDED_HEADER_SIZE : STANDARD_HEADER_SIZE;
  const buf = new Uint8Array(headerSize + frame.payload.length);

  buf[0] = frame.channel & 0xff;
  buf[1] = frame.type & 0xff;

  // Reflect the actual encoding in the wire flags
  const wireFlags = useExtended
    ? frame.flags | FrameFlags.EXTENDED_LENGTH
    : frame.flags & ~FrameFlags.EXTENDED_LENGTH;
  buf[2] = wireFlags & 0xff;
  buf[3] = HEADER_RESERVED_BYTE;

  if (useExtended) {
    const len = frame.payload.length;
    buf[4] = (len >>> 24) & 0xff;
    buf[5] = (len >>> 16) & 0xff;
    buf[6] = (len >>> 8) & 0xff;
    buf[7] = len & 0xff;
  } else {
    const len = frame.payload.length;
    buf[4] = (len >>> 8) & 0xff;
    buf[5] = len & 0xff;
  }

  buf.set(frame.payload, headerSize);
  return buf;
}
