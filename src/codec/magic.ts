/**
 * OMUX magic number utilities for stream-oriented transports (TCP, stdio).
 *
 * Stream-oriented transports MUST send 4 magic bytes before the first frame.
 * Message-oriented transports (WebSocket, WebRTC, QUIC) MUST NOT send them.
 *
 * @module codec/magic
 */

import { OMUX_MAGIC } from '../types.js';

/**
 * Returns a copy of the 4-byte OMUX magic sequence to send before the first frame
 * on stream-oriented transports.
 */
export function getMagicBytes(): Uint8Array {
  return new Uint8Array(OMUX_MAGIC);
}

/**
 * Validate that the given bytes start with the OMUX magic sequence.
 *
 * @param bytes - Must contain at least 4 bytes.
 * @throws {Error} If the magic bytes do not match or fewer than 4 bytes are provided.
 */
export function validateMagicBytes(bytes: Uint8Array): void {
  if (bytes.length < 4) {
    throw new Error(
      `Magic validation failed: need 4 bytes, got ${bytes.length}`,
    );
  }
  for (let i = 0; i < 4; i++) {
    if ((bytes[i] as number) !== (OMUX_MAGIC[i] as number)) {
      const got = Array.from(bytes.slice(0, 4))
        .map((b) => b.toString(16).padStart(2, '0'))
        .join(' ')
        .toUpperCase();
      throw new Error(
        `Invalid xumux magic bytes: expected "4F 4D 55 58" (OMUX), got "${got}". ` +
          'The remote side does not speak xumux, or magic bytes were sent on a message-oriented transport.',
      );
    }
  }
}
