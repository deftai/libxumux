/**
 * Node.js TCP transport adapter for xumux (net.Socket).
 *
 * Node.js only — not browser-safe. Do not import this in browser-targeted bundles.
 * Writes the OMUX magic bytes before the first frame and validates them on receive.
 *
 * @module transports/tcp-adapter
 */

/// <reference types="node" />

import type { Socket } from 'node:net';
import type { TransportAdapter } from '../types.js';
import { getMagicBytes, validateMagicBytes } from '../codec/magic.js';
import { FrameDecoder } from '../codec/decode-frame.js';

/**
 * xumux transport adapter wrapping a Node.js TCP `net.Socket`.
 *
 * On connect:
 * - Sends `OMUX` magic bytes before the first frame.
 * - Reads and validates the peer's `OMUX` magic bytes before yielding any frames.
 *
 * The socket should already be connected. The caller is responsible for
 * creating and establishing the connection.
 */
export class TcpAdapter implements TransportAdapter {
  private magicSent = false;

  constructor(private readonly socket: Socket) {}

  /** Send a binary frame, prepending magic bytes on the first call. */
  send(data: Uint8Array): void {
    if (!this.magicSent) {
      this.magicSent = true;
      this.socket.write(getMagicBytes());
    }
    this.socket.write(data);
  }

  /**
   * Receive frames as an async iterable.
   * Validates OMUX magic bytes first, then uses FrameDecoder for the byte stream.
   */
  async *receive(): AsyncIterable<Uint8Array> {
    const decoder = new FrameDecoder();
    const magicBuf: number[] = [];
    let magicValidated = false;

    for await (const chunk of this.socket as AsyncIterable<Buffer>) {
      const data = new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);

      if (!magicValidated) {
        // Accumulate bytes until we have 4 magic bytes
        let dataStart = 0;
        for (let i = 0; i < data.length && magicBuf.length < 4; i++) {
          magicBuf.push(data[i] as number);
          dataStart = i + 1;
        }
        if (magicBuf.length < 4) continue;

        validateMagicBytes(new Uint8Array(magicBuf)); // throws if invalid
        magicValidated = true;

        if (dataStart < data.length) {
          const remaining = data.slice(dataStart);
          for (const frameBytes of decoder.push(remaining)) {
            yield frameBytes;
          }
        }
        continue;
      }

      for (const frameBytes of decoder.push(data)) {
        yield frameBytes;
      }
    }
  }

  /** Destroy the socket. */
  close(): void {
    this.socket.destroy();
  }
}
