/**
 * Node.js stdio transport adapter for xumux.
 *
 * Uses `process.stdin` for reading and `process.stdout` for writing.
 * Node.js only — not browser-safe. Do not import this in browser-targeted bundles.
 * Writes the OMUX magic bytes before the first frame and validates them on receive.
 *
 * Suitable for process IPC (parent spawns child and communicates over stdio).
 *
 * @module transports/stdio-adapter
 */

/// <reference types="node" />

import type { Readable, Writable } from 'node:stream';
import type { TransportAdapter } from '../types.js';
import { getMagicBytes, validateMagicBytes } from '../codec/magic.js';
import { FrameDecoder } from '../codec/decode-frame.js';

/**
 * xumux transport adapter using Node.js stdio streams.
 *
 * Defaults to `process.stdin` / `process.stdout`. Custom streams can be
 * injected for testing.
 */
export class StdioAdapter implements TransportAdapter {
  private magicSent = false;
  private readonly input: Readable;
  private readonly output: Writable;

  constructor(input?: Readable, output?: Writable) {
    this.input = input ?? process.stdin;
    this.output = output ?? process.stdout;
  }

  /** Send a binary frame, prepending magic bytes on the first call. */
  send(data: Uint8Array): void {
    if (!this.magicSent) {
      this.magicSent = true;
      this.output.write(getMagicBytes());
    }
    this.output.write(data);
  }

  /**
   * Receive frames as an async iterable.
   * Validates OMUX magic bytes first, then uses FrameDecoder for the byte stream.
   */
  async *receive(): AsyncIterable<Uint8Array> {
    const decoder = new FrameDecoder();
    const magicBuf: number[] = [];
    let magicValidated = false;

    for await (const chunk of this.input as AsyncIterable<Buffer>) {
      const data = new Uint8Array(
        chunk instanceof Uint8Array ? chunk.buffer : Buffer.from(chunk).buffer,
        chunk instanceof Uint8Array ? chunk.byteOffset : 0,
        chunk.length,
      );

      if (!magicValidated) {
        let dataStart = 0;
        for (let i = 0; i < data.length && magicBuf.length < 4; i++) {
          magicBuf.push(data[i] as number);
          dataStart = i + 1;
        }
        if (magicBuf.length < 4) continue;

        validateMagicBytes(new Uint8Array(magicBuf));
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

  /** End the output stream. */
  close(): void {
    this.output.end();
  }
}
