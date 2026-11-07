/// <reference types="node" />
/**
 * stdio transport (binding: stdio). Stream-oriented. Defaults to process.stdin/stdout.
 * @module transports/stdio-adapter
 */

import type { Readable, Writable } from 'node:stream';
import type { TransportAdapter } from '../types.js';

export class StdioAdapter implements TransportAdapter {
  readonly kind = 'stream' as const;
  private readonly input: Readable;
  private readonly output: Writable;

  constructor(input?: Readable, output?: Writable) {
    this.input = input ?? process.stdin;
    this.output = output ?? process.stdout;
  }

  send(data: Uint8Array): void | Promise<void> {
    if (this.output.writableEnded || this.output.destroyed) return;
    if (!this.output.write(data)) {
      return new Promise((resolve) => {
        const done = (): void => {
          this.output.off('drain', done);
          this.output.off('close', done);
          resolve();
        };
        this.output.once('drain', done);
        this.output.once('close', done);
      });
    }
  }

  async *receive(): AsyncIterable<Uint8Array> {
    for await (const chunk of this.input as AsyncIterable<Buffer | string>) {
      const b = typeof chunk === 'string' ? Buffer.from(chunk, 'binary') : chunk;
      yield new Uint8Array(b.buffer, b.byteOffset, b.byteLength);
    }
  }

  close(): void {
    if (!this.output.writableEnded) this.output.end();
  }
}
