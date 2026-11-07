/// <reference types="node" />
/**
 * TCP / Unix-socket transport (binding: tcp). Stream-oriented.
 * The session sends and checks the XMUX magic and parses frames.
 * @module transports/tcp-adapter
 */

import type { Socket } from 'node:net';
import type { TransportAdapter } from '../types.js';

export class TcpAdapter implements TransportAdapter {
  readonly kind = 'stream' as const;

  constructor(private readonly socket: Socket) {}

  send(data: Uint8Array): void | Promise<void> {
    if (this.socket.destroyed) return;
    if (!this.socket.write(data)) {
      return new Promise((resolve) => {
        const done = (): void => {
          this.socket.off('drain', done);
          this.socket.off('close', done);
          resolve();
        };
        this.socket.once('drain', done);
        this.socket.once('close', done);
      });
    }
  }

  async *receive(): AsyncIterable<Uint8Array> {
    for await (const chunk of this.socket as AsyncIterable<Buffer>) {
      yield new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
    }
  }

  /** Half-close (FIN) so the peer reads everything we sent, then destroy shortly after. */
  close(): void {
    if (this.socket.destroyed) return;
    this.socket.end();
    setTimeout(() => this.socket.destroy(), 1000).unref?.();
  }
}
