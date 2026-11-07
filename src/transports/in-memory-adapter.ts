/**
 * In-memory transport pair for tests and in-process use.
 * @module transports/in-memory-adapter
 */

import type { TransportAdapter } from '../types.js';
import { EventQueue } from './event-queue.js';

class InMemoryAdapter implements TransportAdapter {
  closed = false;
  peer!: InMemoryAdapter;
  readonly queue = new EventQueue<Uint8Array>();

  constructor(readonly kind: 'message' | 'stream') {}

  send(data: Uint8Array): void {
    if (this.closed) return;
    this.peer.queue.push(new Uint8Array(data)); // copy so callers may reuse buffers
  }

  receive(): AsyncIterable<Uint8Array> {
    return this.queue;
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.peer.queue.end();
    this.queue.end();
  }
}

/**
 * Two connected transports. `kind: 'stream'` makes the session use magic bytes and
 * stream framing, like TCP.
 */
export function createInMemoryPair(
  kind: 'message' | 'stream' = 'message',
): [TransportAdapter, TransportAdapter] {
  const a = new InMemoryAdapter(kind);
  const b = new InMemoryAdapter(kind);
  a.peer = b;
  b.peer = a;
  return [a, b];
}
