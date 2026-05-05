/**
 * In-memory transport adapter pair for testing.
 *
 * Creates two connected TransportAdapters that route frames between each other
 * entirely in process, without any network I/O. Each adapter's `send()` delivers
 * data to the other adapter's `receive()` async generator.
 *
 * @module transports/in-memory-adapter
 */

import type { TransportAdapter } from '../types.js';

/** State shared between a pair of in-memory adapters. */
interface Channel {
  queue: Uint8Array[];
  resolvers: Array<() => void>;
  closed: boolean;
}

/**
 * Single end of an in-memory transport pair.
 * Send on A → receive on B; send on B → receive on A.
 */
class InMemoryAdapter implements TransportAdapter {
  constructor(
    private readonly outgoing: Channel, // this adapter sends into outgoing
    private readonly incoming: Channel, // this adapter receives from incoming
  ) {}

  /** Send frame bytes to the peer adapter's receive queue. */
  send(data: Uint8Array): void {
    if (this.outgoing.closed) return;
    this.outgoing.queue.push(new Uint8Array(data)); // copy to avoid mutation
    this.outgoing.resolvers.shift()?.();
  }

  /** Receive frame bytes from the peer adapter's send queue. */
  async *receive(): AsyncIterable<Uint8Array> {
    while (!this.incoming.closed || this.incoming.queue.length > 0) {
      if (this.incoming.queue.length === 0) {
        if (this.incoming.closed) break;
        await new Promise<void>((resolve) => {
          this.incoming.resolvers.push(resolve);
        });
        continue;
      }
      const frame = this.incoming.queue.shift()!;
      yield frame;
    }
  }

  /** Close this adapter's outgoing channel, signalling EOF to the peer. */
  close(): void {
    if (!this.outgoing.closed) {
      this.outgoing.closed = true;
      // Wake any waiting receiver
      for (const resolve of this.outgoing.resolvers) resolve();
      this.outgoing.resolvers.length = 0;
    }
  }
}

/**
 * Create a connected pair of in-memory adapters.
 *
 * @returns `[adapterA, adapterB]` — frames sent on A arrive at B and vice versa.
 *
 * @example
 * ```ts
 * const [clientTransport, serverTransport] = createInMemoryPair();
 * const client = new XumuxClient(clientTransport);
 * const server = new XumuxServer(serverTransport);
 * ```
 */
export function createInMemoryPair(): [TransportAdapter, TransportAdapter] {
  // a→b: A sends, B receives
  const aTob: Channel = { queue: [], resolvers: [], closed: false };
  // b→a: B sends, A receives
  const bToa: Channel = { queue: [], resolvers: [], closed: false };

  const adapterA = new InMemoryAdapter(aTob, bToa); // A sends into aTob, receives from bToa
  const adapterB = new InMemoryAdapter(bToa, aTob); // B sends into bToa, receives from aTob

  return [adapterA, adapterB];
}
