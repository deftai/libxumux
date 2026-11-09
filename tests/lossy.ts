/**
 * A transport pair for resume tests that can fail like a network: `cut()` ends both sides at
 * once and loses everything sent but not yet read, in both directions.
 */
import type { TransportAdapter } from '../src/types.js';

class LossyEnd implements TransportAdapter {
  peer!: LossyEnd;
  private items: Uint8Array[] = [];
  private waiter: (() => void) | null = null;
  ended = false;
  closed = false;
  /** Bytes this end has written. */
  sentBytes = 0;

  constructor(
    readonly kind: 'message' | 'stream',
    private readonly pair: LossyPair,
  ) {}

  send(data: Uint8Array): void {
    if (this.closed || this.pair.isCut) return;
    this.sentBytes += data.length;
    this.peer.push(new Uint8Array(data));
  }

  push(d: Uint8Array): void {
    if (this.ended) return;
    this.items.push(d);
    this.wake();
  }

  /** Bytes lost in flight when the pair was cut. */
  lostBytes = 0;

  end(drop: boolean): void {
    if (drop) {
      for (const d of this.items) this.lostBytes += d.length;
      this.items = [];
    }
    this.ended = true;
    this.wake();
  }

  private wake(): void {
    const w = this.waiter;
    this.waiter = null;
    w?.();
  }

  receive(): AsyncIterable<Uint8Array> {
    return {
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<Uint8Array>> => {
          for (;;) {
            const d = this.items.shift();
            if (d) return { done: false, value: d };
            if (this.ended) return { done: true, value: undefined };
            await new Promise<void>((r) => (this.waiter = r));
          }
        },
      }),
    };
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.peer.end(false);
    this.end(false);
  }
}

export class LossyPair {
  readonly a: LossyEnd;
  readonly b: LossyEnd;
  isCut = false;

  constructor(kind: 'message' | 'stream' = 'stream') {
    this.a = new LossyEnd(kind, this);
    this.b = new LossyEnd(kind, this);
    this.a.peer = this.b;
    this.b.peer = this.a;
  }

  get lostBytes(): number {
    return this.a.lostBytes + this.b.lostBytes;
  }

  /** Fail the network: both ends see the transport end, and everything in flight is lost. */
  cut(): void {
    this.isCut = true;
    this.a.end(true);
    this.b.end(true);
  }
}
