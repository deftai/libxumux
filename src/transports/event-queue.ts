/**
 * Small push-to-pull queue used by event-based adapters (WebSocket, WebRTC).
 * @module transports/event-queue
 */

export class EventQueue<T> {
  private items: T[] = [];
  private waiter: (() => void) | null = null;
  private ended = false;
  private failure: Error | undefined = undefined;

  push(item: T): void {
    if (this.ended) return;
    this.items.push(item);
    this.wake();
  }

  /** End the stream; with `error`, the consumer's iteration throws it after draining. */
  end(error?: Error): void {
    if (this.ended) return;
    this.ended = true;
    this.failure = error;
    this.wake();
  }

  private wake(): void {
    const w = this.waiter;
    this.waiter = null;
    w?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<T> {
    for (;;) {
      if (this.items.length > 0) {
        yield this.items.shift() as T;
        continue;
      }
      if (this.ended) {
        if (this.failure !== undefined) throw this.failure;
        return;
      }
      await new Promise<void>((resolve) => (this.waiter = resolve));
    }
  }
}

/** Resolve once `check()` is true, polling every `intervalMs`. */
export function waitUntil(check: () => boolean, intervalMs = 5): Promise<void> {
  if (check()) return Promise.resolve();
  return new Promise((resolve) => {
    const t = setInterval(() => {
      if (check()) {
        clearInterval(t);
        resolve();
      }
    }, intervalMs);
  });
}
