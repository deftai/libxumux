/**
 * Thin shim that wraps a node-datachannel DataChannel to implement DataChannelLike.
 *
 * node-datachannel uses a callback API (onMessage, onClosed, onOpen, onError)
 * rather than addEventListener. This shim bridges the gap so WebRTCAdapter
 * can be used with a real WebRTC DataChannel backed by libdatachannel.
 */
import type NodeDataChannel from 'node-datachannel';
import type { DataChannelLike } from '../../src/transports/webrtc-adapter.js';

type Listener = (...args: unknown[]) => void;

/** Wraps a node-datachannel DataChannel as a DataChannelLike. */
export class NodeDCShim implements DataChannelLike {
  private readonly listeners = new Map<string, Listener[]>();

  constructor(private readonly dc: NodeDataChannel.DataChannel) {
    // Wire native callbacks → internal listener dispatch
    dc.onClosed(() => this.fire('close'));
    dc.onError((err: string) => this.fire('error', new Error(err)));
    dc.onMessage((msg: Buffer | string) => {
      let data: Uint8Array;
      if (Buffer.isBuffer(msg)) {
        // Avoid shared-pool aliasing — copy into a fresh Uint8Array
        data = new Uint8Array(msg);
      } else {
        data = new TextEncoder().encode(msg);
      }
      this.fire('message', { data });
    });
  }

  /** 'open' while the channel is open, 'closed' otherwise. */
  get readyState(): string {
    return this.dc.isOpen() ? 'open' : 'closed';
  }

  /** Send binary data via the underlying DataChannel. */
  send(data: Uint8Array | ArrayBuffer): void {
    const buf =
      data instanceof Uint8Array
        ? Buffer.from(data.buffer, data.byteOffset, data.byteLength)
        : Buffer.from(data);
    this.dc.sendMessageBinary(buf);
  }

  close(): void {
    this.dc.close();
  }

  addEventListener(type: string, listener: Listener): void {
    if (!this.listeners.has(type)) this.listeners.set(type, []);
    this.listeners.get(type)!.push(listener);
  }

  removeEventListener(type: string, listener: Listener): void {
    const arr = this.listeners.get(type) ?? [];
    const idx = arr.indexOf(listener);
    if (idx >= 0) arr.splice(idx, 1);
  }

  private fire(type: string, ...args: unknown[]): void {
    for (const fn of this.listeners.get(type) ?? []) fn(...args);
  }
}

/**
 * Wait for a node-datachannel DataChannel to reach the 'open' state.
 * Resolves immediately if the channel is already open.
 *
 * @param timeoutMs - Fail if not open within this window (default 10 s).
 */
export function waitForChannelOpen(
  dc: NodeDataChannel.DataChannel,
  timeoutMs = 10_000,
): Promise<void> {
  if (dc.isOpen()) return Promise.resolve();
  return new Promise<void>((resolve, reject) => {
    const t = setTimeout(
      () => reject(new Error(`DataChannel '${dc.getLabel()}' did not open within ${timeoutMs}ms`)),
      timeoutMs,
    );
    dc.onOpen(() => {
      clearTimeout(t);
      resolve();
    });
  });
}
