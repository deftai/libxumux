/**
 * WebRTC DataChannel transport adapter for xumux.
 *
 * Works in browser (native RTCDataChannel) and Node.js (node-datachannel or wrtc).
 * Magic bytes are NOT sent — DataChannel is message-oriented and the channel
 * is identified by its `protocol` field (set to `"x-xumux"`).
 *
 * Per the xumux spec, when using WebRTC DataChannels, each xumux channel MAY
 * map 1:1 to a DataChannel (labeled `omux/<channel-name>`). The Channel byte
 * in every frame header MUST still be present for format consistency.
 *
 * @module transports/webrtc-adapter
 */

import type { TransportAdapter } from '../types.js';

/** Minimal RTCDataChannel-like interface accepted by WebRTCAdapter. */
export interface DataChannelLike {
  readonly readyState: string;
  send(data: Uint8Array | ArrayBuffer): void;
  close(): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'close', listener: () => void): void;
  addEventListener(type: 'error', listener: (event: unknown) => void): void;
  removeEventListener(type: string, listener: (...args: unknown[]) => void): void;
}

/**
 * xumux transport adapter wrapping an RTCDataChannel.
 *
 * The DataChannel should already be open (readyState === "open") or opening.
 * Set the DataChannel's `protocol` field to `"x-xumux"` when creating it.
 */
export class WebRTCAdapter implements TransportAdapter {
  constructor(private readonly dc: DataChannelLike) {}

  /** Send a binary frame. */
  send(data: Uint8Array): void {
    this.dc.send(data);
  }

  /** Receive frames as an async iterable. */
  async *receive(): AsyncIterable<Uint8Array> {
    const queue: Uint8Array[] = [];
    const waiters: Array<() => void> = [];
    let closed = false;

    const onMessage = (event: { data: unknown }): void => {
      let frame: Uint8Array;
      if (event.data instanceof Uint8Array) {
        frame = event.data;
      } else if (event.data instanceof ArrayBuffer) {
        frame = new Uint8Array(event.data);
      } else {
        return;
      }
      queue.push(frame);
      waiters.shift()?.();
    };

    const onClose = (): void => {
      closed = true;
      for (const resolve of waiters) resolve();
      waiters.length = 0;
    };

    this.dc.addEventListener('message', onMessage);
    this.dc.addEventListener('close', onClose);

    try {
      while (!closed || queue.length > 0) {
        if (queue.length === 0) {
          if (closed) break;
          await new Promise<void>((resolve) => waiters.push(resolve));
          continue;
        }
        yield queue.shift()!;
      }
    } finally {
      /* eslint-disable @typescript-eslint/no-unnecessary-type-assertion */
      this.dc.removeEventListener('message', onMessage as (...args: unknown[]) => void);
      this.dc.removeEventListener('close', onClose as (...args: unknown[]) => void);
      /* eslint-enable @typescript-eslint/no-unnecessary-type-assertion */
    }
  }

  /** Close the DataChannel. */
  close(): void {
    this.dc.close();
  }
}
