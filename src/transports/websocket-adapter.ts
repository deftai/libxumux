/**
 * WebSocket transport adapter for xumux.
 *
 * Accepts any WebSocket-compatible object (browser native WebSocket, Node.js 22+
 * global WebSocket, or the `ws` npm package). Magic bytes are NOT sent — WebSocket
 * is a message-oriented transport and protocol identification happens at the
 * transport level (via the `x-xumux` subprotocol negotiation).
 *
 * @module transports/websocket-adapter
 */

import type { TransportAdapter } from '../types.js';

/** Minimal WebSocket-like interface accepted by WebSocketAdapter. */
export interface WebSocketLike {
  readonly readyState: number;
  binaryType?: string;
  send(data: Uint8Array | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'close', listener: (event: { code: number }) => void): void;
  addEventListener(type: 'error', listener: (event: unknown) => void): void;
  removeEventListener(type: string, listener: (...args: unknown[]) => void): void;
}

/**
 * xumux transport adapter wrapping a WebSocket.
 *
 * The WebSocket should already be open (readyState === WebSocket.OPEN) or
 * connecting. The caller is responsible for creating and opening the WebSocket.
 */
export class WebSocketAdapter implements TransportAdapter {
  constructor(private readonly ws: WebSocketLike) {
    // Ensure binary messages arrive as ArrayBuffer (not Blob)
    if ('binaryType' in ws) {
      ws.binaryType = 'arraybuffer';
    }
  }

  /** Send a binary frame. */
  send(data: Uint8Array): void {
    this.ws.send(data);
  }

  /** Receive frames as an async iterable. Each value is one complete xumux frame. */
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
        return; // ignore non-binary messages
      }
      queue.push(frame);
      waiters.shift()?.();
    };

    const onClose = (event: { code: number }): void => {
      void event; // code not used
      closed = true;
      for (const resolve of waiters) resolve();
      waiters.length = 0;
    };

    this.ws.addEventListener('message', onMessage);
    this.ws.addEventListener('close', onClose);

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
      this.ws.removeEventListener('message', onMessage as (...args: unknown[]) => void);
      this.ws.removeEventListener('close', onClose as (...args: unknown[]) => void);
    }
  }

  /** Close the WebSocket. */
  close(): void {
    this.ws.close(1000, 'xumux close');
  }
}
