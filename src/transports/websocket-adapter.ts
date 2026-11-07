/**
 * WebSocket transport (binding: websocket). Message-oriented, subprotocol `xumux`.
 * @module transports/websocket-adapter
 */

import type { TransportAdapter } from '../types.js';
import { CloseCode } from '../codec/constants.js';
import { XumuxProtocolError } from '../errors.js';
import { EventQueue, waitUntil } from './event-queue.js';

/** WebSocket subprotocol for xumux. */
export const XUMUX_SUBPROTOCOL = 'xumux';

/** The subset of the WebSocket API (browser or `ws`) this adapter needs. */
export interface WebSocketLike {
  readonly readyState: number;
  readonly bufferedAmount?: number;
  binaryType?: string;
  send(data: Uint8Array | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'close', listener: (event: { code: number }) => void): void;
  addEventListener(type: 'error', listener: (event: unknown) => void): void;
  removeEventListener?(type: string, listener: (event: never) => void): void;
}

export interface WebSocketAdapterOptions {
  /** Pause sending while `bufferedAmount` is above this many bytes. Default 1 MiB. */
  highWaterMark?: number;
}

export class WebSocketAdapter implements TransportAdapter {
  readonly kind = 'message' as const;
  private readonly queue = new EventQueue<Uint8Array>();
  private readonly highWaterMark: number;

  constructor(
    private readonly ws: WebSocketLike,
    options: WebSocketAdapterOptions = {},
  ) {
    this.highWaterMark = options.highWaterMark ?? 1 << 20;
    if ('binaryType' in ws) ws.binaryType = 'arraybuffer';
    // Detach once input ends, so a socket the application keeps doesn't retain this adapter.
    const end = (e?: Error): void => {
      this.queue.end(e);
      ws.removeEventListener?.('message', onMessage);
      ws.removeEventListener?.('close', onEnd);
      ws.removeEventListener?.('error', onEnd);
    };
    const onMessage = (event: { data: unknown }): void => {
      const d = event.data;
      if (d instanceof Uint8Array) this.queue.push(d);
      else if (d instanceof ArrayBuffer) this.queue.push(new Uint8Array(d));
      else end(new XumuxProtocolError(CloseCode.PROTOCOL_ERROR, 'text WebSocket message'));
    };
    const onEnd = (): void => end();
    ws.addEventListener('message', onMessage);
    ws.addEventListener('close', onEnd);
    ws.addEventListener('error', onEnd);
  }

  send(data: Uint8Array): void | Promise<void> {
    this.ws.send(data);
    if ((this.ws.bufferedAmount ?? 0) > this.highWaterMark) {
      return waitUntil(
        () => (this.ws.bufferedAmount ?? 0) <= this.highWaterMark / 2 || this.ws.readyState > 1,
      );
    }
  }

  receive(): AsyncIterable<Uint8Array> {
    return this.queue;
  }

  close(): void {
    this.ws.close(1000);
  }
}
