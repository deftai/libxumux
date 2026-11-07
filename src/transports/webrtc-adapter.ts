/**
 * WebRTC DataChannel transport (binding: webrtc, single DataChannel).
 * @module transports/webrtc-adapter
 */

import type { TransportAdapter } from '../types.js';
import { CloseCode } from '../codec/constants.js';
import { XumuxProtocolError } from '../errors.js';
import { EventQueue, waitUntil } from './event-queue.js';

/** Label and protocol of the base xumux DataChannel. */
export const XUMUX_DATACHANNEL_LABEL = 'xumux';
export const XUMUX_DATACHANNEL_PROTOCOL = 'xumux';

/** Recommended `RTCPeerConnection.createDataChannel` arguments for the base binding. */
export const XUMUX_DATACHANNEL_INIT = {
  negotiated: true,
  id: 0,
  ordered: true,
  protocol: XUMUX_DATACHANNEL_PROTOCOL,
} as const;

/** The subset of RTCDataChannel (browser or node-datachannel shim) this adapter needs. */
export interface DataChannelLike {
  readonly readyState: string;
  readonly bufferedAmount?: number;
  binaryType?: string;
  send(data: Uint8Array | ArrayBuffer): void;
  close(): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'close', listener: () => void): void;
  addEventListener(type: 'error', listener: (event: unknown) => void): void;
  removeEventListener?(type: string, listener: (event: never) => void): void;
}

export interface WebRTCAdapterOptions {
  /** Pause sending while `bufferedAmount` is above this many bytes. Default 1 MiB. */
  highWaterMark?: number;
}

export class WebRTCAdapter implements TransportAdapter {
  readonly kind = 'message' as const;
  private readonly queue = new EventQueue<Uint8Array>();
  private readonly highWaterMark: number;

  constructor(
    private readonly dc: DataChannelLike,
    options: WebRTCAdapterOptions = {},
  ) {
    this.highWaterMark = options.highWaterMark ?? 1 << 20;
    if ('binaryType' in dc) dc.binaryType = 'arraybuffer';
    // Detach once input ends, so a DataChannel the application keeps doesn't retain this adapter.
    const end = (e?: Error): void => {
      this.queue.end(e);
      dc.removeEventListener?.('message', onMessage);
      dc.removeEventListener?.('close', onEnd);
      dc.removeEventListener?.('error', onEnd);
    };
    const onMessage = (event: { data: unknown }): void => {
      const d = event.data;
      if (d instanceof Uint8Array) this.queue.push(d);
      else if (d instanceof ArrayBuffer) this.queue.push(new Uint8Array(d));
      else end(new XumuxProtocolError(CloseCode.PROTOCOL_ERROR, 'non-binary DataChannel message'));
    };
    const onEnd = (): void => end();
    dc.addEventListener('message', onMessage);
    dc.addEventListener('close', onEnd);
    dc.addEventListener('error', onEnd);
  }

  send(data: Uint8Array): void | Promise<void> {
    this.dc.send(data);
    if ((this.dc.bufferedAmount ?? 0) > this.highWaterMark) {
      return waitUntil(
        () =>
          (this.dc.bufferedAmount ?? 0) <= this.highWaterMark / 2 || this.dc.readyState !== 'open',
      );
    }
  }

  receive(): AsyncIterable<Uint8Array> {
    return this.queue;
  }

  close(): void {
    this.dc.close();
  }
}
