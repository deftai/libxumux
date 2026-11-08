/**
 * WebRTC DataChannel transport (binding: webrtc, single DataChannel).
 *
 * The whole xumux connection runs over one reliable, ordered DataChannel labelled
 * `xumux`, one frame per binary message. Signalling (SDP and ICE) is the
 * application's: create the PeerConnection, create the DataChannel with
 * {@link createXumuxDataChannel} on both peers before the offer, exchange
 * descriptions, and hand the DataChannel to {@link WebRTCAdapter}.
 *
 * @module transports/webrtc-adapter
 */

import type { TransportAdapter } from '../types.js';
import { CloseCode, DEFAULT_FRAGMENT_SIZE, HEADER_SIZE } from '../codec/constants.js';
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

/** SCTP maximum message size to assume when the SDP declares none (binding: 64 KiB works everywhere). */
export const DEFAULT_SCTP_MAX_MESSAGE_SIZE = 65536;

/**
 * The parts of the standard `RTCDataChannel` this adapter uses. A browser
 * `RTCDataChannel` satisfies it, as does the `node-datachannel/polyfill` one.
 *
 * Properties marked optional are checked or used when present, so that minimal
 * wrappers (tests, shims) also work.
 */
export interface DataChannelLike {
  readonly readyState: string;
  readonly label?: string;
  readonly protocol?: string;
  readonly ordered?: boolean;
  readonly maxRetransmits?: number | null;
  readonly maxPacketLifeTime?: number | null;
  readonly bufferedAmount?: number;
  bufferedAmountLowThreshold?: number;
  binaryType?: string;
  send(data: Uint8Array<ArrayBuffer>): void;
  close(): void;
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void;
  addEventListener(type: 'open' | 'close' | 'bufferedamountlow', listener: () => void): void;
  addEventListener(type: 'error', listener: (event: unknown) => void): void;
  // `any`: listeners registered above, with whichever event type the implementation uses.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  removeEventListener?(type: string, listener: (event: any) => void): void;
}

// Compile-time check that a standard RTCDataChannel is a DataChannelLike.
type Assert<T extends true> = T;
// eslint-disable-next-line @typescript-eslint/no-unused-vars -- type-level assertion only
type _RTCDataChannelIsDataChannelLike = Assert<
  RTCDataChannel extends DataChannelLike ? true : false
>;

/** The part of `RTCPeerConnection` that {@link createXumuxDataChannel} uses. */
export interface PeerConnectionLike<T> {
  createDataChannel(label: string, init: typeof XUMUX_DATACHANNEL_INIT): T;
}

/**
 * Create the xumux DataChannel on `pc`: label `xumux`, negotiated with ID 0,
 * ordered and reliable, protocol `xumux`. Call it on both peers before the offer
 * is created, and wrap the result in a {@link WebRTCAdapter} straight away.
 */
export function createXumuxDataChannel<T>(pc: PeerConnectionLike<T>): T {
  return pc.createDataChannel(XUMUX_DATACHANNEL_LABEL, XUMUX_DATACHANNEL_INIT);
}

/**
 * Check that `dc` is a valid xumux DataChannel: label and protocol `xumux`,
 * ordered, reliable. Throws a `TypeError` otherwise. Properties `dc` does not
 * expose are not checked.
 */
export function checkXumuxDataChannel(dc: DataChannelLike): void {
  if (dc.label !== undefined && dc.label !== XUMUX_DATACHANNEL_LABEL)
    throw new TypeError(`DataChannel label is "${dc.label}", want "${XUMUX_DATACHANNEL_LABEL}"`);
  if (dc.protocol !== undefined && dc.protocol !== XUMUX_DATACHANNEL_PROTOCOL)
    throw new TypeError(
      `DataChannel protocol is "${dc.protocol}", want "${XUMUX_DATACHANNEL_PROTOCOL}"`,
    );
  if (dc.ordered === false) throw new TypeError('the xumux DataChannel must be ordered');
  if (dc.maxRetransmits != null || dc.maxPacketLifeTime != null)
    throw new TypeError(
      'the xumux DataChannel must be reliable (no maxRetransmits or maxPacketLifeTime)',
    );
}

/**
 * The `fragmentSize` session option for a peer whose SCTP maximum message size
 * is `maxMessageSize` (`pc.sctp?.maxMessageSize`): `preferred`, reduced so that
 * a frame (8-byte header plus payload) fits in one message.
 */
export function fragmentSizeFor(
  maxMessageSize: number | null | undefined,
  preferred: number = DEFAULT_FRAGMENT_SIZE,
): number {
  const mms =
    maxMessageSize && Number.isFinite(maxMessageSize)
      ? maxMessageSize
      : DEFAULT_SCTP_MAX_MESSAGE_SIZE;
  return Math.max(1, Math.min(preferred, mms - HEADER_SIZE));
}

export interface WebRTCAdapterOptions {
  /**
   * Pause sending while `bufferedAmount` is above this many bytes, until the
   * `bufferedamountlow` event reports it has drained to half. Default 1 MiB.
   */
  highWaterMark?: number;
}

/**
 * A message transport over one RTCDataChannel. The DataChannel may still be
 * connecting: sends wait for it to open, and fail if it closes first.
 *
 * The adapter sets the DataChannel's `binaryType` and `bufferedAmountLowThreshold`.
 */
export class WebRTCAdapter implements TransportAdapter {
  readonly kind = 'message' as const;
  /** Resolves when the DataChannel is open; rejects if it closes before opening. */
  readonly opened: Promise<void>;
  private readonly queue = new EventQueue<Uint8Array>();
  private readonly highWaterMark: number;
  private readonly lowWaterMark: number;
  private readonly lowWaiters = new Set<() => void>();
  private readonly eventDriven: boolean;
  /** Blob messages are converted asynchronously; this keeps them in order. */
  private tail: Promise<void> | null = null;
  private ended = false;

  constructor(
    private readonly dc: DataChannelLike,
    options: WebRTCAdapterOptions = {},
  ) {
    checkXumuxDataChannel(dc);
    this.highWaterMark = options.highWaterMark ?? 1 << 20;
    this.lowWaterMark = Math.floor(this.highWaterMark / 2);
    if ('binaryType' in dc) dc.binaryType = 'arraybuffer';
    this.eventDriven = typeof dc.bufferedAmountLowThreshold === 'number';
    if (this.eventDriven) dc.bufferedAmountLowThreshold = this.lowWaterMark;

    let opened: () => void = () => {};
    let failed: (e: Error) => void = () => {};
    this.opened = new Promise<void>((resolve, reject) => {
      opened = resolve;
      failed = reject;
    });
    // Callers that never await `opened` should not see an unhandled rejection.
    this.opened.catch(() => {});
    if (dc.readyState === 'open') opened();
    else if (dc.readyState === 'closing' || dc.readyState === 'closed')
      failed(new Error('DataChannel closed before it opened'));

    // In order: messages (via `tail` while a Blob is converting), then the end.
    const deliver = (f: () => void | Promise<void>): void => {
      if (this.tail) this.tail = this.tail.then(f);
      else {
        const r = f();
        if (r) this.tail = r.then(() => {});
      }
    };
    const end = (e?: Error): void => {
      if (this.ended) return;
      this.ended = true;
      failed(new Error('DataChannel closed before it opened'));
      this.wakeLow();
      // Detach, so a DataChannel the application keeps doesn't retain this adapter.
      dc.removeEventListener?.('message', onMessage);
      dc.removeEventListener?.('open', onOpen);
      dc.removeEventListener?.('close', onEnd);
      dc.removeEventListener?.('error', onEnd);
      dc.removeEventListener?.('bufferedamountlow', onLow);
      deliver(() => this.queue.end(e));
    };
    const onMessage = (event: { data: unknown }): void => {
      const d = event.data;
      if (d instanceof ArrayBuffer) deliver(() => this.queue.push(new Uint8Array(d)));
      else if (ArrayBuffer.isView(d))
        deliver(() => this.queue.push(new Uint8Array(d.buffer, d.byteOffset, d.byteLength)));
      else if (typeof Blob !== 'undefined' && d instanceof Blob)
        deliver(async () => {
          let b: ArrayBuffer;
          try {
            b = await d.arrayBuffer();
          } catch (e) {
            // A message is lost: end input here, so nothing after it is delivered.
            this.queue.end(new Error(`DataChannel Blob message could not be read: ${String(e)}`));
            end();
            return;
          }
          this.queue.push(new Uint8Array(b));
        });
      else end(new XumuxProtocolError(CloseCode.PROTOCOL_ERROR, 'non-binary DataChannel message'));
    };
    const onOpen = (): void => opened();
    const onEnd = (): void => end();
    const onLow = (): void => this.wakeLow();
    dc.addEventListener('message', onMessage);
    dc.addEventListener('open', onOpen);
    dc.addEventListener('close', onEnd);
    dc.addEventListener('error', onEnd);
    dc.addEventListener('bufferedamountlow', onLow);
  }

  send(data: Uint8Array): void | Promise<void> {
    if (this.dc.readyState === 'connecting') return this.opened.then(() => this.sendNow(data));
    return this.sendNow(data);
  }

  private sendNow(data: Uint8Array): void | Promise<void> {
    this.dc.send(data as Uint8Array<ArrayBuffer>);
    if (this.buffered() > this.highWaterMark) return this.drained();
  }

  private buffered(): number {
    return this.dc.bufferedAmount ?? 0;
  }

  /** Resolve once the send buffer is at or below the low-water mark, or the channel stops. */
  private drained(): Promise<void> {
    const done = (): boolean =>
      this.ended || this.dc.readyState !== 'open' || this.buffered() <= this.lowWaterMark;
    if (!this.eventDriven) return waitUntil(done);
    return new Promise<void>((resolve) => {
      const check = (): void => {
        if (!done()) return;
        this.lowWaiters.delete(check);
        resolve();
      };
      this.lowWaiters.add(check);
      check();
    });
  }

  private wakeLow(): void {
    for (const w of [...this.lowWaiters]) w();
  }

  receive(): AsyncIterable<Uint8Array> {
    return this.queue;
  }

  close(): void {
    this.dc.close();
  }
}
