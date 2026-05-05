/**
 * QUIC / WebTransport adapter for xumux.
 *
 * Works in browser and Node.js (where WebTransport is available).
 * Magic bytes are NOT sent — the protocol is identified via ALPN or URL.
 *
 * Accepts a pair of `{ readable: ReadableStream<Uint8Array>, writable: WritableStream<Uint8Array> }`
 * which matches the WebTransport `datagrams` shape or a bidirectional stream.
 *
 * @module transports/quic-adapter
 */

import type { TransportAdapter } from '../types.js';

/** Stream pair compatible with WebTransport datagrams or bidirectional streams. */
export interface StreamPair {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

/**
 * xumux transport adapter for QUIC / WebTransport.
 *
 * Provide a `{ readable, writable }` pair from either:
 * - `transport.datagrams` (for datagram mode — note: fragmentation is forbidden on datagrams)
 * - A bidirectional stream opened via `transport.createBidirectionalStream()`
 */
export class QuicAdapter implements TransportAdapter {
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;

  constructor(private readonly streams: StreamPair) {
    this.writer = streams.writable.getWriter();
  }

  /** Send a binary frame via the writable stream. */
  send(data: Uint8Array): void {
    void this.writer.write(data);
  }

  /** Receive frames from the readable stream. */
  async *receive(): AsyncIterable<Uint8Array> {
    const reader = this.streams.readable.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        yield value;
      }
    } finally {
      reader.releaseLock();
    }
  }

  /** Release writer and close the writable stream. */
  close(): void {
    void this.writer.close().catch(() => { /* ignore */ });
    this.writer.releaseLock();
  }
}
