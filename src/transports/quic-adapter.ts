/**
 * QUIC / WebTransport bidirectional stream (binding: quic, experimental).
 *
 * Treated as a byte stream: the session parses frames by length, so chunk
 * boundaries from the stream never matter.
 *
 * @module transports/quic-adapter
 */

import type { TransportAdapter } from '../types.js';
import { ignore } from '../ignore.js';

/** A WebTransport-style bidirectional stream. */
export interface StreamPair {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array>;
}

export class QuicAdapter implements TransportAdapter {
  /** QUIC streams are byte streams, but the binding sends no magic (ALPN identifies xumux). */
  readonly kind = 'stream' as const;
  readonly magic = false;
  private readonly writer: WritableStreamDefaultWriter<Uint8Array>;

  constructor(private readonly streams: StreamPair) {
    this.writer = streams.writable.getWriter();
  }

  async send(data: Uint8Array): Promise<void> {
    await this.writer.ready;
    // Awaiting the write surfaces stream errors to the session, which fails the send.
    await this.writer.write(data);
  }

  async *receive(): AsyncIterable<Uint8Array> {
    const reader = this.streams.readable.getReader();
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) return;
        yield value;
      }
    } finally {
      reader.releaseLock();
    }
  }

  close(): void {
    this.writer.close().catch(ignore);
  }
}
