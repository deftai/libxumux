/**
 * Transport adapter interface.
 * @module types
 */

/**
 * A bidirectional byte pipe underneath an xumux session.
 *
 * Adapters do **not** frame or parse xumux. The session does that, so limits,
 * magic bytes and framing are handled in one place for every transport.
 *
 * - `kind: 'message'`: each `send` is one transport message and `receive` yields whole
 *   messages (WebSocket, WebRTC DataChannel). The session puts exactly one frame in each.
 * - `kind: 'stream'`: a byte stream (TCP, Unix, stdio, QUIC stream). `receive` yields
 *   arbitrary chunks. The session sends and checks the `XMUX` magic and parses frames.
 */
export interface TransportAdapter {
  readonly kind: 'message' | 'stream';

  /** Stream kind only: send and check the `XMUX` magic bytes. Default true (TCP, Unix, stdio); false for QUIC. */
  readonly magic?: boolean;

  /**
   * Send bytes. May return a promise that resolves when the transport is ready for more
   * (backpressure). The session never has more than one send outstanding.
   */
  send(data: Uint8Array): void | Promise<void>;

  /** Incoming messages (message kind) or chunks (stream kind). Ends when the transport closes. */
  receive(): AsyncIterable<Uint8Array>;

  /** Close the transport. On stream transports, shut down the write side first where possible. */
  close(): void;
}

/** xumux protocol version this library implements. */
export const XUMUX_VERSION = '0.2.0-draft' as const;
