/**
 * Core types for the xumux protocol library.
 * @module types
 */

/**
 * Transport adapter interface — implemented by each transport adapter.
 *
 * Provides a uniform send/receive/close surface over any underlying transport
 * (WebSocket, WebRTC DataChannel, TCP, stdio, QUIC). The shared codec and
 * connection logic depend only on this interface, never on concrete transports.
 */
export interface TransportAdapter {
  /** Send a binary frame to the remote peer. */
  send(data: Uint8Array): void;

  /**
   * Receive frames from the remote peer.
   * Yields complete frames (post-magic-strip for stream transports).
   */
  receive(): AsyncIterable<Uint8Array>;

  /** Close the transport connection. */
  close(): void;
}

/** xumux protocol version this library targets. */
export const XUMUX_VERSION = '0.2.0-draft' as const;

/** OMUX magic bytes sent by stream-oriented transports before the first frame. */
export const OMUX_MAGIC = new Uint8Array([0x4f, 0x4d, 0x55, 0x58]) as Readonly<Uint8Array>;
