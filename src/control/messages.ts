/**
 * Typed interfaces for all xumux control channel (0x00) message payloads.
 *
 * JSON messages: HELLO, WELCOME, OPEN_CHANNEL, CHANNEL_ACK, CHANNEL_REJECT,
 *                CLOSE_CHANNEL, CLOSE, ERROR.
 * Binary messages: PING (4 bytes), PONG (8 bytes).
 *
 * @module control/messages
 */

// ─── Handshake ───────────────────────────────────────────────────────────────

/** Channel request sent in HELLO (initial channel negotiation). */
export interface ChannelRequest {
  /** Unique channel name on this connection. */
  name: string;
  /** Guaranteed delivery. */
  reliable: boolean;
  /** In-order delivery. */
  ordered: boolean;
  /** Maximum retransmission attempts (0 = fire-and-forget). */
  maxRetransmits?: number;
  /** Maximum time to attempt delivery (ms). */
  maxPacketLifeTime?: number;
  /** Application-specific channel metadata. */
  metadata?: Record<string, unknown>;
}

/** HELLO (0x01) — Client → Server. First message after transport establishment. */
export interface HelloMessage {
  /** Protocol version: [major, minor, patch]. Current: [0, 1, 0]. */
  version: [number, number, number];
  /** Application protocol identifier (e.g. "vroom/0.1"). */
  application?: string;
  /** Requested extensions (e.g. ["fragmentation"]). */
  extensions?: string[];
  /** Maximum payload bytes the client can receive. Default: 65535. */
  maxMessageSize?: number;
  /** Keepalive PING interval in seconds. Default: 30. 0 = disabled. */
  pingInterval?: number;
  /** Seconds to wait for PONG before closing. Default: 10. */
  pingTimeout?: number;
  /** Application channels to open during handshake (excluding control channel). */
  channels?: ChannelRequest[];
  /** Authentication credentials. */
  auth?: Record<string, unknown>;
}

/** Channel assignment in WELCOME. */
export interface AssignedChannel {
  /** Channel name (echoed from HELLO). */
  name: string;
  /** Assigned numeric channel ID (1-254). */
  id: number;
}

/** WELCOME (0x02) — Server → Client. Response to HELLO. */
export interface WelcomeMessage {
  /** Server's protocol version: [major, minor, patch]. */
  version: [number, number, number];
  /** Accepted extensions (intersection of client request). */
  extensions?: string[];
  /** Negotiated max message size (minimum of both sides). */
  maxMessageSize?: number;
  /** Negotiated PING interval (server's value wins). */
  pingInterval?: number;
  /** Negotiated PONG timeout (server's value wins). */
  pingTimeout?: number;
  /** Channels with assigned numeric IDs. */
  channels: AssignedChannel[];
}

// ─── Dynamic Channel Management ──────────────────────────────────────────────

/** OPEN_CHANNEL (0x03) — Bidirectional. Dynamically open a channel post-handshake. */
export interface OpenChannelMessage {
  /** Unique request ID for correlating with CHANNEL_ACK or CHANNEL_REJECT. */
  requestId: number;
  /** Channel name (must be unique per connection). */
  name: string;
  /** Delivery guarantee. */
  reliable: boolean;
  /** Ordering guarantee. */
  ordered: boolean;
  /** Maximum retransmission attempts. */
  maxRetransmits?: number;
  /** Maximum delivery time (ms). */
  maxPacketLifeTime?: number;
  /** Application-specific metadata. */
  metadata?: Record<string, unknown>;
}

/** CHANNEL_ACK (0x04) — Bidirectional. Confirms a channel was opened. */
export interface ChannelAckMessage {
  /** Matches the OPEN_CHANNEL requestId. */
  requestId: number;
  /** Assigned channel ID (1-254). */
  id: number;
  /** Channel name (echoed back). */
  name: string;
}

/** CLOSE_CHANNEL (0x05) — Bidirectional. Close an existing channel. */
export interface CloseChannelMessage {
  /** Channel ID to close. MUST NOT be 0 (control channel). */
  id: number;
  /** Human-readable reason. */
  reason?: string;
}

/** CHANNEL_REJECT (0x06) — Bidirectional. Refuse a channel open request. */
export interface ChannelRejectMessage {
  /** Matches the OPEN_CHANNEL requestId. */
  requestId: number;
  /** Error code (see CloseCode). */
  code: number;
  /** Human-readable reason. */
  reason?: string;
}

// ─── Keepalive (binary payloads) ─────────────────────────────────────────────

/**
 * PING (0x10) payload — decoded from 4-byte big-endian binary.
 * Wire: 4 bytes (milliseconds since connection established, wraps at ~49 days).
 */
export interface PingPayload {
  /** Milliseconds since connection established (sender's clock). */
  timestamp: number;
}

/**
 * PONG (0x11) payload — decoded from 8-byte big-endian binary.
 * Wire: 4 bytes echo timestamp + 4 bytes receiver timestamp.
 */
export interface PongPayload {
  /** Echoed from PING — allows RTT calculation: local_now − echoTimestamp. */
  echoTimestamp: number;
  /** Receiver's timestamp at the time of PONG. */
  receiverTimestamp: number;
}

// ─── Connection Control ───────────────────────────────────────────────────────

/** CLOSE (0x20) — Bidirectional. Graceful connection close. */
export interface CloseMessage {
  /** Close code (see CloseCode constants). */
  code: number;
  /** Human-readable reason. */
  reason?: string;
}

/** ERROR (0xF0) — Bidirectional. Non-fatal error notification. */
export interface ErrorMessage {
  /** Error code (see CloseCode constants). */
  code: number;
  /** Channel the error relates to. Omit for connection-level errors. */
  channel?: number;
  /** Human-readable description. */
  reason?: string;
}
