/**
 * xumux protocol constants — frame flags, control message types, error/close codes.
 * @module codec/constants
 */

/** Frame header flag bits (byte 2 of the frame header). */
export const FrameFlags = {
  /** When set, the payload length field is 4 bytes instead of 2 (8-byte header total). */
  EXTENDED_LENGTH: 0x01,
  /** This frame is a fragment of a larger message. */
  FRAGMENT: 0x02,
  /** This is the last fragment of a fragmented message. */
  FRAGMENT_END: 0x04,
} as const;

/** Control channel (0x00) message type bytes. */
export const ControlType = {
  HELLO: 0x01,
  WELCOME: 0x02,
  OPEN_CHANNEL: 0x03,
  CHANNEL_ACK: 0x04,
  CLOSE_CHANNEL: 0x05,
  CHANNEL_REJECT: 0x06,
  PING: 0x10,
  PONG: 0x11,
  CLOSE: 0x20,
  ERROR: 0xf0,
} as const;

/** xumux close / error codes (aligned with WebSocket close codes where noted). */
export const CloseCode = {
  NORMAL: 1000,
  GOING_AWAY: 1001,
  PROTOCOL_ERROR: 1002,
  UNSUPPORTED: 1003,
  AUTH_FAILED: 4000,
  INVALID_MESSAGE: 4001,
  CHANNEL_FULL: 4002,
  CHANNEL_NOT_FOUND: 4003,
  RATE_LIMITED: 4004,
  MESSAGE_TOO_LARGE: 4005,
  VERSION_MISMATCH: 4006,
  HELLO_TIMEOUT: 4007,
} as const;

/** The implicit control channel ID. Never opened via OPEN_CHANNEL. */
export const CONTROL_CHANNEL = 0x00;

/** Maximum assignable application channel ID. Channel 0xFF is reserved. */
export const MAX_CHANNEL_ID = 0xfe;

/** Byte 3 of every frame header — MUST be 0x00. */
export const HEADER_RESERVED_BYTE = 0x00;

/** Standard frame header size in bytes. */
export const STANDARD_HEADER_SIZE = 6;

/** Extended frame header size in bytes (EXTENDED_LENGTH flag set). */
export const EXTENDED_HEADER_SIZE = 8;

/** Maximum payload size in a standard frame. */
export const MAX_STANDARD_PAYLOAD = 65535;

/** Default max message size (bytes). */
export const DEFAULT_MAX_MESSAGE_SIZE = 65535;

/** Default keepalive PING interval (seconds). */
export const DEFAULT_PING_INTERVAL = 30;

/** Default PONG timeout (seconds) — close if no PONG within this window. */
export const DEFAULT_PING_TIMEOUT = 10;

/** Default HELLO timeout (milliseconds) — server closes if no HELLO within this window. */
export const DEFAULT_HELLO_TIMEOUT_MS = 10_000;
