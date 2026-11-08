/**
 * xumux 0.2 protocol constants.
 * @see https://github.com/deftai/xumux/blob/main/spec/core.md
 * @module codec/constants
 */

/** Protocol version spoken by this library. */
export const PROTOCOL_VERSION: readonly [number, number, number] = [0, 2, 0];

/** Frame header flag bits (byte 3). Bit 0 is the least significant. */
export const FrameFlags = {
  /** More fragments of this message follow. */
  MORE: 0x01,
  /** Probe frame: xumux owns the Type field (PING/PONG). */
  PROBE: 0x02,
} as const;

/** Mask of every flag bit defined by the core. Others are reserved. */
export const KNOWN_FLAGS = FrameFlags.MORE | FrameFlags.PROBE;

/** Control-channel message types (channel 0, PROBE clear). */
export const ControlType = {
  HELLO: 0x01,
  WELCOME: 0x02,
  OPEN_CHANNEL: 0x03,
  CHANNEL_ACK: 0x04,
  CLOSE_CHANNEL: 0x05,
  CHANNEL_REJECT: 0x06,
  WINDOW_UPDATE: 0x07,
  CLOSE: 0x20,
  ERROR: 0xf0,
} as const;

/** Probe types (any channel, PROBE set). */
export const ProbeType = {
  PING: 0x10,
  PONG: 0x11,
} as const;

/** Close / error codes shared by CLOSE, CLOSE_CHANNEL, CHANNEL_REJECT and ERROR. */
export const CloseCode = {
  NORMAL: 1000,
  GOING_AWAY: 1001,
  PROTOCOL_ERROR: 1002,
  UNSUPPORTED: 1003,
  AUTH_FAILED: 4000,
  INVALID_MESSAGE: 4001,
  CHANNEL_LIMIT: 4002,
  CHANNEL_NOT_FOUND: 4003,
  RATE_LIMITED: 4004,
  MESSAGE_TOO_LARGE: 4005,
  VERSION_MISMATCH: 4006,
  HELLO_TIMEOUT: 4007,
  KEEPALIVE_TIMEOUT: 4008,
  FLOW_CONTROL_ERROR: 4009,
  CHANNEL_REFUSED: 4010,
} as const;

/** The implicit control channel. */
export const CONTROL_CHANNEL = 0x0000;

/** Reserved channel ID; never valid on the wire. */
export const RESERVED_CHANNEL = 0xffff;

/** Highest assignable channel ID. */
export const MAX_CHANNEL_ID = 0xfffe;

/** Fixed frame header size in bytes. */
export const HEADER_SIZE = 8;

/** Magic bytes "XMUX" sent first on stream bindings (TCP, Unix, stdio). */
export const XMUX_MAGIC: Readonly<Uint8Array> = new Uint8Array([0x58, 0x4d, 0x55, 0x58]);

/** Channel-0 payloads are always capped at this size, whatever maxFramePayload says. */
export const CONTROL_MAX_PAYLOAD = 65536;

/** Defaults for the receiver-declared limits (core §3.4). */
export const DEFAULT_MAX_FRAME_PAYLOAD = 65536;
export const DEFAULT_MAX_MESSAGE_SIZE = 262144;
export const DEFAULT_INITIAL_WINDOW = 262144;
/** Receive-window auto-tuning bounds: per channel, and the sum over a connection. */
export const DEFAULT_MAX_CHANNEL_WINDOW = 16 * 1024 * 1024;
export const DEFAULT_CONNECTION_WINDOW = 64 * 1024 * 1024;
/** Bound on queued control and probe frames, in bytes (see `maxControlQueue`). */
export const DEFAULT_MAX_CONTROL_QUEUE = 1024 * 1024;
/** Cap on open plus pending channels the peer may hold (see `maxChannels`). */
export const DEFAULT_MAX_CHANNELS = 1024;

/** Lower / upper bounds for declared limits. */
export const MIN_MAX_FRAME_PAYLOAD = 4096;
export const MAX_WINDOW = 0x7fffffff;

/** Keepalive defaults (seconds). */
export const DEFAULT_PING_INTERVAL = 30;
export const DEFAULT_PING_TIMEOUT = 10;

/** Handshake / close timeouts (milliseconds). */
export const DEFAULT_HELLO_TIMEOUT_MS = 10_000;
export const DEFAULT_CLOSE_TIMEOUT_MS = 5_000;

/** Fragment size used for fair interleaving on fragmentable channels. */
export const DEFAULT_FRAGMENT_SIZE = 16384;
