/**
 * Control channel message codec.
 *
 * Encodes typed control message objects into wire Frames and decodes them back.
 *
 * JSON messages use UTF-8 TextEncoder/TextDecoder.
 * PING/PONG use fixed-size binary payloads (4 and 8 bytes respectively).
 *
 * @module control/codec
 */

import { encodeFrame } from '../codec/encode-frame.js';
import { ControlType, CONTROL_CHANNEL } from '../codec/constants.js';
import type { Frame } from '../codec/frame.js';
import type {
  HelloMessage,
  WelcomeMessage,
  OpenChannelMessage,
  ChannelAckMessage,
  CloseChannelMessage,
  ChannelRejectMessage,
  CloseMessage,
  ErrorMessage,
  PingPayload,
  PongPayload,
} from './messages.js';

// ─── Helpers ─────────────────────────────────────────────────────────────────

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function encodeJson(obj: unknown): Uint8Array {
  return encoder.encode(JSON.stringify(obj));
}

function decodeJson(data: Uint8Array): unknown {
  return JSON.parse(decoder.decode(data)) as unknown;
}

function makeFrame(type: number, payload: Uint8Array): Uint8Array {
  return encodeFrame({ channel: CONTROL_CHANNEL, type, flags: 0, payload });
}

// ─── PING / PONG binary codec ─────────────────────────────────────────────────

/**
 * Encode a PING timestamp into a 4-byte big-endian binary payload.
 * Returns the complete encoded frame bytes.
 */
export function encodePing(timestamp: number): Uint8Array {
  const payload = new Uint8Array(4);
  const ts = timestamp >>> 0; // clamp to uint32
  payload[0] = (ts >>> 24) & 0xff;
  payload[1] = (ts >>> 16) & 0xff;
  payload[2] = (ts >>> 8) & 0xff;
  payload[3] = ts & 0xff;
  return makeFrame(ControlType.PING, payload);
}

/**
 * Decode a PING payload (4 bytes) into a PingPayload.
 * @param payload - The frame's payload bytes.
 */
export function decodePing(payload: Uint8Array): PingPayload {
  if (payload.length < 4) {
    throw new RangeError(`PING payload too short: expected 4 bytes, got ${payload.length}`);
  }
  const timestamp =
    (((payload[0] as number) << 24) |
      ((payload[1] as number) << 16) |
      ((payload[2] as number) << 8) |
      (payload[3] as number)) >>>
    0;
  return { timestamp };
}

/**
 * Encode PONG (echo timestamp + receiver timestamp) into an 8-byte binary payload.
 * Returns the complete encoded frame bytes.
 */
export function encodePong(echoTimestamp: number, receiverTimestamp: number): Uint8Array {
  const payload = new Uint8Array(8);
  const echo = echoTimestamp >>> 0;
  const recv = receiverTimestamp >>> 0;
  payload[0] = (echo >>> 24) & 0xff;
  payload[1] = (echo >>> 16) & 0xff;
  payload[2] = (echo >>> 8) & 0xff;
  payload[3] = echo & 0xff;
  payload[4] = (recv >>> 24) & 0xff;
  payload[5] = (recv >>> 16) & 0xff;
  payload[6] = (recv >>> 8) & 0xff;
  payload[7] = recv & 0xff;
  return makeFrame(ControlType.PONG, payload);
}

/**
 * Decode a PONG payload (8 bytes) into a PongPayload.
 * @param payload - The frame's payload bytes.
 */
export function decodePong(payload: Uint8Array): PongPayload {
  if (payload.length < 8) {
    throw new RangeError(`PONG payload too short: expected 8 bytes, got ${payload.length}`);
  }
  const echoTimestamp =
    (((payload[0] as number) << 24) |
      ((payload[1] as number) << 16) |
      ((payload[2] as number) << 8) |
      (payload[3] as number)) >>>
    0;
  const receiverTimestamp =
    (((payload[4] as number) << 24) |
      ((payload[5] as number) << 16) |
      ((payload[6] as number) << 8) |
      (payload[7] as number)) >>>
    0;
  return { echoTimestamp, receiverTimestamp };
}

// ─── JSON message frame encoders ─────────────────────────────────────────────

/** Encode a HELLO message into frame bytes. */
export function encodeHello(msg: HelloMessage): Uint8Array {
  return makeFrame(ControlType.HELLO, encodeJson(msg));
}

/** Encode a WELCOME message into frame bytes. */
export function encodeWelcome(msg: WelcomeMessage): Uint8Array {
  return makeFrame(ControlType.WELCOME, encodeJson(msg));
}

/** Encode an OPEN_CHANNEL message into frame bytes. */
export function encodeOpenChannel(msg: OpenChannelMessage): Uint8Array {
  return makeFrame(ControlType.OPEN_CHANNEL, encodeJson(msg));
}

/** Encode a CHANNEL_ACK message into frame bytes. */
export function encodeChannelAck(msg: ChannelAckMessage): Uint8Array {
  return makeFrame(ControlType.CHANNEL_ACK, encodeJson(msg));
}

/** Encode a CLOSE_CHANNEL message into frame bytes. */
export function encodeCloseChannel(msg: CloseChannelMessage): Uint8Array {
  return makeFrame(ControlType.CLOSE_CHANNEL, encodeJson(msg));
}

/** Encode a CHANNEL_REJECT message into frame bytes. */
export function encodeChannelReject(msg: ChannelRejectMessage): Uint8Array {
  return makeFrame(ControlType.CHANNEL_REJECT, encodeJson(msg));
}

/** Encode a CLOSE message into frame bytes. */
export function encodeClose(msg: CloseMessage): Uint8Array {
  return makeFrame(ControlType.CLOSE, encodeJson(msg));
}

/** Encode an ERROR message into frame bytes. */
export function encodeError(msg: ErrorMessage): Uint8Array {
  return makeFrame(ControlType.ERROR, encodeJson(msg));
}

// ─── JSON message decoders ────────────────────────────────────────────────────

/** Decode a HELLO frame payload into a HelloMessage. */
export function decodeHello(frame: Frame): HelloMessage {
  return decodeJson(frame.payload) as HelloMessage;
}

/** Decode a WELCOME frame payload into a WelcomeMessage. */
export function decodeWelcome(frame: Frame): WelcomeMessage {
  return decodeJson(frame.payload) as WelcomeMessage;
}

/** Decode an OPEN_CHANNEL frame payload into an OpenChannelMessage. */
export function decodeOpenChannel(frame: Frame): OpenChannelMessage {
  return decodeJson(frame.payload) as OpenChannelMessage;
}

/** Decode a CHANNEL_ACK frame payload into a ChannelAckMessage. */
export function decodeChannelAck(frame: Frame): ChannelAckMessage {
  return decodeJson(frame.payload) as ChannelAckMessage;
}

/** Decode a CLOSE_CHANNEL frame payload into a CloseChannelMessage. */
export function decodeCloseChannel(frame: Frame): CloseChannelMessage {
  return decodeJson(frame.payload) as CloseChannelMessage;
}

/** Decode a CHANNEL_REJECT frame payload into a ChannelRejectMessage. */
export function decodeChannelReject(frame: Frame): ChannelRejectMessage {
  return decodeJson(frame.payload) as ChannelRejectMessage;
}

/** Decode a CLOSE frame payload into a CloseMessage. */
export function decodeClose(frame: Frame): CloseMessage {
  return decodeJson(frame.payload) as CloseMessage;
}

/** Decode an ERROR frame payload into an ErrorMessage. */
export function decodeError(frame: Frame): ErrorMessage {
  return decodeJson(frame.payload) as ErrorMessage;
}
