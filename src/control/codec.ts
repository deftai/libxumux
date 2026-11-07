/**
 * Encoders and validating decoders for control and probe frames.
 *
 * Decoders throw {@link XumuxProtocolError} 4001 for malformed payloads (core §4.1).
 * Unknown JSON fields are ignored.
 *
 * @module control/codec
 */

import {
  CloseCode,
  ControlType,
  CONTROL_CHANNEL,
  FrameFlags,
  ProbeType,
} from '../codec/constants.js';
import type { Frame } from '../codec/frame.js';
import { XumuxProtocolError } from '../errors.js';
import type {
  Limits,
  ChannelAccept,
  ChannelRefusal,
  ChannelRequest,
  CloseChannelMessage,
  CloseMessage,
  ErrorMessage,
  HelloMessage,
  Version,
  WelcomeMessage,
  WindowUpdateEntry,
} from './messages.js';

const te = new TextEncoder();
const td = new TextDecoder('utf-8', { fatal: true });

// ─── Encoding ────────────────────────────────────────────────────────────────

/** Build a control frame with a JSON payload. */
export function controlFrame(type: number, body: unknown): Frame {
  return { channel: CONTROL_CHANNEL, type, flags: 0, payload: te.encode(JSON.stringify(body)) };
}

export const helloFrame = (m: HelloMessage): Frame => controlFrame(ControlType.HELLO, m);
export const welcomeFrame = (m: WelcomeMessage): Frame => controlFrame(ControlType.WELCOME, m);
export const openChannelFrame = (m: ChannelRequest): Frame =>
  controlFrame(ControlType.OPEN_CHANNEL, m);
export const channelAckFrame = (m: ChannelAccept): Frame =>
  controlFrame(ControlType.CHANNEL_ACK, m);
export const channelRejectFrame = (m: ChannelRefusal): Frame =>
  controlFrame(ControlType.CHANNEL_REJECT, m);
export const closeChannelFrame = (m: CloseChannelMessage): Frame =>
  controlFrame(ControlType.CLOSE_CHANNEL, m);
export const closeFrame = (m: CloseMessage): Frame => controlFrame(ControlType.CLOSE, m);
export const errorFrame = (m: ErrorMessage): Frame => controlFrame(ControlType.ERROR, m);

/** WINDOW_UPDATE: one or more [channel:2][increment:4] entries. */
export function windowUpdateFrame(entries: WindowUpdateEntry[]): Frame {
  const payload = new Uint8Array(entries.length * 6);
  const dv = new DataView(payload.buffer);
  entries.forEach((e, i) => {
    dv.setUint16(i * 6, e.channel);
    dv.setUint32(i * 6 + 2, e.increment);
  });
  return { channel: CONTROL_CHANNEL, type: ControlType.WINDOW_UPDATE, flags: 0, payload };
}

/** PING or PONG on `channel` with a 4-byte token. */
export function probeFrame(channel: number, type: number, token: number): Frame {
  const payload = new Uint8Array(4);
  new DataView(payload.buffer).setUint32(0, token >>> 0);
  return { channel, type, flags: FrameFlags.PROBE, payload };
}
export const pingFrame = (channel: number, token: number): Frame =>
  probeFrame(channel, ProbeType.PING, token);
export const pongFrame = (channel: number, token: number): Frame =>
  probeFrame(channel, ProbeType.PONG, token);

// ─── Decoding helpers ────────────────────────────────────────────────────────

function invalid(msg: string): never {
  throw new XumuxProtocolError(CloseCode.INVALID_MESSAGE, msg);
}

type Obj = Record<string, unknown>;

function parseObject(payload: Uint8Array, what: string): Obj {
  let v: unknown;
  try {
    v = JSON.parse(td.decode(payload));
  } catch {
    return invalid(`${what}: payload is not valid UTF-8 JSON`);
  }
  if (typeof v !== 'object' || v === null || Array.isArray(v))
    invalid(`${what}: payload is not a JSON object`);
  return v as Obj;
}

const isInt = (v: unknown): v is number => typeof v === 'number' && Number.isInteger(v);

function int(o: Obj, k: string, what: string, min = 0, max = 0xffffffff): number {
  const v = o[k];
  if (!isInt(v) || v < min || v > max)
    invalid(`${what}: '${k}' must be an integer in [${min}, ${max}]`);
  return v;
}
function optInt(o: Obj, k: string, what: string, min = 0, max = 0xffffffff): number | undefined {
  return o[k] === undefined ? undefined : int(o, k, what, min, max);
}
function num(o: Obj, k: string, what: string): number | undefined {
  const v = o[k];
  if (v === undefined) return undefined;
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0)
    invalid(`${what}: '${k}' must be a non-negative number`);
  return v;
}
function str(o: Obj, k: string, what: string): string {
  const v = o[k];
  if (typeof v !== 'string') invalid(`${what}: '${k}' must be a string`);
  return v;
}
function optStr(o: Obj, k: string, what: string): string | undefined {
  return o[k] === undefined ? undefined : str(o, k, what);
}
function bool(o: Obj, k: string, what: string): boolean {
  const v = o[k];
  if (typeof v !== 'boolean') invalid(`${what}: '${k}' must be a boolean`);
  return v;
}
function optObj(o: Obj, k: string, what: string): Obj | undefined {
  const v = o[k];
  if (v === undefined) return undefined;
  if (typeof v !== 'object' || v === null || Array.isArray(v))
    invalid(`${what}: '${k}' must be an object`);
  return v as Obj;
}
function optArray(o: Obj, k: string, what: string): unknown[] {
  const v = o[k];
  if (v === undefined) return [];
  if (!Array.isArray(v)) invalid(`${what}: '${k}' must be an array`);
  return v;
}
function asObj(v: unknown, what: string): Obj {
  if (typeof v !== 'object' || v === null || Array.isArray(v))
    invalid(`${what}: entry must be an object`);
  return v as Obj;
}
function version(o: Obj, what: string): Version {
  const v = o.version;
  if (!Array.isArray(v) || v.length !== 3 || !v.every((x) => isInt(x) && x >= 0)) {
    invalid(`${what}: 'version' must be [major, minor, patch]`);
  }
  return v as Version;
}
function strArray(o: Obj, k: string, what: string): string[] {
  const a = optArray(o, k, what);
  if (a.every((x): x is string => typeof x === 'string')) return a;
  return invalid(`${what}: '${k}' must be an array of strings`);
}
function limits(o: Obj, what: string): Limits {
  return {
    maxFramePayload: optInt(o, 'maxFramePayload', what),
    maxMessageSize: optInt(o, 'maxMessageSize', what),
    initialWindow: optInt(o, 'initialWindow', what),
    pingInterval: num(o, 'pingInterval', what),
    pingTimeout: num(o, 'pingTimeout', what),
  };
}
const ID_MAX = 0xffff;

function channelRequest(v: unknown, what: string): ChannelRequest {
  const o = asObj(v, what);
  return {
    id: int(o, 'id', what, 0, ID_MAX),
    name: str(o, 'name', what),
    reliable: bool(o, 'reliable', what),
    ordered: bool(o, 'ordered', what),
    maxRetransmits: optInt(o, 'maxRetransmits', what),
    maxPacketLifeTime: optInt(o, 'maxPacketLifeTime', what),
    metadata: optObj(o, 'metadata', what),
  };
}
function channelAccept(v: unknown, what: string): ChannelAccept {
  const o = asObj(v, what);
  return {
    id: int(o, 'id', what, 0, ID_MAX),
    reliable: bool(o, 'reliable', what),
    ordered: bool(o, 'ordered', what),
    maxRetransmits: optInt(o, 'maxRetransmits', what),
    maxPacketLifeTime: optInt(o, 'maxPacketLifeTime', what),
  };
}
function channelRefusal(v: unknown, what: string): ChannelRefusal {
  const o = asObj(v, what);
  return {
    id: int(o, 'id', what, 0, ID_MAX),
    code: int(o, 'code', what),
    reason: optStr(o, 'reason', what),
  };
}

// ─── Decoders ────────────────────────────────────────────────────────────────

export function decodeHello(p: Uint8Array): HelloMessage {
  const w = 'HELLO';
  const o = parseObject(p, w);
  return {
    version: version(o, w),
    application: optStr(o, 'application', w),
    extensions: strArray(o, 'extensions', w),
    ...limits(o, w),
    channels: optArray(o, 'channels', w).map((c) => channelRequest(c, w)),
    auth: optObj(o, 'auth', w),
  };
}

export function decodeWelcome(p: Uint8Array): WelcomeMessage {
  const w = 'WELCOME';
  const o = parseObject(p, w);
  return {
    version: version(o, w),
    extensions: strArray(o, 'extensions', w),
    ...limits(o, w),
    channels: optArray(o, 'channels', w).map((c) => channelAccept(c, w)),
    rejected: optArray(o, 'rejected', w).map((c) => channelRefusal(c, w)),
  };
}

export const decodeOpenChannel = (p: Uint8Array): ChannelRequest =>
  channelRequest(parseObject(p, 'OPEN_CHANNEL'), 'OPEN_CHANNEL');
export const decodeChannelAck = (p: Uint8Array): ChannelAccept =>
  channelAccept(parseObject(p, 'CHANNEL_ACK'), 'CHANNEL_ACK');
export const decodeChannelReject = (p: Uint8Array): ChannelRefusal =>
  channelRefusal(parseObject(p, 'CHANNEL_REJECT'), 'CHANNEL_REJECT');

export function decodeCloseChannel(p: Uint8Array): CloseChannelMessage {
  const w = 'CLOSE_CHANNEL';
  const o = parseObject(p, w);
  return {
    id: int(o, 'id', w, 0, ID_MAX),
    code: optInt(o, 'code', w),
    reason: optStr(o, 'reason', w),
  };
}

export function decodeClose(p: Uint8Array): CloseMessage {
  const w = 'CLOSE';
  const o = parseObject(p, w);
  return { code: int(o, 'code', w), reason: optStr(o, 'reason', w) };
}

export function decodeError(p: Uint8Array): ErrorMessage {
  const w = 'ERROR';
  const o = parseObject(p, w);
  return {
    code: int(o, 'code', w),
    channel: optInt(o, 'channel', w, 0, ID_MAX),
    reason: optStr(o, 'reason', w),
  };
}

export function decodeWindowUpdate(p: Uint8Array): WindowUpdateEntry[] {
  if (p.length === 0 || p.length % 6 !== 0)
    invalid(`WINDOW_UPDATE: payload length ${p.length} is not a positive multiple of 6`);
  const dv = new DataView(p.buffer, p.byteOffset, p.byteLength);
  const out: WindowUpdateEntry[] = [];
  for (let i = 0; i < p.length; i += 6) {
    const increment = dv.getUint32(i + 2);
    if (increment < 1 || increment > 0x7fffffff)
      invalid(`WINDOW_UPDATE: increment ${increment} out of range`);
    out.push({ channel: dv.getUint16(i), increment });
  }
  return out;
}

/** Decode a probe token, or return null if the payload is not exactly 4 bytes (core §8.1: discard). */
export function decodeProbeToken(p: Uint8Array): number | null {
  if (p.length !== 4) return null;
  return new DataView(p.buffer, p.byteOffset, 4).getUint32(0);
}
