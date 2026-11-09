/**
 * Control-channel message payloads (core §3–§9).
 * @module control/messages
 */

export type Version = [number, number, number];

/**
 * `extensionParams` of HELLO or WELCOME (core §3.8): parameters keyed by extension name
 * (`name/major`). Each value is defined by its extension's document.
 */
export type ExtensionParams = Record<string, unknown>;

/** Channel request (HELLO `channels[]` and OPEN_CHANNEL body). */
export interface ChannelRequest {
  id: number;
  name: string;
  reliable: boolean;
  ordered: boolean;
  maxRetransmits?: number;
  maxPacketLifeTime?: number;
  metadata?: Record<string, unknown>;
}

/** Effective channel properties (WELCOME `channels[]` and CHANNEL_ACK body). */
export interface ChannelAccept {
  id: number;
  reliable: boolean;
  ordered: boolean;
  maxRetransmits?: number;
  maxPacketLifeTime?: number;
}

/** Channel refusal (WELCOME `rejected[]` and CHANNEL_REJECT body). */
export interface ChannelRefusal {
  id: number;
  code: number;
  reason?: string;
}

/** Limits and keepalive parameters declared in HELLO / WELCOME. */
export interface Limits {
  maxFramePayload?: number;
  maxMessageSize?: number;
  initialWindow?: number;
  pingInterval?: number;
  pingTimeout?: number;
}

/** HELLO (0x01), client → server. */
export interface HelloMessage extends Limits {
  version: Version;
  application?: string;
  extensions?: string[];
  extensionParams?: ExtensionParams;
  channels?: ChannelRequest[];
  auth?: Record<string, unknown>;
}

/** WELCOME (0x02), server → client. */
export interface WelcomeMessage extends Limits {
  version: Version;
  extensions?: string[];
  extensionParams?: ExtensionParams;
  channels?: ChannelAccept[];
  rejected?: ChannelRefusal[];
}

/** CLOSE_CHANNEL (0x05). */
export interface CloseChannelMessage {
  id: number;
  code?: number;
  reason?: string;
}

/** One WINDOW_UPDATE (0x07) entry. */
export interface WindowUpdateEntry {
  channel: number;
  increment: number;
}

/** CLOSE (0x20). */
export interface CloseMessage {
  code: number;
  reason?: string;
}

/** ERROR (0xF0). */
export interface ErrorMessage {
  code: number;
  channel?: number;
  reason?: string;
}

// ─── session-resume/1 ────────────────────────────────────────────────────────

/** One `received` entry of RESUME or RESUMED: `[channel, count]` (session-resume/1 §4). */
export type ReceiveCount = [channel: number, count: number];

/** RESUME_TOKEN (0x30), server → client (session-resume/1 §3.1). */
export interface ResumeTokenMessage {
  token: string;
  /** Seconds the server keeps the session after losing the transport, 1–3600. */
  resumeTimeout: number;
}

/** RESUME (0x31), client → server, the first frame on a new transport (session-resume/1 §3.2). */
export interface ResumeMessage {
  version: Version;
  token: string;
  received: ReceiveCount[];
  auth?: Record<string, unknown>;
}

/** RESUMED (0x32), server → client, the answer to RESUME (session-resume/1 §3.3). */
export interface ResumedMessage {
  token: string;
  resumeTimeout: number;
  received: ReceiveCount[];
}

/** One RECEIPT (0x33) entry: `count` replayed frames received on `channel` (session-resume/1 §3.4). */
export interface ReceiptEntry {
  channel: number;
  count: number;
}
