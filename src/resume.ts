/**
 * session-resume/1 (extensions/session-resume.md): frame counting and retention, and the
 * server-side store of resumable sessions.
 *
 * The protocol logic that needs the session's channels (detaching, RESUME / RESUMED,
 * retransmission) lives in the session; this module holds what does not.
 *
 * @module resume
 */

import {
  CloseCode,
  DEFAULT_RESUME_TIMEOUT,
  MAX_RESUME_TIMEOUT,
  PROTOCOL_VERSION,
} from './codec/constants.js';
import { decodeResume } from './control/codec.js';
import type { ResumeMessage, Version } from './control/messages.js';
import type { BasePath } from './base-path.js';
import type { AcceptResult, FirstFrameHandler, XumuxServer } from './session.js';
import { versionsCompatible } from './version.js';
import { XumuxProtocolError } from './errors.js';

/** RFC 1982 serial-number difference of two unsigned 32-bit counts, as a signed 32-bit value. */
export const serialDiff = (a: number, b: number): number => (a - b) | 0;

/** One retained frame: its encoded bytes and its position in the session's send order. */
export interface RetainedFrame {
  order: number;
  bytes: Uint8Array;
}

/**
 * Counts for one replayed channel instance (or channel 0), in both directions, and the copies
 * of sent frames the peer has not acknowledged (session-resume/1 §4, §5).
 */
export class ReplayCounter {
  /** Replayed frames sent, mod 2^32. */
  sent = 0;
  /** Replayed frames received, mod 2^32. */
  recv = 0;
  /** How many of our frames the peer has acknowledged. `retained` holds acked..sent-1. */
  acked = 0;
  /** The `recv` count last sent in a RECEIPT. */
  receipted = 0;
  /** Payload bytes received since the last RECEIPT (for the channel-0 threshold). */
  unreceiptedBytes = 0;
  retained: RetainedFrame[] = [];
  /** Bytes of retained frames, headers included. */
  retainedBytes = 0;

  /** Record a sent frame and keep its copy. */
  retain(order: number, bytes: Uint8Array): void {
    this.retained.push({ order, bytes });
    this.retainedBytes += bytes.length;
    this.sent = (this.sent + 1) >>> 0;
  }

  /** The peer has received `count` frames: drop their copies. `count` must be within acked..sent. */
  ack(count: number): void {
    const n = serialDiff(count, this.acked);
    if (n <= 0) return;
    for (const f of this.retained.splice(0, n)) this.retainedBytes -= f.bytes.length;
    this.acked = count >>> 0;
  }

  /** Drop every copy: the peer will never need them. */
  discard(): void {
    this.retained = [];
    this.retainedBytes = 0;
    this.acked = this.sent;
  }

  /** Unacknowledged frames. */
  get unacked(): number {
    return this.retained.length;
  }
}

// ─── Tokens ─────────────────────────────────────────────────────────────────

const B64 = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';

function base64url(bytes: Uint8Array): string {
  let out = '';
  for (let i = 0; i < bytes.length; i += 3) {
    const n = (bytes[i]! << 16) | ((bytes[i + 1] ?? 0) << 8) | (bytes[i + 2] ?? 0);
    const chars = Math.min(4, Math.ceil(((bytes.length - i) * 8) / 6));
    for (let k = 0; k < chars; k++) out += B64[(n >> (18 - 6 * k)) & 63];
  }
  return out;
}

/** Characters of a token used to look it up (not secret). */
const ID_CHARS = 12;
/** Token length: a 9-byte lookup id and a 24-byte (192-bit) secret, base64url. */
const TOKEN_CHARS = ID_CHARS + 32;

/** A new token from the CSPRNG: a lookup id followed by a 192-bit secret (session-resume/1 §8). */
export function newResumeToken(): string {
  const b = new Uint8Array(33);
  globalThis.crypto.getRandomValues(b);
  return base64url(b);
}

/** Compare two strings in time that depends only on their lengths. */
export function constantTimeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let d = 0;
  for (let i = 0; i < a.length; i++) d |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return d === 0;
}

// ─── Server store ───────────────────────────────────────────────────────────

/** Options of a {@link SessionResumeStore}. */
export interface SessionResumeOptions {
  /**
   * Seconds a server keeps a session after losing its transport (RESUME_TOKEN
   * `resumeTimeout`), 1–3600. Default 120.
   */
  resumeTimeout?: number;
  /**
   * Most detached sessions kept at once. Past it, the session detached longest ago is
   * discarded, and resuming it fails with 4100. Default 1000.
   */
  maxDetached?: number;
}

/** Refusal of a RESUME, as from {@link AcceptOptions.authorize}. */
export type ResumeRefusal = { code: number; reason?: string };

/** What the store needs from a resumable server session. */
export interface ResumableSession {
  /** The session authenticated its client in HELLO (AcceptOptions.authorize was set). */
  readonly _authenticated: boolean;
  /** Take the session over onto `path`: check counts, answer RESUMED, retransmit. */
  _takeOver(path: BasePath, msg: ResumeMessage): AcceptResult;
  /** End the session locally, without sending CLOSE. */
  _discard(code: number, reason: string): void;
}

interface Live {
  token: string;
  session: ResumableSession;
}

/**
 * The resumable sessions of one server (session-resume/1). Pass the same store to every
 * `accept()` that should be able to resume each other's sessions:
 *
 * ```ts
 * const resume = new SessionResumeStore({ resumeTimeout: 60 });
 * const r = await server.accept({ sessionResume: resume });
 * if (r.resumed) return; // the transport now carries an earlier session, r.resumed
 * ```
 *
 * It keeps the bearer tokens it issues (never logged, compared in constant time), detached
 * sessions until `resumeTimeout`, and for as long again a record of expired tokens, so that
 * resuming one fails with 4101 rather than 4100.
 */
export class SessionResumeStore {
  readonly resumeTimeout: number;
  readonly maxDetached: number;
  private readonly live = new Map<string, Live>();
  private readonly expired = new Map<string, { token: string; until: number }>();
  private readonly detachedSet = new Set<ResumableSession>();

  constructor(options: SessionResumeOptions = {}) {
    const t = options.resumeTimeout ?? DEFAULT_RESUME_TIMEOUT;
    if (typeof t !== 'number' || !(t >= 1 && t <= MAX_RESUME_TIMEOUT))
      throw new RangeError(`resumeTimeout must be from 1 to ${MAX_RESUME_TIMEOUT} seconds`);
    const m = options.maxDetached ?? 1000;
    if (!Number.isSafeInteger(m) || m < 1)
      throw new RangeError('maxDetached must be a positive integer');
    this.resumeTimeout = t;
    this.maxDetached = m;
  }

  /** Detached sessions held now. */
  get detachedSessions(): number {
    return this.detachedSet.size;
  }

  /** @internal Issue a new token for `session`. */
  issue(session: ResumableSession): string {
    let token = newResumeToken();
    while (this.live.has(token.slice(0, ID_CHARS))) token = newResumeToken();
    this.live.set(token.slice(0, ID_CHARS), { token, session });
    return token;
  }

  /** @internal Stop accepting `token`. */
  revoke(token: string): void {
    const id = token.slice(0, ID_CHARS);
    const e = this.live.get(id);
    if (e && e.token === token) this.live.delete(id);
  }

  /** @internal The session a presented token resumes, 'expired', or undefined. */
  find(token: string): ResumableSession | 'expired' | undefined {
    if (token.length !== TOKEN_CHARS) return undefined;
    const id = token.slice(0, ID_CHARS);
    const e = this.live.get(id);
    if (e && constantTimeEqual(e.token, token)) return e.session;
    const x = this.expired.get(id);
    if (x && x.until <= Date.now()) this.expired.delete(id);
    else if (x && constantTimeEqual(x.token, token)) return 'expired';
    return undefined;
  }

  /** @internal `session` lost its transport. Discards the oldest detached sessions past the cap. */
  detached(session: ResumableSession): void {
    this.detachedSet.delete(session);
    this.detachedSet.add(session);
    for (const s of this.detachedSet) {
      if (this.detachedSet.size <= this.maxDetached) break;
      this.detachedSet.delete(s);
      s._discard(CloseCode.RESUME_NOT_FOUND, 'too many detached sessions');
    }
  }

  /** @internal `session` has a transport again. */
  attached(session: ResumableSession): void {
    this.detachedSet.delete(session);
  }

  /**
   * @internal `session` has ended. With `expired`, its tokens are remembered for another
   * `resumeTimeout`, so resuming fails with 4101 instead of 4100.
   */
  forget(session: ResumableSession, tokens: readonly string[], expired: boolean): void {
    this.detachedSet.delete(session);
    const until = Date.now() + this.resumeTimeout * 1000;
    for (const token of tokens) {
      this.revoke(token);
      if (!expired) continue;
      this.expired.set(token.slice(0, ID_CHARS), { token, until });
      // Bounded: drop the oldest records first (a Map iterates in insertion order).
      for (const k of this.expired.keys()) {
        if (this.expired.size <= this.maxDetached * 4) break;
        this.expired.delete(k);
      }
    }
  }

  /**
   * @internal The first-frame handler for RESUME (core §3.1). Checks, in order, the version
   * (4006), the token (4100, 4101 when it expired) and authentication (4000), then hands the
   * transport to the session, which checks the receive counts (4103, 4102).
   */
  resumeHandler(
    authorize:
      | ((
          resume: ResumeMessage,
          session: XumuxServer,
        ) => void | ResumeRefusal | Promise<void | ResumeRefusal>)
      | undefined,
  ): FirstFrameHandler {
    return async (frame) => {
      const msg = decodeResume(frame.payload);
      if (!versionsCompatible(msg.version, PROTOCOL_VERSION as Version))
        return {
          code: CloseCode.VERSION_MISMATCH,
          reason: `unsupported version ${msg.version.join('.')}`,
        };
      const found = this.find(msg.token);
      if (found === 'expired')
        return { code: CloseCode.RESUME_EXPIRED, reason: 'the session has expired' };
      if (!found) return { code: CloseCode.RESUME_NOT_FOUND, reason: 'unknown session' };
      let verdict: void | ResumeRefusal;
      try {
        verdict = authorize
          ? await authorize(msg, found as unknown as XumuxServer)
          : found._authenticated
            ? { code: CloseCode.AUTH_FAILED, reason: 'RESUME cannot be authenticated' }
            : undefined;
      } catch (e) {
        verdict = {
          code: CloseCode.AUTH_FAILED,
          reason: e instanceof Error ? e.message : String(e),
        };
      }
      if (verdict) return { code: verdict.code, reason: verdict.reason ?? 'refused' };
      // Synchronous from here: the session may have changed while authorize() ran.
      return {
        handOff: (path) => {
          if (this.find(msg.token) !== found)
            throw new XumuxProtocolError(CloseCode.RESUME_NOT_FOUND, 'unknown session');
          return found._takeOver(path, msg);
        },
      };
    };
  }
}
