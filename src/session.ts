/**
 * xumux 0.3 session: XumuxPeer, XumuxClient, XumuxServer.
 *
 * One base path → one session → N channels. The base path ({@link BasePath}) owns
 * framing and magic bytes over the transport; the session owns limits, the outbound
 * scheduler (control first, then channels round-robin), flow control, fragmentation,
 * probes, keepalive and the close procedure. An extension can keep the session alive when
 * its base path is lost and attach a new one (core §11.1).
 *
 * @see https://github.com/deftai/xumux/blob/main/spec/core.md
 * @module session
 */

import {
  CloseCode,
  CONTROL_CHANNEL,
  ControlType,
  DEFAULT_CLOSE_TIMEOUT_MS,
  DEFAULT_FRAGMENT_SIZE,
  DEFAULT_HELLO_TIMEOUT_MS,
  DEFAULT_INITIAL_WINDOW,
  DEFAULT_MAX_CHANNEL_WINDOW,
  DEFAULT_CONNECTION_WINDOW,
  DEFAULT_MAX_CONTROL_QUEUE,
  DEFAULT_MAX_CHANNELS,
  DEFAULT_MAX_FRAME_PAYLOAD,
  DEFAULT_MAX_MESSAGE_SIZE,
  DEFAULT_PING_INTERVAL,
  DEFAULT_PING_TIMEOUT,
  EXTENSION_CONTROL_TYPE_MAX,
  EXTENSION_CONTROL_TYPE_MIN,
  CONTROL_MAX_PAYLOAD,
  FrameFlags,
  MAX_CHANNEL_ID,
  MAX_WINDOW,
  MIN_MAX_FRAME_PAYLOAD,
  PROTOCOL_VERSION,
  ProbeType,
} from './codec/constants.js';
import { validateFrame } from './codec/decode-frame.js';
import { encodeFrame } from './codec/encode-frame.js';
import type { Frame } from './codec/frame.js';
import {
  channelAckFrame,
  channelRejectFrame,
  closeChannelFrame,
  closeFrame,
  decodeChannelAck,
  decodeChannelReject,
  decodeClose,
  decodeCloseChannel,
  decodeError,
  decodeHello,
  decodeOpenChannel,
  decodeProbeToken,
  decodeWelcome,
  decodeWindowUpdate,
  errorFrame,
  helloFrame,
  openChannelFrame,
  pingFrame,
  pongFrame,
  welcomeFrame,
  windowUpdateFrame,
} from './control/codec.js';
import type {
  ChannelAccept,
  ChannelRefusal,
  ChannelRequest,
  ErrorMessage,
  ExtensionParams,
  HelloMessage,
  Limits,
  Version,
  WelcomeMessage,
} from './control/messages.js';
import {
  ProbeStatsRecorder,
  XumuxChannel,
  type ChannelCloseInfo,
  type ChannelHost,
  type ProbeStats,
} from './channel.js';
import { XumuxClosedError, XumuxProtocolError } from './errors.js';
import type { TransportAdapter } from './types.js';
import { ignore } from './ignore.js';
import { BasePath, type BasePathSink } from './base-path.js';

// ─── Public types ────────────────────────────────────────────────────────────

/** Decision for a peer's channel request: `true` to accept, or a refusal. */
export type ChannelDecision = true | { code: number; reason?: string };

/** Policy for channels the peer opens (OPEN_CHANNEL, or HELLO channels on a server). */
export type ChannelPolicy = (request: ChannelRequest) => ChannelDecision | Promise<ChannelDecision>;

export interface XumuxOptions {
  /** Largest frame payload this side accepts. Default 65536. */
  maxFramePayload?: number;
  /** Largest reassembled message this side accepts. Default 262144. */
  maxMessageSize?: number;
  /** Credit this side grants per channel. Must be ≥ maxMessageSize. Default 262144. */
  initialWindow?: number;
  /** Keepalive idle interval in seconds (server's value applies). 0 disables. Default 30. */
  pingInterval?: number;
  /** Keepalive timeout in seconds (server's value applies). Default 10. */
  pingTimeout?: number;
  /** Fragment size for outgoing messages on fragmentable channels. Default 16384. */
  fragmentSize?: number;
  /** Server: close if HELLO hasn't arrived. Client: give up if WELCOME hasn't. Default 10000 ms. */
  handshakeTimeoutMs?: number;
  /** How long CLOSE waits for the peer's CLOSE. Default 5000 ms. */
  closeTimeoutMs?: number;
  /** Decide on channels the peer opens. Default: reject with 1003. */
  onChannelRequest?: ChannelPolicy;
  /**
   * How far receive-window auto-tuning may grow one channel's window, in bytes. Default
   * 16 MiB. Each channel starts at `initialWindow`; when the peer uses a whole window in under
   * two round trips (so the window, not the link, limits it), the window doubles. A value at
   * or below `initialWindow`, or negative, disables auto-tuning.
   */
  maxChannelWindow?: number;
  /**
   * Limits auto-tuning across the connection, in bytes. Default 64 MiB. No window grows once
   * the sum of all channel receive windows would exceed it. Initial windows are always granted,
   * even on top of windows that already grew, so the total is at most connectionWindow + open
   * channels × initialWindow. Windows don't
   * shrink; a channel's window leaves the sum when it closes, though data it received that the
   * application hasn't read yet stays buffered.
   */
  connectionWindow?: number;
  /**
   * Cap, in bytes, on control and probe frames (CHANNEL_ACK, WINDOW_UPDATE, PONG, ERROR, ...)
   * queued for sending. Default 1 MiB. They pile up only if the peer stops reading while it keeps
   * sending frames that need a reply; past the cap the connection closes with 4004. Data frames
   * are bounded by flow control instead. Negative: no cap.
   */
  maxControlQueue?: number;
  /**
   * Cap on channels open or being opened on the connection, default 1024. Past it, the peer's
   * OPEN_CHANNEL (or HELLO channel) is refused with 4002. Each channel holds a receive window
   * (initialWindow), so this bounds what a peer can make this side buffer. Must be positive.
   */
  maxChannels?: number;
}

export interface ConnectOptions {
  application?: string;
  extensions?: string[];
  /**
   * HELLO `extensionParams` (core §3.8), keyed by extension name. Keys for extensions that
   * are not in `extensions` are not sent. Counts toward the 65536-byte HELLO limit.
   */
  extensionParams?: ExtensionParams;
  /**
   * Validate the WELCOME parameters of an extension the server accepted. Called once per
   * accepted extension, with its WELCOME `extensionParams` value (undefined if WELCOME has
   * none). Returning false, or throwing, closes the connection with 4001 and rejects
   * connect() (core §3.8). Default: every value is valid.
   */
  checkExtensionParams?: (extension: string, params: unknown) => boolean;
  auth?: Record<string, unknown>;
  /** Channels to open in HELLO. IDs are assigned by the client. */
  channels?: Array<Omit<ChannelRequest, 'id'>>;
}

export interface ConnectResult {
  welcome: WelcomeMessage;
  /** Extensions the server accepted (WELCOME `extensions`). */
  extensions: string[];
  /** WELCOME `extensionParams`, restricted to accepted extensions (other keys are ignored). */
  extensionParams: ExtensionParams;
  /** Accepted channels, in HELLO order. */
  channels: XumuxChannel[];
  rejected: ChannelRefusal[];
}

export interface AcceptOptions {
  /** Inspect HELLO (auth, application). Return a refusal to reject the connection with CLOSE. */
  authorize?: (
    hello: HelloMessage,
  ) => void | { code: number; reason?: string } | Promise<void | { code: number; reason?: string }>;
  /** Extensions this server supports. WELCOME lists the intersection with the client's request. */
  extensions?: string[];
  /**
   * Negotiate the parameters of a supported extension the client requested (core §3.8).
   * Called once per such extension, in HELLO order, with its HELLO `extensionParams` value
   * (undefined if HELLO has none). Return `true` to accept it without WELCOME parameters,
   * `{ params }` to accept it and send `params` in WELCOME `extensionParams`, or `false` to
   * leave it out of WELCOME (for example because its HELLO parameters are unacceptable).
   * Throwing counts as `false`. Default: accept, without parameters. WELCOME never carries
   * parameters for an extension it does not accept.
   */
  negotiateExtension?: (
    extension: string,
    params: unknown,
    hello: HelloMessage,
  ) => ExtensionDecision | Promise<ExtensionDecision>;
  /**
   * Extension-defined first frames (core §3.1), keyed by control type (0x30–0x7F, as
   * allocated to the extension). When the client's first frame on the transport is a control
   * frame of one of these types instead of HELLO, its handler decides the answer; the HELLO
   * timeout applies to its arrival as to HELLO's. A first frame of any other type than HELLO
   * still closes with 1002.
   *
   * In this version the handler can only refuse: the server sends CLOSE with the returned
   * code (core §3.7) and accept() rejects with {@link XumuxClosedError}. A handler that
   * throws refuses with the error's code if it is an {@link XumuxProtocolError}, else 1002.
   * Answering the frame (as session-resume/1's RESUMED does) and handing the transport to
   * an existing connection is left to the extension implementations that need it.
   */
  firstFrames?: Readonly<Record<number, FirstFrameHandler>>;
}

/** Server decision on one requested extension (see {@link AcceptOptions.negotiateExtension}). */
export type ExtensionDecision = boolean | { params: unknown };

/** An extension-defined first frame that arrived instead of HELLO (core §3.1). */
export interface FirstFrame {
  /** Control type, 0x30–0x7F. */
  type: number;
  /** The raw payload; its format is the extension's. */
  payload: Uint8Array;
}

/** Answer to an extension-defined first frame. Only refusal (CLOSE) is defined so far. */
export type FirstFrameDecision = { code: number; reason?: string };

/** Handles an extension-defined first frame (see {@link AcceptOptions.firstFrames}). */
export type FirstFrameHandler = (
  frame: FirstFrame,
) => FirstFrameDecision | Promise<FirstFrameDecision>;

export interface AcceptResult {
  hello: HelloMessage;
  /** Extensions this server accepted (WELCOME `extensions`). */
  extensions: string[];
  /** HELLO `extensionParams`, restricted to accepted extensions (other keys are ignored). */
  extensionParams: ExtensionParams;
  /** HELLO channels this server accepted. */
  channels: XumuxChannel[];
}

/** Detail of the `'close'` event. */
export type CloseInfo = ChannelCloseInfo;

/** Detail of the `'detach'` event: the base path was lost and the session kept (core §11.1). */
export interface DetachInfo {
  reason: string;
}

/** Detail of the `'channel-close'` event. */
export interface ChannelCloseEventDetail extends ChannelCloseInfo {
  channel: XumuxChannel;
}

type SessionState = 'idle' | 'handshaking' | 'open' | 'closing' | 'closed';

/** What the handshake waits for: HELLO (server), WELCOME (client), or an extension first frame. */
type HandshakeMessage = HelloMessage | WelcomeMessage | { firstFrame: FirstFrame };

interface Out {
  bytes: Uint8Array;
  resolve: () => void;
  reject: (e: unknown) => void;
}

interface PendingPing {
  resolve: (rtt: number) => void;
  reject: (e: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

/** A send waiting for credit. `partial`: take what is free (at least 1 byte), up to n. */
interface CreditWaiter {
  n: number;
  partial: boolean;
  resolve: (got: number) => void;
  reject: (e: Error) => void;
}

/** Per-channel session state that the channel object doesn't need to see. */
interface ChannelSlot {
  ch: XumuxChannel;
  sentClose: boolean;
  recvClose: boolean;
  closeInfo?: ChannelCloseInfo;
  sendCredit: number;
  creditWaiters: CreditWaiter[];
  recvCredit: number;
  unreturned: number;
  reasm: { type: number; parts: Uint8Array[]; size: number } | null;
  held: Frame[];
  heldBytes: number;
  sendChain: Promise<void>;
  open?: { resolve: (ch: XumuxChannel) => void; reject: (e: Error) => void };
  pings: Map<number, PendingPing>;
  /** Receive-window auto-tuning: current window, and the measurement in progress. */
  recvWindow: number;
  tuneStart: number;
  tuneBytes: number;
}

interface ResolvedLimits {
  maxFramePayload: number;
  maxMessageSize: number;
  initialWindow: number;
}

const unref = <T>(t: T): T => {
  (t as { unref?: () => void }).unref?.();
  return t;
};

function resolveLimits(l: Limits): ResolvedLimits {
  return {
    maxFramePayload: l.maxFramePayload ?? DEFAULT_MAX_FRAME_PAYLOAD,
    maxMessageSize: l.maxMessageSize ?? DEFAULT_MAX_MESSAGE_SIZE,
    initialWindow: l.initialWindow ?? DEFAULT_INITIAL_WINDOW,
  };
}

/** Core §3.4 bounds. Returns an error message, or null if valid. */
function checkLimits(l: ResolvedLimits): string | null {
  for (const [name, v] of Object.entries(l))
    if (!Number.isSafeInteger(v)) return `${name} must be an integer, got ${String(v)}`;
  if (l.maxFramePayload < MIN_MAX_FRAME_PAYLOAD)
    return `maxFramePayload must be ≥ ${MIN_MAX_FRAME_PAYLOAD}`;
  if (l.maxMessageSize < l.maxFramePayload) return 'maxMessageSize must be ≥ maxFramePayload';
  if (l.initialWindow < l.maxMessageSize) return 'initialWindow must be ≥ maxMessageSize';
  if (l.initialWindow > MAX_WINDOW) return 'initialWindow must be ≤ 2^31-1';
  return null;
}

/** Auto-tuning options: integers; connectionWindow must not be negative. */
function checkWindowOptions(o: XumuxOptions): string | null {
  const { maxChannelWindow: m, connectionWindow: c } = o;
  if (m !== undefined && !Number.isSafeInteger(m))
    return `maxChannelWindow must be an integer, got ${String(m)}`;
  if (c !== undefined && (!Number.isSafeInteger(c) || c < 0))
    return `connectionWindow must be a non-negative integer, got ${String(c)}`;
  const q = o.maxControlQueue;
  if (q !== undefined && !Number.isSafeInteger(q))
    return `maxControlQueue must be an integer, got ${String(q)}`;
  const mc = o.maxChannels;
  if (mc !== undefined && (!Number.isSafeInteger(mc) || mc < 1))
    return `maxChannels must be a positive integer, got ${String(mc)}`;
  return null;
}

/** Core §3.5: 0.x needs major and minor equal; ≥1.x needs major equal. */
export function versionsCompatible(a: Version, b: Version): boolean {
  if (a[0] !== b[0]) return false;
  return a[0] !== 0 || a[1] === b[1];
}

/**
 * The entries of `params` for the extensions in `names`, as own properties of a fresh object.
 * Keys for any other extension are ignored (core §3.8).
 */
function pickParams(
  params: ExtensionParams | undefined,
  names: readonly string[],
): ExtensionParams {
  const out: ExtensionParams = {};
  if (!params) return out;
  for (const n of names) if (Object.hasOwn(params, n)) setParam(out, n, params[n]);
  return out;
}

/** One entry, or undefined: never a value inherited from Object.prototype. */
const getParam = (params: ExtensionParams, name: string): unknown =>
  Object.hasOwn(params, name) ? params[name] : undefined;

/** Set one entry as an own property, even for a name such as `__proto__`. */
function setParam(params: ExtensionParams, name: string, value: unknown): void {
  Object.defineProperty(params, name, {
    value,
    enumerable: true,
    writable: true,
    configurable: true,
  });
}

/** An AbortSignal's reason, as an Error. */
const abortReason = (signal: AbortSignal): Error =>
  signal.reason instanceof Error ? signal.reason : new Error('aborted', { cause: signal.reason });

const protocolError = (msg: string): XumuxProtocolError =>
  new XumuxProtocolError(CloseCode.PROTOCOL_ERROR, msg);

// ─── XumuxPeer ───────────────────────────────────────────────────────────────

/**
 * Shared session logic. Use {@link XumuxClient} or {@link XumuxServer}.
 *
 * Events (CustomEvent):
 * - `'channel'`: the peer opened a channel and the policy accepted it (detail: XumuxChannel)
 * - `'channel-close'`: a channel fully closed (detail: ChannelCloseEventDetail)
 * - `'peer-error'`: the peer sent ERROR (detail: ErrorMessage)
 * - `'close'`: the connection closed (detail: CloseInfo)
 */
export abstract class XumuxPeer extends EventTarget implements ChannelHost {
  protected abstract readonly role: 'client' | 'server';

  protected state: SessionState = 'idle';
  protected readonly local: ResolvedLimits;
  protected remote: ResolvedLimits = resolveLimits({});
  protected readonly options: XumuxOptions;

  private readonly slots = new Map<number, ChannelSlot>();
  /** Peer-opened IDs awaiting a policy decision (reserved, not yet channels). */
  private readonly pendingRemote = new Set<number>();
  /**
   * The base path frames travel on, or null while detached (an extension kept the session
   * when the path was lost, core §11.1). Only frames from this path are processed.
   */
  protected path: BasePath | null;

  // Outbound scheduler
  private readonly urgent: Out[] = [];
  private readonly dataQueues = new Map<number, Out[]>();
  private readonly rr: number[] = [];
  private idleWaiters: Array<() => void> = [];
  private sendingClosed = false;

  // Flow-control credit return
  private flushTimer: ReturnType<typeof setTimeout> | null = null;
  /** Sum of channel receive windows (the auto-tuning budget). */
  private windowTotal = 0;
  /** Bytes queued in `urgent`, for maxControlQueue. */
  private urgentBytes = 0;
  private rttProbing = false;
  /** When auto-tuning last sent a probe (performance.now() ms). */
  private rttProbedAt = -Infinity;

  // Probes and keepalive
  private readonly epoch = performance.now();
  private lastToken = -1;
  private readonly controlPings = new Map<number, PendingPing>();
  private readonly controlStats = new ProbeStatsRecorder();
  private lastRx = performance.now();
  private keepaliveTimer: ReturnType<typeof setInterval> | null = null;
  private keepalivePingAt: number | null = null;
  /** Effective keepalive parameters (seconds), kept to restart keepalive on a new base path. */
  private keepaliveParams: { interval: number; timeout: number } | null = null;

  // Close
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private closeInfo: CloseInfo | null = null;
  private closeSent: Promise<void> | null = null;
  private resolveClosed!: (info: CloseInfo) => void;
  /** Resolves when the connection has closed. */
  readonly closed: Promise<CloseInfo>;

  // Handshake plumbing
  protected handshakeWaiter: {
    resolve: (m: HandshakeMessage) => void;
    reject: (e: Error) => void;
  } | null = null;
  private receiving = false;
  /** Client only: frames received after WELCOME but before connect() has processed it. */
  protected afterHandshake: Frame[] | null = null;
  /** Server only: control types an extension has claimed as first frames (core §3.1). */
  protected firstFrameTypes: ReadonlySet<number> = new Set();

  constructor(
    protected readonly transport: TransportAdapter,
    options: XumuxOptions = {},
  ) {
    super();
    this.options = options;
    this.local = resolveLimits(options);
    const err = checkLimits(this.local) ?? checkWindowOptions(options);
    if (err) throw new RangeError(err);
    this.path = new BasePath(transport, this.local.maxFramePayload);
    this.closed = new Promise((r) => (this.resolveClosed = r));
  }

  /** Connection-level (channel 0) probe statistics. */
  get stats(): ProbeStats {
    return this.controlStats.snapshot();
  }

  /** Smoothed connection RTT in ms, if any probe has completed. */
  get rtt(): number | undefined {
    return this.controlStats.srtt;
  }

  /** Open channels, by ID. */
  get channels(): ReadonlyMap<number, XumuxChannel> {
    const out = new Map<number, XumuxChannel>();
    for (const [id, s] of this.slots) if (s.ch.state === 'open') out.set(id, s.ch);
    return out;
  }

  /** First open channel with this name. */
  channel(name: string): XumuxChannel | undefined {
    for (const s of this.slots.values())
      if (s.ch.state === 'open' && s.ch.name === name) return s.ch;
    return undefined;
  }

  /** Measure connection RTT with a PING on channel 0 (ms). */
  ping(timeoutMs = 5000): Promise<number> {
    return this.pingChannel(CONTROL_CHANNEL, timeoutMs);
  }

  /** Open a channel (core §5.2). Resolves when the peer accepts; rejects with XumuxClosedError if refused. */
  openChannel(request: Omit<ChannelRequest, 'id'>): Promise<XumuxChannel> {
    if (this.state !== 'open')
      return Promise.reject(new Error(`cannot open a channel while ${this.state}`));
    const id = this.allocateId();
    if (id === null)
      return Promise.reject(new XumuxClosedError(CloseCode.CHANNEL_LIMIT, 'no free channel IDs'));
    const slot = this.createSlot({ ...request, id }, true, 'opening');
    const p = new Promise<XumuxChannel>((resolve, reject) => (slot.open = { resolve, reject }));
    void this.enqueueUrgent(openChannelFrame({ ...request, id })).catch(ignore);
    return p;
  }

  /** Graceful close: flush queued data, CLOSE, wait for the peer's CLOSE, close the transport. */
  async close(code: number = CloseCode.NORMAL, reason?: string): Promise<CloseInfo> {
    if (this.state === 'closed' || this.state === 'closing') return this.closed;
    if (this.state === 'open' && this.path) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([
        this.drain(),
        new Promise((r) => (timer = unref(setTimeout(r, this.closeTimeoutMs)))),
      ]);
      clearTimeout(timer);
    }
    this.beginClose(code, reason, false);
    return this.closed;
  }

  // ─── ChannelHost ────────────────────────────────────────────────────────────

  /** @internal */
  sendMessage(ch: XumuxChannel, type: number, payload: Uint8Array): Promise<void> {
    const slot = this.slots.get(ch.id);
    if (!slot || slot.ch !== ch || ch.state !== 'open' || slot.sentClose) {
      return Promise.reject(new Error(`channel ${ch.id} is not open`));
    }
    if (!Number.isInteger(type) || type < 0 || type > 0xff)
      return Promise.reject(new RangeError('type must be 0–255'));
    if (payload.length > this.remote.maxMessageSize) {
      return Promise.reject(
        new RangeError(
          `message of ${payload.length} bytes exceeds peer maxMessageSize ${this.remote.maxMessageSize}`,
        ),
      );
    }
    const { reliable, ordered } = ch.properties;
    const fragmentable = reliable && ordered;
    if (!fragmentable && payload.length > this.remote.maxFramePayload) {
      return Promise.reject(
        new RangeError(
          'message exceeds peer maxFramePayload and this channel cannot be fragmented',
        ),
      );
    }
    const chunk = fragmentable
      ? Math.max(
          1,
          Math.min(this.remote.maxFramePayload, this.options.fragmentSize ?? DEFAULT_FRAGMENT_SIZE),
        )
      : Math.max(1, payload.length);

    const run = async (): Promise<void> => {
      let off = 0;
      do {
        const end = Math.min(off + chunk, payload.length);
        const piece = payload.subarray(off, end);
        const closed = (): boolean => slot.sentClose || ch.state !== 'open';
        if (closed()) throw new Error(`channel ${ch.id} closed while sending`);
        if (reliable) await this.takeCredit(slot, piece.length);
        if (closed()) throw new Error(`channel ${ch.id} closed while sending`);
        const flags = end < payload.length ? FrameFlags.MORE : 0;
        await this.enqueueData(ch.id, { channel: ch.id, type, flags, payload: piece });
        off = end;
      } while (off < payload.length);
    };
    const p = slot.sendChain.then(run);
    slot.sendChain = p.catch(ignore);
    return p;
  }

  /** @internal */
  sendPrefix(
    ch: XumuxChannel,
    type: number,
    payload: Uint8Array,
    signal?: AbortSignal,
  ): Promise<number> {
    const slot = this.slots.get(ch.id);
    if (!slot || slot.ch !== ch || ch.state !== 'open' || slot.sentClose) {
      return Promise.reject(new Error(`channel ${ch.id} is not open`));
    }
    if (!Number.isInteger(type) || type < 0 || type > 0xff)
      return Promise.reject(new RangeError('type must be 0–255'));
    const max = Math.max(
      1,
      Math.min(this.remote.maxFramePayload, this.options.fragmentSize ?? DEFAULT_FRAGMENT_SIZE),
    );
    const closed = (): boolean => slot.sentClose || ch.state !== 'open';
    const run = async (): Promise<number> => {
      if (signal?.aborted) throw abortReason(signal);
      if (closed()) throw new Error(`channel ${ch.id} closed while sending`);
      let n = Math.min(payload.length, max);
      if (ch.properties.reliable && n > 0) n = await this.takeCredit(slot, n, true, signal);
      if (closed()) throw new Error(`channel ${ch.id} closed while sending`);
      // One whole frame, never MORE: the frame is encoded (copied) as it is queued.
      await this.enqueueData(ch.id, {
        channel: ch.id,
        type,
        flags: 0,
        payload: payload.subarray(0, n),
      });
      return n;
    };
    if (signal?.aborted) return Promise.reject(abortReason(signal));
    // An abort while this call waits for its turn (behind a send that is itself waiting for
    // credit) rejects it at once; its turn then passes without sending anything. Once its
    // turn has come, takeCredit handles the abort.
    let gaveUp = false;
    let onAbort = (): void => undefined;
    const aborted = new Promise<never>((_, reject) => {
      onAbort = () => {
        gaveUp = true;
        reject(abortReason(signal!));
      };
    });
    aborted.catch(ignore);
    signal?.addEventListener('abort', onAbort, { once: true });
    const turn = slot.sendChain.then(() => {
      signal?.removeEventListener('abort', onAbort);
      return gaveUp ? 0 : run();
    });
    slot.sendChain = turn.then(
      () => undefined,
      () => undefined,
    );
    return Promise.race([turn, aborted]);
  }

  /** @internal */
  closeChannel(ch: XumuxChannel, code: number, reason?: string): Promise<ChannelCloseInfo> {
    const slot = this.slots.get(ch.id);
    if (!slot || slot.ch !== ch || ch.state === 'closed') return ch.closed;
    // Unreachable through the public API (channels are handed out once open); core §5.2 forbids it anyway.
    if (ch.state === 'opening')
      return Promise.reject(new Error('cannot close a channel that is still opening'));
    if (!slot.sentClose) {
      // Let queued sends go out first, then CLOSE_CHANNEL. A send blocked on credit the peer
      // never returns must not hold the close forever: after closeTimeoutMs, close anyway
      // (which fails the blocked sends).
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<void>((r) => (timer = unref(setTimeout(r, this.closeTimeoutMs))));
      void Promise.race([slot.sendChain, timeout])
        .then(() => this.sendCloseChannel(slot, code, reason))
        .catch(ignore)
        .finally(() => clearTimeout(timer));
    }
    return ch.closed;
  }

  /** @internal */
  pingChannel(channelId: number, timeoutMs: number): Promise<number> {
    if (this.state !== 'open') return Promise.reject(new Error(`cannot ping while ${this.state}`));
    let pings: Map<number, PendingPing>;
    let stats: ProbeStatsRecorder;
    if (channelId === CONTROL_CHANNEL) {
      pings = this.controlPings;
      stats = this.controlStats;
    } else {
      const slot = this.slots.get(channelId);
      if (!slot || slot.ch.state !== 'open' || slot.sentClose)
        return Promise.reject(new Error(`channel ${channelId} is not open`));
      pings = slot.pings;
      stats = slot.ch._stats;
    }
    const token = this.nextToken(pings);
    stats.probesSent++;
    return new Promise<number>((resolve, reject) => {
      const timer = unref(
        setTimeout(() => {
          pings.delete(token);
          stats.probesLost++;
          reject(new Error(`PING on channel ${channelId} timed out`));
        }, timeoutMs),
      );
      pings.set(token, {
        resolve: (rtt) => {
          stats.sample(rtt);
          resolve(rtt);
        },
        reject,
        timer,
      });
      void this.enqueueUrgent(pingFrame(channelId, token)).catch(ignore);
    });
  }

  /** @internal The application read `bytes` from `ch`: return credit (core §6). */
  consumed(ch: XumuxChannel, bytes: number): void {
    const slot = this.slots.get(ch.id);
    if (!slot || slot.ch !== ch || !ch.properties.reliable || ch.state !== 'open' || slot.sentClose)
      return;
    slot.unreturned += bytes;
    if (slot.unreturned >= this.local.initialWindow / 2) this.flushWindow();
    else if (!this.flushTimer) this.flushTimer = setTimeout(() => this.flushWindow(), 0);
  }

  /** @internal Current receive window of `ch`. */
  receiveWindow(ch: XumuxChannel): number {
    const slot = this.slots.get(ch.id);
    return slot && slot.ch === ch ? slot.recvWindow : this.local.initialWindow;
  }

  /** @internal Sum of channel receive windows (for tests). */
  get _windowTotal(): number {
    return this.windowTotal;
  }

  private deleteSlot(slot: ChannelSlot): void {
    if (this.slots.get(slot.ch.id) !== slot) return;
    this.slots.delete(slot.ch.id);
    this.windowTotal -= slot.recvWindow;
  }

  /**
   * Receive-window auto-tuning, called as returned credit goes out in a WINDOW_UPDATE.
   * Returns extra credit to grant on top. A sender held back only by the window delivers
   * one window per round trip; if a whole window was used in under two smoothed RTTs,
   * the window doubles, within maxChannelWindow and the connectionWindow budget.
   */
  private growWindow(slot: ChannelSlot, returned: number): number {
    const maxChannel = this.maxChannelWindow();
    if (maxChannel <= slot.recvWindow || !slot.ch.properties.reliable) return 0;
    // A measurement starts when a window's worth of credit is granted (channel open, then
    // after each full window), so it includes receiving the bytes, not only returning them.
    const now = performance.now();
    slot.tuneBytes += returned;
    if (slot.tuneBytes < slot.recvWindow) return 0;
    const elapsed = now - slot.tuneStart;
    slot.tuneStart = now;
    slot.tuneBytes = 0;
    // The larger of SRTT and the latest sample: smoothing alone would take several
    // samples to notice that the path got slower.
    const { srtt, rtt: latest } = this.controlStats;
    if (srtt === undefined) {
      this.probeRtt();
      return 0;
    }
    const rtt = Math.max(srtt, latest ?? 0);
    if (elapsed >= 2 * rtt) {
      // Not window-limited, or the RTT sample is stale (the path got slower).
      // Refresh it, at most once a second, so growth can resume.
      if (now - this.rttProbedAt > 1000) this.probeRtt();
      return 0;
    }
    const budget = (this.options.connectionWindow ?? DEFAULT_CONNECTION_WINDOW) - this.windowTotal;
    const extra = Math.min(slot.recvWindow, maxChannel - slot.recvWindow, budget);
    if (extra <= 0) return 0;
    slot.recvWindow += extra;
    this.windowTotal += extra;
    return extra;
  }

  private maxChannelWindow(): number {
    const v = this.options.maxChannelWindow ?? DEFAULT_MAX_CHANNEL_WINDOW;
    return v < 0 ? this.local.initialWindow : Math.min(v, MAX_WINDOW);
  }

  /** Measure the RTT once, for auto-tuning, when no probe has completed yet. */
  private probeRtt(): void {
    if (this.rttProbing || this.state !== 'open') return;
    this.rttProbing = true;
    this.rttProbedAt = performance.now();
    this.ping((this.options.pingTimeout ?? DEFAULT_PING_TIMEOUT) * 1000)
      .catch(ignore)
      .finally(() => (this.rttProbing = false));
  }

  // ─── Handshake helpers for subclasses ───────────────────────────────────────

  /** Start reading from the transport (idempotent). */
  protected startReceiving(): void {
    if (this.receiving || !this.path) return;
    this.receiving = true;
    this.path.start(this.sinkFor(this.path));
  }

  protected get closeTimeoutMs(): number {
    return this.options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
  }

  protected get handshakeTimeoutMs(): number {
    return this.options.handshakeTimeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS;
  }

  /** Wait for HELLO or WELCOME (whichever the role expects), a CLOSE, or the timeout. */
  protected awaitHandshake(): Promise<HandshakeMessage> {
    return new Promise((resolve, reject) => {
      const timer = unref(
        setTimeout(() => {
          this.handshakeWaiter = null;
          if (this.role === 'server')
            this.beginClose(CloseCode.HELLO_TIMEOUT, 'no HELLO received', false);
          else this.beginClose(CloseCode.GOING_AWAY, 'no WELCOME received', false);
          reject(new XumuxClosedError(CloseCode.HELLO_TIMEOUT, 'handshake timed out'));
        }, this.handshakeTimeoutMs),
      );
      this.handshakeWaiter = {
        resolve: (m) => (clearTimeout(timer), resolve(m)),
        reject: (e) => (clearTimeout(timer), reject(e)),
      };
    });
  }

  protected setRemoteLimits(l: Limits): void {
    this.remote = resolveLimits(l);
    const err = checkLimits(this.remote);
    if (err) throw new XumuxProtocolError(CloseCode.INVALID_MESSAGE, `peer limits invalid: ${err}`);
  }

  protected enterOpen(pingInterval: number, pingTimeout: number): void {
    this.state = 'open';
    this.startKeepalive(pingInterval, pingTimeout);
  }

  /** Client: process frames that arrived between WELCOME and the end of connect(). */
  protected replayAfterHandshake(): void {
    const frames = this.afterHandshake ?? [];
    this.afterHandshake = null;
    for (const f of frames) {
      if (this.state !== 'open') break;
      this.handleFrame(f);
    }
  }

  protected createSlot(
    req: ChannelRequest,
    localOpener: boolean,
    state: 'opening' | 'open',
    props?: ChannelAccept,
  ): ChannelSlot {
    const ch = new XumuxChannel(
      this,
      req.id,
      req.name,
      localOpener,
      props
        ? {
            reliable: props.reliable,
            ordered: props.ordered,
            maxRetransmits: props.maxRetransmits,
            maxPacketLifeTime: props.maxPacketLifeTime,
          }
        : {
            reliable: req.reliable,
            ordered: req.ordered,
            maxRetransmits: req.maxRetransmits,
            maxPacketLifeTime: req.maxPacketLifeTime,
          },
      req.metadata,
      state,
    );
    const slot: ChannelSlot = {
      ch,
      sentClose: false,
      recvClose: false,
      sendCredit: this.remote.initialWindow,
      creditWaiters: [],
      recvCredit: this.local.initialWindow,
      unreturned: 0,
      reasm: null,
      held: [],
      heldBytes: 0,
      sendChain: Promise.resolve(),
      pings: new Map(),
      recvWindow: this.local.initialWindow,
      // The first measurement starts when the initial window is granted.
      tuneStart: performance.now(),
      tuneBytes: 0,
    };
    this.windowTotal += slot.recvWindow;
    this.slots.set(req.id, slot);
    return slot;
  }

  /** Mark a locally opened channel accepted with effective properties, then replay held frames. */
  protected acceptOpening(slot: ChannelSlot, props: ChannelAccept): void {
    slot.ch._props = {
      reliable: props.reliable,
      ordered: props.ordered,
      maxRetransmits: props.maxRetransmits,
      maxPacketLifeTime: props.maxPacketLifeTime,
    };
    slot.ch._state = 'open';
    slot.sendCredit = this.remote.initialWindow;
    const held = slot.held;
    slot.held = [];
    slot.heldBytes = 0;
    slot.open?.resolve(slot.ch);
    slot.open = undefined;
    for (const f of held) this.handleFrame(f);
  }

  /**
   * Hold a frame that overtook the ACK or WELCOME (core §5.2). The peer may only have sent
   * up to our initialWindow on the channel, so more than that is a flow-control violation.
   */
  private hold(slot: ChannelSlot, f: Frame): void {
    if (f.flags & FrameFlags.PROBE) {
      // Probes never use credit (core §6). Keep a few PINGs to answer once open; drop the
      // rest, as a receiver may drop PINGs it is not willing to answer (core §8.1).
      if (
        f.type === ProbeType.PING &&
        slot.held.filter((h) => h.flags & FrameFlags.PROBE).length < 8
      )
        slot.held.push(f);
      return;
    }
    slot.heldBytes += f.payload.length;
    if (slot.heldBytes > this.local.initialWindow)
      throw new XumuxProtocolError(
        CloseCode.FLOW_CONTROL_ERROR,
        `peer sent more than initialWindow on channel ${f.channel} before it was acknowledged`,
      );
    slot.held.push(f);
  }

  /** Drop a locally opened channel that was refused. */
  protected refuseOpening(slot: ChannelSlot, refusal: ChannelRefusal): void {
    this.deleteSlot(slot);
    slot.ch._finish({ code: refusal.code, reason: refusal.reason, remote: true });
    slot.open?.reject(new XumuxClosedError(refusal.code, refusal.reason, true));
    slot.open = undefined;
  }

  /** Effective properties on this binding. Every current binding is single-stream: reliable + ordered. */
  protected effective(req: ChannelRequest): ChannelAccept {
    return { id: req.id, reliable: true, ordered: true };
  }

  /** Run the channel policy, mapping exceptions to a refusal. */
  protected async decide(req: ChannelRequest): Promise<ChannelDecision> {
    const policy = this.options.onChannelRequest;
    if (!policy)
      return {
        code: CloseCode.UNSUPPORTED,
        reason: 'channels opened by the peer are not accepted',
      };
    try {
      return await policy(req);
    } catch (e) {
      return {
        code: CloseCode.CHANNEL_REFUSED,
        reason: e instanceof Error ? e.message : String(e),
      };
    }
  }

  protected sendControl(frame: Frame): Promise<void> {
    return this.enqueueUrgent(frame);
  }

  protected slotFor(id: number): ChannelSlot | undefined {
    return this.slots.get(id);
  }

  // ─── Receive path ───────────────────────────────────────────────────────────

  /** A sink that feeds `path`'s frames to the session for as long as it is the current path. */
  protected sinkFor(path: BasePath): BasePathSink {
    return {
      frame: (f) => {
        if (path === this.path && this.state !== 'closed') this.handleFrame(f);
      },
      end: (cause) => void this.pathEnded(path, cause),
    };
  }

  /**
   * The session keeps running when its base path is lost, instead of closing, if this returns
   * true (core §11.1, "keep the connection alive when its base path is lost"). Called only
   * while open, when the transport ends, fails to read or write, or misses keepalive, without
   * a completed CLOSE exchange. Default false: the connection closes, as the core requires.
   */
  protected keepOnPathLoss(): boolean {
    return false;
  }

  private detachOnLoss(): boolean {
    return this.state === 'open' && this.path !== null && this.keepOnPathLoss();
  }

  /** The current base path's input ended. */
  private async pathEnded(
    path: BasePath,
    cause: { readError?: unknown; parseError?: unknown },
  ): Promise<void> {
    if (path !== this.path) return;
    if ('parseError' in cause) {
      // The input can't be parsed any further: a protocol violation, never a lost path.
      // fail() queues CLOSE with the right code; let it reach the wire, then finish with that
      // cause (core §9.1).
      this.fail(cause.parseError);
    } else if (this.detachOnLoss()) {
      const e = cause.readError;
      this.detachBasePath(
        'readError' in cause
          ? `transport failed: ${e instanceof Error ? e.message : String(e)}`
          : 'transport closed',
      );
      return;
    } else if ('readError' in cause) {
      this.fail(cause.readError);
    }
    // The input ended. If we are closing (for example the peer sent CLOSE and then
    // shut its write side), let our CLOSE reach the wire before finishing; the close
    // timer bounds the wait (core §9.1).
    if (this.state === 'closing') await this.closeSent?.catch(ignore);
    if (this.state !== 'closed')
      this.finishClose(
        this.state === 'closing' && this.closeInfo
          ? this.closeInfo
          : { code: CloseCode.GOING_AWAY, reason: 'transport closed', remote: true },
      );
  }

  /**
   * Detach the base path: close its transport without sending anything, and keep the
   * session, its channels and their queued sends as they are. Keepalive stops and pending
   * PINGs are abandoned (rejected, but not counted as lost). Emits `'detach'`.
   *
   * Frames that were being written when the path was lost are rejected (a write that never
   * finishes must not hold up its channel), and the transport may have lost them. Making them
   * good is the extension's job: session-resume/1 retains every frame it may need and replays
   * it, so its frames are never rejected here. Without that, a fragmented message cut short
   * leaves the peer's reassembly incomplete.
   */
  protected detachBasePath(reason: string): void {
    const path = this.path;
    if (!path) return;
    this.path = null;
    path.close();
    // A write that never finishes on the dead transport must not hold up its channel's sends.
    const stuck = path.inFlight as Out | null;
    path.inFlight = null;
    stuck?.reject(new Error(`base path lost: ${reason}`));
    this.stopKeepalive();
    this.keepalivePingAt = null;
    const err = new Error(`base path lost: ${reason}`);
    const abandon = (pings: Map<number, PendingPing>): void => {
      for (const p of pings.values()) {
        clearTimeout(p.timer);
        p.reject(err);
      }
      pings.clear();
    };
    abandon(this.controlPings);
    for (const slot of this.slots.values()) abandon(slot.pings);
    this.dispatchEvent(new CustomEvent<DetachInfo>('detach', { detail: { reason } }));
  }

  /**
   * Attach `path` as the new base path of a detached, open session: read from it, send the
   * queued frames on it, and restart keepalive.
   */
  protected attachBasePath(path: BasePath): void {
    if (this.path) throw new Error('a base path is already attached');
    if (this.state !== 'open') throw new Error(`cannot attach a base path while ${this.state}`);
    this.path = path;
    path.setMaxFramePayload(this.local.maxFramePayload);
    path.start(this.sinkFor(path));
    this.lastRx = performance.now();
    if (this.keepaliveParams)
      this.startKeepalive(this.keepaliveParams.interval, this.keepaliveParams.timeout);
    void this.pump();
  }

  /** True while the session has no base path (detached, waiting for a new one). */
  get detached(): boolean {
    return this.path === null && this.state === 'open';
  }

  /** Turn an error from frame processing into the right close. */
  private fail(e: unknown): void {
    const err =
      e instanceof XumuxProtocolError
        ? e
        : new XumuxProtocolError(
            CloseCode.PROTOCOL_ERROR,
            e instanceof Error ? e.message : String(e),
          );
    if (!(e instanceof XumuxProtocolError))
      this.dispatchEvent(new CustomEvent('error', { detail: e }));
    if (err.scope === 'transport') {
      this.finishClose({ code: err.code, reason: err.message, remote: false });
    } else if (err.scope === 'channel' && err.channel !== undefined) {
      const slot = this.slots.get(err.channel);
      if (slot) this.channelError(slot, err.code, err.message);
    } else if (this.state === 'closing') {
      this.finishClose(this.closeInfo ?? { code: err.code, reason: err.message, remote: false });
    } else if (this.state !== 'closed') {
      this.beginClose(err.code, err.message, false);
    }
  }

  private handleFrame(f: Frame): void {
    try {
      this.lastRx = performance.now();
      this.keepalivePingAt = null;
      validateFrame(f);

      // After sending CLOSE, only the peer's CLOSE matters (core §9.1).
      if (this.state === 'closing') {
        if (
          f.channel === CONTROL_CHANNEL &&
          !(f.flags & FrameFlags.PROBE) &&
          f.type === ControlType.CLOSE
        ) {
          this.finishClose(this.closeInfo ?? { code: CloseCode.NORMAL, remote: false });
        }
        return;
      }
      if (this.state !== 'open') return this.handlePreOpen(f);
      if (f.flags & FrameFlags.PROBE) return this.handleProbe(f);
      if (f.channel === CONTROL_CHANNEL) return this.handleControl(f);
      return this.handleData(f);
    } catch (e) {
      this.fail(e);
    }
  }

  /** Before the handshake completes, only HELLO/WELCOME/CLOSE on channel 0 (core §3.1, §4.1). */
  private handlePreOpen(f: Frame): void {
    const isControl = f.channel === CONTROL_CHANNEL && !(f.flags & FrameFlags.PROBE);
    if (isControl && f.type === ControlType.CLOSE) {
      const msg = decodeClose(f.payload);
      this.handshakeWaiter?.reject(new XumuxClosedError(msg.code, msg.reason, true));
      this.handshakeWaiter = null;
      return this.remoteClose(msg.code, msg.reason);
    }
    const expected = this.role === 'server' ? ControlType.HELLO : ControlType.WELCOME;
    if (isControl && f.type === expected && this.handshakeWaiter) {
      const msg =
        expected === ControlType.HELLO ? decodeHello(f.payload) : decodeWelcome(f.payload);
      const w = this.handshakeWaiter;
      this.handshakeWaiter = null;
      if (this.role === 'client') this.afterHandshake = [];
      w.resolve(msg);
      return;
    }
    // An extension-defined first frame instead of HELLO (core §3.1).
    if (isControl && this.handshakeWaiter && this.firstFrameTypes.has(f.type)) {
      const w = this.handshakeWaiter;
      this.handshakeWaiter = null;
      w.resolve({ firstFrame: { type: f.type, payload: f.payload } });
      return;
    }
    // The server may send right after WELCOME (OPEN_CHANNEL, data, probes) and those frames
    // can arrive in the same read. Keep them until connect() has applied the WELCOME.
    if (this.afterHandshake) {
      this.afterHandshake.push(f);
      return;
    }
    // A responder's frames for a channel we opened in HELLO may overtake WELCOME (core §5.2).
    if (this.role === 'client' && f.channel !== CONTROL_CHANNEL) {
      const slot = this.slots.get(f.channel);
      if (slot && slot.ch.state === 'opening') {
        this.hold(slot, f);
        return;
      }
    }
    throw protocolError(
      this.role === 'server' ? 'first frame must be HELLO' : 'unexpected frame before WELCOME',
    );
  }

  private handleProbe(f: Frame): void {
    const token = decodeProbeToken(f.payload);
    if (token === null) return; // core §8.1: discard
    let pings: Map<number, PendingPing>;
    if (f.channel === CONTROL_CHANNEL) {
      pings = this.controlPings;
    } else {
      const slot = this.slots.get(f.channel);
      if (!slot) return;
      if (slot.ch.state === 'opening' && slot.ch.localOpener) {
        this.hold(slot, f);
        return;
      }
      if (slot.ch.state !== 'open' || slot.sentClose) return;
      pings = slot.pings;
    }
    if (f.type === ProbeType.PING) {
      void this.enqueueUrgent(pongFrame(f.channel, token)).catch(ignore);
    } else if (f.type === ProbeType.PONG) {
      const p = pings.get(token);
      if (!p) return;
      pings.delete(token);
      clearTimeout(p.timer);
      const nowUs = this.nowUs();
      p.resolve(((nowUs - token) >>> 0) / 1000);
    }
  }

  private handleControl(f: Frame): void {
    switch (f.type) {
      case ControlType.HELLO:
      case ControlType.WELCOME:
        throw protocolError(
          `unexpected ${f.type === ControlType.HELLO ? 'HELLO' : 'WELCOME'} after handshake`,
        );
      case ControlType.OPEN_CHANNEL:
        return this.onOpenChannel(decodeOpenChannel(f.payload));
      case ControlType.CHANNEL_ACK: {
        const ack = decodeChannelAck(f.payload);
        const slot = this.slots.get(ack.id);
        if (!slot || slot.ch.state !== 'opening' || !slot.ch.localOpener)
          throw protocolError(`CHANNEL_ACK for ${ack.id}, which is not opening`);
        return this.acceptOpening(slot, ack);
      }
      case ControlType.CHANNEL_REJECT: {
        const rej = decodeChannelReject(f.payload);
        const slot = this.slots.get(rej.id);
        if (!slot || slot.ch.state !== 'opening' || !slot.ch.localOpener)
          throw protocolError(`CHANNEL_REJECT for ${rej.id}, which is not opening`);
        return this.refuseOpening(slot, rej);
      }
      case ControlType.CLOSE_CHANNEL:
        return this.onCloseChannel(decodeCloseChannel(f.payload));
      case ControlType.WINDOW_UPDATE:
        return this.onWindowUpdate(decodeWindowUpdate(f.payload));
      case ControlType.CLOSE: {
        const msg = decodeClose(f.payload);
        return this.remoteClose(msg.code, msg.reason);
      }
      case ControlType.ERROR:
        this.dispatchEvent(
          new CustomEvent<ErrorMessage>('peer-error', { detail: decodeError(f.payload) }),
        );
        return;
      default:
        void this.enqueueUrgent(
          errorFrame({
            code: CloseCode.UNSUPPORTED,
            reason: `unknown control type 0x${f.type.toString(16)}`,
          }),
        ).catch(ignore);
    }
  }

  private onOpenChannel(req: ChannelRequest): void {
    const peerParity = this.role === 'server' ? 1 : 0;
    if (req.id === 0 || req.id > MAX_CHANNEL_ID || req.id % 2 !== peerParity)
      throw protocolError(`OPEN_CHANNEL with invalid ID ${req.id}`);
    if (this.slots.has(req.id) || this.pendingRemote.has(req.id))
      throw protocolError(`OPEN_CHANNEL for ID ${req.id}, which is in use`);
    if (this.slots.size + this.pendingRemote.size >= this.maxChannels()) {
      void this.enqueueUrgent(
        channelRejectFrame({
          id: req.id,
          code: CloseCode.CHANNEL_LIMIT,
          reason: 'too many channels',
        }),
      ).catch(ignore);
      return;
    }
    this.pendingRemote.add(req.id);
    void this.decide(req).then((d) => {
      this.pendingRemote.delete(req.id);
      if (this.state !== 'open') return;
      if (d === true) {
        const props = this.effective(req);
        const slot = this.createSlot(req, false, 'open', props);
        void this.enqueueUrgent(channelAckFrame(props)).catch(ignore);
        this.dispatchEvent(new CustomEvent('channel', { detail: slot.ch }));
      } else {
        void this.enqueueUrgent(
          channelRejectFrame({ id: req.id, code: d.code, reason: d.reason }),
        ).catch(ignore);
      }
    });
  }

  private onCloseChannel(msg: { id: number; code?: number; reason?: string }): void {
    if (msg.id === CONTROL_CHANNEL) {
      void this.enqueueUrgent(
        errorFrame({ code: CloseCode.PROTOCOL_ERROR, reason: 'CLOSE_CHANNEL for channel 0' }),
      ).catch(ignore);
      return;
    }
    const slot = this.slots.get(msg.id);
    if (!slot || slot.ch.state === 'opening' || slot.ch.state === 'closed') return; // core §5.4: ignore
    slot.recvClose = true;
    slot.reasm = null;
    if (!slot.sentClose) {
      slot.closeInfo = { code: msg.code ?? CloseCode.NORMAL, reason: msg.reason, remote: true };
      this.sendCloseChannel(slot, msg.code ?? CloseCode.NORMAL, undefined);
    }
    // Both CLOSE_CHANNELs are now sent (ours at least queued, in order) and received, so the
    // close is complete (core §5.4). Don't wait for our send to finish: the peer may reuse the
    // ID at once, and its OPEN_CHANNEL can arrive first. Anything we later send on a reused ID
    // is queued behind our CLOSE_CHANNEL.
    this.finishChannel(slot);
  }

  private onWindowUpdate(entries: Array<{ channel: number; increment: number }>): void {
    for (const { channel, increment } of entries) {
      if (channel === CONTROL_CHANNEL) throw protocolError('WINDOW_UPDATE for channel 0');
      const slot = this.slots.get(channel);
      if (!slot || slot.ch.state !== 'open' || slot.sentClose) continue;
      if (!slot.ch.properties.reliable)
        throw protocolError(`WINDOW_UPDATE for unreliable channel ${channel}`);
      slot.sendCredit += increment;
      if (slot.sendCredit > MAX_WINDOW)
        throw new XumuxProtocolError(
          CloseCode.FLOW_CONTROL_ERROR,
          `credit on channel ${channel} exceeds 2^31-1`,
        );
      this.wakeCredit(slot);
    }
  }

  private handleData(f: Frame): void {
    const slot = this.slots.get(f.channel);
    if (!slot) {
      void this.enqueueUrgent(
        errorFrame({ code: CloseCode.CHANNEL_NOT_FOUND, channel: f.channel }),
      ).catch(ignore);
      return;
    }
    const ch = slot.ch;
    if (ch.state === 'opening' && ch.localOpener) {
      this.hold(slot, f);
      return;
    }
    if (ch.state !== 'open' || slot.sentClose) return; // closing: discard (core §5.5)

    const { reliable, ordered } = ch.properties;
    if (reliable) {
      if (f.payload.length > slot.recvCredit) {
        throw new XumuxProtocolError(
          CloseCode.FLOW_CONTROL_ERROR,
          `peer exceeded credit on channel ${f.channel}`,
        );
      }
      slot.recvCredit -= f.payload.length;
    }
    const more = (f.flags & FrameFlags.MORE) !== 0;
    const channelErr = (code: number, msg: string): never => {
      throw new XumuxProtocolError(code, msg, 'channel', f.channel);
    };
    if (more && !(reliable && ordered))
      channelErr(CloseCode.PROTOCOL_ERROR, 'MORE on a channel that cannot be fragmented');

    if (slot.reasm) {
      if (f.type !== slot.reasm.type)
        channelErr(CloseCode.PROTOCOL_ERROR, 'fragment type mismatch');
      slot.reasm.parts.push(f.payload);
      slot.reasm.size += f.payload.length;
      if (slot.reasm.size > this.local.maxMessageSize)
        channelErr(CloseCode.MESSAGE_TOO_LARGE, 'reassembled message too large');
      if (!more) {
        const { type, parts, size } = slot.reasm;
        slot.reasm = null;
        const payload = new Uint8Array(size);
        let off = 0;
        for (const p of parts) {
          payload.set(p, off);
          off += p.length;
        }
        ch._deliver({ type, payload });
      }
    } else if (more) {
      slot.reasm = { type: f.type, parts: [f.payload], size: f.payload.length };
    } else {
      ch._deliver({ type: f.type, payload: f.payload });
    }
  }

  // ─── Channel close helpers ──────────────────────────────────────────────────

  private sendCloseChannel(slot: ChannelSlot, code: number, reason?: string): void {
    if (slot.sentClose || slot.ch.state === 'closed') return;
    slot.sentClose = true;
    slot.ch._state = 'closing';
    slot.closeInfo ??= { code, reason, remote: false };
    const err = new Error(`channel ${slot.ch.id} closed`);
    this.rejectCredit(slot, err);
    this.dropQueued(slot.ch.id, err);
    void this.enqueueUrgent(
      closeChannelFrame(
        code === CloseCode.NORMAL && !reason
          ? { id: slot.ch.id }
          : { id: slot.ch.id, code, reason },
      ),
    ).catch(ignore); // the close completes when the peer's CLOSE_CHANNEL arrives
  }

  /** Close one channel because of a channel-level protocol error. */
  private channelError(slot: ChannelSlot, code: number, reason: string): void {
    slot.reasm = null;
    slot.closeInfo = { code, reason, remote: false };
    this.sendCloseChannel(slot, code, reason);
  }

  private finishChannel(slot: ChannelSlot): void {
    if (slot.ch.state === 'closed') return;
    this.deleteSlot(slot);
    this.rejectCredit(slot, new Error(`channel ${slot.ch.id} closed`));
    for (const p of slot.pings.values()) {
      clearTimeout(p.timer);
      p.reject(new Error(`channel ${slot.ch.id} closed`));
    }
    slot.pings.clear();
    const info = slot.closeInfo ?? { code: CloseCode.NORMAL, remote: false };
    slot.ch._finish(info);
    slot.open?.reject(new XumuxClosedError(info.code, info.reason, info.remote));
    this.dispatchEvent(
      new CustomEvent<ChannelCloseEventDetail>('channel-close', {
        detail: { channel: slot.ch, ...info },
      }),
    );
  }

  // ─── Flow control ───────────────────────────────────────────────────────────

  /**
   * Wait for n bytes of send credit. With `partial`, take whatever is free once there is at
   * least 1 byte, up to n, and resolve with the amount taken. An abort rejects with the
   * signal's reason and takes nothing.
   */
  private takeCredit(
    slot: ChannelSlot,
    n: number,
    partial = false,
    signal?: AbortSignal,
  ): Promise<number> {
    if (slot.sentClose || slot.ch.state !== 'open')
      return Promise.reject(new Error(`channel ${slot.ch.id} closed`));
    if (signal?.aborted) return Promise.reject(abortReason(signal));
    if (slot.creditWaiters.length === 0 && slot.sendCredit >= (partial ? 1 : n)) {
      const got = Math.min(n, slot.sendCredit);
      slot.sendCredit -= got;
      return Promise.resolve(got);
    }
    return new Promise((resolve, reject) => {
      const onAbort = (): void => {
        const i = slot.creditWaiters.indexOf(w);
        if (i < 0) return;
        slot.creditWaiters.splice(i, 1);
        reject(abortReason(signal!));
        this.wakeCredit(slot);
      };
      const w: CreditWaiter = {
        n,
        partial,
        resolve: (got) => {
          signal?.removeEventListener('abort', onAbort);
          resolve(got);
        },
        reject: (e) => {
          signal?.removeEventListener('abort', onAbort);
          reject(e);
        },
      };
      slot.creditWaiters.push(w);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }

  private wakeCredit(slot: ChannelSlot): void {
    while (slot.creditWaiters.length > 0) {
      const w = slot.creditWaiters[0]!;
      if (slot.sendCredit < (w.partial ? 1 : w.n)) break;
      const got = Math.min(w.n, slot.sendCredit);
      slot.sendCredit -= got;
      slot.creditWaiters.shift();
      w.resolve(got);
    }
  }

  private rejectCredit(slot: ChannelSlot, e: Error): void {
    for (const w of slot.creditWaiters) w.reject(e);
    slot.creditWaiters = [];
  }

  private flushWindow(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
    if (this.state !== 'open') return;
    const entries: Array<{ channel: number; increment: number }> = [];
    for (const [id, slot] of this.slots) {
      if (slot.unreturned > 0 && slot.ch.state === 'open' && !slot.sentClose) {
        const increment = slot.unreturned + this.growWindow(slot, slot.unreturned);
        entries.push({ channel: id, increment });
        slot.recvCredit += increment;
        slot.unreturned = 0;
      }
    }
    for (let i = 0; i < entries.length; i += 10000) {
      void this.enqueueUrgent(windowUpdateFrame(entries.slice(i, i + 10000))).catch(ignore);
    }
  }

  // ─── Outbound scheduler ─────────────────────────────────────────────────────

  protected maxChannels(): number {
    return this.options.maxChannels ?? DEFAULT_MAX_CHANNELS;
  }

  private controlQueueLimit(): number {
    const q = this.options.maxControlQueue ?? 0;
    return q < 0 ? Infinity : q === 0 ? DEFAULT_MAX_CONTROL_QUEUE : q;
  }

  private enqueueUrgent(frame: Frame): Promise<void> {
    return this.enqueue(frame, null);
  }

  private enqueueData(channel: number, frame: Frame): Promise<void> {
    return this.enqueue(frame, channel);
  }

  private enqueue(frame: Frame, channel: number | null): Promise<void> {
    if (this.sendingClosed || this.state === 'closed')
      return Promise.reject(new Error('connection is closing'));
    return new Promise<void>((resolve, reject) => {
      const out: Out = { bytes: encodeFrame(frame), resolve, reject };
      if (channel === null) {
        const limit = this.controlQueueLimit();
        if (this.state === 'open' && this.urgentBytes + out.bytes.length > limit) {
          // The peer keeps sending frames that need a reply but doesn't read the replies. Close
          // rather than queue without bound. (Pausing reads instead could deadlock two such peers.)
          this.beginClose(
            CloseCode.RATE_LIMITED,
            `peer is not reading: over ${limit} bytes of control frames queued`,
            false,
          );
          reject(new Error('connection is closing'));
          return;
        }
        this.urgentBytes += out.bytes.length;
        this.urgent.push(out);
      } else {
        let q = this.dataQueues.get(channel);
        if (!q) this.dataQueues.set(channel, (q = []));
        if (q.length === 0) this.rr.push(channel);
        q.push(out);
      }
      void this.pump();
    });
  }

  private next(): Out | undefined {
    const u = this.urgent.shift();
    if (u) {
      this.urgentBytes -= u.bytes.length;
      return u;
    }
    const id = this.rr.shift();
    if (id === undefined) return undefined;
    const q = this.dataQueues.get(id)!;
    const out = q.shift()!;
    if (q.length > 0) this.rr.push(id);
    else this.dataQueues.delete(id);
    return out;
  }

  private async pump(): Promise<void> {
    const path = this.path;
    if (!path || path.pumping) return;
    path.pumping = true;
    try {
      // Stop as soon as the path is detached: what is still queued waits for the next one.
      for (let out = this.next(); out; out = this.path === path ? this.next() : undefined) {
        try {
          path.inFlight = out;
          const r = path.write(out.bytes);
          if (r) await r;
          out.resolve();
        } catch (e) {
          out.reject(e);
          if (this.path !== path) break;
          if (this.detachOnLoss()) {
            const reason = `transport failed: ${e instanceof Error ? e.message : String(e)}`;
            this.detachBasePath(reason);
          } else {
            this.rejectQueued(e);
          }
          break;
        }
      }
    } finally {
      path.inFlight = null;
      path.pumping = false;
      if (this.path === path || !this.path) {
        const ws = this.idleWaiters;
        this.idleWaiters = [];
        for (const w of ws) w();
      }
    }
  }

  /** Discard a channel's frames that are still waiting in the outbound queue. */
  private dropQueued(channel: number, e: unknown): void {
    const q = this.dataQueues.get(channel);
    if (!q) return;
    this.dataQueues.delete(channel);
    const i = this.rr.indexOf(channel);
    if (i >= 0) this.rr.splice(i, 1);
    for (const o of q) o.reject(e);
  }

  private rejectQueued(e: unknown): void {
    for (const o of this.urgent.splice(0)) o.reject(e);
    this.urgentBytes = 0;
    for (const q of this.dataQueues.values()) for (const o of q.splice(0)) o.reject(e);
    this.dataQueues.clear();
    this.rr.length = 0;
  }

  /** Resolve once every channel's queued sends and the outbound queue are flushed. */
  private async drain(): Promise<void> {
    await Promise.all([...this.slots.values()].map((s) => s.sendChain));
    if (this.path?.pumping || this.urgent.length > 0 || this.rr.length > 0) {
      await new Promise<void>((r) => this.idleWaiters.push(r));
    }
  }

  // ─── Close ──────────────────────────────────────────────────────────────────

  /** Send CLOSE and wait for the peer's CLOSE, the transport ending, or the timeout (core §9.1). */
  protected beginClose(code: number, reason: string | undefined, remote: boolean): void {
    if (this.state === 'closing' || this.state === 'closed') return;
    this.state = 'closing';
    this.closeInfo = { code, reason, remote };
    this.stopKeepalive();
    this.handshakeWaiter?.reject(new XumuxClosedError(code, reason, remote));
    this.handshakeWaiter = null;
    const frame = closeFrame(reason ? { code, reason } : { code });
    if (!this.path) {
      // Detached: there is no path to send CLOSE on, and CLOSE ends the session anyway.
      this.sendingClosed = true;
      this.finishClose(this.closeInfo);
      return;
    }
    const sent = this.enqueueUrgent(frame);
    this.closeSent = sent;
    this.sendingClosed = true;
    this.closeTimer = unref(
      setTimeout(() => this.finishClose(this.closeInfo!), this.closeTimeoutMs),
    );
    if (remote) {
      // Our CLOSE is the reply: close as soon as it is on the wire.
      void sent.then(
        () => this.finishClose(this.closeInfo!),
        () => this.finishClose(this.closeInfo!),
      );
    } else {
      sent.catch(() => this.finishClose(this.closeInfo!));
    }
  }

  private remoteClose(code: number, reason?: string): void {
    this.beginClose(code, reason, true);
  }

  private finishClose(info: CloseInfo): void {
    if (this.state === 'closed') return;
    this.state = 'closed';
    this.closeInfo = info;
    this.stopKeepalive();
    if (this.closeTimer) clearTimeout(this.closeTimer);
    if (this.flushTimer) clearTimeout(this.flushTimer);
    this.handshakeWaiter?.reject(new XumuxClosedError(info.code, info.reason, info.remote));
    this.handshakeWaiter = null;
    const err = new XumuxClosedError(info.code, info.reason, info.remote);
    this.rejectQueued(err);
    for (const p of this.controlPings.values()) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.controlPings.clear();
    for (const slot of [...this.slots.values()]) {
      slot.closeInfo =
        info.code === CloseCode.NORMAL
          ? info
          : { code: info.code, reason: info.reason, remote: info.remote };
      this.finishChannel(slot);
    }
    this.path?.close();
    this.resolveClosed(info);
    this.dispatchEvent(new CustomEvent<CloseInfo>('close', { detail: info }));
  }

  // ─── Keepalive ──────────────────────────────────────────────────────────────

  private startKeepalive(intervalS: number, timeoutS: number): void {
    this.keepaliveParams = { interval: intervalS, timeout: timeoutS };
    if (!(intervalS > 0)) return;
    const intervalMs = intervalS * 1000;
    const timeoutMs = timeoutS * 1000;
    const tick = Math.max(10, Math.min(intervalMs, timeoutMs) / 4);
    this.lastRx = performance.now();
    this.keepaliveTimer = unref(
      setInterval(() => {
        if (this.state !== 'open') return;
        const now = performance.now();
        if (this.keepalivePingAt !== null) {
          if (now - this.keepalivePingAt >= timeoutMs) {
            if (this.detachOnLoss()) {
              // Close the transport without CLOSE 4008, which would end the session.
              this.detachBasePath('keepalive timeout');
            } else {
              this.beginClose(CloseCode.KEEPALIVE_TIMEOUT, 'keepalive timeout', false);
              this.finishClose(this.closeInfo!);
            }
          }
          return;
        }
        if (now - this.lastRx >= intervalMs) {
          this.keepalivePingAt = now;
          void this.enqueueUrgent(
            pingFrame(CONTROL_CHANNEL, this.nextToken(this.controlPings)),
          ).catch(ignore);
        }
      }, tick),
    );
  }

  private stopKeepalive(): void {
    if (this.keepaliveTimer) clearInterval(this.keepaliveTimer);
    this.keepaliveTimer = null;
  }

  // ─── Utilities ──────────────────────────────────────────────────────────────

  private nowUs(): number {
    return Math.floor((performance.now() - this.epoch) * 1000) >>> 0;
  }

  /** Microseconds since the session started, mod 2^32, never repeating the previous token. */
  /** A token that differs from the last one and from every probe still pending in `pending`. */
  private nextToken(pending?: Map<number, unknown>): number {
    let t = this.nowUs();
    while (t === this.lastToken || pending?.has(t)) t = (t + 1) >>> 0;
    this.lastToken = t;
    return t;
  }

  private allocateId(): number | null {
    const start = this.role === 'client' ? 1 : 2;
    for (let id = start; id <= MAX_CHANNEL_ID; id += 2) if (!this.slots.has(id)) return id;
    return null;
  }

  /** Allocate `n` client IDs for HELLO channels. */
  protected allocateIds(n: number): number[] {
    const ids: number[] = [];
    for (let id = 1; ids.length < n && id <= MAX_CHANNEL_ID; id += 2)
      if (!this.slots.has(id)) ids.push(id);
    if (ids.length < n) throw new RangeError('too many channels');
    return ids;
  }
}

// ─── XumuxClient ─────────────────────────────────────────────────────────────

/** The side that sends HELLO. */
export class XumuxClient extends XumuxPeer {
  protected readonly role = 'client' as const;

  /** Send HELLO and wait for WELCOME. Rejects with XumuxClosedError if the server refuses. */
  async connect(opts: ConnectOptions = {}): Promise<ConnectResult> {
    if (this.state !== 'idle') throw new Error('connect() may only be called once');

    const requests = opts.channels ?? [];
    const ids = this.allocateIds(requests.length);
    const channels: ChannelRequest[] = requests.map((r, i) => ({ ...r, id: ids[i]! }));
    const extensionParams = pickParams(opts.extensionParams, opts.extensions ?? []);

    const hello: HelloMessage = {
      version: [...PROTOCOL_VERSION] as Version,
      ...(opts.application !== undefined && { application: opts.application }),
      ...(opts.extensions?.length && { extensions: opts.extensions }),
      ...(Object.keys(extensionParams).length && { extensionParams }),
      maxFramePayload: this.local.maxFramePayload,
      maxMessageSize: this.local.maxMessageSize,
      initialWindow: this.local.initialWindow,
      pingInterval: this.options.pingInterval ?? DEFAULT_PING_INTERVAL,
      pingTimeout: this.options.pingTimeout ?? DEFAULT_PING_TIMEOUT,
      ...(channels.length && { channels }),
      ...(opts.auth && { auth: opts.auth }),
    };
    const frame = helloFrame(hello);
    // Like every channel-0 payload, HELLO (with its extensionParams) is capped (core §3.1).
    if (frame.payload.length > CONTROL_MAX_PAYLOAD)
      throw new RangeError(
        `HELLO is ${frame.payload.length} bytes, over the ${CONTROL_MAX_PAYLOAD}-byte limit`,
      );
    this.state = 'handshaking';
    this.startReceiving();
    for (const req of channels) this.createSlot(req, true, 'opening');
    const waiting = this.awaitHandshake();
    void this.sendControl(frame).catch(ignore);
    const welcome = (await waiting) as WelcomeMessage;
    const extensions = [...new Set(welcome.extensions ?? [])];
    let welcomeParams: ExtensionParams = {};

    try {
      if (!versionsCompatible(welcome.version, PROTOCOL_VERSION as Version))
        throw protocolError(`incompatible server version ${welcome.version.join('.')}`);
      this.setRemoteLimits(welcome);
      const offered = new Set(opts.extensions ?? []);
      if (!extensions.every((e) => offered.has(e)))
        throw protocolError('WELCOME accepted an extension that was not requested');
      // Keys for extensions that were not accepted are ignored (core §3.8).
      welcomeParams = pickParams(welcome.extensionParams, extensions);
      for (const ext of extensions) {
        let ok: boolean;
        try {
          ok = opts.checkExtensionParams?.(ext, getParam(welcomeParams, ext)) ?? true;
        } catch {
          ok = false;
        }
        if (!ok)
          throw new XumuxProtocolError(
            CloseCode.INVALID_MESSAGE,
            `WELCOME extensionParams for ${ext} are invalid`,
          );
      }
      const answered = new Set<number>();
      const requested = new Set(ids);
      for (const c of [...(welcome.channels ?? []), ...(welcome.rejected ?? [])]) {
        if (!requested.has(c.id) || answered.has(c.id))
          throw protocolError(`WELCOME lists channel ${c.id} incorrectly`);
        answered.add(c.id);
      }
      if (answered.size !== ids.length)
        throw protocolError('WELCOME does not answer every HELLO channel');
    } catch (e) {
      this.afterHandshake = null;
      this.beginCloseFromError(e);
      throw e;
    }

    this.enterOpen(
      welcome.pingInterval ?? DEFAULT_PING_INTERVAL,
      welcome.pingTimeout ?? DEFAULT_PING_TIMEOUT,
    );
    for (const r of welcome.rejected ?? []) this.refuseOpening(this.slotFor(r.id)!, r);
    const accepted = new Map((welcome.channels ?? []).map((c) => [c.id, c]));
    const open: XumuxChannel[] = [];
    for (const id of ids) {
      const a = accepted.get(id);
      if (!a) continue;
      const slot = this.slotFor(id)!;
      this.acceptOpening(slot, a);
      open.push(slot.ch);
    }
    this.replayAfterHandshake();
    return {
      welcome,
      extensions,
      extensionParams: welcomeParams,
      channels: open,
      rejected: welcome.rejected ?? [],
    };
  }

  private beginCloseFromError(e: unknown): void {
    const code = e instanceof XumuxProtocolError ? e.code : CloseCode.PROTOCOL_ERROR;
    this.beginClose(code, e instanceof Error ? e.message : undefined, false);
  }
}

// ─── XumuxServer ─────────────────────────────────────────────────────────────

/** The side that answers HELLO. */
export class XumuxServer extends XumuxPeer {
  protected readonly role = 'server' as const;

  /** Wait for HELLO, validate it, decide on its channels, and send WELCOME (or CLOSE). */
  async accept(opts: AcceptOptions = {}): Promise<AcceptResult> {
    if (this.state !== 'idle') throw new Error('accept() may only be called once');
    const firstFrames = opts.firstFrames ?? {};
    const types = Object.keys(firstFrames).map(Number);
    for (const t of types)
      if (!Number.isInteger(t) || t < EXTENSION_CONTROL_TYPE_MIN || t > EXTENSION_CONTROL_TYPE_MAX)
        throw new RangeError(`first-frame type ${t} is outside the extension range 0x30-0x7f`);
    this.firstFrameTypes = new Set(types);
    this.state = 'handshaking';
    this.startReceiving();
    const first = await this.awaitHandshake();

    const refuse = (code: number, reason: string): never => {
      this.beginClose(code, reason, false);
      throw new XumuxClosedError(code, reason, false);
    };

    if ('firstFrame' in first) {
      let d: FirstFrameDecision;
      try {
        d = await firstFrames[first.firstFrame.type]!(first.firstFrame);
      } catch (e) {
        d = {
          code: e instanceof XumuxProtocolError ? e.code : CloseCode.PROTOCOL_ERROR,
          reason: e instanceof Error ? e.message : String(e),
        };
      }
      if (this.state !== 'handshaking')
        throw new XumuxClosedError(CloseCode.GOING_AWAY, 'closed during handshake');
      return refuse(d.code, d.reason ?? 'refused');
    }
    const hello = first as HelloMessage;

    if (!versionsCompatible(hello.version, PROTOCOL_VERSION as Version)) {
      refuse(CloseCode.VERSION_MISMATCH, `unsupported version ${hello.version.join('.')}`);
    }
    try {
      this.setRemoteLimits(hello);
    } catch (e) {
      refuse(CloseCode.INVALID_MESSAGE, (e as Error).message);
    }
    const seen = new Set<number>();
    for (const c of hello.channels ?? []) {
      if (c.id === 0 || c.id > MAX_CHANNEL_ID || c.id % 2 !== 1 || seen.has(c.id))
        refuse(CloseCode.PROTOCOL_ERROR, `invalid HELLO channel ID ${c.id}`);
      seen.add(c.id);
    }
    if (opts.authorize) {
      let verdict: void | { code: number; reason?: string };
      try {
        verdict = await opts.authorize(hello);
      } catch (e) {
        verdict = {
          code: CloseCode.AUTH_FAILED,
          reason: e instanceof Error ? e.message : String(e),
        };
      }
      if (verdict) refuse(verdict.code, verdict.reason ?? 'refused');
    }
    if (this.state !== 'handshaking')
      throw new XumuxClosedError(CloseCode.GOING_AWAY, 'closed during handshake');

    const supported = new Set(opts.extensions ?? []);
    const offered = pickParams(hello.extensionParams, hello.extensions ?? []);
    const extensions: string[] = [];
    const extensionParams: ExtensionParams = {};
    for (const ext of new Set(hello.extensions ?? [])) {
      if (!supported.has(ext)) continue;
      let d: ExtensionDecision;
      try {
        d = opts.negotiateExtension
          ? await opts.negotiateExtension(ext, getParam(offered, ext), hello)
          : true;
      } catch {
        d = false;
      }
      if (d === false) continue;
      extensions.push(ext);
      if (d !== true && d.params !== undefined) setParam(extensionParams, ext, d.params);
    }
    const accepted: ChannelAccept[] = [];
    const rejected: ChannelRefusal[] = [];
    const openReqs: ChannelRequest[] = [];
    for (const req of hello.channels ?? []) {
      if (accepted.length >= this.maxChannels()) {
        // No reason: a long list of refusals must still fit in WELCOME.
        rejected.push({ id: req.id, code: CloseCode.CHANNEL_LIMIT });
        continue;
      }
      const d = await this.decide(req);
      if (d === true) {
        accepted.push(this.effective(req));
        openReqs.push(req);
      } else {
        rejected.push({
          id: req.id,
          code: d.code,
          ...(d.reason !== undefined && { reason: d.reason }),
        });
      }
    }
    // The connection may have ended while the policy was deciding.
    if (this.state !== 'handshaking')
      throw new XumuxClosedError(CloseCode.GOING_AWAY, 'closed during handshake');

    const pingInterval = this.options.pingInterval ?? DEFAULT_PING_INTERVAL;
    const pingTimeout = this.options.pingTimeout ?? DEFAULT_PING_TIMEOUT;
    const welcome: WelcomeMessage = {
      version: [...PROTOCOL_VERSION] as Version,
      ...(extensions.length && { extensions }),
      ...(Object.keys(extensionParams).length && { extensionParams }),
      maxFramePayload: this.local.maxFramePayload,
      maxMessageSize: this.local.maxMessageSize,
      initialWindow: this.local.initialWindow,
      pingInterval,
      pingTimeout,
      ...(accepted.length && { channels: accepted }),
      ...(rejected.length && { rejected }),
    };
    let frame: Frame;
    try {
      frame = welcomeFrame(welcome);
    } catch (e) {
      // For example negotiateExtension returned params that JSON cannot encode.
      return refuse(
        CloseCode.GOING_AWAY,
        `cannot encode WELCOME: ${e instanceof Error ? e.message : String(e)}`,
      );
    }
    // The server's extensionParams count toward the 65536-byte WELCOME limit (core §3.8).
    if (frame.payload.length > CONTROL_MAX_PAYLOAD)
      refuse(
        CloseCode.GOING_AWAY,
        `WELCOME would be ${frame.payload.length} bytes, over the ${CONTROL_MAX_PAYLOAD}-byte limit`,
      );
    const channels = openReqs.map((req, i) => this.createSlot(req, false, 'open', accepted[i]).ch);
    void this.sendControl(frame).catch(ignore);
    this.enterOpen(pingInterval, pingTimeout);
    return {
      hello,
      extensions,
      extensionParams: pickParams(hello.extensionParams, extensions),
      channels,
    };
  }
}
