/**
 * xumux 0.2 session: XumuxPeer, XumuxClient, XumuxServer.
 *
 * One transport → one session → N channels. The session owns framing, magic bytes,
 * limits, the outbound scheduler (control first, then channels round-robin), flow
 * control, fragmentation, probes, keepalive and the close procedure.
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
  DEFAULT_MAX_FRAME_PAYLOAD,
  DEFAULT_MAX_MESSAGE_SIZE,
  DEFAULT_PING_INTERVAL,
  DEFAULT_PING_TIMEOUT,
  FrameFlags,
  MAX_CHANNEL_ID,
  MAX_WINDOW,
  MIN_MAX_FRAME_PAYLOAD,
  PROTOCOL_VERSION,
  ProbeType,
  XMUX_MAGIC,
} from './codec/constants.js';
import { decodeFrame, StreamFramer, validateFrame } from './codec/decode-frame.js';
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
}

export interface ConnectOptions {
  application?: string;
  extensions?: string[];
  auth?: Record<string, unknown>;
  /** Channels to open in HELLO. IDs are assigned by the client. */
  channels?: Array<Omit<ChannelRequest, 'id'>>;
}

export interface ConnectResult {
  welcome: WelcomeMessage;
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
}

export interface AcceptResult {
  hello: HelloMessage;
  /** HELLO channels this server accepted. */
  channels: XumuxChannel[];
}

/** Detail of the `'close'` event. */
export type CloseInfo = ChannelCloseInfo;

/** Detail of the `'channel-close'` event. */
export interface ChannelCloseEventDetail extends ChannelCloseInfo {
  channel: XumuxChannel;
}

type SessionState = 'idle' | 'handshaking' | 'open' | 'closing' | 'closed';

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

/** Per-channel session state that the channel object doesn't need to see. */
interface ChannelSlot {
  ch: XumuxChannel;
  sentClose: boolean;
  recvClose: boolean;
  closeInfo?: ChannelCloseInfo;
  sendCredit: number;
  creditWaiters: Array<{ n: number; resolve: () => void; reject: (e: unknown) => void }>;
  recvCredit: number;
  unreturned: number;
  reasm: { type: number; parts: Uint8Array[]; size: number } | null;
  held: Frame[];
  heldBytes: number;
  sendChain: Promise<void>;
  open?: { resolve: (ch: XumuxChannel) => void; reject: (e: Error) => void };
  pings: Map<number, PendingPing>;
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
  if (l.maxFramePayload < MIN_MAX_FRAME_PAYLOAD)
    return `maxFramePayload must be ≥ ${MIN_MAX_FRAME_PAYLOAD}`;
  if (l.maxMessageSize < l.maxFramePayload) return 'maxMessageSize must be ≥ maxFramePayload';
  if (l.initialWindow < l.maxMessageSize) return 'initialWindow must be ≥ maxMessageSize';
  if (l.initialWindow > MAX_WINDOW) return 'initialWindow must be ≤ 2^31-1';
  return null;
}

/** Core §3.5: 0.x needs major and minor equal; ≥1.x needs major equal. */
export function versionsCompatible(a: Version, b: Version): boolean {
  if (a[0] !== b[0]) return false;
  return a[0] !== 0 || a[1] === b[1];
}

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
  private readonly framer: StreamFramer | null;
  private magicSent: boolean;

  // Outbound scheduler
  private readonly urgent: Out[] = [];
  private readonly dataQueues = new Map<number, Out[]>();
  private readonly rr: number[] = [];
  private pumping = false;
  private idleWaiters: Array<() => void> = [];
  private sendingClosed = false;

  // Flow-control credit return
  private flushTimer: ReturnType<typeof setTimeout> | null = null;

  // Probes and keepalive
  private readonly epoch = performance.now();
  private lastToken = -1;
  private readonly controlPings = new Map<number, PendingPing>();
  private readonly controlStats = new ProbeStatsRecorder();
  private lastRx = performance.now();
  private keepaliveTimer: ReturnType<typeof setInterval> | null = null;
  private keepalivePingAt: number | null = null;

  // Close
  private closeTimer: ReturnType<typeof setTimeout> | null = null;
  private closeInfo: CloseInfo | null = null;
  private closeSent: Promise<void> | null = null;
  private resolveClosed!: (info: CloseInfo) => void;
  /** Resolves when the connection has closed. */
  readonly closed: Promise<CloseInfo>;

  // Handshake plumbing
  protected handshakeWaiter: {
    resolve: (m: HelloMessage | WelcomeMessage) => void;
    reject: (e: Error) => void;
  } | null = null;
  private receiving = false;
  /** Client only: frames received after WELCOME but before connect() has processed it. */
  protected afterHandshake: Frame[] | null = null;

  constructor(
    protected readonly transport: TransportAdapter,
    options: XumuxOptions = {},
  ) {
    super();
    this.options = options;
    this.local = resolveLimits(options);
    const err = checkLimits(this.local);
    if (err) throw new RangeError(err);
    const stream = transport.kind === 'stream';
    const magic = stream && transport.magic !== false;
    this.framer = stream ? new StreamFramer(this.local.maxFramePayload, magic) : null;
    this.magicSent = !magic;
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
    void this.enqueueUrgent(openChannelFrame({ ...request, id })).catch(() => undefined);
    return p;
  }

  /** Graceful close: flush queued data, CLOSE, wait for the peer's CLOSE, close the transport. */
  async close(code: number = CloseCode.NORMAL, reason?: string): Promise<CloseInfo> {
    if (this.state === 'closed' || this.state === 'closing') return this.closed;
    if (this.state === 'open') {
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
    slot.sendChain = p.catch(() => undefined);
    return p;
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
        .catch(() => undefined)
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
      void this.enqueueUrgent(pingFrame(channelId, token)).catch(() => undefined);
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

  // ─── Handshake helpers for subclasses ───────────────────────────────────────

  /** Start reading from the transport (idempotent). */
  protected startReceiving(): void {
    if (this.receiving) return;
    this.receiving = true;
    void this.receiveLoop();
  }

  protected get closeTimeoutMs(): number {
    return this.options.closeTimeoutMs ?? DEFAULT_CLOSE_TIMEOUT_MS;
  }

  protected get handshakeTimeoutMs(): number {
    return this.options.handshakeTimeoutMs ?? DEFAULT_HELLO_TIMEOUT_MS;
  }

  /** Wait for HELLO or WELCOME (whichever the role expects), a CLOSE, or the timeout. */
  protected awaitHandshake(): Promise<HelloMessage | WelcomeMessage> {
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
    };
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
    this.slots.delete(slot.ch.id);
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

  private async receiveLoop(): Promise<void> {
    try {
      for await (const chunk of this.transport.receive()) {
        if (this.state === 'closed') break;
        const frames = this.framer
          ? this.framer.push(chunk)
          : [decodeFrame(chunk, this.local.maxFramePayload)];
        for (const f of frames) {
          this.handleFrame(f);
          if ((this.state as SessionState) === 'closed') break;
        }
      }
    } catch (e) {
      // The input can't be parsed any further. fail() queues CLOSE with the right code;
      // let it reach the wire, then finish with that cause (core §9.1).
      this.fail(e);
    }
    // The input ended. If we are closing (for example the peer sent CLOSE and then
    // shut its write side), let our CLOSE reach the wire before finishing; the close
    // timer bounds the wait (core §9.1).
    if (this.state === 'closing') await this.closeSent?.catch(() => undefined);
    if (this.state !== 'closed')
      this.finishClose(
        this.state === 'closing' && this.closeInfo
          ? this.closeInfo
          : { code: CloseCode.GOING_AWAY, reason: 'transport closed', remote: true },
      );
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
      void this.enqueueUrgent(pongFrame(f.channel, token)).catch(() => undefined);
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
        ).catch(() => undefined);
    }
  }

  private onOpenChannel(req: ChannelRequest): void {
    const peerParity = this.role === 'server' ? 1 : 0;
    if (req.id === 0 || req.id > MAX_CHANNEL_ID || req.id % 2 !== peerParity)
      throw protocolError(`OPEN_CHANNEL with invalid ID ${req.id}`);
    if (this.slots.has(req.id) || this.pendingRemote.has(req.id))
      throw protocolError(`OPEN_CHANNEL for ID ${req.id}, which is in use`);
    this.pendingRemote.add(req.id);
    void this.decide(req).then((d) => {
      this.pendingRemote.delete(req.id);
      if (this.state !== 'open') return;
      if (d === true) {
        const props = this.effective(req);
        const slot = this.createSlot(req, false, 'open', props);
        void this.enqueueUrgent(channelAckFrame(props)).catch(() => undefined);
        this.dispatchEvent(new CustomEvent('channel', { detail: slot.ch }));
      } else {
        void this.enqueueUrgent(
          channelRejectFrame({ id: req.id, code: d.code, reason: d.reason }),
        ).catch(() => undefined);
      }
    });
  }

  private onCloseChannel(msg: { id: number; code?: number; reason?: string }): void {
    if (msg.id === CONTROL_CHANNEL) {
      void this.enqueueUrgent(
        errorFrame({ code: CloseCode.PROTOCOL_ERROR, reason: 'CLOSE_CHANNEL for channel 0' }),
      ).catch(() => undefined);
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
      ).catch(() => undefined);
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
    ).catch(() => undefined); // the close completes when the peer's CLOSE_CHANNEL arrives
  }

  /** Close one channel because of a channel-level protocol error. */
  private channelError(slot: ChannelSlot, code: number, reason: string): void {
    slot.reasm = null;
    slot.closeInfo = { code, reason, remote: false };
    this.sendCloseChannel(slot, code, reason);
  }

  private finishChannel(slot: ChannelSlot): void {
    if (slot.ch.state === 'closed') return;
    this.slots.delete(slot.ch.id);
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

  private takeCredit(slot: ChannelSlot, n: number): Promise<void> {
    if (slot.sentClose || slot.ch.state !== 'open')
      return Promise.reject(new Error(`channel ${slot.ch.id} closed`));
    if (slot.creditWaiters.length === 0 && slot.sendCredit >= n) {
      slot.sendCredit -= n;
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => slot.creditWaiters.push({ n, resolve, reject }));
  }

  private wakeCredit(slot: ChannelSlot): void {
    while (slot.creditWaiters.length > 0) {
      const w = slot.creditWaiters[0]!;
      if (slot.sendCredit < w.n) break;
      slot.sendCredit -= w.n;
      slot.creditWaiters.shift();
      w.resolve();
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
        entries.push({ channel: id, increment: slot.unreturned });
        slot.recvCredit += slot.unreturned;
        slot.unreturned = 0;
      }
    }
    for (let i = 0; i < entries.length; i += 10000) {
      void this.enqueueUrgent(windowUpdateFrame(entries.slice(i, i + 10000))).catch(
        () => undefined,
      );
    }
  }

  // ─── Outbound scheduler ─────────────────────────────────────────────────────

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
    if (u) return u;
    const id = this.rr.shift();
    if (id === undefined) return undefined;
    const q = this.dataQueues.get(id)!;
    const out = q.shift()!;
    if (q.length > 0) this.rr.push(id);
    else this.dataQueues.delete(id);
    return out;
  }

  private async pump(): Promise<void> {
    if (this.pumping) return;
    this.pumping = true;
    try {
      for (let out = this.next(); out; out = this.next()) {
        try {
          if (!this.magicSent) {
            this.magicSent = true;
            const r = this.transport.send(new Uint8Array(XMUX_MAGIC));
            if (r) await r;
          }
          const r = this.transport.send(out.bytes);
          if (r) await r;
          out.resolve();
        } catch (e) {
          out.reject(e);
          this.rejectQueued(e);
          break;
        }
      }
    } finally {
      this.pumping = false;
      const ws = this.idleWaiters;
      this.idleWaiters = [];
      for (const w of ws) w();
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
    for (const q of this.dataQueues.values()) for (const o of q.splice(0)) o.reject(e);
    this.dataQueues.clear();
    this.rr.length = 0;
  }

  /** Resolve once every channel's queued sends and the outbound queue are flushed. */
  private async drain(): Promise<void> {
    await Promise.all([...this.slots.values()].map((s) => s.sendChain));
    if (this.pumping || this.urgent.length > 0 || this.rr.length > 0) {
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
    try {
      this.transport.close();
    } catch {
      /* already closed */
    }
    this.resolveClosed(info);
    this.dispatchEvent(new CustomEvent<CloseInfo>('close', { detail: info }));
  }

  // ─── Keepalive ──────────────────────────────────────────────────────────────

  private startKeepalive(intervalS: number, timeoutS: number): void {
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
            this.beginClose(CloseCode.KEEPALIVE_TIMEOUT, 'keepalive timeout', false);
            this.finishClose(this.closeInfo!);
          }
          return;
        }
        if (now - this.lastRx >= intervalMs) {
          this.keepalivePingAt = now;
          void this.enqueueUrgent(
            pingFrame(CONTROL_CHANNEL, this.nextToken(this.controlPings)),
          ).catch(() => undefined);
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
    this.state = 'handshaking';
    this.startReceiving();

    const requests = opts.channels ?? [];
    const ids = this.allocateIds(requests.length);
    const channels: ChannelRequest[] = requests.map((r, i) => ({ ...r, id: ids[i]! }));
    for (const req of channels) this.createSlot(req, true, 'opening');

    const hello: HelloMessage = {
      version: [...PROTOCOL_VERSION] as Version,
      ...(opts.application !== undefined && { application: opts.application }),
      ...(opts.extensions?.length && { extensions: opts.extensions }),
      maxFramePayload: this.local.maxFramePayload,
      maxMessageSize: this.local.maxMessageSize,
      initialWindow: this.local.initialWindow,
      pingInterval: this.options.pingInterval ?? DEFAULT_PING_INTERVAL,
      pingTimeout: this.options.pingTimeout ?? DEFAULT_PING_TIMEOUT,
      ...(channels.length && { channels }),
      ...(opts.auth && { auth: opts.auth }),
    };
    const waiting = this.awaitHandshake();
    void this.sendControl(helloFrame(hello)).catch(() => undefined);
    const welcome = (await waiting) as WelcomeMessage;

    try {
      if (!versionsCompatible(welcome.version, PROTOCOL_VERSION as Version))
        throw protocolError(`incompatible server version ${welcome.version.join('.')}`);
      this.setRemoteLimits(welcome);
      const offered = new Set(opts.extensions ?? []);
      if (!(welcome.extensions ?? []).every((e) => offered.has(e)))
        throw protocolError('WELCOME accepted an extension that was not requested');
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
    return { welcome, channels: open, rejected: welcome.rejected ?? [] };
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
    this.state = 'handshaking';
    this.startReceiving();
    const hello = (await this.awaitHandshake()) as HelloMessage;

    const refuse = (code: number, reason: string): never => {
      this.beginClose(code, reason, false);
      throw new XumuxClosedError(code, reason, false);
    };

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
    const extensions = (hello.extensions ?? []).filter((e) => supported.has(e));
    const accepted: ChannelAccept[] = [];
    const rejected: ChannelRefusal[] = [];
    const openReqs: ChannelRequest[] = [];
    for (const req of hello.channels ?? []) {
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
      maxFramePayload: this.local.maxFramePayload,
      maxMessageSize: this.local.maxMessageSize,
      initialWindow: this.local.initialWindow,
      pingInterval,
      pingTimeout,
      ...(accepted.length && { channels: accepted }),
      ...(rejected.length && { rejected }),
    };
    const channels = openReqs.map((req, i) => this.createSlot(req, false, 'open', accepted[i]).ch);
    void this.sendControl(welcomeFrame(welcome)).catch(() => undefined);
    this.enterOpen(pingInterval, pingTimeout);
    return { hello, channels };
  }
}
