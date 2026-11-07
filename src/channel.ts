/**
 * XumuxChannel: one logical channel of a session.
 * @module channel
 */

import { CloseCode } from './codec/constants.js';
import { XumuxClosedError } from './errors.js';
import type { ChannelAccept } from './control/messages.js';

export type ChannelState = 'opening' | 'open' | 'closing' | 'closed';

/** One application message: the frame Type plus the (reassembled) payload. */
export interface ChannelMessage {
  type: number;
  payload: Uint8Array;
}

/** Effective delivery properties (what the binding actually provides). */
export type ChannelProperties = Omit<ChannelAccept, 'id'>;

/** Probe statistics, in milliseconds. SRTT/RTTVAR follow RFC 6298. */
export interface ProbeStats {
  rtt?: number;
  srtt?: number;
  rttvar?: number;
  probesSent: number;
  probesLost: number;
}

/** How the channel ended. */
export interface ChannelCloseInfo {
  code: number;
  reason?: string;
  /** True when the peer initiated the close. */
  remote: boolean;
}

/** What a channel needs from its session. Implemented by XumuxPeer. */
export interface ChannelHost {
  sendMessage(ch: XumuxChannel, type: number, payload: Uint8Array): Promise<void>;
  closeChannel(ch: XumuxChannel, code: number, reason?: string): Promise<ChannelCloseInfo>;
  pingChannel(channelId: number, timeoutMs: number): Promise<number>;
  consumed(ch: XumuxChannel, bytes: number): void;
  receiveWindow(ch: XumuxChannel): number;
}

/** Records RTT samples and loss for one channel (or the connection). */
export class ProbeStatsRecorder implements ProbeStats {
  rtt?: number;
  srtt?: number;
  rttvar?: number;
  probesSent = 0;
  probesLost = 0;

  sample(r: number): void {
    this.rtt = r;
    if (this.srtt === undefined || this.rttvar === undefined) {
      this.srtt = r;
      this.rttvar = r / 2;
    } else {
      this.rttvar = 0.75 * this.rttvar + 0.25 * Math.abs(this.srtt - r);
      this.srtt = 0.875 * this.srtt + 0.125 * r;
    }
  }

  snapshot(): ProbeStats {
    return {
      rtt: this.rtt,
      srtt: this.srtt,
      rttvar: this.rttvar,
      probesSent: this.probesSent,
      probesLost: this.probesLost,
    };
  }
}

/**
 * A channel. Obtained from `connect()` / `accept()` results, `openChannel()`,
 * or the session's `'channel'` event for peer-opened channels.
 *
 * Receiving: `for await (const msg of channel)` or `await channel.receive()`.
 * Reading a message returns its flow-control credit to the sender, so a channel
 * nobody reads from eventually stops the sender (backpressure).
 */
export class XumuxChannel implements AsyncIterable<ChannelMessage> {
  /** @internal */ _state: ChannelState;
  /** @internal */ _props: ChannelProperties;
  /** @internal */ readonly _stats = new ProbeStatsRecorder();

  private readonly inbox: ChannelMessage[] = [];
  private reader: {
    resolve: (m: ChannelMessage | null) => void;
    reject: (e: unknown) => void;
  } | null = null;
  private ended: ChannelCloseInfo | null = null;
  private resolveClosed!: (info: ChannelCloseInfo) => void;

  /** Resolves when the channel is fully closed. */
  readonly closed: Promise<ChannelCloseInfo>;

  constructor(
    private readonly host: ChannelHost,
    readonly id: number,
    readonly name: string,
    /** True if this side opened the channel. */
    readonly localOpener: boolean,
    requested: ChannelProperties,
    readonly metadata: Record<string, unknown> | undefined,
    state: ChannelState,
  ) {
    this._state = state;
    this._props = requested;
    this.closed = new Promise((r) => (this.resolveClosed = r));
  }

  get state(): ChannelState {
    return this._state;
  }

  /** Effective properties once open; the requested ones while opening. */
  get properties(): ChannelProperties {
    return this._props;
  }

  /**
   * Current receive window in bytes: the most this side lets the peer send ahead of what
   * the application has read. Starts at `initialWindow` and grows with auto-tuning.
   */
  get receiveWindow(): number {
    return this.host.receiveWindow(this);
  }

  /** Probe statistics for this channel. */
  get stats(): ProbeStats {
    return this._stats.snapshot();
  }

  /**
   * Send one message. Resolves once every frame has been handed to the transport,
   * which may wait for flow-control credit and transport backpressure.
   */
  send(type: number, payload: Uint8Array): Promise<void> {
    return this.host.sendMessage(this, type, payload);
  }

  /**
   * Next message, or `null` once the channel has closed normally and the inbox is empty.
   * Rejects with {@link XumuxClosedError} if the channel closed abnormally.
   */
  receive(): Promise<ChannelMessage | null> {
    const m = this.inbox.shift();
    if (m) {
      this.host.consumed(this, m.payload.length);
      return Promise.resolve(m);
    }
    if (this.ended) return this.endResult(this.ended);
    if (this.reader) return Promise.reject(new Error('receive() already pending on this channel'));
    return new Promise((resolve, reject) => (this.reader = { resolve, reject }));
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<ChannelMessage> {
    for (;;) {
      const m = await this.receive();
      if (m === null) return;
      yield m;
    }
  }

  /** Measure round-trip time on this channel with a PING (ms). */
  ping(timeoutMs = 5000): Promise<number> {
    return this.host.pingChannel(this.id, timeoutMs);
  }

  /** Close the channel (two-step, core §5.4). Resolves when fully closed. */
  close(code: number = CloseCode.NORMAL, reason?: string): Promise<ChannelCloseInfo> {
    return this.host.closeChannel(this, code, reason);
  }

  // ─── Session-side hooks ─────────────────────────────────────────────────────

  /** @internal Deliver a received message. */
  _deliver(m: ChannelMessage): void {
    if (this.reader) {
      const r = this.reader;
      this.reader = null;
      this.host.consumed(this, m.payload.length);
      r.resolve(m);
    } else {
      this.inbox.push(m);
    }
  }

  /** @internal The channel is fully closed. Queued messages stay readable. */
  _finish(info: ChannelCloseInfo): void {
    if (this._state === 'closed') return;
    this._state = 'closed';
    this.ended = info;
    if (this.reader && this.inbox.length === 0) {
      const r = this.reader;
      this.reader = null;
      this.endResult(info).then(r.resolve, r.reject);
    }
    this.resolveClosed(info);
  }

  private endResult(info: ChannelCloseInfo): Promise<null> {
    return info.code === CloseCode.NORMAL
      ? Promise.resolve(null)
      : Promise.reject(new XumuxClosedError(info.code, info.reason, info.remote));
  }
}
