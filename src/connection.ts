/**
 * XumuxPeer base class + XumuxClient and XumuxServer implementations.
 *
 * XumuxPeer wires together the transport, codec, control handler, and channel map.
 * XumuxClient sends HELLO and awaits WELCOME (client role).
 * XumuxServer awaits HELLO and sends WELCOME (server role).
 *
 * @module connection
 */

import type { TransportAdapter } from './types.js';
import { encodeFrame } from './codec/encode-frame.js';
import { decodeFrame } from './codec/decode-frame.js';
import { Reassembler } from './codec/fragment.js';
import { CONTROL_CHANNEL, CloseCode } from './codec/constants.js';
import type { Frame } from './codec/frame.js';
import {
  ControlHandler,
  type ControlHandlerOptions,
} from './control/handler.js';
import type {
  HelloMessage,
  WelcomeMessage,
  ChannelAckMessage,
  OpenChannelMessage,
} from './control/messages.js';
import { createChannel, type XumuxChannel, type ChannelController } from './channel.js';

// ─── Options ─────────────────────────────────────────────────────────────────

/** Options for creating a client or server peer. */
export interface XumuxOptions extends ControlHandlerOptions {
  /** Maximum payload size per frame (fragments used above this). Default: 65535. */
  maxPayloadSize?: number;
  /** Application protocol name (sent in HELLO). */
  application?: string;
}

// ─── Events ──────────────────────────────────────────────────────────────────

/** Emitted when a channel is opened by the remote peer. */
export interface ChannelOpenEvent extends Event {
  readonly detail: XumuxChannel;
}

/** Emitted when the connection closes. */
export interface CloseEvent extends Event {
  readonly detail: { code: number; reason?: string };
}

/** Emitted on a non-fatal protocol error. */
export interface ErrorEvent extends Event {
  readonly detail: Error;
}

// ─── XumuxPeer ────────────────────────────────────────────────────────────────

/**
 * Base class for xumux connections.
 *
 * Extends `EventTarget` so consumers can call `addEventListener('channel-open', ...)` etc.
 *
 * Events:
 * - `'channel-open'` — A remote peer opened a new channel (detail: XumuxChannel).
 * - `'channel-close'` — A remote peer closed a channel (detail: { id, reason }).
 * - `'error'` — Non-fatal protocol error received (detail: Error).
 * - `'close'` — Connection closed (detail: { code, reason }).
 */
export class XumuxPeer extends EventTarget {
  protected readonly controlHandler: ControlHandler;
  private readonly channelMap = new Map<number, ChannelController>();
  private readonly reassemblers = new Map<number, Reassembler>();
  private readonly maxPayloadSize: number;
  private receiveLoopRunning = false;

  constructor(
    protected readonly transport: TransportAdapter,
    options: XumuxOptions = {},
  ) {
    super();
    this.maxPayloadSize = options.maxPayloadSize ?? 65535;

    this.controlHandler = new ControlHandler(
      (frameBytes) => this.transport.send(frameBytes),
      {
        onChannelOpen: (msg) => this.handleRemoteChannelOpen(msg),
        onChannelClose: (id, reason) => this.handleRemoteChannelClose(id, reason),
        onError: (msg) => {
          this.dispatchEvent(
            new CustomEvent('error', {
              detail: new Error(`[${msg.code}] ${msg.reason ?? 'protocol error'}`),
            }),
          );
        },
        onClose: (msg) => {
          this.dispatchEvent(new CustomEvent('close', { detail: msg }));
        },
        onTransportClose: () => {
          this.transport.close();
          // Terminate all open channels
          for (const ctrl of this.channelMap.values()) {
            ctrl.terminate(new Error('Connection closed'));
          }
          this.channelMap.clear();
          this.reassemblers.clear();
        },
      },
      options,
    );
  }

  /** All currently open channels. */
  get channels(): ReadonlyMap<number, XumuxChannel> {
    const out = new Map<number, XumuxChannel>();
    for (const [id, ctrl] of this.channelMap) {
      out.set(id, ctrl.channel);
    }
    return out;
  }

  /**
   * Send a frame on a data channel.
   * (Used by WritableStream handlers via `createChannel`.)
   */
  protected sendFrame(frame: Frame): void {
    this.transport.send(encodeFrame(frame));
  }

  /**
   * Start the receive loop (fire-and-forget; errors dispatch 'error' event).
   */
  protected startReceiveLoop(): void {
    if (this.receiveLoopRunning) return;
    this.receiveLoopRunning = true;
    void this.receiveLoop().catch((err: unknown) => {
      this.dispatchEvent(new CustomEvent('error', { detail: err }));
    });
  }

  private async receiveLoop(): Promise<void> {
    for await (const frameBytes of this.transport.receive()) {
      const frame = decodeFrame(frameBytes);
      this.processFrame(frame);
    }
    // Transport EOF
    if (this.controlHandler.connectionState !== 'closed') {
      this.controlHandler.destroy();
      this.dispatchEvent(
        new CustomEvent('close', { detail: { code: CloseCode.GOING_AWAY, reason: 'transport closed' } }),
      );
    }
  }

  private processFrame(frame: Frame): void {
    if (frame.channel === CONTROL_CHANNEL) {
      this.controlHandler.handleFrame(frame);
      return;
    }

    // Data frame — reassemble fragments and enqueue
    const ctrl = this.channelMap.get(frame.channel);
    if (!ctrl) {
      // Unknown channel — send ERROR
      this.controlHandler.sendError({
        code: CloseCode.CHANNEL_NOT_FOUND,
        channel: frame.channel,
        reason: `Unknown channel ${frame.channel}`,
      });
      return;
    }

    let reassembler = this.reassemblers.get(frame.channel);
    if (!reassembler) {
      reassembler = new Reassembler();
      this.reassemblers.set(frame.channel, reassembler);
    }

    try {
      const payload = reassembler.push(frame);
      if (payload !== null) {
        ctrl.enqueue(payload);
      }
    } catch (err) {
      this.controlHandler.sendError({
        code: CloseCode.PROTOCOL_ERROR,
        channel: frame.channel,
        reason: err instanceof Error ? err.message : String(err),
      });
    }
  }

  /**
   * Handle a remote OPEN_CHANNEL request (server-side).
   * Assigns a channel ID and sends CHANNEL_ACK.
   */
  private handleRemoteChannelOpen(msg: ChannelAckMessage): void {
    // Assign next available channel ID
    const id = this.allocateChannelId();
    if (id === null) {
      this.controlHandler.sendChannelReject({
        requestId: msg.requestId,
        code: CloseCode.CHANNEL_FULL,
        reason: 'Max channels reached',
      });
      return;
    }

    const ctrl = createChannel(id, msg.name, (f) => this.sendFrame(f), this.maxPayloadSize);
    this.channelMap.set(id, ctrl);

    this.controlHandler.sendChannelAck({ requestId: msg.requestId, id, name: msg.name });

    this.dispatchEvent(new CustomEvent('channel-open', { detail: ctrl.channel }));
  }

  private handleRemoteChannelClose(channelId: number, reason?: string): void {
    const ctrl = this.channelMap.get(channelId);
    if (ctrl) {
      ctrl.terminate(reason ? new Error(reason) : undefined);
      this.channelMap.delete(channelId);
      this.reassemblers.delete(channelId);
    }
    this.dispatchEvent(
      new CustomEvent('channel-close', { detail: { id: channelId, reason } }),
    );
  }

  private allocateChannelId(): number | null {
    for (let id = 1; id <= 254; id++) {
      if (!this.channelMap.has(id)) return id;
    }
    return null;
  }

  /**
   * Register a channel that was established during the HELLO/WELCOME handshake.
   * Called by XumuxClient and XumuxServer after a successful handshake.
   */
  protected registerChannel(id: number, name: string): XumuxChannel {
    const ctrl = createChannel(id, name, (f) => this.sendFrame(f), this.maxPayloadSize);
    this.channelMap.set(id, ctrl);
    return ctrl.channel;
  }

  /**
   * Dynamically open a new channel. The remote peer will respond with CHANNEL_ACK
   * or CHANNEL_REJECT.
   */
  async openChannel(
    request: Omit<OpenChannelMessage, 'requestId'>,
  ): Promise<XumuxChannel> {
    const ack = await this.controlHandler.openChannel(request);
    const ctrl = createChannel(ack.id, ack.name, (f) => this.sendFrame(f), this.maxPayloadSize);
    this.channelMap.set(ack.id, ctrl);
    return ctrl.channel;
  }

  /**
   * Close a specific channel by ID.
   */
  closeChannel(channelId: number, reason?: string): void {
    const ctrl = this.channelMap.get(channelId);
    if (ctrl) {
      ctrl.terminate();
      this.channelMap.delete(channelId);
      this.reassemblers.delete(channelId);
    }
    this.controlHandler.closeChannel(channelId, reason);
  }

  /**
   * Initiate a graceful connection close.
   */
  close(code = CloseCode.NORMAL, reason?: string): Promise<void> {
    return this.controlHandler.close(code, reason);
  }
}

// ─── XumuxClient ─────────────────────────────────────────────────────────────

/**
 * xumux client — initiates the HELLO/WELCOME handshake.
 *
 * @example
 * ```ts
 * const [clientT, serverT] = createInMemoryPair();
 * const client = new XumuxClient(clientT);
 * const channels = await client.connect();
 * ```
 */
export class XumuxClient extends XumuxPeer {
  /**
   * Connect to the server: sends HELLO and resolves when WELCOME is received.
   *
   * @param hello - Optional overrides for the HELLO message (channels, auth, etc.).
   * @returns Map of channel name → XumuxChannel for any channels negotiated in HELLO.
   */
  async connect(hello?: Partial<HelloMessage>): Promise<Map<string, XumuxChannel>> {
    this.startReceiveLoop();

    const helloMsg: HelloMessage = {
      version: [0, 1, 0],
      channels: [],
      ...hello,
    };

    const welcome = await this.controlHandler.sendHello(helloMsg);

    const channelsByName = new Map<string, XumuxChannel>();
    for (const assigned of welcome.channels) {
      const ch = this.registerChannel(assigned.id, assigned.name);
      channelsByName.set(assigned.name, ch);
    }

    return channelsByName;
  }
}

// ─── XumuxServer ─────────────────────────────────────────────────────────────

/**
 * xumux server — awaits HELLO and responds with WELCOME.
 *
 * @example
 * ```ts
 * const server = new XumuxServer(serverTransport);
 * const channels = await server.accept();
 * ```
 */
export class XumuxServer extends XumuxPeer {
  /** Next channel ID to assign (server starts at 1, increments per request). */
  private nextChannelId = 1;

  /**
   * Accept an incoming connection: waits for HELLO and responds with WELCOME.
   *
   * @param welcomeOverrides - Optional overrides for the WELCOME message.
   * @returns Map of channel name → XumuxChannel for any channels opened during HELLO.
   */
  async accept(welcomeOverrides?: Partial<WelcomeMessage>): Promise<Map<string, XumuxChannel>> {
    this.startReceiveLoop();

    const hello = await this.controlHandler.awaitHello();

    // Assign IDs to requested channels
    const assignedChannels: Array<{ name: string; id: number }> = [];
    for (const req of hello.channels ?? []) {
      const id = this.nextChannelId++;
      assignedChannels.push({ name: req.name, id });
    }

    const welcome: WelcomeMessage = {
      version: [0, 1, 0],
      channels: assignedChannels,
      maxMessageSize: hello.maxMessageSize,
      pingInterval: hello.pingInterval,
      pingTimeout: hello.pingTimeout,
      ...welcomeOverrides,
    };

    this.controlHandler.sendWelcome(welcome);

    const channelsByName = new Map<string, XumuxChannel>();
    for (const { name, id } of assignedChannels) {
      const ch = this.registerChannel(id, name);
      channelsByName.set(name, ch);
    }

    return channelsByName;
  }
}
