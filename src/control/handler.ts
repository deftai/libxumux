/**
 * xumux control channel handler — lifecycle state machine.
 *
 * Manages the connection state (connecting → connected → closing → closed),
 * HELLO/WELCOME handshake, dynamic channel management (OPEN_CHANNEL / CHANNEL_ACK),
 * keepalive (PING/PONG), and graceful close.
 *
 * @module control/handler
 */

import { ControlType, CloseCode, DEFAULT_PING_INTERVAL, DEFAULT_PING_TIMEOUT } from '../codec/constants.js';
import type { Frame } from '../codec/frame.js';
import {
  encodePing, encodePong, decodePing, decodePong,
  encodeHello, encodeWelcome,
  encodeOpenChannel, encodeChannelAck, encodeChannelReject, encodeCloseChannel,
  encodeClose, encodeError,
  decodeHello, decodeWelcome,
  decodeOpenChannel, decodeChannelAck, decodeChannelReject, decodeCloseChannel,
  decodeClose, decodeError,
} from './codec.js';
import type {
  HelloMessage, WelcomeMessage,
  OpenChannelMessage, ChannelAckMessage, ChannelRejectMessage,
  CloseMessage, ErrorMessage,
} from './messages.js';

/** Connection state machine states. */
export type ConnectionState =
  | 'connecting'
  | 'handshaking'
  | 'connected'
  | 'closing'
  | 'closed';

/** Options for the control handler. */
export interface ControlHandlerOptions {
  /** PING interval in seconds. 0 = disabled. Default: 30. */
  pingInterval?: number;
  /** PONG timeout in seconds. Default: 10. */
  pingTimeout?: number;
  /** Timestamp baseline for PING payloads (ms). Defaults to Date.now() at construction. */
  connectionStart?: number;
}

/** Callbacks the ControlHandler calls on lifecycle events. */
export interface ControlHandlerCallbacks {
  /** Called when a remote peer requests to open a channel. */
  onChannelOpen(msg: ChannelAckMessage): void;
  /** Called when a remote peer closes a channel. */
  onChannelClose(channelId: number, reason?: string): void;
  /** Called on a non-fatal ERROR message from the remote peer. */
  onError(msg: ErrorMessage): void;
  /** Called when the connection closes (either side). */
  onClose(msg: CloseMessage): void;
  /** Called when the transport must be closed immediately. */
  onTransportClose(): void;
}

/**
 * Control channel handler for a single xumux connection.
 *
 * The handler is transport-agnostic: it receives Frame objects via `handleFrame()`
 * and sends frame bytes via the `sendRaw` callback provided at construction time.
 */
export class ControlHandler {
  private state: ConnectionState = 'connecting';
  private pingTimer: ReturnType<typeof setInterval> | null = null;
  private pongDeadline: ReturnType<typeof setTimeout> | null = null;
  private readonly connectionStart: number;

  // Pending OPEN_CHANNEL requests: requestId → {resolve, reject}
  private pendingOpen = new Map<
    number,
    { resolve: (ack: ChannelAckMessage) => void; reject: (err: Error) => void }
  >();
  private nextRequestId = 1;

  // Handshake promises
  private helloResolver: ((msg: HelloMessage) => void) | null = null;
  private welcomeResolver: ((msg: WelcomeMessage) => void) | null = null;
  private closeResolver: (() => void) | null = null;

  // Buffered messages received before resolvers were set
  private bufferedHello: HelloMessage | null = null;
  private bufferedWelcome: WelcomeMessage | null = null;

  constructor(
    private readonly sendRaw: (frameBytes: Uint8Array) => void,
    private readonly callbacks: ControlHandlerCallbacks,
    private readonly options: ControlHandlerOptions = {},
  ) {
    this.connectionStart = options.connectionStart ?? Date.now();
  }

  /** Current connection state. */
  get connectionState(): ConnectionState {
    return this.state;
  }

  // ─── Handshake ──────────────────────────────────────────────────────────────

  /** (Client) Send HELLO and return a Promise that resolves with the WELCOME. */
  sendHello(hello: HelloMessage): Promise<WelcomeMessage> {
    this.state = 'handshaking';
    this.sendRaw(encodeHello(hello));
    // Check if WELCOME already arrived (synchronous in-memory transport)
    if (this.bufferedWelcome !== null) {
      const welcome = this.bufferedWelcome;
      this.bufferedWelcome = null;
      return Promise.resolve(welcome);
    }
    return new Promise<WelcomeMessage>((resolve) => {
      this.welcomeResolver = resolve;
    });
  }

  /** (Server) Wait for the client's HELLO. Returns a Promise that resolves with the HELLO. */
  awaitHello(): Promise<HelloMessage> {
    this.state = 'handshaking';
    // Check if HELLO already arrived (synchronous in-memory transport)
    if (this.bufferedHello !== null) {
      const hello = this.bufferedHello;
      this.bufferedHello = null;
      return Promise.resolve(hello);
    }
    return new Promise<HelloMessage>((resolve) => {
      this.helloResolver = resolve;
    });
  }

  /** (Server) Send WELCOME after receiving HELLO. Marks the connection as connected. */
  sendWelcome(welcome: WelcomeMessage): void {
    this.sendRaw(encodeWelcome(welcome));
    this.state = 'connected';
    this.startKeepalive();
  }

  // ─── Dynamic Channel Management ─────────────────────────────────────────────

  /** Request to open a new channel. Returns a Promise that resolves with the CHANNEL_ACK. */
  openChannel(request: Omit<OpenChannelMessage, 'requestId'>): Promise<ChannelAckMessage> {
    const requestId = this.nextRequestId++;
    const msg: OpenChannelMessage = { ...request, requestId };
    // Register resolver BEFORE sendRaw so synchronous ACKs can resolve it.
    const p = new Promise<ChannelAckMessage>((resolve, reject) => {
      this.pendingOpen.set(requestId, { resolve, reject });
    });
    this.sendRaw(encodeOpenChannel(msg));
    return p;
  }

  /** Send a CLOSE_CHANNEL message (without waiting for acknowledgement). */
  closeChannel(channelId: number, reason?: string): void {
    this.sendRaw(encodeCloseChannel({ id: channelId, reason }));
  }

  /** Send a CHANNEL_ACK (server-side response to OPEN_CHANNEL). */
  sendChannelAck(ack: ChannelAckMessage): void {
    this.sendRaw(encodeChannelAck(ack));
  }

  /** Send a CHANNEL_REJECT (server-side response to OPEN_CHANNEL). */
  sendChannelReject(reject: ChannelRejectMessage): void {
    this.sendRaw(encodeChannelReject(reject));
  }

  // ─── Close ───────────────────────────────────────────────────────────────────

  /** Initiate a graceful close. Returns a Promise that resolves when the echo arrives. */
  close(code: number = CloseCode.NORMAL, reason?: string): Promise<void> {
    if (this.state === 'closed' || this.state === 'closing') {
      return Promise.resolve();
    }
    this.state = 'closing';
    this.stopKeepalive();
    // Set resolver BEFORE calling sendRaw so synchronous echoes can resolve it.
    const p = new Promise<void>((resolve) => {
      this.closeResolver = resolve;
    });
    this.sendRaw(encodeClose({ code, reason }));
    return p;
  }

  /** Send a non-fatal ERROR message. */
  sendError(msg: ErrorMessage): void {
    this.sendRaw(encodeError(msg));
  }

  // ─── Frame Dispatch ──────────────────────────────────────────────────────────

  /** Dispatch an incoming control frame to the appropriate handler. */
  handleFrame(frame: Frame): void {
    if (frame.channel !== 0) return; // not a control frame

    switch (frame.type) {
      case ControlType.HELLO:
        this.onHello(frame);
        break;
      case ControlType.WELCOME:
        this.onWelcome(frame);
        break;
      case ControlType.OPEN_CHANNEL:
        this.onOpenChannel(frame);
        break;
      case ControlType.CHANNEL_ACK:
        this.onChannelAck(frame);
        break;
      case ControlType.CLOSE_CHANNEL:
        this.onCloseChannel(frame);
        break;
      case ControlType.CHANNEL_REJECT:
        this.onChannelReject(frame);
        break;
      case ControlType.PING:
        this.onPing(frame);
        break;
      case ControlType.PONG:
        this.onPong(frame);
        break;
      case ControlType.CLOSE:
        this.onClose(frame);
        break;
      case ControlType.ERROR:
        this.onErrorFrame(frame);
        break;
      default:
        // Unknown control message — send ERROR
        this.sendRaw(
          encodeError({
            code: CloseCode.UNSUPPORTED,
            reason: `Unknown control type 0x${frame.type.toString(16)}`,
          }),
        );
    }
  }

  // ─── Keepalive ───────────────────────────────────────────────────────────────

  private startKeepalive(): void {
    const interval = (this.options.pingInterval ?? DEFAULT_PING_INTERVAL) * 1000;
    const timeout = (this.options.pingTimeout ?? DEFAULT_PING_TIMEOUT) * 1000;
    if (interval === 0) return;

    this.pingTimer = setInterval(() => {
      if (this.state !== 'connected') return;
      const ts = (Date.now() - this.connectionStart) & 0xffffffff;
      this.sendRaw(encodePing(ts));
      this.pongDeadline = setTimeout(() => {
        // PONG timeout — close the connection
        void this.close(CloseCode.PROTOCOL_ERROR, 'PONG timeout');
        this.callbacks.onTransportClose();
      }, timeout);
    }, interval);
  }

  private stopKeepalive(): void {
    if (this.pingTimer !== null) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    if (this.pongDeadline !== null) {
      clearTimeout(this.pongDeadline);
      this.pongDeadline = null;
    }
  }

  /** Stop all timers. Call when the transport closes unexpectedly. */
  destroy(): void {
    this.stopKeepalive();
    this.state = 'closed';
    // Reject any pending open-channel requests
    for (const [, pending] of this.pendingOpen) {
      pending.reject(new Error('Connection closed'));
    }
    this.pendingOpen.clear();
  }

  // ─── Frame Handlers ──────────────────────────────────────────────────────────

  private onHello(frame: Frame): void {
    const msg = decodeHello(frame);
    if (this.helloResolver) {
      this.helloResolver(msg);
      this.helloResolver = null;
    } else {
      this.bufferedHello = msg; // buffer until awaitHello() is called
    }
  }

  private onWelcome(frame: Frame): void {
    const msg = decodeWelcome(frame);
    this.state = 'connected';
    this.startKeepalive();
    if (this.welcomeResolver) {
      this.welcomeResolver(msg);
      this.welcomeResolver = null;
    } else {
      this.bufferedWelcome = msg; // buffer until sendHello()'s Promise reads it
    }
  }

  private onOpenChannel(frame: Frame): void {
    if (this.state !== 'connected') return;
    const msg = decodeOpenChannel(frame);
    // The connection layer must handle actual channel creation.
    // We emit the ack via the callbacks after the connection layer
    // calls sendChannelAck() or sendChannelReject().
    // For now, forward to callbacks as a synthetic ack creation event.
    const ack: ChannelAckMessage = {
      requestId: msg.requestId,
      id: 0, // placeholder — connection layer fills this in
      name: msg.name,
    };
    this.callbacks.onChannelOpen(ack);
  }

  private onChannelAck(frame: Frame): void {
    const msg = decodeChannelAck(frame);
    const pending = this.pendingOpen.get(msg.requestId);
    if (pending) {
      pending.resolve(msg);
      this.pendingOpen.delete(msg.requestId);
    }
  }

  private onChannelReject(frame: Frame): void {
    const msg = decodeChannelReject(frame);
    const pending = this.pendingOpen.get(msg.requestId);
    if (pending) {
      pending.reject(new Error(`Channel rejected: [${msg.code}] ${msg.reason ?? ''}`));
      this.pendingOpen.delete(msg.requestId);
    }
  }

  private onCloseChannel(frame: Frame): void {
    const msg = decodeCloseChannel(frame);
    // Guard: CLOSE_CHANNEL MUST NOT be sent for channel 0
    if (msg.id === 0) {
      this.sendRaw(
        encodeError({ code: CloseCode.PROTOCOL_ERROR, reason: 'CLOSE_CHANNEL on control channel' }),
      );
      return;
    }
    this.callbacks.onChannelClose(msg.id, msg.reason);
  }

  private onPing(frame: Frame): void {
    const { timestamp } = decodePing(frame.payload);
    const receiverTs = (Date.now() - this.connectionStart) & 0xffffffff;
    this.sendRaw(encodePong(timestamp, receiverTs));
  }

  private onPong(frame: Frame): void {
    decodePong(frame.payload); // validate payload structure; result not needed here
    // Clear the PONG deadline timer — keepalive satisfied
    if (this.pongDeadline !== null) {
      clearTimeout(this.pongDeadline);
      this.pongDeadline = null;
    }
  }

  private onClose(frame: Frame): void {
    const msg = decodeClose(frame);
    this.stopKeepalive();
    if (this.state === 'closing') {
      // We initiated the close — this is the echo
      this.state = 'closed';
      if (this.closeResolver) {
        this.closeResolver();
        this.closeResolver = null;
      }
      this.callbacks.onTransportClose();
    } else {
      // Remote initiated close — echo back then close
      this.state = 'closing';
      this.sendRaw(encodeClose({ code: msg.code, reason: 'ack' }));
      this.state = 'closed';
      this.callbacks.onClose(msg);
      this.callbacks.onTransportClose();
    }
  }

  private onErrorFrame(frame: Frame): void {
    const msg = decodeError(frame);
    this.callbacks.onError(msg);
  }
}
