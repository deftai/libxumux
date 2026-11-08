/**
 * PipeListener: accepts the pipe/1 channels peers open, like a `net.Server`.
 * @module pipe/listener
 */

import { EventEmitter } from 'node:events';
import { CloseCode } from '../codec/constants.js';
import type { XumuxChannel } from '../channel.js';
import type { ChannelRequest } from '../control/messages.js';
import type { ChannelDecision, ChannelPolicy, XumuxPeer } from '../session.js';
import { checkPipeRequest, PIPE_PROFILE } from './profile.js';
import { PipeSocket, type PipeSocketOptions } from './socket.js';

export interface PipeListenerOptions {
  /**
   * Decide on each pipe/1 request before it is accepted: `true` accepts, a `{code, reason}`
   * refuses, and a throw refuses with 4010. Check `pipeTarget(req)` against an allow-list
   * here if targets mean anything to you: an unrestricted target makes an open proxy.
   */
  check?: (request: ChannelRequest) => ChannelDecision | Promise<ChannelDecision>;
  /** Options for every accepted socket. */
  socket?: PipeSocketOptions;
}

interface PipeListenerEvents {
  connection: [socket: PipeSocket];
  close: [];
}

/**
 * Accepts the pipe/1 channels peers open, as {@link PipeSocket}s. One listener can serve any
 * number of sessions:
 *
 * ```ts
 * const listener = new PipeListener({
 *   check: (req) => pipeTarget(req) === 'web' || { code: 4010, reason: 'not allowed' },
 * });
 * const server = new XumuxServer(transport, { onChannelRequest: listener.policy });
 * listener.attach(server);
 * // Serve HTTP on the pipes:
 * const web = http.createServer(handler);
 * listener.on('connection', (socket) => web.emit('connection', socket));
 * ```
 *
 * Sockets go to `'connection'` listeners if there are any, and otherwise wait for
 * {@link accept} (or `for await`).
 */
export class PipeListener extends EventEmitter<PipeListenerEvents> {
  private readonly options: PipeListenerOptions;
  private readonly backlog: PipeSocket[] = [];
  private readonly waiters: Array<{
    resolve: (s: PipeSocket) => void;
    reject: (e: Error) => void;
  }> = [];
  private closed = false;

  constructor(options: PipeListenerOptions = {}) {
    super();
    this.options = options;
  }

  /** True until {@link close}. */
  get listening(): boolean {
    return !this.closed;
  }

  /**
   * The channel policy for `XumuxOptions.onChannelRequest`: accepts pipe/1 requests that are
   * reliable, ordered and pass `check`, and refuses everything else with 4010. To serve
   * other profiles too, call it from your own policy for pipe/1 requests.
   */
  readonly policy: ChannelPolicy = async (request) => {
    if (this.closed) return { code: CloseCode.CHANNEL_REFUSED, reason: 'not accepting channels' };
    const d = checkPipeRequest(request);
    if (d !== true || !this.options.check) return d;
    let decision: ChannelDecision;
    try {
      decision = await this.options.check(request);
    } catch (e) {
      return {
        code: CloseCode.CHANNEL_REFUSED,
        reason: e instanceof Error ? e.message : String(e),
      };
    }
    // close() may have come while check ran.
    return this.closed
      ? { code: CloseCode.CHANNEL_REFUSED, reason: 'not accepting channels' }
      : decision;
  };

  /**
   * Take the pipe/1 channels that `peer` accepts (its `'channel'` events). Returns a function
   * that stops it. Other channels are left alone. Once the listener is closed, pipes still
   * arriving (accepted by a policy decision made before the close) are closed with 1001.
   */
  attach(peer: XumuxPeer): () => void {
    const onChannel = (e: Event): void => {
      const ch = (e as CustomEvent<XumuxChannel>).detail;
      if (ch.metadata?.profile !== PIPE_PROFILE) return;
      if (this.closed) {
        void ch.close(CloseCode.GOING_AWAY, 'listener closed').catch(() => undefined);
        return;
      }
      this.deliver(new PipeSocket(ch, this.options.socket));
    };
    peer.addEventListener('channel', onChannel);
    return () => peer.removeEventListener('channel', onChannel);
  }

  /** The next pipe. Rejects once the listener is closed. */
  accept(): Promise<PipeSocket> {
    const s = this.backlog.shift();
    if (s) return Promise.resolve(this.handOver(s));
    if (this.closed) return Promise.reject(listenerClosed());
    return new Promise((resolve, reject) => this.waiters.push({ resolve, reject }));
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<PipeSocket> {
    for (;;) {
      let s: PipeSocket;
      try {
        s = await this.accept();
      } catch {
        return;
      }
      yield s;
    }
  }

  /**
   * Stop accepting: refuse new requests, close pipes nobody has accepted yet with 1001, and
   * reject pending `accept()`s (a `for await` loop ends). Sessions and accepted pipes are not
   * affected.
   */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    for (const s of this.backlog.splice(0)) s._abandon(CloseCode.GOING_AWAY, 'listener closed');
    for (const w of this.waiters.splice(0)) w.reject(listenerClosed());
    this.emit('close');
  }

  private deliver(s: PipeSocket): void {
    if (this.listenerCount('connection') > 0) {
      this.emit('connection', s);
      return;
    }
    // Until the application has the socket, an 'error' (the peer's RESET, say) has nowhere
    // to go and would crash the process. Hold it here; the socket still shows it in
    // `errored` and `destroyed` once accepted.
    s.on('error', ignore);
    if (this.waiters.length > 0) this.waiters.shift()!.resolve(this.handOver(s));
    else this.backlog.push(s);
  }

  /**
   * Hand a socket to accept()'s caller. The guard stays until the caller's continuation has
   * run (it can't add its own handler any sooner), then the socket's errors are theirs.
   */
  private handOver(s: PipeSocket): PipeSocket {
    setImmediate(() => s.off('error', ignore));
    return s;
  }
}

const ignore = (): void => undefined;

function listenerClosed(): Error {
  return Object.assign(new Error('pipe listener closed'), { code: 'ERR_SERVER_NOT_RUNNING' });
}
