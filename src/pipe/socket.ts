/**
 * PipeSocket: a pipe/1 channel as a Node `stream.Duplex`.
 * @module pipe/socket
 */

import { Duplex, type Writable } from 'node:stream';
import { CloseCode } from '../codec/constants.js';
import type { ChannelMessage, XumuxChannel } from '../channel.js';
import { PipeMessageType, PipeResetError, pipeTarget } from './profile.js';

/** Longest RESET reason sent, in UTF-8 bytes. */
const MAX_RESET_REASON = 1024;
/** Largest RESET body sent: the smallest maxMessageSize a peer may declare (core §3.4). */
const MAX_RESET_BODY = 4096;

const EMPTY = new Uint8Array(0);
const noop = (): void => undefined;

export interface PipeSocketOptions {
  /**
   * Keep the writable side open after the peer's EOF, like `net.Socket`. Default `true`,
   * which is what pipe/1 half-close means. With `false`, the peer's EOF also ends this side.
   */
  allowHalfOpen?: boolean;
  readableHighWaterMark?: number;
  writableHighWaterMark?: number;
}

/** How this side is ending the pipe, set just before `destroy()`. */
type Ending = { reset: boolean; code: number; reason?: string };

/**
 * A pipe/1 channel as a `stream.Duplex`, the analogue of a `net.Socket`, so
 * `stream.pipeline()`, `http` and `tls` run over it.
 *
 * - Writing sends DATA, never fragmented, waiting for flow-control credit as needed: the
 *   write callback runs once the bytes are on the transport.
 * - `end()` sends EOF (half-close). Reading goes on until the peer's EOF, which ends the
 *   readable side. Once both EOFs have been sent and received, the channel closes with 1000.
 * - `destroy()` sends EOF, unless `end()` already did, and closes the channel at once, like
 *   closing a socket. `destroy(err)` and {@link resetAndDestroy} send RESET instead.
 * - The peer's RESET destroys the socket with a {@link PipeResetError}; a message type
 *   pipe/1 doesn't define closes the channel with 1002 and destroys it with an Error. Either
 *   way, bytes that arrived before are still delivered first.
 *
 * Credit goes back to the peer only as the consumer takes bytes out of the readable side
 * (`read()`, `'data'`, `pipe()`, async iteration), so a consumer that stops reading stops the
 * peer's writer once the window is full (pipe/1 backpressure). The readable buffer can
 * therefore hold up to the channel's receive window, regardless of `readableHighWaterMark`.
 */
export class PipeSocket extends Duplex {
  /** The underlying channel. */
  readonly channel: XumuxChannel;

  /** Bytes pushed to the readable side, and bytes of credit returned for them. */
  private pushed = 0;
  private returned = 0;
  /** Bytes {@link writeTo} has taken out but its destination has not yet written. */
  private unwritten = 0;
  /** writeTo() is reading: what read() returns is held until written. */
  private holding = false;
  /** Running writeTo() copies, told when the pipe fails. */
  private readonly wakers = new Set<() => void>();
  private sentEOF = false;
  private gotEOF = false;
  /** The pipe has failed: destroy with this once the readable side has drained. */
  private failure: Error | null = null;
  /** A write or end callback held while a failure waits for the readable side to drain. */
  private held: ((e?: Error | null) => void) | null = null;
  /** Aborts a write waiting for credit, so a close or RESET can go out at once. */
  private readonly writeAbort = new AbortController();
  private ending: Ending | null = null;
  private idleMs = 0;
  private idleTimer: ReturnType<typeof setTimeout> | null = null;

  /**
   * Wrap an open pipe/1 channel, such as one from `openChannel(pipeRequest(...))`. Create it
   * before anything else receives on the channel: the socket takes over receiving, and
   * returns the channel's credit itself.
   */
  constructor(channel: XumuxChannel, options: PipeSocketOptions = {}) {
    super({
      allowHalfOpen: options.allowHalfOpen ?? true,
      readableHighWaterMark: options.readableHighWaterMark,
      writableHighWaterMark: options.writableHighWaterMark,
    });
    this.channel = channel;
    channel.setManualCredit(true);
    void this.receiveLoop();
  }

  /** The target named in the channel request, if any. */
  get target(): string | undefined {
    return pipeTarget(this.channel);
  }

  /**
   * Send RESET with `code` and `reason` and destroy the socket, as a relay does when its own
   * connection breaks. Like `net.Socket.resetAndDestroy()`, it emits no `'error'` locally.
   */
  resetAndDestroy(code: number = CloseCode.GOING_AWAY, reason?: string): this {
    if (!this.destroyed) this.ending ??= { reset: true, code, reason };
    return this.destroy();
  }

  /**
   * Emit `'timeout'` after `ms` without reads or writes, like `net.Socket.setTimeout()`. The
   * socket stays open; `0` turns it off.
   */
  setTimeout(ms: number, callback?: () => void): this {
    this.idleMs = ms;
    if (callback && ms === 0) this.off('timeout', callback);
    else if (callback) this.once('timeout', callback);
    this.touch();
    return this;
  }

  /** No-op, for `net.Socket` compatibility. */
  setNoDelay(_noDelay?: boolean): this {
    return this;
  }

  /** No-op, for `net.Socket` compatibility. */
  setKeepAlive(_enable?: boolean, _initialDelay?: number): this {
    return this;
  }

  /** No-op, for `net.Socket` compatibility. */
  ref(): this {
    return this;
  }

  /** No-op, for `net.Socket` compatibility. */
  unref(): this {
    return this;
  }

  // ─── Readable side ──────────────────────────────────────────────────────────

  /** Every way of consuming the readable side goes through read(): credit follows it. */
  override read(size?: number): string | Buffer | null {
    const chunk = super.read(size) as string | Buffer | null;
    if (this.holding && chunk !== null) this.unwritten += Buffer.byteLength(chunk);
    this.settle();
    return chunk;
  }

  /**
   * Copy the readable side to `dest` until the peer's EOF, returning credit for each chunk
   * only once `dest` has written it (its write callback), not as it is read. pipe/1 asks for
   * this: credit follows writes to the local end, so a slow `dest` slows the peer exactly,
   * where `pipe()` returns credit as soon as a chunk reaches the destination's buffer. It
   * waits for `'drain'` when `dest` is full.
   *
   * Resolves with the bytes written once they all have been; `dest` is not ended. Rejects
   * if this socket fails (after the bytes that came before the failure have been passed to
   * `dest`) or `dest` fails. Errors on either stream reject the copy rather than going
   * unhandled while it runs. Meanwhile, consume the readable side no other way, and don't
   * set an encoding.
   *
   * ```ts
   * await socket.writeTo(socket); // an echo
   * socket.end();
   * ```
   */
  async writeTo(dest: Writable): Promise<number> {
    if (this.readableEncoding !== null)
      throw new Error('writeTo() needs a socket without an encoding');
    if (this.readableFlowing) throw new Error('writeTo() needs a paused socket');
    let total = 0;
    let pending = 0;
    // In an object: TypeScript doesn't see the callbacks' assignments to a plain variable.
    const failed: { dest: Error | null } = { dest: null };
    let wake: (() => void) | null = null;
    const poke = (): void => {
      const w = wake;
      wake = null;
      w?.();
    };
    const sleep = (): Promise<void> => new Promise((r) => (wake = r));
    // Streams that have emitted 'error' or 'close' (`errored` and `closed` are set before).
    const settled = new Set<Writable>();
    const onDestError = (e: Error): void => {
      settled.add(dest);
      failed.dest ??= e;
      poke();
    };
    const onSourceEvent = (): void => void settled.add(this);
    const onDestClose = (): void => {
      settled.add(dest);
      // A destination that closes before the copy is done fails it (unless it is this
      // socket, whose own state says why).
      if (dest !== this) failed.dest ??= dest.errored ?? new Error('destination closed');
      poke();
    };
    const sourceEvents = ['readable', 'end', 'close', 'error'] as const;
    for (const e of sourceEvents) this.on(e, poke);
    this.on('error', onSourceEvent);
    this.on('close', onSourceEvent);
    this.wakers.add(poke);
    dest.on('drain', poke);
    dest.on('error', onDestError);
    dest.on('close', onDestClose);
    try {
      for (;;) {
        if (this.errored) throw this.errored;
        if (failed.dest) throw failed.dest;
        if (this.readableEnded) {
          if (pending === 0) return total;
        } else if (this.destroyed) {
          throw new Error('pipe destroyed');
        } else if (!dest.writableNeedDrain || this.failure) {
          // After a failure, keep passing on the bytes that came before it, without
          // waiting for a write the failure holds (this socket as its own destination).
          this.holding = true;
          let chunk: Buffer | null;
          try {
            chunk = this.read() as Buffer | null;
          } finally {
            this.holding = false;
          }
          if (chunk !== null) {
            const n = chunk.length;
            pending++;
            dest.write(chunk, (e) => {
              pending--;
              if (e) failed.dest ??= e;
              else {
                this.unwritten -= n;
                total += n;
                this.settle();
              }
              poke();
            });
            continue;
          }
        }
        await sleep();
      }
    } finally {
      for (const e of sourceEvents) if (e !== 'error') this.off(e, poke);
      this.off('close', onSourceEvent);
      this.wakers.delete(poke);
      dest.off('drain', poke);
      dest.off('close', onDestClose);
      // A failed stream emits 'error' after the copy has seen the failure: keep the copy's
      // handlers until then, since the rejection has already reported the error.
      offErrorLater(this, [poke, onSourceEvent], settled.has(this));
      offErrorLater(dest, [onDestError], settled.has(dest));
    }
  }

  override _read(): void {
    // Messages are received as they arrive; credit, not this, bounds what the peer sends.
  }

  private async receiveLoop(): Promise<void> {
    const ch = this.channel;
    for (;;) {
      let m: ChannelMessage | null;
      try {
        m = await ch.receive();
      } catch (e) {
        // The channel or connection closed abnormally.
        if (!this.destroyed) this.fail(e instanceof Error ? e : new Error(String(e)), null);
        return;
      }
      if (this.destroyed) return;
      if (m === null) {
        // The channel closed normally: no more data will come, even without an EOF.
        this.endReadable();
        return;
      }
      switch (m.type) {
        case PipeMessageType.DATA:
          if (this.gotEOF) return this.protocolError('pipe/1 DATA after EOF');
          if (m.payload.length > 0) {
            this.touch();
            this.pushed += m.payload.length;
            this.push(Buffer.from(m.payload.buffer, m.payload.byteOffset, m.payload.length));
            this.settle();
          }
          break;
        case PipeMessageType.EOF:
          if (this.gotEOF) return this.protocolError('pipe/1 EOF after EOF');
          this.endReadable();
          if (this.sentEOF) void ch.close(CloseCode.NORMAL).catch(noop);
          break;
        case PipeMessageType.RESET: {
          const r = parseReset(m.payload);
          this.fail(new PipeResetError(r.code, r.reason, true), CloseCode.GOING_AWAY);
          return;
        }
        default:
          return this.protocolError(`unknown pipe/1 message type 0x${m.type.toString(16)}`);
      }
    }
  }

  private endReadable(): void {
    if (this.gotEOF) return;
    this.gotEOF = true;
    this.push(null);
  }

  private protocolError(msg: string): void {
    this.fail(new Error(msg), CloseCode.PROTOCOL_ERROR, msg);
  }

  /**
   * Return credit for the bytes the consumer has taken from the readable buffer, and finish
   * a failure once everything that arrived before it has been read.
   */
  private settle(): void {
    // With setEncoding, readableLength counts characters, not bytes, so this can run ahead of
    // the consumer by up to the buffered bytes. It never returns more than was received.
    const consumed = this.pushed - this.readableLength - this.unwritten;
    if (consumed > this.returned) {
      this.channel.returnCredit(consumed - this.returned);
      this.returned = consumed;
    }
    if (this.failure && !this.destroyed && this.readableLength === 0) this.destroy(this.failure);
  }

  /**
   * The pipe has failed. Release a pending write, close the channel with `code` (null: it
   * is already closed), and destroy with `err` once the bytes before the failure are read.
   */
  private fail(err: Error, code: number | null, reason?: string): void {
    if (this.failure || this.destroyed) return;
    this.failure = err;
    this.writeAbort.abort(err);
    if (code !== null) void this.channel.close(code, reason).catch(noop);
    this.settle();
    for (const w of this.wakers) w();
  }

  // ─── Writable side ──────────────────────────────────────────────────────────

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (e?: Error | null) => void,
  ): void {
    if (this.failure) {
      this.held = callback;
      return;
    }
    this.touch();
    void this.writeAll(chunk).then(
      () => callback(),
      (e: unknown) => this.writeFailed(e, callback),
    );
  }

  private async writeAll(chunk: Uint8Array): Promise<void> {
    const signal = this.writeAbort.signal;
    for (let off = 0; off < chunk.length; )
      off += await this.channel.sendPrefix(PipeMessageType.DATA, chunk.subarray(off), signal);
  }

  override _final(callback: (e?: Error | null) => void): void {
    if (this.failure) {
      this.held = callback;
      return;
    }
    // Queued now, so no DATA can follow it and destroy() won't send another. EOF needs no
    // credit, and once committed it must go out, so destroy()'s abort doesn't cancel it.
    this.sentEOF = true;
    this.channel.sendPrefix(PipeMessageType.EOF, EMPTY).then(
      () => {
        if (this.gotEOF) void this.channel.close(CloseCode.NORMAL).catch(noop);
        callback();
      },
      (e: unknown) => this.writeFailed(e, callback),
    );
  }

  private writeFailed(e: unknown, callback: (e?: Error | null) => void): void {
    // A failure ends the stream once the readable side drains; until then, hold the callback
    // (an error now would destroy the socket and drop the bytes still to be read).
    if (this.failure && !this.destroyed) this.held = callback;
    else callback(this.failure ?? (e instanceof Error ? e : new Error(String(e))));
  }

  override _destroy(err: Error | null, callback: (e?: Error | null) => void): void {
    this.clearIdle();
    this.writeAbort.abort(err ?? new Error('pipe destroyed'));
    const ch = this.channel;
    if (!this.failure && ch.state === 'open') {
      const ending: Ending = this.ending ?? resetFor(err);
      if (ending.reset) {
        const { body, reason } = resetBody(ending.code, ending.reason);
        // RESET is one JSON message: send it whole, fragmented if need be (only DATA may not be).
        void ch.send(PipeMessageType.RESET, body).catch(noop);
        void ch.close(CloseCode.GOING_AWAY, reason).catch(noop);
      } else {
        if (!this.sentEOF) {
          this.sentEOF = true;
          void ch.sendPrefix(PipeMessageType.EOF, EMPTY).catch(noop);
        }
        void ch.close(ending.code, ending.reason).catch(noop);
      }
    }
    const held = this.held;
    this.held = null;
    held?.(err ?? this.failure);
    callback(err);
  }

  /** @internal Close the channel with `code` instead of the usual 1000, then destroy. */
  _abandon(code: number, reason?: string): void {
    if (!this.destroyed) this.ending ??= { reset: false, code, reason };
    this.destroy();
  }

  // ─── Idle timeout ───────────────────────────────────────────────────────────

  private touch(): void {
    this.clearIdle();
    if (this.idleMs > 0 && !this.destroyed) {
      this.idleTimer = setTimeout(() => {
        this.idleTimer = null;
        this.emit('timeout');
      }, this.idleMs);
      this.idleTimer.unref?.();
    }
  }

  private clearIdle(): void {
    if (this.idleTimer) clearTimeout(this.idleTimer);
    this.idleTimer = null;
  }
}

/**
 * Remove 'error' listeners now or, if `s` has failed but not yet emitted 'error' or 'close',
 * once it emits either: a failed stream emits 'error' a tick after it is marked errored.
 */
function offErrorLater(s: Writable, fns: Array<(err: Error) => void>, settled: boolean): void {
  const off = (): void => {
    for (const fn of fns) s.off('error', fn);
    s.off('error', off);
    s.off('close', off);
  };
  if (s.errored && !settled) {
    // Listeners run from a copy of the list, so the copy's own still see this 'error'.
    s.on('error', off);
    s.on('close', off);
  } else off();
}

/** How destroy(err) ends the pipe: RESET for an error, else EOF and 1000. */
function resetFor(err: Error | null): Ending {
  if (!err) return { reset: false, code: CloseCode.NORMAL };
  if (err instanceof PipeResetError) return { reset: true, code: err.code, reason: err.reason };
  return { reset: true, code: CloseCode.GOING_AWAY, reason: err.message };
}

/**
 * The RESET body for `code` and `reason`, with the reason cut to MAX_RESET_REASON bytes of
 * UTF-8 (at a character boundary) and further if JSON escaping would push the body past
 * MAX_RESET_BODY, so every peer can receive it. Returns the reason actually sent.
 */
function resetBody(code: number, reason?: string): { body: Uint8Array; reason?: string } {
  const te = new TextEncoder();
  let r = reason ? truncateUtf8(reason, MAX_RESET_REASON) : undefined;
  for (;;) {
    const body = te.encode(JSON.stringify(r ? { code, reason: r } : { code }));
    if (body.length <= MAX_RESET_BODY || !r) return { body, reason: r || undefined };
    r = truncateUtf8(r, Math.floor(te.encode(r).length / 2));
  }
}

/** The longest prefix of `s` whose UTF-8 encoding is at most `max` bytes. */
function truncateUtf8(s: string, max: number): string {
  const bytes = new TextEncoder().encode(s);
  if (bytes.length <= max) return s;
  let end = max;
  while (end > 0 && (bytes[end]! & 0xc0) === 0x80) end--; // don't split a character
  return new TextDecoder().decode(bytes.subarray(0, end));
}

/** A RESET body; a malformed one still resets, with code 0. */
function parseReset(payload: Uint8Array): { code: number; reason?: string } {
  try {
    const v: unknown = JSON.parse(new TextDecoder().decode(payload));
    if (v && typeof v === 'object') {
      const { code, reason } = v as { code?: unknown; reason?: unknown };
      return {
        code: typeof code === 'number' ? code : 0,
        reason: typeof reason === 'string' ? reason : undefined,
      };
    }
  } catch {
    // fall through
  }
  return { code: 0 };
}
