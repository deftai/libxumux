/**
 * BasePath: the framing layer over one transport (core §1.2, layer 2).
 *
 * A base path turns a {@link TransportAdapter} into whole, length-checked frames: it writes
 * and checks the `XMUX` magic on stream bindings, finds frame boundaries, and applies
 * core §2.3 steps 1–2 with the limit the session supplies. It does not interpret Type or Flags.
 *
 * The session above it holds a base path rather than a transport, so the path can be
 * detached and replaced under a live session (core §11.1, used by session-resume/1), and an
 * unstarted or paused path can be handed from one session to another with the bytes it has
 * already read.
 *
 * @module base-path
 */

import { XMUX_MAGIC } from './codec/constants.js';
import { decodeFrame, StreamFramer } from './codec/decode-frame.js';
import type { Frame } from './codec/frame.js';
import type { TransportAdapter } from './types.js';
import { ignore } from './ignore.js';

/** Where a base path delivers what it reads. */
export interface BasePathSink {
  /** A received frame. */
  frame(f: Frame): void;
  /**
   * The input ended: the transport finished (`cause` undefined), reading it threw
   * (`readError`), or its bytes could not be parsed into frames (`parseError`).
   */
  end(cause: { readError?: unknown; parseError?: unknown }): void;
}

/**
 * The framing layer over one transport. Created by the session for its transport; create one
 * yourself only to hand a transport to a session that accepts a base path.
 */
export class BasePath {
  private readonly framer: StreamFramer | null;
  private magicSent: boolean;
  private iter: AsyncIterator<Uint8Array> | null = null;
  /** Frames parsed but not yet delivered (the path was paused in the middle of a chunk). */
  private readonly pending: Frame[] = [];
  private sink: BasePathSink | null = null;
  private reading = false;
  private ended: { readError?: unknown; parseError?: unknown } | null = null;
  private closedFlag = false;
  /** @internal The session's outbound pump is writing to this path. */
  pumping = false;

  constructor(
    readonly transport: TransportAdapter,
    private maxFramePayload: number,
  ) {
    const stream = transport.kind === 'stream';
    const magic = stream && transport.magic !== false;
    this.framer = stream ? new StreamFramer(maxFramePayload, magic) : null;
    this.magicSent = !magic;
  }

  /** True once {@link close} has been called. */
  get closed(): boolean {
    return this.closedFlag;
  }

  /** Set the receiver's maxFramePayload used for core §2.3 step 1. */
  setMaxFramePayload(n: number): void {
    this.maxFramePayload = n;
    if (this.framer) this.framer.maxFramePayload = n;
  }

  /**
   * Deliver frames to `sink`, starting with any already read but not delivered, then reading
   * the transport. Calling it again replaces the sink (for example after a {@link pause}).
   */
  start(sink: BasePathSink): void {
    this.sink = sink;
    if (this.reading || this.closedFlag) return;
    this.reading = true;
    void this.loop(sink);
  }

  /**
   * Stop delivering after the current frame and stop reading the transport, keeping the
   * transport open and everything already read, so that {@link start} (possibly with another
   * sink) carries on exactly where this left off.
   */
  pause(): void {
    this.sink = null;
  }

  /** Write bytes (one encoded frame), preceded by the magic bytes the first time on stream bindings. */
  write(bytes: Uint8Array): void | Promise<void> {
    if (!this.magicSent) {
      this.magicSent = true;
      const r = this.transport.send(new Uint8Array(XMUX_MAGIC));
      if (r) return r.then(() => this.transport.send(bytes));
    }
    return this.transport.send(bytes);
  }

  /** Close the transport and stop delivering. Idempotent. */
  close(): void {
    if (this.closedFlag) return;
    this.closedFlag = true;
    this.sink = null;
    try {
      this.transport.close();
    } catch {
      /* already closed */
    }
  }

  private async loop(first: BasePathSink): Promise<void> {
    let sink: BasePathSink | null = first;
    for (;;) {
      while (this.pending.length > 0 && sink && sink === this.sink && !this.closedFlag) {
        sink.frame(this.pending.shift()!);
      }
      if (this.closedFlag) {
        this.reading = false;
        void this.iter?.return?.().catch(ignore);
        return;
      }
      sink = this.sink;
      if (!sink) {
        // Paused: keep the iterator and whatever is buffered for whoever starts us next.
        this.reading = false;
        return;
      }
      if (this.ended) {
        this.reading = false;
        this.sink = null;
        sink.end(this.ended);
        return;
      }
      let r: IteratorResult<Uint8Array>;
      try {
        this.iter ??= this.transport.receive()[Symbol.asyncIterator]();
        r = await this.iter.next();
      } catch (e) {
        this.ended = { readError: e };
        continue;
      }
      if (r.done) {
        this.ended = {};
        continue;
      }
      if (this.closedFlag) continue;
      try {
        if (this.framer) this.pending.push(...this.framer.push(r.value));
        else this.pending.push(decodeFrame(r.value, this.maxFramePayload));
      } catch (e) {
        // Frames from earlier chunks are still delivered, then the error.
        this.ended = { parseError: e };
      }
      sink = this.sink;
    }
  }
}
