/**
 * A scripted peer for tests: sends arbitrary bytes and records the frames it receives,
 * so a test can drive a library session through exact (including invalid) sequences.
 */
import { createInMemoryPair } from '../src/transports/in-memory-adapter.js';
import { decodeFrame, StreamFramer } from '../src/codec/decode-frame.js';
import { encodeFrame } from '../src/codec/encode-frame.js';
import { ControlType, XMUX_MAGIC } from '../src/codec/constants.js';
import type { Frame } from '../src/codec/frame.js';
import type { TransportAdapter } from '../src/types.js';

const te = new TextEncoder();
const td = new TextDecoder();

export const json = (f: Frame): Record<string, unknown> =>
  JSON.parse(td.decode(f.payload)) as Record<string, unknown>;
export const ctrl = (type: number, body: unknown): Frame => ({
  channel: 0,
  type,
  flags: 0,
  payload: te.encode(JSON.stringify(body)),
});

export class RawPeer {
  readonly frames: Frame[] = [];
  private cursor = 0;
  private waiters: Array<() => void> = [];
  private magicSent = false;
  ended = false;
  readonly done: Promise<void>;

  constructor(
    readonly transport: TransportAdapter,
    private readonly kind: 'message' | 'stream',
  ) {
    this.done = this.read();
  }

  private async read(): Promise<void> {
    const framer = this.kind === 'stream' ? new StreamFramer(0x7fffffff, true) : null;
    try {
      for await (const chunk of this.transport.receive()) {
        const frames = framer ? framer.push(chunk) : [decodeFrame(chunk)];
        for (const f of frames) this.frames.push({ ...f, payload: new Uint8Array(f.payload) });
        this.wake();
      }
    } finally {
      this.ended = true;
      this.wake();
    }
  }

  private wake(): void {
    const w = this.waiters;
    this.waiters = [];
    for (const x of w) x();
  }

  /** Send raw bytes (magic is prepended once on stream transports unless `noMagic`). */
  sendBytes(bytes: Uint8Array, noMagic = false): void {
    if (this.kind === 'stream' && !this.magicSent && !noMagic) {
      this.magicSent = true;
      void this.transport.send(new Uint8Array(XMUX_MAGIC));
    }
    void this.transport.send(bytes);
  }

  send(f: Frame): void {
    this.sendBytes(encodeFrame(f));
  }

  /** Next unconsumed frame matching `pred`, waiting up to `timeoutMs`. */
  async next(pred: (f: Frame) => boolean = () => true, timeoutMs = 1000): Promise<Frame> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      for (let i = this.cursor; i < this.frames.length; i++) {
        const f = this.frames[i]!;
        if (pred(f)) {
          this.cursor = i + 1;
          return f;
        }
      }
      if (this.ended) throw new Error('transport ended before a matching frame arrived');
      const left = deadline - Date.now();
      if (left <= 0) throw new Error('timed out waiting for frame');
      await new Promise<void>((r) => {
        const t = setTimeout(r, left);
        this.waiters.push(() => (clearTimeout(t), r()));
      });
    }
  }

  control(type: number, timeoutMs?: number): Promise<Frame> {
    return this.next(
      (f) => f.channel === 0 && (f.flags & 0x02) === 0 && f.type === type,
      timeoutMs,
    );
  }

  /** Act as a client: send HELLO, return the WELCOME frame. */
  async helloAndWelcome(hello: Record<string, unknown> = {}): Promise<Frame> {
    this.send(ctrl(ControlType.HELLO, { version: [0, 3, 0], ...hello }));
    return this.control(ControlType.WELCOME);
  }

  /** Act as a server: wait for HELLO, send WELCOME. */
  async awaitHelloSendWelcome(welcome: Record<string, unknown> = {}): Promise<Frame> {
    const hello = await this.control(ControlType.HELLO);
    const channels = ((json(hello).channels as Array<{ id: number }>) ?? []).map((c) => ({
      id: c.id,
      reliable: true,
      ordered: true,
    }));
    this.send(ctrl(ControlType.WELCOME, { version: [0, 3, 0], channels, ...welcome }));
    return hello;
  }

  /** Wait until the transport ends. */
  async waitEnded(timeoutMs = 2000): Promise<void> {
    await Promise.race([
      this.done,
      new Promise((_, rej) => setTimeout(() => rej(new Error('transport did not end')), timeoutMs)),
    ]);
  }
}

/** A library-side transport plus a RawPeer on the other end. */
export function rawPair(kind: 'message' | 'stream' = 'message'): {
  transport: TransportAdapter;
  raw: RawPeer;
} {
  const [a, b] = createInMemoryPair(kind);
  return { transport: a, raw: new RawPeer(b, kind) };
}
