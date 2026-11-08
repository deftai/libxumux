import { describe, it, expect, vi } from 'vitest';
import { EventEmitter } from 'node:events';
import { PassThrough } from 'node:stream';
import type { Socket } from 'node:net';
import { WebSocketAdapter, type WebSocketLike } from './websocket-adapter.js';
import { WebRTCAdapter, type DataChannelLike } from './webrtc-adapter.js';
import { TcpAdapter } from './tcp-adapter.js';
import { StdioAdapter } from './stdio-adapter.js';
import { QuicAdapter } from './quic-adapter.js';
import { createInMemoryPair } from './in-memory-adapter.js';
import { EventQueue, waitUntil } from './event-queue.js';
import { XumuxProtocolError } from '../errors.js';

async function collect(it: AsyncIterable<Uint8Array>): Promise<Uint8Array[]> {
  const out: Uint8Array[] = [];
  for await (const x of it) out.push(x);
  return out;
}

/** Event-target mock shared by the WebSocket and DataChannel fakes. */
class FakeEventTarget {
  listeners = new Map<string, Array<(e: never) => void>>();
  sent: Uint8Array[] = [];
  bufferedAmount = 0;
  binaryType = 'blob';
  addEventListener(type: string, l: (e: never) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), l]);
  }
  emit(type: string, e?: unknown): void {
    for (const l of this.listeners.get(type) ?? []) l(e as never);
  }
  send(d: Uint8Array): void {
    this.sent.push(d);
  }
}
class FakeWs extends FakeEventTarget {
  readyState = 1;
  close = vi.fn();
}
class FakeDc extends FakeEventTarget {
  readyState = 'open';
  close = vi.fn();
}

describe('EventQueue', () => {
  it('delivers items, then ends', async () => {
    const q = new EventQueue<number>();
    q.push(1);
    setTimeout(() => (q.push(2), q.end(), q.push(3)), 1);
    const got: number[] = [];
    for await (const x of q) got.push(x);
    expect(got).toEqual([1, 2]);
  });
  it('throws the end error after draining', async () => {
    const q = new EventQueue<number>();
    q.push(1);
    q.end(new Error('boom'));
    q.end();
    const got: number[] = [];
    await expect(
      (async () => {
        for await (const x of q) got.push(x);
      })(),
    ).rejects.toThrow('boom');
    expect(got).toEqual([1]);
  });
  it('waitUntil polls until true', async () => {
    let n = 0;
    await waitUntil(() => ++n > 3, 1);
    expect(n).toBeGreaterThan(3);
    await waitUntil(() => true);
  });
});

describe.each([
  [
    'WebSocketAdapter',
    () => new FakeWs(),
    (f: FakeEventTarget) =>
      new WebSocketAdapter(f as unknown as WebSocketLike, { highWaterMark: 100 }),
  ],
  [
    'WebRTCAdapter',
    () => new FakeDc(),
    (f: FakeEventTarget) =>
      new WebRTCAdapter(f as unknown as DataChannelLike, { highWaterMark: 100 }),
  ],
] as const)('%s', (_name, makeFake, makeAdapter) => {
  it('is message-oriented and forces binary', () => {
    const f = makeFake();
    const a = makeAdapter(f);
    expect(a.kind).toBe('message');
    expect(f.binaryType).toBe('arraybuffer');
  });
  it('yields ArrayBuffer and Uint8Array messages and ends on close', async () => {
    const f = makeFake();
    const a = makeAdapter(f);
    const got = collect(a.receive());
    f.emit('message', { data: new Uint8Array([1]).buffer });
    f.emit('message', { data: new Uint8Array([2]) });
    f.emit('close', { code: 1000 });
    expect((await got).map((x) => x[0])).toEqual([1, 2]);
  });
  it('treats a text message as a protocol error', async () => {
    const f = makeFake();
    const a = makeAdapter(f);
    const got = collect(a.receive());
    f.emit('message', { data: 'hello' });
    await expect(got).rejects.toBeInstanceOf(XumuxProtocolError);
  });
  it('ends on error', async () => {
    const f = makeFake();
    const a = makeAdapter(f);
    const got = collect(a.receive());
    f.emit('error', {});
    expect(await got).toEqual([]);
  });
  it('applies backpressure while bufferedAmount is high', async () => {
    const f = makeFake();
    const a = makeAdapter(f);
    expect(a.send(new Uint8Array(1))).toBeUndefined();
    f.bufferedAmount = 500;
    const p = a.send(new Uint8Array(1));
    expect(p).toBeInstanceOf(Promise);
    let done = false;
    void p!.then(() => (done = true));
    await new Promise((r) => setTimeout(r, 15));
    expect(done).toBe(false);
    f.bufferedAmount = 10;
    await p;
    expect(f.sent).toHaveLength(2);
  });
  it('close() closes the underlying object', () => {
    const f = makeFake();
    makeAdapter(f).close();
    expect(f.close).toHaveBeenCalled();
  });
});

class FakeSocket extends EventEmitter {
  destroyed = false;
  chunks: Uint8Array[] = [];
  writeReturns = true;
  ended = false;
  private readonly input = new PassThrough();
  write(d: Uint8Array): boolean {
    this.chunks.push(d);
    return this.writeReturns;
  }
  end(): void {
    this.ended = true;
  }
  destroy(): void {
    this.destroyed = true;
    this.input.end();
  }
  feed(b: Uint8Array): void {
    this.input.write(Buffer.from(b));
  }
  finish(): void {
    this.input.end();
  }
  [Symbol.asyncIterator](): AsyncIterator<Buffer> {
    return this.input[Symbol.asyncIterator]() as AsyncIterator<Buffer>;
  }
}

describe('TcpAdapter', () => {
  it('is a byte stream that passes chunks through unchanged', async () => {
    const s = new FakeSocket();
    const a = new TcpAdapter(s as unknown as Socket);
    expect(a.kind).toBe('stream');
    const got = collect(a.receive());
    s.feed(new Uint8Array([1, 2, 3]));
    s.finish();
    expect(Array.from((await got)[0]!)).toEqual([1, 2, 3]);
  });
  it('waits for drain when write() returns false', async () => {
    const s = new FakeSocket();
    const a = new TcpAdapter(s as unknown as Socket);
    expect(a.send(new Uint8Array([1]))).toBeUndefined();
    s.writeReturns = false;
    const p = a.send(new Uint8Array([2]));
    expect(p).toBeInstanceOf(Promise);
    s.emit('drain');
    await p;
    expect(s.chunks).toHaveLength(2);
  });
  it('half-closes, and ignores sends after destroy', () => {
    const s = new FakeSocket();
    const a = new TcpAdapter(s as unknown as Socket);
    a.close();
    expect(s.ended).toBe(true);
    s.destroyed = true;
    a.close();
    expect(a.send(new Uint8Array([1]))).toBeUndefined();
    expect(s.chunks).toHaveLength(0);
  });
});

describe('StdioAdapter', () => {
  it('passes bytes both ways and ends output on close', async () => {
    const input = new PassThrough();
    const output = new PassThrough({ highWaterMark: 1 });
    const a = new StdioAdapter(input, output);
    expect(a.kind).toBe('stream');
    const got = collect(a.receive());
    input.end(Buffer.from([7, 8]));
    expect(Array.from((await got)[0]!)).toEqual([7, 8]);
    const p = a.send(new Uint8Array(16));
    if (p) {
      output.read();
      await p;
    }
    a.close();
    expect(output.writableEnded).toBe(true);
    expect(a.send(new Uint8Array(1))).toBeUndefined();
  });
  it('accepts string chunks', async () => {
    const input = new PassThrough({ encoding: 'binary' });
    const a = new StdioAdapter(input, new PassThrough());
    const got = collect(a.receive());
    input.end(Buffer.from([0xff]));
    expect(Array.from((await got)[0]!)).toEqual([0xff]);
  });
});

describe('QuicAdapter', () => {
  it('is a stream without magic, and writes and reads chunks', async () => {
    const written: Uint8Array[] = [];
    let ctrl!: ReadableStreamDefaultController<Uint8Array>;
    const a = new QuicAdapter({
      readable: new ReadableStream({ start: (c) => void (ctrl = c) }),
      writable: new WritableStream({ write: (c) => void written.push(c) }),
    });
    expect([a.kind, a.magic]).toEqual(['stream', false]);
    await a.send(new Uint8Array([1]));
    const got = collect(a.receive());
    ctrl.enqueue(new Uint8Array([2]));
    ctrl.close();
    expect((await got).map((x) => x[0])).toEqual([2]);
    a.close();
    await new Promise((r) => setTimeout(r, 1));
    expect(written.map((x) => x[0])).toEqual([1]);
  });
});

describe('createInMemoryPair', () => {
  it('copies data, honours kind, and closes both directions', async () => {
    const [a, b] = createInMemoryPair('stream');
    expect(a.kind).toBe('stream');
    const buf = new Uint8Array([1]);
    void a.send(buf);
    buf[0] = 9;
    a.close();
    a.close();
    void a.send(new Uint8Array([2]));
    expect((await collect(b.receive())).map((x) => x[0])).toEqual([1]);
  });
});

describe('review regressions (adapters)', () => {
  it('QuicAdapter.send rejects when the stream write fails', async () => {
    const writable = new WritableStream<Uint8Array>({
      write: () => Promise.reject(new Error('stream reset')),
    });
    const q = new QuicAdapter({ readable: new ReadableStream(), writable });
    await expect(q.send(new Uint8Array([1]))).rejects.toThrow('stream reset');
  });

  it.each(['websocket', 'webrtc'] as const)(
    '%s adapter removes its listeners when input ends',
    (kind) => {
      const target = new EventTarget();
      const like = {
        readyState: kind === 'websocket' ? 1 : 'open',
        send: () => undefined,
        close: () => undefined,
        addEventListener: (t: string, l: (...a: unknown[]) => void) =>
          target.addEventListener(t, l),
        removeEventListener: vi.fn((t: string, l: (...a: unknown[]) => void) =>
          target.removeEventListener(t, l),
        ),
      };
      if (kind === 'websocket') new WebSocketAdapter(like as unknown as WebSocketLike);
      else new WebRTCAdapter(like as unknown as DataChannelLike);
      target.dispatchEvent(new Event('close'));
      expect(like.removeEventListener.mock.calls.map((c) => c[0]).sort()).toEqual(
        kind === 'websocket'
          ? ['close', 'error', 'message']
          : ['bufferedamountlow', 'close', 'error', 'message', 'open'],
      );
    },
  );
});
