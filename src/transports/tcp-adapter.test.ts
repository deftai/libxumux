import { describe, it, expect } from 'vitest';
import { TcpAdapter } from './tcp-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';
import { getMagicBytes } from '../codec/magic.js';
import type { Socket } from 'node:net';

/** Minimal mock that behaves like a net.Socket for TcpAdapter's needs. */
class MockSocket {
  written: Uint8Array[] = [];
  destroyed = false;

  private queue: Buffer[] = [];
  private waiters: Array<() => void> = [];
  private closed = false;

  write(data: Uint8Array | Buffer): boolean {
    this.written.push(new Uint8Array(data instanceof Uint8Array ? data : data));
    return true;
  }

  destroy(): void {
    this.destroyed = true;
    this.closeStream();
  }

  closeStream(): void {
    this.closed = true;
    for (const w of this.waiters) w();
    this.waiters.length = 0;
  }

  push(data: Buffer): void {
    this.queue.push(data);
    this.waiters.shift()?.();
  }

  async *[Symbol.asyncIterator](): AsyncGenerator<Buffer> {
    while (!this.closed || this.queue.length > 0) {
      if (this.queue.length === 0) {
        if (this.closed) break;
        await new Promise<void>((r) => this.waiters.push(r));
        continue;
      }
      yield this.queue.shift()!;
    }
  }
}

describe('TcpAdapter', () => {
  it('send() prepends OMUX magic bytes on the first call only', () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);
    const frame = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array([1]) });

    adapter.send(frame);
    adapter.send(frame);

    // First write is magic, second is frame, third is frame (second send)
    expect(socket.written[0]).toEqual(getMagicBytes());
    expect(socket.written[1]).toEqual(frame);
    expect(socket.written[2]).toEqual(frame);
    expect(socket.written).toHaveLength(3);
  });

  it('receive() validates OMUX magic bytes and yields frames', async () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);

    const frameBytes = encodeFrame({ channel: 1, type: 2, flags: 0, payload: new Uint8Array([42]) });

    // Push magic + frame as one chunk
    const combined = new Uint8Array(4 + frameBytes.length);
    combined.set(getMagicBytes());
    combined.set(frameBytes, 4);
    socket.push(Buffer.from(combined));
    socket.closeStream();

    const frames: Uint8Array[] = [];
    for await (const f of adapter.receive()) {
      frames.push(f);
    }

    expect(frames).toHaveLength(1);
    const decoded = decodeFrame(frames[0]!);
    expect(decoded.channel).toBe(1);
    expect(decoded.payload[0]).toBe(42);
  });

  it('receive() handles magic split across multiple chunks', async () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);
    const frameBytes = encodeFrame({ channel: 2, type: 3, flags: 0, payload: new Uint8Array([7]) });
    const magic = getMagicBytes();

    // Push magic 2 bytes at a time, then frame
    socket.push(Buffer.from(magic.slice(0, 2)));
    socket.push(Buffer.from(magic.slice(2, 4)));
    socket.push(Buffer.from(frameBytes));
    socket.closeStream();

    const frames: Uint8Array[] = [];
    for await (const f of adapter.receive()) frames.push(f);
    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).channel).toBe(2);
  });

  it('receive() throws when magic bytes are invalid', async () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);

    socket.push(Buffer.from([0x00, 0x00, 0x00, 0x00])); // wrong magic
    socket.closeStream();

    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _frame of adapter.receive()) { /* consume */ }
    }).rejects.toThrow();
  });

  it('receive() decodes frames arriving after magic in separate chunks', async () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);
    const frameBytes = encodeFrame({ channel: 3, type: 1, flags: 0, payload: new Uint8Array([99]) });

    // Magic in one chunk, frame split across two chunks
    socket.push(Buffer.from(getMagicBytes()));
    socket.push(Buffer.from(frameBytes.slice(0, 4)));
    socket.push(Buffer.from(frameBytes.slice(4)));
    socket.closeStream();

    const frames: Uint8Array[] = [];
    for await (const f of adapter.receive()) frames.push(f);
    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).payload[0]).toBe(99);
  });

  it('close() calls socket.destroy()', () => {
    const socket = new MockSocket();
    const adapter = new TcpAdapter(socket as unknown as Socket);
    adapter.close();
    expect(socket.destroyed).toBe(true);
  });
});
