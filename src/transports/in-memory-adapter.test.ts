import { describe, it, expect } from 'vitest';
import { createInMemoryPair } from './in-memory-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';

async function collectN(
  iterable: AsyncIterable<Uint8Array>,
  n: number,
): Promise<Uint8Array[]> {
  const results: Uint8Array[] = [];
  for await (const item of iterable) {
    results.push(item);
    if (results.length >= n) break;
  }
  return results;
}

describe('createInMemoryPair', () => {
  it('frames sent on A arrive at B', async () => {
    const [a, b] = createInMemoryPair();
    const frame = encodeFrame({ channel: 1, type: 2, flags: 0, payload: new Uint8Array([42]) });

    const receivePromise = collectN(b.receive(), 1);
    a.send(frame);
    const received = await receivePromise;

    expect(received).toHaveLength(1);
    const decoded = decodeFrame(received[0]!);
    expect(decoded.channel).toBe(1);
    expect(decoded.type).toBe(2);
    expect(decoded.payload[0]).toBe(42);
  });

  it('frames sent on B arrive at A', async () => {
    const [a, b] = createInMemoryPair();
    const frame = encodeFrame({ channel: 5, type: 3, flags: 0, payload: new Uint8Array([99]) });

    const receivePromise = collectN(a.receive(), 1);
    b.send(frame);
    const received = await receivePromise;

    expect(received).toHaveLength(1);
    expect(decodeFrame(received[0]!).channel).toBe(5);
  });

  it('multiple frames are delivered in order', async () => {
    const [a, b] = createInMemoryPair();
    const f1 = encodeFrame({ channel: 1, type: 1, flags: 0, payload: new Uint8Array([1]) });
    const f2 = encodeFrame({ channel: 2, type: 2, flags: 0, payload: new Uint8Array([2]) });
    const f3 = encodeFrame({ channel: 3, type: 3, flags: 0, payload: new Uint8Array([3]) });

    const receivePromise = collectN(b.receive(), 3);
    a.send(f1);
    a.send(f2);
    a.send(f3);

    const received = await receivePromise;
    expect(received).toHaveLength(3);
    expect(decodeFrame(received[0]!).channel).toBe(1);
    expect(decodeFrame(received[1]!).channel).toBe(2);
    expect(decodeFrame(received[2]!).channel).toBe(3);
  });

  it('closing A terminates B\'s receive iterator', async () => {
    const [a, b] = createInMemoryPair();

    const frames: Uint8Array[] = [];
    const done = (async () => {
      for await (const f of b.receive()) {
        frames.push(f);
      }
    })();

    const f1 = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array(0) });
    a.send(f1);
    a.close();

    await done;
    expect(frames).toHaveLength(1);
  });

  it('send copies data (mutations to original do not affect queued frame)', async () => {
    const [a, b] = createInMemoryPair();
    const payload = new Uint8Array([10, 20, 30]);
    const frame = encodeFrame({ channel: 0, type: 1, flags: 0, payload });

    const receivePromise = collectN(b.receive(), 1);
    a.send(frame);
    frame[0] = 0xff; // mutate after send

    const received = await receivePromise;
    expect(received[0]![0]).not.toBe(0xff); // original byte preserved
  });

  it('two pairs are independent', async () => {
    const [a1, b1] = createInMemoryPair();
    const [a2, b2] = createInMemoryPair();

    const frame = encodeFrame({ channel: 7, type: 7, flags: 0, payload: new Uint8Array([7]) });

    const p1 = collectN(b1.receive(), 1);
    const p2 = collectN(b2.receive(), 1);

    a1.send(frame);
    a2.send(encodeFrame({ channel: 8, type: 8, flags: 0, payload: new Uint8Array([8]) }));

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(decodeFrame(r1[0]!).channel).toBe(7);
    expect(decodeFrame(r2[0]!).channel).toBe(8);
  });
});
