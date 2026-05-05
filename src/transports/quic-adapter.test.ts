import { describe, it, expect } from 'vitest';
import { QuicAdapter } from './quic-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';

/** Create a paired ReadableStream/WritableStream backed by a queue. */
function makeStreamPair(): {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
  enqueue: (data: Uint8Array) => void;
  close: () => void;
} {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  const readable = new ReadableStream<Uint8Array>({
    start(ctrl) { controller = ctrl; },
  });
  const written: Uint8Array[] = [];
  const writable = new WritableStream<Uint8Array>({
    write(chunk) { written.push(chunk); },
  });
  return {
    readable,
    writable,
    enqueue: (data: Uint8Array) => { controller.enqueue(data); },
    close: () => { controller.close(); },
  };
}

describe('QuicAdapter', () => {
  it('send() writes to the writable stream', async () => {
    const written: Uint8Array[] = [];
    const readable = new ReadableStream<Uint8Array>({ start() {} });
    const writable = new WritableStream<Uint8Array>({ write(chunk) { written.push(chunk); } });
    const adapter = new QuicAdapter({ readable, writable });

    const frame = encodeFrame({ channel: 1, type: 1, flags: 0, payload: new Uint8Array([11]) });
    adapter.send(frame);

    // Allow the microtask to flush
    await Promise.resolve();
    expect(written).toHaveLength(1);
    expect(written[0]).toEqual(frame);
  });

  it('receive() yields chunks from the readable stream', async () => {
    const { readable, writable, enqueue, close } = makeStreamPair();
    const adapter = new QuicAdapter({ readable, writable });

    const frames: Uint8Array[] = [];
    const done = (async () => {
      for await (const f of adapter.receive()) {
        frames.push(f);
      }
    })();

    const frame = encodeFrame({ channel: 7, type: 3, flags: 0, payload: new Uint8Array([88]) });
    enqueue(frame);
    close();
    await done;

    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).channel).toBe(7);
  });

  it('close() closes the writable stream writer', () => {
    const { readable, writable } = makeStreamPair();
    const adapter = new QuicAdapter({ readable, writable });
    // Should not throw
    expect(() => adapter.close()).not.toThrow();
  });
});
