import { describe, it, expect, vi } from 'vitest';
import { WebSocketAdapter } from './websocket-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';
import type { WebSocketLike } from './websocket-adapter.js';

function makeMockWs(): WebSocketLike & {
  listeners: Map<string, Array<(...args: unknown[]) => void>>;
  sent: Uint8Array[];
  triggerMessage(data: ArrayBuffer): void;
  triggerClose(): void;
} {
  const listeners = new Map<string, Array<(...args: unknown[]) => void>>();
  const sent: Uint8Array[] = [];

  const ws = {
    readyState: 1,
    binaryType: 'arraybuffer' as string | undefined,
    sent,
    listeners,
    send(data: Uint8Array | ArrayBuffer) {
      sent.push(data instanceof Uint8Array ? data : new Uint8Array(data));
    },
    close: vi.fn(),
    addEventListener(type: string, listener: (...args: unknown[]) => void) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type)!.push(listener);
    },
    removeEventListener(type: string, listener: (...args: unknown[]) => void) {
      const arr = listeners.get(type) ?? [];
      const idx = arr.indexOf(listener);
      if (idx >= 0) arr.splice(idx, 1);
    },
    triggerMessage(data: ArrayBuffer) {
      for (const listener of listeners.get('message') ?? []) {
        listener({ data });
      }
    },
    triggerClose() {
      for (const listener of listeners.get('close') ?? []) {
        listener({ code: 1000 });
      }
    },
  };
  return ws;
}

describe('WebSocketAdapter', () => {
  it('send() passes data to the underlying WebSocket', () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    const frame = encodeFrame({ channel: 1, type: 2, flags: 0, payload: new Uint8Array([7]) });
    adapter.send(frame);
    expect(ws.sent).toHaveLength(1);
    expect(ws.sent[0]).toEqual(frame);
  });

  it('sets binaryType to arraybuffer on construction', () => {
    const ws = makeMockWs();
    new WebSocketAdapter(ws);
    expect(ws.binaryType).toBe('arraybuffer');
  });

  it('receive() yields incoming ArrayBuffer messages as frames', async () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    const frameBytes = encodeFrame({ channel: 3, type: 1, flags: 0, payload: new Uint8Array([55]) });

    const gen = adapter.receive()[Symbol.asyncIterator]();
    const next = gen.next();

    ws.triggerMessage(frameBytes.buffer);
    const result = await next;
    const raw = result.value as Uint8Array | undefined;

    const frame = decodeFrame(raw!);
    expect(frame.channel).toBe(3);
    expect(frame.payload[0]).toBe(55);
  });

  it('receive() terminates when WebSocket closes', async () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    const frames: Uint8Array[] = [];

    const done = (async () => {
      for await (const f of adapter.receive()) {
        frames.push(f);
      }
    })();

    ws.triggerClose();
    await done;
    expect(frames).toHaveLength(0);
  });

  it('receive() yields incoming Uint8Array messages (not just ArrayBuffer)', async () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    const frameBytes = encodeFrame({ channel: 5, type: 1, flags: 0, payload: new Uint8Array([99]) });

    const gen = adapter.receive()[Symbol.asyncIterator]();
    const next = gen.next();

    // Trigger as Uint8Array (not ArrayBuffer)
    for (const listener of ws.listeners.get('message') ?? []) {
      listener({ data: frameBytes }); // Uint8Array directly
    }
    const result = await next;
    const raw = result.value as Uint8Array | undefined;
    expect(decodeFrame(raw!).channel).toBe(5);
  });

  it('receive() ignores non-binary (string) messages', async () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    const received: Uint8Array[] = [];

    const done = (async () => {
      for await (const f of adapter.receive()) received.push(f);
    })();

    // Trigger a string message — should be ignored
    for (const listener of ws.listeners.get('message') ?? []) {
      listener({ data: 'not binary' });
    }
    // Close to terminate the iterator
    ws.triggerClose();
    await done;
    expect(received).toHaveLength(0);
  });

  it('close() calls ws.close with code 1000', () => {
    const ws = makeMockWs();
    const adapter = new WebSocketAdapter(ws);
    adapter.close();
    // eslint-disable-next-line @typescript-eslint/unbound-method
    expect(vi.isMockFunction(ws.close) && ws.close).toHaveBeenCalledWith(1000, 'xumux close');
  });
});
