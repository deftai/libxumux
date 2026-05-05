import { describe, it, expect, vi } from 'vitest';
import { WebRTCAdapter } from './webrtc-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';
import type { DataChannelLike } from './webrtc-adapter.js';

function makeMockDC(): DataChannelLike & {
  listeners: Map<string, Array<(...args: unknown[]) => void>>;
  sent: Uint8Array[];
  triggerMessage(data: ArrayBuffer): void;
  triggerClose(): void;
} {
  const listeners = new Map<string, Array<(...args: unknown[]) => void>>();
  const sent: Uint8Array[] = [];

  return {
    readyState: 'open',
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
        listener();
      }
    },
  };
}

describe('WebRTCAdapter', () => {
  it('send() passes data to the DataChannel', () => {
    const dc = makeMockDC();
    const adapter = new WebRTCAdapter(dc);
    const frame = encodeFrame({ channel: 2, type: 1, flags: 0, payload: new Uint8Array([33]) });
    adapter.send(frame);
    expect(dc.sent).toHaveLength(1);
    expect(dc.sent[0]).toEqual(frame);
  });

  it('receive() yields incoming frames', async () => {
    const dc = makeMockDC();
    const adapter = new WebRTCAdapter(dc);
    const frameBytes = encodeFrame({ channel: 4, type: 2, flags: 0, payload: new Uint8Array([77]) });

    const gen = adapter.receive()[Symbol.asyncIterator]();
    const next = gen.next();

    dc.triggerMessage(frameBytes.buffer);
    const result = await next;
    const raw = result.value as Uint8Array | undefined;

    expect(decodeFrame(raw!).channel).toBe(4);
  });

  it('receive() terminates when DataChannel closes', async () => {
    const dc = makeMockDC();
    const adapter = new WebRTCAdapter(dc);
    const frames: Uint8Array[] = [];

    const done = (async () => {
      for await (const f of adapter.receive()) frames.push(f);
    })();

    dc.triggerClose();
    await done;
    expect(frames).toHaveLength(0);
  });

  it('close() calls dc.close()', () => {
    const dc = makeMockDC();
    const adapter = new WebRTCAdapter(dc);
    adapter.close();
    // eslint-disable-next-line @typescript-eslint/unbound-method
    expect(vi.isMockFunction(dc.close) && dc.close).toHaveBeenCalled();
  });
});
