/**
 * E2E tests: WebRTC DataChannel adapter — loopback mock pair.
 *
 * Two DataChannelLike objects are wired together in memory:
 *   dc1.send(data) → fires dc2's 'message' listeners
 *   dc2.send(data) → fires dc1's 'message' listeners
 *   close() on either → fires both sides' 'close' listeners
 *
 * This tests that WebRTCAdapter correctly adapts the DataChannel event model
 * (addEventListener/onmessage/onclose) into the async-iterable receive() protocol
 * that XumuxClient/XumuxServer consume — without any native binary dependencies
 * or real ICE negotiation.
 */
import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { WebRTCAdapter } from '../../src/transports/webrtc-adapter.js';
import type { DataChannelLike } from '../../src/transports/webrtc-adapter.js';
import { runProtocolScenarios, expectAll, E2E_CHANNELS, E2E_OPTIONS } from './helpers.js';

// ─── Loopback DC Pair ─────────────────────────────────────────────────────────

type Listener = (...args: unknown[]) => void;

/**
 * Create a pair of in-memory DataChannelLike objects.
 * Frames sent on dc[0] arrive at dc[1]'s 'message' listeners and vice-versa.
 * Calling close() on either side fires 'close' on both.
 */
function createLoopbackDCPair(): [DataChannelLike, DataChannelLike] {
  const listenersA = new Map<string, Listener[]>();
  const listenersB = new Map<string, Listener[]>();
  let closed = false;

  function addListener(map: Map<string, Listener[]>, type: string, fn: Listener): void {
    if (!map.has(type)) map.set(type, []);
    map.get(type)!.push(fn);
  }

  function removeListener(map: Map<string, Listener[]>, type: string, fn: Listener): void {
    const arr = map.get(type) ?? [];
    const idx = arr.indexOf(fn);
    if (idx >= 0) arr.splice(idx, 1);
  }

  function fire(map: Map<string, Listener[]>, type: string, ...args: unknown[]): void {
    for (const fn of map.get(type) ?? []) fn(...args);
  }

  function closeAll(): void {
    if (closed) return;
    closed = true;
    fire(listenersA, 'close');
    fire(listenersB, 'close');
  }

  const dcA: DataChannelLike = {
    get readyState() {
      return closed ? 'closed' : 'open';
    },
    send(data: Uint8Array | ArrayBuffer) {
      if (closed) return;
      const frame = data instanceof Uint8Array ? new Uint8Array(data) : new Uint8Array(data);
      // Deliver as a microtask so the receiving side's addEventListener('message')
      // has time to register before delivery (mirrors real async transport behavior).
      // NOTE: do NOT re-check `closed` inside the microtask — the CLOSE_ack frame
      // is sent before closeAll() runs synchronously, and must still be delivered.
      queueMicrotask(() => fire(listenersB, 'message', { data: frame }));
    },
    close() {
      closeAll();
    },
    addEventListener(type: string, fn: Listener) {
      addListener(listenersA, type, fn);
    },
    removeEventListener(type: string, fn: Listener) {
      removeListener(listenersA, type, fn);
    },
  };

  const dcB: DataChannelLike = {
    get readyState() {
      return closed ? 'closed' : 'open';
    },
    send(data: Uint8Array | ArrayBuffer) {
      if (closed) return;
      const frame = data instanceof Uint8Array ? new Uint8Array(data) : new Uint8Array(data);
      queueMicrotask(() => fire(listenersA, 'message', { data: frame }));
    },
    close() {
      closeAll();
    },
    addEventListener(type: string, fn: Listener) {
      addListener(listenersB, type, fn);
    },
    removeEventListener(type: string, fn: Listener) {
      removeListener(listenersB, type, fn);
    },
  };

  return [dcA, dcB];
}

// ─── Tests ────────────────────────────────────────────────────────────────────

describe('WebRTC Loopback DC — all 5 protocol scenarios', () => {
  it('completes handshake, data exchange, fragmentation, dynamic channel, and graceful close', async () => {
    const [dcClient, dcServer] = createLoopbackDCPair();

    const clientTransport = new WebRTCAdapter(dcClient);
    const serverTransport = new WebRTCAdapter(dcServer);

    const client = new XumuxClient(clientTransport, { pingInterval: 0 });
    const server = new XumuxServer(serverTransport, E2E_OPTIONS);

    const [clientCh, serverCh] = await Promise.all([
      client.connect({ channels: E2E_CHANNELS }),
      server.accept(),
    ]);

    const results = await runProtocolScenarios(client, server, clientCh, serverCh);

    expectAll(results, expect);
  });

  it('send() on a closed loopback DC is a no-op (does not throw)', () => {
    const [dcA] = createLoopbackDCPair();
    dcA.close();
    expect(() => dcA.send(new Uint8Array([1, 2, 3]))).not.toThrow();
  });

  it('close() is idempotent — calling it twice does not throw', () => {
    const [dcA] = createLoopbackDCPair();
    dcA.close();
    expect(() => dcA.close()).not.toThrow();
  });

  it('readyState reflects closed state after close()', () => {
    const [dcA, dcB] = createLoopbackDCPair();
    expect(dcA.readyState).toBe('open');
    expect(dcB.readyState).toBe('open');
    dcA.close();
    expect(dcA.readyState).toBe('closed');
    expect(dcB.readyState).toBe('closed');
  });

  it('closing one side terminates both sides receive() iterators', async () => {
    const [dcA, dcB] = createLoopbackDCPair();
    const adapterA = new WebRTCAdapter(dcA);
    const adapterB = new WebRTCAdapter(dcB);

    const framesA: Uint8Array[] = [];
    const framesB: Uint8Array[] = [];

    const doneA = (async () => {
      for await (const f of adapterA.receive()) framesA.push(f);
    })();
    const doneB = (async () => {
      for await (const f of adapterB.receive()) framesB.push(f);
    })();

    dcA.close(); // closes both sides
    await Promise.all([doneA, doneB]);

    expect(framesA).toHaveLength(0);
    expect(framesB).toHaveLength(0);
  });
});
