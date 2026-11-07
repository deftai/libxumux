import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { DEFAULT_INITIAL_WINDOW, DEFAULT_MAX_CHANNEL_WINDOW } from './codec/constants.js';
import type { TransportAdapter } from './types.js';
import type { XumuxChannel } from './channel.js';

/** Delays every send by a fixed one-way latency, in order, without limiting bandwidth. */
function delayed(t: TransportAdapter, oneWayMs: number): TransportAdapter {
  let chain = Promise.resolve();
  return {
    kind: t.kind,
    magic: t.magic,
    send(data) {
      const copy = new Uint8Array(data);
      const due = performance.now() + oneWayMs;
      chain = chain.then(async () => {
        const wait = due - performance.now();
        if (wait > 0) await new Promise((r) => setTimeout(r, wait));
        await t.send(copy);
      });
    },
    receive: () => t.receive(),
    close() {
      void chain.then(() => t.close());
    },
  };
}

const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 500 };

async function delayedPair(oneWayMs: number, sopts: XumuxOptions = {}, channels = 1) {
  const [ct, st] = createInMemoryPair('message');
  const client = new XumuxClient(delayed(ct, oneWayMs), base);
  const server = new XumuxServer(delayed(st, oneWayMs), {
    ...base,
    onChannelRequest: () => true,
    ...sopts,
  });
  const specs = Array.from({ length: channels }, (_, i) => ({
    name: `bulk${i}`,
    reliable: true,
    ordered: true,
  }));
  const [c, s] = await Promise.all([client.connect({ channels: specs }), server.accept()]);
  return { client, server, sending: c.channels, receiving: s.channels };
}

/** Send n bytes on ch; resolve with ms until peer has read them all. */
async function blast(ch: XumuxChannel, peer: XumuxChannel, n: number): Promise<number> {
  const start = performance.now();
  const chunk = new Uint8Array(64 * 1024);
  const sending = (async () => {
    for (let sent = 0; sent < n; sent += chunk.length) await ch.send(1, chunk);
  })();
  let got = 0;
  for await (const m of peer) {
    got += m.payload.length;
    if (got >= n) break;
  }
  await sending;
  return performance.now() - start;
}

describe('receive-window auto-tuning', () => {
  it('grows the window on a long-RTT link', async () => {
    const oneWay = 20; // 40 ms RTT
    const { client, server, sending, receiving } = await delayedPair(oneWay);
    const n = 32 * 1024 * 1024;
    const ms = await blast(sending[0]!, receiving[0]!, n);
    const rate = n / (ms / 1000);
    const fixed = DEFAULT_INITIAL_WINDOW / ((2 * oneWay) / 1000);
    expect(receiving[0]!.receiveWindow).toBeGreaterThan(DEFAULT_INITIAL_WINDOW);
    expect(receiving[0]!.receiveWindow).toBeLessThanOrEqual(DEFAULT_MAX_CHANNEL_WINDOW);
    expect(rate).toBeGreaterThan(4 * fixed);
    await client.close();
    await server.closed;
  }, 30_000);

  it('can be disabled with maxChannelWindow', async () => {
    const { client, sending, receiving } = await delayedPair(10, { maxChannelWindow: -1 });
    await blast(sending[0]!, receiving[0]!, 4 * 1024 * 1024);
    expect(receiving[0]!.receiveWindow).toBe(DEFAULT_INITIAL_WINDOW);
    await client.close();
  }, 30_000);

  it('keeps the sum of windows within connectionWindow, and releases it on close', async () => {
    const budget = 1024 * 1024;
    const { client, server, sending, receiving } = await delayedPair(
      10,
      { connectionWindow: budget },
      2,
    );
    await Promise.all(
      sending.map((ch, i) =>
        blast(ch, receiving.find((r) => r.id === ch.id)!, 8 * 1024 * 1024 + i),
      ),
    );
    const total = receiving.reduce((a, c) => a + c.receiveWindow, 0);
    expect(total).toBeLessThanOrEqual(budget);
    expect(total).toBeGreaterThan(2 * DEFAULT_INITIAL_WINDOW);
    expect(server._windowTotal).toBe(total);
    await Promise.all(sending.map((ch) => ch.close()));
    await Promise.all(receiving.map((ch) => ch.closed));
    expect(server._windowTotal).toBe(0);
    await client.close();
  }, 30_000);

  it('does not grow from a single message without a timed sample', async () => {
    // SRTT is known and tiny; one message as large as the whole window arrives long after
    // the channel opened, so the window was not the limit.
    const { client, server, sending, receiving } = await delayedPair(0);
    await server.ping();
    await new Promise((r) => setTimeout(r, 100));
    await sending[0]!.send(1, new Uint8Array(DEFAULT_INITIAL_WINDOW));
    for await (const m of receiving[0]!) {
      expect(m.payload.length).toBe(DEFAULT_INITIAL_WINDOW);
      break;
    }
    await new Promise((r) => setTimeout(r, 20));
    expect(receiving[0]!.receiveWindow).toBe(DEFAULT_INITIAL_WINDOW);
    await client.close();
  });

  it('re-measures a stale RTT so growth can resume', async () => {
    // The only RTT sample says 0.001 ms, but the link is 40 ms: every growth check fails
    // against the stale sample until a fresh probe replaces it.
    const { client, server, sending, receiving } = await delayedPair(20);
    const internals = server as unknown as { controlStats: { sample(ms: number): void } };
    internals.controlStats.sample(0.001);
    await blast(sending[0]!, receiving[0]!, 32 * 1024 * 1024);
    expect(receiving[0]!.receiveWindow).toBeGreaterThan(DEFAULT_INITIAL_WINDOW);
    expect(server.rtt).toBeGreaterThan(1);
    await client.close();
  }, 30_000);

  it('rejects invalid window options', () => {
    const [t] = createInMemoryPair('message');
    for (const bad of [
      { connectionWindow: Number.NaN },
      { connectionWindow: -1 },
      { connectionWindow: 1.5 },
      { maxChannelWindow: Number.POSITIVE_INFINITY },
      { maxChannelWindow: Number('lots') },
      { initialWindow: Number.NaN },
    ])
      expect(() => new XumuxServer(t, bad)).toThrow(RangeError);
    expect(() => new XumuxServer(t, { maxChannelWindow: -1, connectionWindow: 0 })).not.toThrow();
  });
});
