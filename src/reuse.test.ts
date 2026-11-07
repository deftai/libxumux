import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import type { TransportAdapter } from './types.js';
import type { XumuxChannel } from './channel.js';

/**
 * Delivers each frame at once but resolves send() later, as a stream transport does under
 * backpressure: the bytes are already on their way while the write is still "pending".
 */
function lateAck(t: TransportAdapter, ms: number): TransportAdapter {
  return {
    kind: t.kind,
    magic: t.magic,
    async send(data) {
      await t.send(data);
      await new Promise((r) => setTimeout(r, ms));
    },
    receive: () => t.receive(),
    close: () => t.close(),
  };
}

async function pair(serverCloses: boolean) {
  const [ct, st] = createInMemoryPair('message');
  const opts = { pingInterval: 0, closeTimeoutMs: 500 };
  const client = new XumuxClient(lateAck(ct, 20), opts);
  const server = new XumuxServer(lateAck(st, 20), { ...opts, onChannelRequest: () => true });
  server.addEventListener('channel', (e) => {
    if (serverCloses) void (e as CustomEvent<XumuxChannel>).detail.close();
  });
  await Promise.all([client.connect(), server.accept()]);
  return { client, server };
}

// Core §5.4: a channel is fully closed once a side has sent and received CLOSE_CHANNEL, and
// the opener may then reuse its ID. The peer must not still treat the ID as in use.
describe('channel ID reuse', () => {
  for (const serverCloses of [true, false]) {
    const who = serverCloses ? 'acceptor' : 'opener';
    it(`frees the ID when the close completes (${who} closes first)`, async () => {
      const { client, server } = await pair(serverCloses);
      let failure: unknown;
      void server.closed.then((info) => (failure ??= info));
      void client.closed.then((info) => (failure ??= info));
      for (let i = 0; i < 20 && !failure; i++) {
        const ch = await client.openChannel({ name: 'x', reliable: true, ordered: true });
        expect(ch.id).toBe(1); // the lowest free odd ID: reused every time
        if (serverCloses) await ch.closed;
        else await ch.close();
      }
      expect(failure).toBeUndefined();
      await client.close();
    }, 30_000);
  }
});
