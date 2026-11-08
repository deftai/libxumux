import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { errorFrame } from './control/codec.js';
import { CloseCode, DEFAULT_MAX_CONTROL_QUEUE } from './codec/constants.js';
import type { TransportAdapter } from './types.js';
import type { Frame } from './codec/frame.js';

/** Passes sends through until held; then they never complete, as when the peer stops reading. */
function holdable(t: TransportAdapter) {
  let held = false;
  const adapter: TransportAdapter = {
    kind: t.kind,
    magic: t.magic,
    send: (d) => (held ? new Promise<void>(() => undefined) : t.send(d)),
    receive: () => t.receive(),
    close: () => t.close(),
  };
  return { adapter, hold: () => (held = true) };
}

async function serverWithHeldSends(opts: XumuxOptions) {
  const [ct, st] = createInMemoryPair('message');
  const h = holdable(st);
  const client = new XumuxClient(ct, { pingInterval: 0, closeTimeoutMs: 100 });
  const server = new XumuxServer(h.adapter, { pingInterval: 0, closeTimeoutMs: 100, ...opts });
  await Promise.all([client.connect(), server.accept()]);
  h.hold();
  return { client, server };
}

type Internals = { enqueueUrgent(f: Frame): Promise<void>; urgentBytes: number };

function queueErrors(server: XumuxServer, n: number): number {
  const s = server as unknown as Internals;
  let queued = 0;
  for (; queued < n; queued++) {
    if (server.state !== 'open') break;
    s.enqueueUrgent(errorFrame({ code: CloseCode.UNSUPPORTED, reason: 'unsupported' })).catch(
      () => undefined,
    );
  }
  return queued;
}

describe('control queue cap', () => {
  it('closes with 4004 once queued control frames pass maxControlQueue', async () => {
    const limit = 64 * 1024;
    const { server } = await serverWithHeldSends({ maxControlQueue: limit });
    const queued = queueErrors(server, 1_000_000);
    expect(queued).toBeLessThan(1_000_000);
    // The cap, plus the CLOSE frame queued after it (exempt, so the close can be sent).
    expect((server as unknown as Internals).urgentBytes).toBeLessThanOrEqual(limit + 256);
    const info = await server.closed;
    expect(info.code).toBe(CloseCode.RATE_LIMITED);
    expect(info.reason).toContain('not reading');
  });

  it('defaults to 1 MiB, and a negative value turns it off', async () => {
    expect(DEFAULT_MAX_CONTROL_QUEUE).toBe(1024 * 1024);
    const { server, client } = await serverWithHeldSends({ maxControlQueue: -1 });
    expect(queueErrors(server, 100_000)).toBe(100_000);
    expect((server as unknown as Internals).urgentBytes).toBeGreaterThan(DEFAULT_MAX_CONTROL_QUEUE);
    expect(server.state).toBe('open');
    await client.close();
  });

  it('rejects a non-integer maxControlQueue', () => {
    const [t] = createInMemoryPair('message');
    expect(() => new XumuxServer(t, { maxControlQueue: Number.NaN })).toThrow(RangeError);
  });

  it('the default leaves room for a peer opening 1024 channels at once', async () => {
    const [ct, st] = createInMemoryPair('message');
    const client = new XumuxClient(ct, { pingInterval: 0 });
    const server = new XumuxServer(st, { pingInterval: 0, onChannelRequest: () => true });
    await Promise.all([client.connect(), server.accept()]);
    const chans = await Promise.all(
      Array.from({ length: 1024 }, (_, i) =>
        client.openChannel({ name: `c${i}`, reliable: true, ordered: true }),
      ),
    );
    expect(chans).toHaveLength(1024);
    expect(server.state).toBe('open');
    await client.close();
  });
});
