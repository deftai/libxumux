import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { decodeFrame } from './codec/decode-frame.js';
import { DEFAULT_INITIAL_WINDOW, FrameFlags } from './codec/constants.js';
import type { Frame } from './codec/frame.js';
import type { TransportAdapter } from './types.js';

const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 300 };
const later = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Records every frame sent on a message transport (one frame per send). */
function tapped(t: TransportAdapter, frames: Frame[]): TransportAdapter {
  return {
    kind: t.kind,
    send: (d) => {
      frames.push(decodeFrame(new Uint8Array(d)));
      return t.send(d);
    },
    receive: () => t.receive(),
    close: () => t.close(),
  };
}

async function pair(copts: XumuxOptions = {}) {
  const [ct, st] = createInMemoryPair('message');
  const sent: Frame[] = [];
  const client = new XumuxClient(tapped(ct, sent), { ...base, ...copts });
  const server = new XumuxServer(st, { ...base, onChannelRequest: () => true });
  const [c, s] = await Promise.all([
    client.connect({ channels: [{ name: 'data', reliable: true, ordered: true }] }),
    server.accept(),
  ]);
  const data = () => sent.filter((f) => f.channel === 1);
  return { client, server, a: c.channels[0]!, b: s.channels[0]!, data };
}

describe('XumuxChannel.sendPrefix', () => {
  it('sends one unfragmented frame of at most fragmentSize', async () => {
    const { a, b, data } = await pair({ fragmentSize: 4096 });
    expect(await a.sendPrefix(7, new Uint8Array(10_000).fill(1))).toBe(4096);
    const m = await b.receive();
    expect(m!.type).toBe(7);
    expect(m!.payload.length).toBe(4096);
    expect(data()).toHaveLength(1);
    expect(data()[0]!.flags & FrameFlags.MORE).toBe(0);
  });

  it('sends only the free credit, whole, instead of fragmenting', async () => {
    const { a, b, data } = await pair();
    // b doesn't read, so its credit isn't returned: leave 5 bytes of the window free.
    await a.send(1, new Uint8Array(DEFAULT_INITIAL_WINDOW - 5));
    const before = data().length;
    expect(await a.sendPrefix(2, new Uint8Array(100).fill(9))).toBe(5);
    const frames = data().slice(before);
    expect(frames).toHaveLength(1);
    expect(frames[0]!.flags).toBe(0);
    expect(frames[0]!.payload).toEqual(new Uint8Array(5).fill(9));
    // Drain b: the message after the big one is those 5 bytes, delivered as their own message.
    let m = await b.receive();
    while (m!.type === 1) m = await b.receive();
    expect(m).toEqual({ type: 2, payload: new Uint8Array(5).fill(9) });
  });

  it('waits for credit; an abort while waiting sends nothing and frees the channel', async () => {
    const { a, b, data } = await pair();
    await a.send(1, new Uint8Array(DEFAULT_INITIAL_WINDOW));
    const before = data().length;
    const ac = new AbortController();
    const p = a.sendPrefix(2, new Uint8Array(10), ac.signal);
    await later(20);
    ac.abort(new Error('gave up'));
    await expect(p).rejects.toThrow('gave up');
    expect(data().length).toBe(before);

    // A later send goes once credit returns: the aborted one no longer holds the channel.
    const next = a.sendPrefix(3, new Uint8Array([1, 2, 3]));
    const first = await b.receive(); // returns credit for the window
    expect(first!.payload.length).toBe(DEFAULT_INITIAL_WINDOW);
    expect(await next).toBe(3);
    expect(await b.receive()).toEqual({ type: 3, payload: new Uint8Array([1, 2, 3]) });

    // And the aborted wait took no credit: the rest of the window is all still free.
    const rest = a.send(5, new Uint8Array(DEFAULT_INITIAL_WINDOW - 3));
    expect(await Promise.race([rest.then(() => 'sent'), later(200).then(() => 'stuck')])).toBe(
      'sent',
    );
  });

  it('rejects at once when the signal has already aborted', async () => {
    const { a, data } = await pair();
    const before = data().length;
    const ac = new AbortController();
    ac.abort();
    await expect(a.sendPrefix(1, new Uint8Array(1), ac.signal)).rejects.toThrow();
    expect(data().length).toBe(before);
  });

  it('sends an empty message without needing credit', async () => {
    const { a, b } = await pair();
    await a.send(1, new Uint8Array(DEFAULT_INITIAL_WINDOW));
    expect(await a.sendPrefix(4, new Uint8Array(0))).toBe(0);
    await b.receive();
    expect(await b.receive()).toEqual({ type: 4, payload: new Uint8Array(0) });
  });

  it('rejects on a closed channel and for an invalid type', async () => {
    const { a } = await pair();
    await expect(a.sendPrefix(256, new Uint8Array(1))).rejects.toThrow(RangeError);
    await a.close();
    await expect(a.sendPrefix(1, new Uint8Array(1))).rejects.toThrow('not open');
  });
});

describe('manual credit', () => {
  it('returns credit only through returnCredit', async () => {
    const { a, b } = await pair();
    b.setManualCredit(true);
    await a.send(1, new Uint8Array(DEFAULT_INITIAL_WINDOW));
    await b.receive();
    let sent = false;
    const p = a.send(2, new Uint8Array(1000)).then(() => (sent = true));
    await later(30);
    expect(sent).toBe(false); // reading alone returned nothing
    b.returnCredit(999);
    await later(30);
    expect(sent).toBe(false); // one byte short
    b.returnCredit(1);
    await p;
    b.returnCredit(0); // ignored
    b.returnCredit(-1); // ignored
  });
});
