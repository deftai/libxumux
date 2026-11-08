/**
 * session-resume/1 (extensions/session-resume.md): negotiation, counting and RECEIPT,
 * detaching, RESUME / RESUMED, retransmission, token rotation, takeover, refusals and the
 * fall-back to HELLO.
 */
import { describe, it, expect, vi } from 'vitest';
import { XumuxClient, XumuxServer, type XumuxOptions } from './session.js';
import {
  SessionResumeStore,
  ReplayCounter,
  serialDiff,
  constantTimeEqual,
  newResumeToken,
} from './resume.js';
import { XumuxClosedError, XumuxResumeError } from './errors.js';
import {
  CloseCode,
  ControlType,
  FrameFlags,
  ResumeControlType as RT,
  SESSION_RESUME,
} from './codec/constants.js';
import type { Frame } from './codec/frame.js';
import { receiptFrame } from './control/codec.js';
import type { ReceiveCount } from './control/messages.js';
import type { XumuxChannel } from './channel.js';
import { waitUntil } from './transports/event-queue.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { LossyPair } from '../tests/lossy.js';
import { RawPeer, ctrl, json } from '../tests/raw-peer.js';

const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 200, onChannelRequest: () => true };
const CH = [
  { name: 'up', reliable: true, ordered: true },
  { name: 'down', reliable: true, ordered: true },
];
const te = new TextEncoder();
const td = new TextDecoder();
const tick = (ms = 5): Promise<void> => new Promise((r) => setTimeout(r, ms));

/** The client's current token (tests only). */
const tokenOf = (c: XumuxClient): string => (c as unknown as { rs: { token: string } }).rs.token;

async function resumablePair(
  o: {
    store?: SessionResumeStore;
    client?: XumuxOptions;
    server?: XumuxOptions;
    accept?: Parameters<XumuxServer['accept']>[0];
    auth?: Record<string, unknown>;
    kind?: 'message' | 'stream';
  } = {},
) {
  const store = o.store ?? new SessionResumeStore();
  const net = new LossyPair(o.kind ?? 'stream');
  const client = new XumuxClient(net.a, { ...base, ...o.client });
  const server = new XumuxServer(net.b, { ...base, ...o.server });
  const [c, s] = await Promise.all([
    client.connect({ extensions: [SESSION_RESUME], channels: CH, auth: o.auth }),
    server.accept({ sessionResume: store, ...o.accept }),
  ]);
  await waitUntil(() => client.resumable);
  return { store, net, client, server, c, s };
}

/** Resume `client` on a fresh transport, accepted by a fresh XumuxServer with `store`. */
async function reconnect(
  client: XumuxClient,
  store: SessionResumeStore,
  accept: Parameters<XumuxServer['accept']>[0] = {},
  kind: 'message' | 'stream' = 'stream',
) {
  const net = new LossyPair(kind);
  const fresh = new XumuxServer(net.b, base);
  const [, r] = await Promise.all([
    client.resume(net.a),
    fresh.accept({ sessionResume: store, ...accept }),
  ]);
  return { net, r, fresh };
}

/** Read every message of `ch` into a list (until it ends or errors), calling `each` on each. */
function drain(ch: XumuxChannel, each?: (n: number) => void): Uint8Array[] {
  const got: Uint8Array[] = [];
  void (async () => {
    for await (const m of ch) {
      got.push(m.payload);
      each?.(got.length);
    }
  })().catch(() => undefined);
  return got;
}

const numbered = (i: number, size: number): Uint8Array => {
  const b = new Uint8Array(Math.max(4, size));
  new DataView(b.buffer).setUint32(0, i);
  for (let k = 4; k < b.length; k++) b[k] = (i * 31 + k) & 0xff;
  return b;
};
const sameBytes = (a: Uint8Array[], b: Uint8Array[]): boolean =>
  a.length === b.length &&
  a.every((x, i) => x.length === b[i]!.length && x.every((v, k) => v === b[i]![k]));

const once = <T>(t: EventTarget, type: string): Promise<T> =>
  new Promise((r) =>
    t.addEventListener(type, (e) => r((e as CustomEvent<T>).detail), { once: true }),
  );

// ─── Unit pieces ───────────────────────────────────────────────────────────────

describe('counting helpers', () => {
  it('compares counts with serial-number arithmetic', () => {
    expect(serialDiff(5, 3)).toBe(2);
    expect(serialDiff(1, 0xffffffff)).toBe(2);
    expect(serialDiff(0xffffffff, 1)).toBe(-2);
  });

  it('retains, acknowledges and discards, across the 2^32 wrap', () => {
    const c = new ReplayCounter();
    c.sent = c.acked = 0xfffffffe;
    for (let i = 0; i < 4; i++) c.retain(i, new Uint8Array(10));
    expect(c.sent).toBe(2);
    expect(c.unacked).toBe(4);
    c.ack(0xffffffff);
    expect(c.unacked).toBe(3);
    c.ack(0xfffffffe); // stale
    expect(c.unacked).toBe(3);
    c.ack(1);
    expect([c.unacked, c.retainedBytes, c.acked]).toEqual([1, 10, 1]);
    c.discard();
    expect([c.unacked, c.retainedBytes, c.acked]).toEqual([0, 0, 2]);
  });

  it('makes distinct tokens of 44 base64url characters and compares them exactly', () => {
    const a = newResumeToken();
    expect(a).toMatch(/^[A-Za-z0-9_-]{44}$/);
    expect(newResumeToken()).not.toBe(a);
    expect(constantTimeEqual(a, a)).toBe(true);
    expect(constantTimeEqual(a, a.slice(0, 43) + (a[43] === 'A' ? 'B' : 'A'))).toBe(false);
    expect(constantTimeEqual(a, a.slice(1))).toBe(false);
  });

  it('validates store options', () => {
    expect(() => new SessionResumeStore({ resumeTimeout: 0 })).toThrow(RangeError);
    expect(() => new SessionResumeStore({ resumeTimeout: 3601 })).toThrow(RangeError);
    expect(() => new SessionResumeStore({ maxDetached: 0 })).toThrow(RangeError);
    expect(new SessionResumeStore({ resumeTimeout: 1.5 }).resumeTimeout).toBe(1.5);
    expect(() => new XumuxClient(createInMemoryPair()[0], { resumeFrameLimit: 127 })).toThrow(
      /resumeFrameLimit/,
    );
  });
});

// ─── Negotiation ───────────────────────────────────────────────────────────────

describe('negotiation (§2)', () => {
  it('applies when both sides enable it; RESUME_TOKEN is the first frame after WELCOME', async () => {
    const store = new SessionResumeStore({ resumeTimeout: 30 });
    const [t, rawEnd] = createInMemoryPair();
    const raw = new RawPeer(rawEnd, 'message');
    const server = new XumuxServer(t, base);
    const accepted = server.accept({
      sessionResume: store,
      negotiateExtension: () => ({ params: { ignored: true } }),
    });
    const welcome = json(await raw.helloAndWelcome({ extensions: [SESSION_RESUME] }));
    expect(welcome.extensions).toEqual([SESSION_RESUME]);
    expect(welcome.extensionParams).toBeUndefined(); // the extension defines none
    const first = raw.frames[1]!;
    expect([first.channel, first.type]).toEqual([0, RT.RESUME_TOKEN]);
    const tok = json(first);
    expect(tok.resumeTimeout).toBe(30);
    expect(tok.token).toMatch(/^[A-Za-z0-9_-]{44}$/);
    expect((await accepted).extensions).toEqual([SESSION_RESUME]);
    await server.close();
  });

  it('does not apply when the server has no store, and the session closes on loss as before', async () => {
    const net = new LossyPair();
    const client = new XumuxClient(net.a, base);
    const server = new XumuxServer(net.b, base);
    const [c] = await Promise.all([
      client.connect({ extensions: [SESSION_RESUME] }),
      server.accept(),
    ]);
    expect(c.extensions).toEqual([]);
    expect(client.resumable).toBe(false);
    net.cut();
    expect((await client.closed).code).toBe(CloseCode.GOING_AWAY);
    await expect(client.resume(createInMemoryPair()[0])).rejects.toThrow(/cannot be resumed/);
  });

  it('does not apply when the client does not ask, or negotiateExtension refuses', async () => {
    const store = new SessionResumeStore();
    for (const [ask, negotiate] of [
      [false, undefined],
      [true, () => false],
    ] as const) {
      const net = new LossyPair();
      const client = new XumuxClient(net.a, base);
      const server = new XumuxServer(net.b, base);
      await Promise.all([
        client.connect({ extensions: ask ? [SESSION_RESUME] : [] }),
        server.accept({ sessionResume: store, negotiateExtension: negotiate }),
      ]);
      await tick();
      expect(client.resumable).toBe(false);
      expect(() => server.rotateResumeToken()).toThrow(/does not apply/);
      net.cut();
      expect((await server.closed).code).toBe(CloseCode.GOING_AWAY);
    }
    expect(store.detachedSessions).toBe(0);
  });

  it('rejects misconfiguration', async () => {
    const s1 = new XumuxServer(createInMemoryPair()[0], base);
    await expect(s1.accept({ extensions: [SESSION_RESUME] })).rejects.toThrow(
      /needs AcceptOptions/,
    );
    const s2 = new XumuxServer(createInMemoryPair()[0], base);
    await expect(
      s2.accept({
        sessionResume: new SessionResumeStore(),
        firstFrames: { [RT.RESUME]: () => ({ code: 1 }) },
      }),
    ).rejects.toThrow(/0x31/);
  });

  it('the client closes with 1002 if the first frame after WELCOME is not RESUME_TOKEN', async () => {
    const [t, rawEnd] = createInMemoryPair();
    const raw = new RawPeer(rawEnd, 'message');
    const client = new XumuxClient(t, base);
    const p = client.connect({ extensions: [SESSION_RESUME] });
    await raw.awaitHelloSendWelcome({ extensions: [SESSION_RESUME] });
    await p;
    raw.send({ channel: 0, type: 0x10, flags: FrameFlags.PROBE, payload: new Uint8Array(4) });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.PROTOCOL_ERROR);
  });

  it('the client closes with 4001 on an invalid RESUME_TOKEN', async () => {
    for (const body of [
      { token: 'x', resumeTimeout: 0 },
      { token: '', resumeTimeout: 10 },
      { token: 'x'.repeat(513), resumeTimeout: 10 },
      { resumeTimeout: 10 },
    ]) {
      const [t, rawEnd] = createInMemoryPair();
      const raw = new RawPeer(rawEnd, 'message');
      const client = new XumuxClient(t, base);
      const p = client.connect({ extensions: [SESSION_RESUME] });
      await raw.awaitHelloSendWelcome({ extensions: [SESSION_RESUME] });
      await p;
      raw.send(ctrl(RT.RESUME_TOKEN, body));
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
    }
  });
});

// ─── RECEIPT ───────────────────────────────────────────────────────────────────

/** A raw client in a resumable session with a library server, with HELLO channel 1 open. */
async function rawClient(server: Partial<XumuxOptions> = {}, store = new SessionResumeStore()) {
  const [t, rawEnd] = createInMemoryPair();
  const raw = new RawPeer(rawEnd, 'message');
  const srv = new XumuxServer(t, { ...base, ...server });
  const accepted = srv.accept({ sessionResume: store });
  await raw.helloAndWelcome({
    extensions: [SESSION_RESUME],
    channels: [{ id: 1, name: 'x', reliable: true, ordered: true }],
  });
  const token = json(await raw.control(RT.RESUME_TOKEN)).token as string;
  const s = await accepted;
  return { raw, srv, s, token, store, ch: s.channels[0]! };
}

const isReceipt = (f: Frame): boolean => f.channel === 0 && f.type === RT.RECEIPT;
const receiptEntries = (f: Frame): Array<[number, number]> => {
  const dv = new DataView(f.payload.buffer, f.payload.byteOffset, f.payload.byteLength);
  const out: Array<[number, number]> = [];
  for (let i = 0; i < f.payload.length; i += 6) out.push([dv.getUint16(i), dv.getUint32(i + 2)]);
  return out;
};

describe('RECEIPT (§3.4, §5)', () => {
  it('is sent promptly, and before the WINDOW_UPDATE that returns credit', async () => {
    const { raw, srv, ch } = await rawClient({ initialWindow: 262144 });
    raw.send({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(60_000) });
    await ch.receive(); // consumed: credit goes back
    const wu = await raw.control(ControlType.WINDOW_UPDATE);
    const i = raw.frames.indexOf(wu);
    const r = raw.frames.slice(0, i).filter(isReceipt).flatMap(receiptEntries);
    expect(r).toContainEqual([1, 1]);
    // Channel 0 frames are acknowledged too.
    raw.send(ctrl(ControlType.ERROR, { code: 1 }));
    await waitUntil(() =>
      raw.frames
        .filter(isReceipt)
        .flatMap(receiptEntries)
        .some(([c, n]) => c === 0 && n === 1),
    );
    await srv.close();
  });

  it('is sent at 64 unacknowledged frames without waiting', async () => {
    const { raw, srv } = await rawClient();
    const many = Array.from({ length: 130 }, () => ({
      channel: 1,
      type: 1,
      flags: 0,
      payload: new Uint8Array(1),
    }));
    for (const f of many) raw.send(f);
    await waitUntil(() =>
      raw.frames
        .filter(isReceipt)
        .flatMap(receiptEntries)
        .some(([c, n]) => c === 1 && n === 130),
    );
    expect(raw.frames.filter(isReceipt).length).toBeGreaterThanOrEqual(2);
    await srv.close();
  });

  it('frees retained frames; at the frame limit a channel waits for one', async () => {
    const { raw, srv, ch } = await rawClient({ resumeFrameLimit: 128 });
    const sends = Array.from({ length: 200 }, (_, i) => ch.send(1, new Uint8Array([i])));
    const data = (): Frame[] => raw.frames.filter((f) => f.channel === 1);
    await waitUntil(() => data().length === 128);
    await tick(20);
    expect(data()).toHaveLength(128);
    // Other traffic is not held up.
    srv.rotateResumeToken();
    await raw.control(RT.RESUME_TOKEN);
    raw.send(receiptFrame([{ channel: 1, count: 100 }]));
    await waitUntil(() => data().length === 200);
    await Promise.all(sends);
    // Stale and unknown entries are ignored.
    raw.send(
      receiptFrame([
        { channel: 1, count: 50 },
        { channel: 9, count: 5 },
      ]),
    );
    await tick();
    expect(srv.channels.size).toBe(1);
    await srv.close();
  });

  it('does not count frames on unknown channels', async () => {
    const { raw, srv } = await rawClient();
    raw.send({ channel: 99, type: 1, flags: 0, payload: new Uint8Array(1) });
    expect(json(await raw.control(ControlType.ERROR)).code).toBe(CloseCode.CHANNEL_NOT_FOUND);
    await tick();
    expect(
      raw.frames
        .filter(isReceipt)
        .flatMap(receiptEntries)
        .some(([c]) => c === 99),
    ).toBe(false);
    await srv.close();
  });

  it('closes with 4103 for a count above what was sent, 4001 for a bad length', async () => {
    const a = await rawClient();
    a.raw.send(receiptFrame([{ channel: 0, count: 99 }]));
    expect(json(await a.raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_STATE_MISMATCH);
    const b = await rawClient();
    b.raw.send({ channel: 0, type: RT.RECEIPT, flags: 0, payload: new Uint8Array(5) });
    expect(json(await b.raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
  });

  it('RESUME, RESUMED and a client RESUME_TOKEN on an established connection close with 1002', async () => {
    for (const type of [RT.RESUME, RT.RESUMED, RT.RESUME_TOKEN]) {
      const { raw } = await rawClient();
      raw.send(ctrl(type, {}));
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.PROTOCOL_ERROR);
    }
  });

  it('without the extension the control types are unknown (ERROR 1003)', async () => {
    const [t, rawEnd] = createInMemoryPair();
    const raw = new RawPeer(rawEnd, 'message');
    const srv = new XumuxServer(t, base);
    const accepted = srv.accept();
    await raw.helloAndWelcome();
    await accepted;
    raw.send(receiptFrame([{ channel: 0, count: 1 }]));
    expect(json(await raw.control(ControlType.ERROR)).code).toBe(CloseCode.UNSUPPORTED);
    await srv.close();
  });
});

// ─── Detach and resume ───────────────────────────────────────────────────────

describe('losing the transport and resuming (§6, §7)', () => {
  for (const kind of ['stream', 'message'] as const) {
    it(`delivers every byte exactly once in both directions after a mid-transfer loss (${kind})`, async () => {
      const { store, net, client, server, c, s } = await resumablePair({ kind });
      const [cUp, cDown] = c.channels as [XumuxChannel, XumuxChannel];
      const [sUp, sDown] = s.channels as [XumuxChannel, XumuxChannel];
      // Cut the network as the 5th message arrives, with plenty more in flight both ways.
      const gotUp = drain(sUp, (n) => n === 5 && net.cut());
      const gotDown = drain(cDown);
      const up = Array.from({ length: 40 }, (_, i) => numbered(i, 1 + ((i * 7919) % 70_000)));
      const down = Array.from({ length: 40 }, (_, i) =>
        numbered(1000 + i, 1 + ((i * 104_729) % 90_000)),
      );
      const sent = [...up.map((m) => cUp.send(1, m)), ...down.map((m) => sDown.send(2, m))];
      const [cd, sd] = [once(client, 'detach'), once(server, 'detach')];
      await Promise.all([cd, sd]);
      expect(net.lostBytes).toBeGreaterThan(0);
      expect(client.detached && server.detached).toBe(true);
      expect(store.detachedSessions).toBe(1);
      // Sends go on queueing while detached.
      const late = cUp.send(1, numbered(99, 10));
      const resumed = [once(client, 'resume'), once(server, 'resume')];
      const { r } = await reconnect(client, store, {}, kind);
      await Promise.all(resumed);
      expect(r.resumed).toBe(server);
      expect(r.channels.map((ch) => ch.name)).toEqual(['up', 'down']);
      expect(store.detachedSessions).toBe(0);
      await Promise.all([...sent, late]);
      await waitUntil(() => gotUp.length === 41 && gotDown.length === 40);
      expect(sameBytes(gotUp, [...up, numbered(99, 10)])).toBe(true);
      expect(sameBytes(gotDown, down)).toBe(true);
      // The session carries on normally.
      expect(await client.ping()).toBeGreaterThanOrEqual(0);
      const ch = await server.openChannel({ name: 'later', reliable: true, ordered: true });
      await ch.send(1, te.encode('after'));
      const closed = server.closed;
      expect((await client.close(1000)).code).toBe(1000);
      expect((await closed).remote).toBe(true);
    });
  }

  it('survives several losses in a row, including one right after resuming', async () => {
    const { store, net, client, c, s } = await resumablePair();
    const got = drain(s.channels[0]!);
    const msgs = Array.from({ length: 30 }, (_, i) => numbered(i, 30_000));
    const sent = msgs.map((m) => c.channels[0]!.send(1, m));
    let cur = net;
    for (let round = 0; round < 3; round++) {
      const n = got.length;
      await waitUntil(() => got.length > n || got.length === 30);
      cur.cut();
      await waitUntil(() => client.detached);
      const next = new LossyPair();
      const fresh = new XumuxServer(next.b, base);
      const accepted = fresh.accept({ sessionResume: store });
      await client.resume(next.a);
      // Lose this one again before the server has even returned from accept().
      if (round === 1) next.cut();
      await accepted;
      cur = next;
      if (round === 1) {
        await waitUntil(() => client.detached);
        cur = (await reconnect(client, store)).net;
      }
    }
    await Promise.all(sent);
    await waitUntil(() => got.length === 30);
    expect(sameBytes(got, msgs)).toBe(true);
    await client.close();
  });

  it('pings fail while detached and keepalive restarts after resuming', async () => {
    const { store, net, client, server } = await resumablePair({
      client: { pingInterval: 0.05, pingTimeout: 0.05 },
      server: { pingInterval: 0.05, pingTimeout: 0.05 },
    });
    net.cut();
    await waitUntil(() => client.detached);
    await expect(client.ping()).rejects.toThrow(/detached/);
    await reconnect(client, store);
    expect(await server.ping()).toBeGreaterThanOrEqual(0);
    expect(client.rtt).toBeUndefined(); // RTT smoothing started again
    await tick(150); // idle: keepalive PINGs keep both sides attached
    expect(client.detached || server.detached).toBe(false);
    await client.close();
  });

  it('a keepalive timeout detaches instead of closing with 4008', async () => {
    const { client, server } = await resumablePair({
      client: { pingInterval: 0.03, pingTimeout: 0.03 },
      server: { pingInterval: 0.03, pingTimeout: 0.03 },
    });
    // A transport that stops delivering anything, but does not end.
    (client as unknown as { path: { pause(): void } }).path.pause();
    (server as unknown as { path: { pause(): void } }).path.pause();
    expect((await once<{ reason: string }>(client, 'detach')).reason).toBe('keepalive timeout');
    await server.close();
    await client.close();
  });

  it('takes over a session the server still thinks is attached (§6)', async () => {
    const { store, net, client, server, c, s } = await resumablePair();
    // The old transport dies on the client's side only: the server still reads it.
    net.a.close = () => undefined;
    const sd = once<{ reason: string }>(server, 'detach');
    const { r } = await reconnect(client, store); // the client resumes on its own initiative
    expect((await sd).reason).toBe('taken over by a new transport');
    expect(r.resumed).toBe(server);
    await c.channels[0]!.send(1, te.encode('over the new transport'));
    expect(td.decode((await s.channels[0]!.receive())!.payload)).toBe('over the new transport');
    await client.close();
  });

  it('closes channels properly when a CLOSE_CHANNEL reply is lost (§7 rule 2)', async () => {
    const { store, net, client, server, c, s } = await resumablePair();
    // The client closes "down"; the server's reply is lost with the transport, so the client
    // still has the channel closing, and the server's count shows it received the close.
    server.addEventListener('channel-close', () => net.cut());
    const closing = c.channels[1]!.close(1000, 'bye');
    await waitUntil(() => client.detached && server.detached);
    expect(c.channels[1]!.state).toBe('closing');
    await reconnect(client, store);
    expect((await closing).code).toBe(1000);
    expect(s.channels[1]!.state).toBe('closed');
    // The ID is free again on both sides.
    const again = await client.openChannel({ name: 'again', reliable: true, ordered: true });
    expect(again.id).toBe(c.channels[1]!.id);
    await client.close();
  });

  it('drops what it sent on a channel the peer closed meanwhile (§7 rule 4)', async () => {
    const { store, net, client, server, c, s } = await resumablePair();
    const got = drain(s.channels[1]!);
    net.isCut = true; // from here everything sent is lost
    const sent = c.channels[1]!.send(1, te.encode('never needed'));
    await tick();
    const serverClose = s.channels[1]!.close(1000);
    await tick();
    net.cut();
    await waitUntil(() => client.detached && server.detached);
    await sent; // retained, so not an error
    await reconnect(client, store);
    // The server's CLOSE_CHANNEL is retransmitted; the client's data on the channel is not.
    expect((await c.channels[1]!.closed).remote).toBe(true);
    expect((await serverClose).code).toBe(1000);
    expect(got).toHaveLength(0);
    await client.close();
  });

  it('expires after resumeTimeout on both sides, and the token then answers 4101', async () => {
    const store = new SessionResumeStore({ resumeTimeout: 1 });
    const { net, client, server } = await resumablePair({ store });
    const token = tokenOf(client);
    net.cut();
    const [ci, si] = await Promise.all([client.closed, server.closed]);
    expect([ci.code, si.code]).toEqual([CloseCode.RESUME_EXPIRED, CloseCode.RESUME_EXPIRED]);
    const { raw } = rawResume(store, { token, received: [[0, 1]] });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_EXPIRED);
  });

  it('discards the oldest detached session past maxDetached', async () => {
    const store = new SessionResumeStore({ maxDetached: 1 });
    const a = await resumablePair({ store });
    const b = await resumablePair({ store });
    a.net.cut();
    await waitUntil(() => a.server.detached);
    b.net.cut();
    const info = await a.server.closed;
    expect(info.code).toBe(CloseCode.RESUME_NOT_FOUND);
    expect(store.detachedSessions).toBe(1);
    expect(a.client.detached).toBe(true);
    await b.server.close();
  });

  it('close() while detached ends the session and forgets its token (4100)', async () => {
    const { store, net, client, server } = await resumablePair();
    const token = tokenOf(client);
    net.cut();
    await waitUntil(() => server.detached);
    expect((await server.close()).code).toBe(1000);
    const { raw } = rawResume(store, { token, received: [[0, 1]] });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_NOT_FOUND);
    await client.close();
  });
});

// ─── Server refusals ─────────────────────────────────────────────────────────

/** Send RESUME as the first frame of a new transport to a server using `store`. */
function rawResume(
  store: SessionResumeStore,
  body: { version?: number[]; token: string; received: ReceiveCount[]; auth?: unknown },
  accept: Parameters<XumuxServer['accept']>[0] = {},
) {
  const [t, rawEnd] = createInMemoryPair();
  const raw = new RawPeer(rawEnd, 'message');
  const fresh = new XumuxServer(t, base);
  const accepted = fresh.accept({ sessionResume: store, ...accept }).catch((e: unknown) => e);
  raw.send(ctrl(RT.RESUME, { version: [0, 3, 0], ...body }));
  return { raw, fresh, accepted };
}

describe('RESUME refusals (§7 step 2)', () => {
  it('4100 for an unknown token; 4001 for a malformed RESUME; 4006 for another version', async () => {
    const { store, net, client, server } = await resumablePair();
    net.cut();
    await waitUntil(() => server.detached);
    const token = tokenOf(client);
    const cases: Array<[Parameters<typeof rawResume>[1], number]> = [
      [{ token: newResumeToken(), received: [[0, 1]] }, CloseCode.RESUME_NOT_FOUND],
      [{ token: 'short', received: [[0, 1]] }, CloseCode.RESUME_NOT_FOUND],
      [{ token, received: [[1, 1]] }, CloseCode.INVALID_MESSAGE],
      [
        {
          token,
          received: [
            [0, 1],
            [0, 2],
          ],
        },
        CloseCode.INVALID_MESSAGE,
      ],
      [{ token, received: [[0, -1]] }, CloseCode.INVALID_MESSAGE],
      [{ token, version: [0, 2, 0], received: [[0, 1]] }, CloseCode.VERSION_MISMATCH],
    ];
    for (const [body, code] of cases) {
      const { raw, accepted } = rawResume(store, body);
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(code);
      expect(((await accepted) as XumuxClosedError).code).toBe(code);
    }
    // None of these ends the detached session.
    expect(server.detached).toBe(true);
    await reconnect(client, store);
    await client.close();
  });

  it('4103 and 4102 end the session', async () => {
    for (const [received, code] of [
      [[[0, 99]], CloseCode.RESUME_STATE_MISMATCH],
      [
        [
          [0, 1],
          [1, 5],
        ],
        CloseCode.RESUME_STATE_MISMATCH,
      ],
      [
        [
          [0, 1],
          [7, 5],
        ],
        CloseCode.RESUME_STATE_MISMATCH,
      ],
      [[[0, 0]], CloseCode.RESUME_DATA_LOST],
    ] as Array<[ReceiveCount[], number]>) {
      const { store, net, client, server } = await resumablePair();
      await tick(10); // the client's RECEIPT for RESUME_TOKEN arrives
      net.cut();
      await waitUntil(() => server.detached);
      const { raw } = rawResume(store, { token: tokenOf(client), received });
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(code);
      expect((await server.closed).code).toBe(code);
      await client.close();
    }
  });

  it('authenticates RESUME like HELLO (§8)', async () => {
    const authorize = (h: { auth?: Record<string, unknown> }) =>
      h.auth?.user === 'ann' ? undefined : { code: CloseCode.AUTH_FAILED };
    // Without authorizeResume, a session that was authenticated cannot be resumed.
    {
      const { store, net, client, server } = await resumablePair({
        accept: { authorize },
        auth: { user: 'ann' },
      });
      net.cut();
      await waitUntil(() => server.detached);
      const net2 = new LossyPair();
      const fresh = new XumuxServer(net2.b, base);
      void fresh.accept({ sessionResume: store, authorize }).catch(() => undefined);
      const e = (await client.resume(net2.a).catch((x: unknown) => x)) as XumuxResumeError;
      expect([e.code, e.sessionLost, e.remote]).toEqual([CloseCode.AUTH_FAILED, false, true]);
      expect(client.detached).toBe(true);
      await client.close();
    }
    // With it: the identity must match; a throw refuses with 4000.
    const seen: unknown[] = [];
    const authorizeResume = (m: { auth?: Record<string, unknown> }, session: XumuxServer) => {
      seen.push(session);
      if (m.auth?.boom) throw new Error('boom');
      return m.auth?.user === 'ann' ? undefined : { code: CloseCode.BUSY };
    };
    const { store, net, client, server } = await resumablePair({
      accept: { authorize },
      auth: { user: 'ann' },
    });
    net.cut();
    await waitUntil(() => server.detached);
    const tries: Array<[Record<string, unknown>, number]> = [
      [{ user: 'bob' }, CloseCode.BUSY],
      [{ boom: true }, CloseCode.AUTH_FAILED],
    ];
    for (const [auth, code] of tries) {
      const net2 = new LossyPair();
      const fresh = new XumuxServer(net2.b, base);
      void fresh.accept({ sessionResume: store, authorizeResume }).catch(() => undefined);
      const e = (await client
        .resume(net2.a, { auth })
        .catch((x: unknown) => x)) as XumuxResumeError;
      expect([e.code, e.sessionLost]).toEqual([code, false]);
    }
    const { r } = await reconnect(client, store, { authorizeResume }); // HELLO's auth again
    expect(r.resumed).toBe(server);
    expect(seen.every((x) => x === server)).toBe(true);
    await client.close();
  });

  it('a session that ended while authorizeResume ran answers 4100', async () => {
    const { store, net, client, server } = await resumablePair();
    net.cut();
    await waitUntil(() => server.detached);
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const { raw } = rawResume(
      store,
      { token: tokenOf(client), received: [[0, 1]] },
      { authorizeResume: () => gate },
    );
    await tick();
    await server.close();
    release();
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_NOT_FOUND);
    await client.close();
  });
});

// ─── Tokens ─────────────────────────────────────────────────────────────────

describe('tokens (§3.1, §3.3, §8)', () => {
  it('rotates: the replacement is used, and the old token stops working once acknowledged', async () => {
    const { store, net, client, server } = await resumablePair();
    const old = tokenOf(client);
    server.rotateResumeToken();
    await waitUntil(() => tokenOf(client) !== old);
    await tick(10); // the client's RECEIPT covers the new RESUME_TOKEN
    net.cut();
    await waitUntil(() => server.detached);
    const { raw } = rawResume(store, { token: old, received: [[0, 2]] });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_NOT_FOUND);
    await reconnect(client, store);
    await client.close();
  });

  it('a RESUME_TOKEN lost with the transport is retransmitted after RESUMED and wins', async () => {
    const { store, net, client, server } = await resumablePair();
    await tick(10);
    net.isCut = true; // the rotation is written but never arrives
    server.rotateResumeToken();
    await tick();
    net.cut();
    await waitUntil(() => client.detached && server.detached);
    const before = tokenOf(client);
    const { net: net2 } = await reconnect(client, store);
    // RESUMED's token first, then the retransmitted RESUME_TOKEN, which replaces it.
    await waitUntil(() => tokenOf(client) !== before);
    const latest = tokenOf(client);
    await tick(10);
    net2.cut();
    await waitUntil(() => client.detached && server.detached);
    expect(tokenOf(client)).toBe(latest);
    const { r } = await reconnect(client, store);
    expect(r.resumed).toBe(server);
    await client.close();
  });

  it('keeps the old token valid until the client confirms RESUMED, then only the new one', async () => {
    // A raw client, so the test controls what it sends after RESUMED.
    const store = new SessionResumeStore();
    const { raw, srv, token } = await rawClient({}, store);
    raw.transport.close();
    await waitUntil(() => srv.detached);
    // RESUMED arrives, but the transport fails before the client sends anything.
    const a = rawResume(store, {
      token,
      received: [
        [0, 1],
        [1, 0],
      ],
    });
    const t2 = json(await a.raw.control(RT.RESUMED)).token as string;
    expect(t2).not.toBe(token);
    a.raw.transport.close();
    await waitUntil(() => srv.detached);
    // So the old token still resumes (and invalidates t2).
    const b = rawResume(store, {
      token,
      received: [
        [0, 1],
        [1, 0],
      ],
    });
    const t3 = json(await b.raw.control(RT.RESUMED)).token as string;
    expect(((await b.accepted) as { resumed?: XumuxServer }).resumed).toBe(srv);
    b.raw.send(receiptFrame([{ channel: 0, count: 1 }])); // confirms t3
    await tick();
    b.raw.transport.close();
    await waitUntil(() => srv.detached);
    for (const t of [token, t2]) {
      const c = rawResume(store, {
        token: t,
        received: [
          [0, 1],
          [1, 0],
        ],
      });
      expect(json(await c.raw.control(ControlType.CLOSE)).code).toBe(CloseCode.RESUME_NOT_FOUND);
    }
    const d = rawResume(store, {
      token: t3,
      received: [
        [0, 1],
        [1, 0],
      ],
    });
    await d.raw.control(RT.RESUMED);
    await srv.close();
  });
});

// ─── Client side ───────────────────────────────────────────────────────────

/** A raw server on the far end of a client's resume() attempt. */
function rawServerFor(client: XumuxClient) {
  const [t, rawEnd] = createInMemoryPair();
  const raw = new RawPeer(rawEnd, 'message');
  const p = client.resume(t).catch((e: unknown) => e as XumuxResumeError);
  return { raw, p };
}

describe('client resume() (§7, §9)', () => {
  it('falls back to HELLO when the session is gone (4100), as the error says', async () => {
    const { store, net, client, server } = await resumablePair();
    net.cut();
    await waitUntil(() => server.detached);
    await server.close(); // the server forgets the session
    const closed = client.closed;
    const net2 = new LossyPair();
    const fresh = new XumuxServer(net2.b, base);
    const accepted = fresh.accept({ sessionResume: store }).catch((e: unknown) => e);
    const e = (await client.resume(net2.a).catch((x: unknown) => x)) as XumuxResumeError;
    expect(e).toBeInstanceOf(XumuxResumeError);
    expect([e.code, e.sessionLost, e.remote]).toEqual([CloseCode.RESUME_NOT_FOUND, true, true]);
    expect((await closed).code).toBe(CloseCode.RESUME_NOT_FOUND);
    expect(((await accepted) as XumuxClosedError).code).toBe(CloseCode.RESUME_NOT_FOUND);
    // A new connection with HELLO.
    const net3 = new LossyPair();
    const again = new XumuxClient(net3.a, base);
    const srv3 = new XumuxServer(net3.b, base);
    const [c3] = await Promise.all([
      again.connect({ extensions: [SESSION_RESUME] }),
      srv3.accept({ sessionResume: store }),
    ]);
    expect(c3.extensions).toEqual([SESSION_RESUME]);
    await again.close();
  });

  it('treats 1002 from a server without the extension as a lost session', async () => {
    const { net, client, server } = await resumablePair();
    net.cut();
    await waitUntil(() => client.detached);
    const net2 = new LossyPair();
    const plain = new XumuxServer(net2.b, base);
    void plain.accept().catch(() => undefined);
    const e = (await client.resume(net2.a).catch((x: unknown) => x)) as XumuxResumeError;
    expect([e.code, e.sessionLost]).toEqual([CloseCode.PROTOCOL_ERROR, true]);
    expect(client.resumable).toBe(false);
    await server.close();
  });

  it('stays detached when the attempt fails without an answer', async () => {
    const { store, net, client } = await resumablePair({ client: { handshakeTimeoutMs: 50 } });
    net.cut();
    await waitUntil(() => client.detached);
    // No answer within handshakeTimeoutMs.
    const a = rawServerFor(client);
    const first = await a.raw.control(RT.RESUME);
    const body = json(first);
    expect(body.token).toBe(tokenOf(client));
    expect(body.version).toEqual([0, 3, 0]);
    expect((body.received as ReceiveCount[])[0]![0]).toBe(0);
    await expect(client.resume(createInMemoryPair()[0])).rejects.toThrow(/in progress/);
    const e1 = await a.p;
    expect([e1.code, e1.sessionLost]).toEqual([CloseCode.HELLO_TIMEOUT, false]);
    // The transport ends before RESUMED.
    const b = rawServerFor(client);
    await b.raw.control(RT.RESUME);
    b.raw.transport.close();
    const e2 = await b.p;
    expect([e2.code, e2.sessionLost]).toEqual([CloseCode.GOING_AWAY, false]);
    // The transport cannot even be written.
    const e4 = (await client
      .resume({
        kind: 'message',
        send: () => {
          throw new Error('EBADF');
        },
        receive: () => new LossyPair().a.receive(),
        close: () => undefined,
      })
      .catch((x: unknown) => x)) as XumuxResumeError;
    expect([e4.code, e4.reason]).toEqual([CloseCode.GOING_AWAY, 'transport failed: EBADF']);
    const e3 = (await client
      .resume({
        kind: 'message',
        send: () => Promise.reject(new Error('EPIPE')),
        receive: () => new LossyPair().a.receive(),
        close: () => undefined,
      })
      .catch((x: unknown) => x)) as XumuxResumeError;
    expect([e3.code, e3.sessionLost]).toEqual([CloseCode.GOING_AWAY, false]);
    expect(client.detached).toBe(true);
    await reconnect(client, store);
    await client.close();
  });

  it('closes with 4103 / 4102 / 1002 / 4001 when the answer is wrong, and the session is lost', async () => {
    const answers: Array<[Frame, number]> = [
      [
        ctrl(RT.RESUMED, { token: 't', resumeTimeout: 5, received: [[0, 999]] }),
        CloseCode.RESUME_STATE_MISMATCH,
      ],
      [
        ctrl(RT.RESUMED, { token: 't', resumeTimeout: 5, received: [[0, 0]] }),
        CloseCode.RESUME_DATA_LOST,
      ],
      [ctrl(RT.RESUMED, { token: 't', resumeTimeout: 5 }), CloseCode.INVALID_MESSAGE],
      [ctrl(ControlType.WELCOME, { version: [0, 3, 0] }), CloseCode.PROTOCOL_ERROR],
      [{ channel: 1, type: 1, flags: 0x80, payload: new Uint8Array(0) }, CloseCode.PROTOCOL_ERROR],
    ];
    for (const [answer, code] of answers) {
      const { net, client, c } = await resumablePair();
      void c;
      // A channel-0 frame of the client's that the server acknowledges.
      await client.openChannel({ name: 'z', reliable: true, ordered: true });
      await tick(10);
      net.cut();
      await waitUntil(() => client.detached);
      const a = rawServerFor(client);
      await a.raw.control(RT.RESUME);
      a.raw.send(answer);
      expect(json(await a.raw.control(ControlType.CLOSE)).code).toBe(code);
      const e = await a.p;
      expect([e.code, e.sessionLost]).toEqual([code, true]);
      expect((await client.closed).code).toBe(code);
    }
  });

  it('replies to a CLOSE that is not a lost session and stays detached', async () => {
    const { store, net, client } = await resumablePair();
    net.cut();
    await waitUntil(() => client.detached);
    const a = rawServerFor(client);
    await a.raw.control(RT.RESUME);
    a.raw.send(ctrl(ControlType.CLOSE, { code: CloseCode.BUSY }));
    expect(json(await a.raw.control(ControlType.CLOSE)).code).toBe(CloseCode.BUSY);
    const e = await a.p;
    expect([e.code, e.reason, e.sessionLost]).toEqual([CloseCode.BUSY, 'RESUME refused', false]);
    await reconnect(client, store);
    await client.close();
  });

  it('does not send a RESUME over 65536 bytes: the session is lost and the transport unused', async () => {
    const { net, client } = await resumablePair();
    net.cut();
    await waitUntil(() => client.detached);
    const big: ReceiveCount[] = [
      [0, 1],
      ...Array.from({ length: 8000 }, (_, i): ReceiveCount => [2 * i + 1, 4_000_000_000]),
    ];
    vi.spyOn(
      client as unknown as { receivedList(): ReceiveCount[] },
      'receivedList',
    ).mockReturnValue(big);
    const [t, rawEnd] = createInMemoryPair();
    const raw = new RawPeer(rawEnd, 'message');
    const e = (await client.resume(t).catch((x: unknown) => x)) as XumuxResumeError;
    expect([e.code, e.sessionLost]).toEqual([CloseCode.MESSAGE_TOO_LARGE, true]);
    await tick();
    expect(raw.frames).toHaveLength(0);
    expect((await client.closed).code).toBe(CloseCode.MESSAGE_TOO_LARGE);
  });

  it('a server whose RESUMED would exceed 65536 bytes ends the session with 4102', async () => {
    const { store, net, client, server } = await resumablePair();
    net.cut();
    await waitUntil(() => server.detached);
    const big: ReceiveCount[] = [
      [0, 1],
      ...Array.from({ length: 8000 }, (_, i): ReceiveCount => [2 * i + 2, 4_000_000_000]),
    ];
    vi.spyOn(
      server as unknown as { receivedList(): ReceiveCount[] },
      'receivedList',
    ).mockReturnValue(big);
    const net2 = new LossyPair();
    const fresh = new XumuxServer(net2.b, base);
    const accepted = fresh.accept({ sessionResume: store }).catch((x: unknown) => x);
    const e = (await client.resume(net2.a).catch((x: unknown) => x)) as XumuxResumeError;
    expect([e.code, e.sessionLost]).toEqual([CloseCode.RESUME_DATA_LOST, true]);
    expect(((await accepted) as XumuxClosedError).code).toBe(CloseCode.RESUME_DATA_LOST);
    expect((await server.closed).code).toBe(CloseCode.RESUME_DATA_LOST);
  });

  it('close() during an attempt rejects it as lost', async () => {
    const { net, client } = await resumablePair();
    net.cut();
    await waitUntil(() => client.detached);
    const a = rawServerFor(client);
    await a.raw.control(RT.RESUME);
    await client.close();
    const e = await a.p;
    expect([e.code, e.sessionLost]).toEqual([1000, true]);
  });
});
