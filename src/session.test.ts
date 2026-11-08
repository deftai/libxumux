import { describe, it, expect, vi } from 'vitest';
import { XumuxClient, XumuxServer, versionsCompatible, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { CloseCode, ControlType, FrameFlags, ProbeType } from './codec/constants.js';
import { XumuxClosedError } from './errors.js';
import type { XumuxChannel } from './channel.js';
import { rawPair, ctrl, json } from '../tests/raw-peer.js';
import { encodeFrame } from './codec/encode-frame.js';

const accept = { onChannelRequest: () => true as const };
const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 300, ...accept };
const te = new TextEncoder();
const td = new TextDecoder();

async function pair(
  copts: XumuxOptions = {},
  sopts: XumuxOptions = {},
  channels: Array<{ name: string; reliable?: boolean; ordered?: boolean }> = [{ name: 'data' }],
  kind: 'message' | 'stream' = 'message',
) {
  const [ct, st] = createInMemoryPair(kind);
  const client = new XumuxClient(ct, { ...base, ...copts });
  const server = new XumuxServer(st, { ...base, ...sopts });
  const [c, s] = await Promise.all([
    client.connect({ channels: channels.map((ch) => ({ reliable: true, ordered: true, ...ch })) }),
    server.accept(),
  ]);
  return { client, server, c, s };
}

const later = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('handshake', () => {
  it('opens HELLO channels with client-chosen odd IDs', async () => {
    const { c, s } = await pair({}, {}, [{ name: 'a' }, { name: 'b' }]);
    expect(c.channels.map((x) => x.id)).toEqual([1, 3]);
    expect(s.channels.map((x) => [x.id, x.name])).toEqual([
      [1, 'a'],
      [3, 'b'],
    ]);
    expect(c.channels[0]!.properties).toEqual({ reliable: true, ordered: true });
  });

  it('reports effective properties: unreliable requests become reliable+ordered on a single stream', async () => {
    const { c } = await pair({}, {}, [{ name: 'pointer', reliable: false, ordered: false }]);
    expect(c.channels[0]!.properties).toMatchObject({ reliable: true, ordered: true });
  });

  it('rejects individual HELLO channels via WELCOME.rejected', async () => {
    const { c, s } = await pair(
      {},
      { onChannelRequest: (r) => (r.name === 'ok' ? true : { code: 4010, reason: 'nope' }) },
      [{ name: 'ok' }, { name: 'bad' }],
    );
    expect(c.channels.map((x) => x.name)).toEqual(['ok']);
    expect(c.rejected).toEqual([{ id: 3, code: 4010, reason: 'nope' }]);
    expect(s.channels).toHaveLength(1);
  });

  it('rejects peer channels with 1003 when no policy is set', async () => {
    const { c } = await pair({}, { onChannelRequest: undefined }, [{ name: 'x' }]);
    expect(c.rejected[0]!.code).toBe(CloseCode.UNSUPPORTED);
  });

  it('authorize() refusal closes with that code and connect() rejects', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const s = server.accept({
      authorize: (h) =>
        h.auth?.token === 'good' ? undefined : { code: CloseCode.AUTH_FAILED, reason: 'bad token' },
    });
    const err = await client.connect({ auth: { token: 'bad' } }).catch((e: unknown) => e);
    expect(err).toBeInstanceOf(XumuxClosedError);
    expect((err as XumuxClosedError).code).toBe(4000);
    await expect(s).rejects.toBeInstanceOf(XumuxClosedError);
  });

  it('rejects an incompatible version with 4006', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const s = server.accept().catch((e: unknown) => e);
    raw.send(ctrl(ControlType.HELLO, { version: [0, 1, 0] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(4006);
    expect(await s).toBeInstanceOf(XumuxClosedError);
  });

  it('enforces the HELLO timeout (4007)', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, { ...base, handshakeTimeoutMs: 30 });
    const s = server.accept().catch((e: unknown) => e);
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(4007);
    expect(await s).toBeInstanceOf(XumuxClosedError);
  });

  it('client gives up when WELCOME never comes', async () => {
    const { transport } = rawPair();
    const client = new XumuxClient(transport, { ...base, handshakeTimeoutMs: 30 });
    await expect(client.connect()).rejects.toBeInstanceOf(XumuxClosedError);
  });

  it('client closes if WELCOME is invalid (unrequested channel)', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect().catch((e: unknown) => e);
    await raw.control(ControlType.HELLO);
    raw.send(
      ctrl(ControlType.WELCOME, {
        version: [0, 3, 0],
        channels: [{ id: 9, reliable: true, ordered: true }],
      }),
    );
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(1002);
    expect(await p).toBeInstanceOf(Error);
  });

  it('server negotiates extensions as an intersection', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const [c] = await Promise.all([
      client.connect({ extensions: ['a/1', 'b/1'] }),
      server.accept({ extensions: ['b/1', 'c/1'] }),
    ]);
    expect(c.welcome.extensions).toEqual(['b/1']);
  });

  it('constructor rejects inconsistent limits', () => {
    const [t] = createInMemoryPair();
    expect(() => new XumuxClient(t, { maxMessageSize: 100_000, initialWindow: 50_000 })).toThrow(
      RangeError,
    );
    expect(() => new XumuxClient(t, { maxFramePayload: 1000 })).toThrow(RangeError);
  });

  it('versionsCompatible follows core §3.5', () => {
    expect(versionsCompatible([0, 3, 0], [0, 3, 5])).toBe(true);
    expect(versionsCompatible([0, 3, 0], [0, 2, 0])).toBe(false);
    expect(versionsCompatible([0, 3, 0], [0, 1, 0])).toBe(false);
    expect(versionsCompatible([1, 2, 0], [1, 0, 0])).toBe(true);
    expect(versionsCompatible([1, 0, 0], [2, 0, 0])).toBe(false);
  });
});

describe('messages', () => {
  it('carries the application type byte both ways', async () => {
    const { c, s } = await pair();
    const [cc, sc] = [c.channels[0]!, s.channels[0]!];
    await cc.send(0x42, te.encode('hi'));
    expect(await sc.receive()).toEqual({ type: 0x42, payload: te.encode('hi') });
    await sc.send(0x07, new Uint8Array(0));
    expect(await cc.receive()).toEqual({ type: 0x07, payload: new Uint8Array(0) });
  });

  for (const kind of ['message', 'stream'] as const) {
    it(`fragments and reassembles large messages (${kind})`, async () => {
      const { c, s } = await pair({}, {}, [{ name: 'bulk' }], kind);
      const big = new Uint8Array(200_000).map((_, i) => i & 0xff);
      const sent = c.channels[0]!.send(9, big);
      const got = await s.channels[0]!.receive();
      await sent;
      expect(got!.type).toBe(9);
      expect(got!.payload).toEqual(big);
    });
  }

  it('rejects messages over the peer maxMessageSize', async () => {
    const { c } = await pair({}, { maxMessageSize: 65536, initialWindow: 65536 });
    await expect(c.channels[0]!.send(1, new Uint8Array(70_000))).rejects.toBeInstanceOf(RangeError);
  });

  it('async iteration ends with null on normal close', async () => {
    const { c, s } = await pair();
    const got: string[] = [];
    const reading = (async () => {
      for await (const m of s.channels[0]!) got.push(td.decode(m.payload));
    })();
    await c.channels[0]!.send(1, te.encode('one'));
    await c.channels[0]!.send(1, te.encode('two'));
    await c.channels[0]!.close();
    await reading;
    expect(got).toEqual(['one', 'two']);
  });
});

describe('channels', () => {
  it('both sides can open channels at the same time without colliding', async () => {
    const { client, server } = await pair({}, {}, []);
    const [a, b] = await Promise.all([
      client.openChannel({ name: 'x', reliable: true, ordered: true }),
      server.openChannel({ name: 'y', reliable: true, ordered: true }),
    ]);
    expect(a.id % 2).toBe(1);
    expect(b.id % 2).toBe(0);
    expect(client.channels.size).toBe(2);
  });

  it('emits "channel" for peer-opened channels', async () => {
    const { client, server } = await pair({}, {}, []);
    const seen = new Promise<XumuxChannel>((r) =>
      server.addEventListener('channel', (e) => r((e as CustomEvent<XumuxChannel>).detail)),
    );
    const ch = await client.openChannel({
      name: 'dyn',
      reliable: true,
      ordered: true,
      metadata: { k: 1 },
    });
    const remote = await seen;
    expect([remote.id, remote.name, remote.metadata]).toEqual([ch.id, 'dyn', { k: 1 }]);
  });

  it('openChannel rejects when the peer refuses', async () => {
    const { client } = await pair(
      {},
      { onChannelRequest: () => ({ code: 4010, reason: 'no' }) },
      [],
    );
    const err = await client
      .openChannel({ name: 'z', reliable: true, ordered: true })
      .catch((e: unknown) => e);
    expect((err as XumuxClosedError).code).toBe(4010);
  });

  it('two-step close frees the ID for reuse only after both sides closed', async () => {
    const { client, server } = await pair({}, {}, []);
    const ch = await client.openChannel({ name: 'x', reliable: true, ordered: true });
    const closes: number[] = [];
    server.addEventListener('channel-close', () => closes.push(1));
    const info = await ch.close(1000, 'done');
    expect(info).toMatchObject({ code: 1000, remote: false });
    expect(ch.state).toBe('closed');
    await later(5);
    expect(closes).toHaveLength(1);
    const again = await client.openChannel({ name: 'x2', reliable: true, ordered: true });
    expect(again.id).toBe(ch.id);
  });

  it('a remote abnormal close makes receive() reject', async () => {
    const { c, s } = await pair();
    const r = c.channels[0]!.receive();
    await s.channels[0]!.close(4500, 'target gone');
    await expect(r).rejects.toMatchObject({ code: 4500, remote: true });
  });
});

describe('flow control', () => {
  const small: XumuxOptions = { maxFramePayload: 4096, maxMessageSize: 4096, initialWindow: 8192 };

  it('blocks the sender when the receiver stops reading, and resumes on read', async () => {
    const { c, s } = await pair({}, small);
    const ch = c.channels[0]!;
    const msg = new Uint8Array(4096);
    await ch.send(1, msg);
    await ch.send(1, msg);
    let third = false;
    const p = ch.send(1, msg).then(() => (third = true));
    await later(20);
    expect(third).toBe(false);
    await s.channels[0]!.receive();
    await s.channels[0]!.receive();
    await p;
    expect(third).toBe(true);
  });

  it('closes with 4009 when the peer exceeds its credit', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, { ...base, ...small });
    void server.accept();
    await raw.helloAndWelcome({ channels: [{ id: 1, name: 'd', reliable: true, ordered: true }] });
    for (let i = 0; i < 3; i++)
      raw.send({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(4096) });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(4009);
  });

  it('returns credit with WINDOW_UPDATE as the app reads', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const acc = server.accept();
    await raw.helloAndWelcome({ channels: [{ id: 1, name: 'd', reliable: true, ordered: true }] });
    const { channels } = await acc;
    raw.send({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(1000) });
    await channels[0]!.receive();
    const wu = await raw.control(ControlType.WINDOW_UPDATE);
    expect(Array.from(wu.payload)).toEqual([0, 1, 0, 0, 0x03, 0xe8]);
  });
});

describe('probes and keepalive', () => {
  it('measures per-channel and connection RTT', async () => {
    const { client, c } = await pair();
    const rtt = await c.channels[0]!.ping();
    expect(rtt).toBeGreaterThanOrEqual(0);
    expect(c.channels[0]!.stats).toMatchObject({ probesSent: 1, probesLost: 0 });
    await client.ping();
    expect(client.rtt).toBeGreaterThanOrEqual(0);
  });

  it('answers PING on the same channel with the same token, bypassing the app', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const acc = server.accept();
    await raw.helloAndWelcome({ channels: [{ id: 1, name: 'd', reliable: true, ordered: true }] });
    await acc;
    raw.send({
      channel: 1,
      type: ProbeType.PING,
      flags: FrameFlags.PROBE,
      payload: new Uint8Array([1, 2, 3, 4]),
    });
    const pong = await raw.next((f) => f.type === ProbeType.PONG);
    expect(pong).toMatchObject({ channel: 1, flags: FrameFlags.PROBE });
    expect(Array.from(pong.payload)).toEqual([1, 2, 3, 4]);
  });

  it('counts a lost probe on timeout', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.awaitHelloSendWelcome();
    await p;
    await expect(client.ping(20)).rejects.toThrow(/timed out/);
    expect(client.stats.probesLost).toBe(1);
  });

  it('closes with 4008 when the peer goes silent', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, { ...base, pingInterval: 0.03, pingTimeout: 0.03 });
    void server.accept();
    await raw.helloAndWelcome();
    await raw.next((f) => f.type === ProbeType.PING && (f.flags & FrameFlags.PROBE) !== 0);
    expect((await server.closed).code).toBe(CloseCode.KEEPALIVE_TIMEOUT);
  });

  it('keeps an idle connection alive when both sides answer', async () => {
    const { client } = await pair({}, { pingInterval: 0.02, pingTimeout: 0.05 });
    await later(150);
    expect(client.channels.size).toBe(1);
  });
});

describe('robustness', () => {
  async function connectedServer() {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const acc = server.accept();
    await raw.helloAndWelcome({ channels: [{ id: 1, name: 'd', reliable: true, ordered: true }] });
    await acc;
    return { server, raw };
  }

  it('answers an unknown control type with ERROR 1003 and stays open', async () => {
    const { server, raw } = await connectedServer();
    raw.send(ctrl(0x0e, {}));
    expect(json(await raw.control(ControlType.ERROR)).code).toBe(1003);
    expect(server.channels.size).toBe(1);
  });

  it('sends ERROR 4003 for frames on unknown channels', async () => {
    const { raw } = await connectedServer();
    raw.send({ channel: 9, type: 1, flags: 0, payload: new Uint8Array(1) });
    expect(json(await raw.control(ControlType.ERROR))).toMatchObject({ code: 4003, channel: 9 });
  });

  it('answers CLOSE_CHANNEL for channel 0 with ERROR 1002', async () => {
    const { raw } = await connectedServer();
    raw.send(ctrl(ControlType.CLOSE_CHANNEL, { id: 0 }));
    expect(json(await raw.control(ControlType.ERROR)).code).toBe(1002);
  });

  it('closes only the channel on a fragment type mismatch', async () => {
    const { server, raw } = await connectedServer();
    raw.send({ channel: 1, type: 1, flags: FrameFlags.MORE, payload: new Uint8Array(10) });
    raw.send({ channel: 1, type: 2, flags: 0, payload: new Uint8Array(10) });
    expect(json(await raw.control(ControlType.CLOSE_CHANNEL))).toMatchObject({ id: 1, code: 1002 });
    raw.send(ctrl(ControlType.CLOSE_CHANNEL, { id: 1 }));
    await later(5);
    expect(server.channels.size).toBe(0);
  });

  it('peer-error event carries ERROR messages', async () => {
    const { server, raw } = await connectedServer();
    const got = new Promise((r) =>
      server.addEventListener('peer-error', (e) => r((e as CustomEvent).detail)),
    );
    raw.send(ctrl(ControlType.ERROR, { code: 4004, reason: 'slow down' }));
    expect(await got).toMatchObject({ code: 4004, reason: 'slow down' });
  });
});

describe('connection close', () => {
  it('flushes queued data before CLOSE, and both sides see 1000', async () => {
    const { client, server, s, c } = await pair();
    void c.channels[0]!.send(1, te.encode('last words'));
    const closing = client.close();
    expect(td.decode((await s.channels[0]!.receive())!.payload)).toBe('last words');
    expect(await closing).toMatchObject({ code: 1000, remote: false });
    expect(await server.closed).toMatchObject({ code: 1000, remote: true });
  });

  it('treats transport EOF as 1001 and fails open channels', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const [c] = await Promise.all([
      client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] }),
      server.accept(),
    ]);
    const r = c.channels[0]!.receive();
    st.close();
    expect((await client.closed).code).toBe(1001);
    await expect(r).rejects.toMatchObject({ code: 1001 });
  });
});

describe('edge paths', () => {
  it('channel(name) finds open channels', async () => {
    const { client } = await pair({}, {}, [{ name: 'a' }]);
    expect(client.channel('a')?.id).toBe(1);
    expect(client.channel('zz')).toBeUndefined();
  });

  it('send() rejects on a closed channel and for an out-of-range type', async () => {
    const { c } = await pair();
    const ch = c.channels[0]!;
    await expect(ch.send(256, new Uint8Array(0))).rejects.toBeInstanceOf(RangeError);
    await ch.close();
    await expect(ch.send(1, new Uint8Array(0))).rejects.toThrow(/not open/);
    await expect(ch.ping()).rejects.toThrow(/not open/);
  });

  it('a throwing policy refuses with 4010', async () => {
    const { client } = await pair(
      {},
      {
        onChannelRequest: () => {
          throw new Error('policy broke');
        },
      },
      [],
    );
    await expect(
      client.openChannel({ name: 'x', reliable: true, ordered: true }),
    ).rejects.toMatchObject({ code: 4010 });
  });

  it('a throwing authorize() refuses with 4000', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    void server
      .accept({
        authorize: () => {
          throw new Error('no');
        },
      })
      .catch(() => undefined);
    await expect(client.connect()).rejects.toMatchObject({ code: 4000 });
  });

  it('holds frames for a HELLO channel that overtake WELCOME, then delivers them', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] });
    await raw.control(ControlType.HELLO);
    raw.send({ channel: 1, type: 5, flags: 0, payload: te.encode('early') });
    raw.send(
      ctrl(ControlType.WELCOME, {
        version: [0, 3, 0],
        channels: [{ id: 1, reliable: true, ordered: true }],
      }),
    );
    const { channels } = await p;
    expect(td.decode((await channels[0]!.receive())!.payload)).toBe('early');
  });

  it('holds probes and data for a dynamically opened channel until CHANNEL_ACK', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.awaitHelloSendWelcome();
    await p;
    const opening = client.openChannel({ name: 'x', reliable: true, ordered: true });
    const open = await raw.control(ControlType.OPEN_CHANNEL);
    const id = json(open).id as number;
    raw.send({
      channel: id,
      type: ProbeType.PING,
      flags: FrameFlags.PROBE,
      payload: new Uint8Array([0, 0, 0, 7]),
    });
    raw.send({ channel: id, type: 1, flags: 0, payload: te.encode('x') });
    raw.send(ctrl(ControlType.CHANNEL_ACK, { id, reliable: true, ordered: true }));
    const ch = await opening;
    expect(td.decode((await ch.receive())!.payload)).toBe('x');
    const pong = await raw.next((f) => f.type === ProbeType.PONG);
    expect(pong.channel).toBe(id);
  });

  it('rejects pending pings when the channel and the connection close', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] });
    await raw.awaitHelloSendWelcome();
    const { channels } = await p;
    const chPing = channels[0]!.ping();
    const connPing = client.ping();
    raw.send(ctrl(ControlType.CLOSE_CHANNEL, { id: 1 }));
    await expect(chPing).rejects.toThrow(/closed/);
    raw.send(ctrl(ControlType.CLOSE, { code: 1001 }));
    await expect(connPing).rejects.toBeInstanceOf(XumuxClosedError);
  });

  it('fails queued sends when the transport throws', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const [c] = await Promise.all([
      client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] }),
      server.accept(),
    ]);
    (ct as { send: (d: Uint8Array) => void }).send = () => {
      throw new Error('wire cut');
    };
    await expect(c.channels[0]!.send(1, new Uint8Array(1))).rejects.toThrow('wire cut');
  });

  it('a protocol error while closing finishes the close', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, { ...base, closeTimeoutMs: 5000 });
    const acc = server.accept();
    await raw.helloAndWelcome();
    await acc;
    void server.close();
    await raw.control(ControlType.CLOSE);
    raw.send({ channel: 1, type: 1, flags: 0x40, payload: new Uint8Array(0) });
    expect((await server.closed).code).toBe(1000);
  });

  it('openChannel and ping refuse before the handshake', async () => {
    const [t] = createInMemoryPair();
    const client = new XumuxClient(t, { ...base, handshakeTimeoutMs: 50 });
    await expect(client.openChannel({ name: 'x', reliable: true, ordered: true })).rejects.toThrow(
      /idle/,
    );
    await expect(client.ping()).rejects.toThrow(/idle/);
    void client.connect().catch(() => undefined);
    await expect(client.connect()).rejects.toThrow(/once/);
    await client.close();
  });
});

describe('review regressions', () => {
  const hello = (ch: { id: number; name: string }[] = []) => ({
    channels: ch.map((c) => ({ ...c, reliable: true, ordered: true })),
  });
  const small = { maxFramePayload: 4096, maxMessageSize: 4096, initialWindow: 4096 };

  it('handles frames that arrive in the same read as WELCOME', async () => {
    const { transport, raw } = rawPair('stream');
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.control(ControlType.HELLO);
    const welcome = encodeFrame(ctrl(ControlType.WELCOME, { version: [0, 3, 0] }));
    const open = encodeFrame(
      ctrl(ControlType.OPEN_CHANNEL, { id: 2, name: 'srv', reliable: true, ordered: true }),
    );
    raw.sendBytes(new Uint8Array([...welcome, ...open]));
    await p;
    const ack = await raw.control(ControlType.CHANNEL_ACK);
    expect(json(ack).id).toBe(2);
    expect([...client.channels.values()].map((c) => c.name)).toEqual(['srv']);
  });

  it('does not open channels if the connection ends while the policy decides', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, {
      ...base,
      onChannelRequest: () => later(30).then(() => true as const),
    });
    const acc = server.accept();
    raw.send(ctrl(ControlType.HELLO, { version: [0, 3, 0], ...hello([{ id: 1, name: 'd' }]) }));
    await later(5);
    raw.transport.close();
    await expect(acc).rejects.toBeInstanceOf(XumuxClosedError);
    expect(server.channels.size).toBe(0);
  });

  it('a channel blocked on credit still closes, and every queued send fails', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] });
    await raw.awaitHelloSendWelcome(small);
    const ch = (await p).channels[0]!;
    const sends = [
      ch.send(1, new Uint8Array(4096)),
      ch.send(1, new Uint8Array(4096)),
      ch.send(1, new Uint8Array(10)),
    ];
    await raw.next((f) => f.channel === 1);
    const closing = ch.close();
    await raw.control(ControlType.CLOSE_CHANNEL, 2000);
    raw.send(ctrl(ControlType.CLOSE_CHANNEL, { id: 1 }));
    expect((await closing).code).toBe(1000);
    const results = await Promise.allSettled(sends);
    expect(results.map((r) => r.status)).toEqual(['fulfilled', 'rejected', 'rejected']);
  });

  it('closes with 4009 when an unacknowledged channel receives more than initialWindow', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.awaitHelloSendWelcome();
    await p;
    void client.openChannel({ name: 'x', reliable: true, ordered: true }).catch(() => undefined);
    const id = json(await raw.control(ControlType.OPEN_CHANNEL)).id as number;
    for (let i = 0; i < 5; i++)
      raw.send({ channel: id, type: 1, flags: 0, payload: new Uint8Array(65536) });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.FLOW_CONTROL_ERROR);
  });

  it('keeps the close cause when a frame cannot be decoded', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.awaitHelloSendWelcome();
    await p;
    raw.sendBytes(new Uint8Array([0, 1, 2]));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
    expect((await client.closed).code).toBe(CloseCode.INVALID_MESSAGE);
  });

  it('drops queued data for a channel the peer closes', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect({ channels: [{ name: 'd', reliable: true, ordered: true }] });
    await raw.awaitHelloSendWelcome();
    const ch = (await p).channels[0]!;
    const t = transport as { send: (d: Uint8Array) => void | Promise<void> };
    const orig = t.send.bind(transport);
    let open!: () => void;
    const gate = new Promise<void>((r) => (open = r));
    t.send = async (d) => {
      await gate;
      return orig(d);
    };
    void client.ping().catch(() => undefined); // occupies the pump
    const sent = ch.send(7, te.encode('stale'));
    raw.send(ctrl(ControlType.CLOSE_CHANNEL, { id: 1 }));
    await later(10);
    open();
    await expect(sent).rejects.toThrow(/closed/);
    await raw.control(ControlType.CLOSE_CHANNEL);
    await later(10);
    expect(raw.frames.some((f) => f.channel === 1 && f.type === 7)).toBe(false);
  });
});

describe('review regressions (probes)', () => {
  it('concurrent pings get distinct tokens even when the clock does not advance', async () => {
    const { client } = await pair();
    const spy = vi.spyOn(performance, 'now').mockReturnValue(performance.now());
    try {
      const rtts = await Promise.all([client.ping(), client.ping(), client.ping(), client.ping()]);
      expect(rtts).toHaveLength(4);
      expect(client.stats.probesLost).toBe(0);
    } finally {
      spy.mockRestore();
    }
  });
});

describe('review regressions (held probes)', () => {
  it('probes held before CHANNEL_ACK do not use data credit', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect();
    await raw.awaitHelloSendWelcome();
    await p;
    const opening = client.openChannel({ name: 'x', reliable: true, ordered: true });
    const id = json(await raw.control(ControlType.OPEN_CHANNEL)).id as number;
    raw.send({
      channel: id,
      type: ProbeType.PING,
      flags: FrameFlags.PROBE,
      payload: new Uint8Array([0, 0, 0, 1]),
    });
    for (let i = 0; i < 4; i++)
      raw.send({ channel: id, type: 1, flags: 0, payload: new Uint8Array(65536) });
    raw.send(ctrl(ControlType.CHANNEL_ACK, { id, reliable: true, ordered: true }));
    const ch = await opening;
    expect((await ch.receive())?.payload.length).toBe(65536);
    expect((await raw.next((f) => f.type === ProbeType.PONG)).channel).toBe(id);
    expect(client.state).toBe('open');
  });
});

describe('review regressions (close reply)', () => {
  it('still sends its CLOSE reply when the peer ends its input right after CLOSE', async () => {
    // A transport whose writes are slow and whose input ends right after the peer's CLOSE,
    // like a TCP peer that sends CLOSE and then shuts down its write side.
    const sent: number[] = [];
    let welcomed!: () => void;
    const ready = new Promise<void>((r) => (welcomed = r));
    let closeNow!: () => void;
    const peerClose = new Promise<void>((r) => (closeNow = r));
    const transport = {
      kind: 'message' as const,
      async send(data: Uint8Array) {
        const ch = data[0]! * 256 + data[1]!;
        if (ch === 0) sent.push(data[2]!);
        if (data[2] === ControlType.HELLO) welcomed();
        await later(20);
      },
      async *receive() {
        await ready;
        yield encodeFrame(
          ctrl(ControlType.WELCOME, {
            version: [0, 3, 0],
            channels: [{ id: 1, name: 'd', reliable: true, ordered: true }],
          }),
        );
        await peerClose;
        yield encodeFrame(ctrl(ControlType.CLOSE, { code: CloseCode.NORMAL }));
      },
      close() {},
    };
    const client = new XumuxClient(transport, base);
    const { channels } = await client.connect({
      channels: [{ name: 'd', reliable: true, ordered: true }],
    });
    void channels[0]!.send(1, new Uint8Array(10)).catch(() => undefined); // keep the writer busy
    closeNow();
    await client.closed.catch(() => undefined);
    expect(sent).toContain(ControlType.CLOSE);
  });
});
