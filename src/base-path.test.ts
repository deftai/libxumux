/**
 * The base path (framing over one transport) and the session seam that lets an extension
 * detach it and attach a new one under a live session (core §1.2, §11.1).
 */
import { describe, it, expect, vi } from 'vitest';
import { BasePath, type BasePathSink } from './base-path.js';
import { XumuxClient, XumuxServer, type DetachInfo, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { encodeFrame } from './codec/encode-frame.js';
import { CloseCode, ControlType, XMUX_MAGIC } from './codec/constants.js';
import type { Frame } from './codec/frame.js';
import { XumuxClosedError, XumuxProtocolError } from './errors.js';
import type { TransportAdapter } from './types.js';
import { waitUntil } from './transports/event-queue.js';
import { ctrl } from '../tests/raw-peer.js';

const te = new TextEncoder();
const td = new TextDecoder();

/** A transport driven by the test: push chunks in, see what was written. */
function scripted(kind: 'message' | 'stream' = 'stream'): {
  t: TransportAdapter;
  push: (b: Uint8Array) => void;
  end: (e?: Error) => void;
  written: Uint8Array[];
  closes: () => number;
} {
  const chunks: Uint8Array[] = [];
  let ended: { e?: Error } | null = null;
  let wake: (() => void) | null = null;
  const written: Uint8Array[] = [];
  let closes = 0;
  const t: TransportAdapter = {
    kind,
    send: (d) => void written.push(new Uint8Array(d)),
    receive: () => ({
      [Symbol.asyncIterator]: () => ({
        next: async (): Promise<IteratorResult<Uint8Array>> => {
          for (;;) {
            if (chunks.length) return { done: false, value: chunks.shift()! };
            if (ended) {
              if (ended.e) throw ended.e;
              return { done: true, value: undefined };
            }
            await new Promise<void>((r) => (wake = r));
          }
        },
      }),
    }),
    close: () => void closes++,
  };
  const poke = (): void => {
    const w = wake;
    wake = null;
    w?.();
  };
  return {
    t,
    push: (b) => (chunks.push(b), poke()),
    end: (e) => ((ended = { e }), poke()),
    written,
    closes: () => closes,
  };
}

function collector(): BasePathSink & { frames: Frame[]; ends: unknown[] } {
  const frames: Frame[] = [];
  const ends: unknown[] = [];
  return { frames, ends, frame: (f) => void frames.push(f), end: (c) => void ends.push(c) };
}

const data = (channel: number, s: string): Frame => ({
  channel,
  type: 1,
  flags: 0,
  payload: te.encode(s),
});

const cat = (...parts: Uint8Array[]): Uint8Array => {
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0;
  for (const p of parts) {
    out.set(p, o);
    o += p.length;
  }
  return out;
};

describe('BasePath', () => {
  it('writes the magic once on stream bindings, and never on message bindings', async () => {
    const s = scripted('stream');
    const p = new BasePath(s.t, 65536);
    await p.write(new Uint8Array([1]));
    await p.write(new Uint8Array([2]));
    expect(s.written).toEqual([
      new Uint8Array(XMUX_MAGIC),
      new Uint8Array([1]),
      new Uint8Array([2]),
    ]);
    const m = scripted('message');
    await new BasePath(m.t, 65536).write(new Uint8Array([3]));
    expect(m.written).toEqual([new Uint8Array([3])]);
  });

  it('waits for a slow magic write before writing the frame', async () => {
    const sent: string[] = [];
    let release!: () => void;
    const t: TransportAdapter = {
      kind: 'stream',
      send: (d) => {
        sent.push(d.length === 4 ? 'magic' : 'frame');
        if (d.length === 4) return new Promise<void>((r) => (release = r));
      },
      receive: () => ({ [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => {}) }) }),
      close: () => undefined,
    };
    const w = new BasePath(t, 65536).write(new Uint8Array([9]));
    expect(sent).toEqual(['magic']);
    release();
    await w;
    expect(sent).toEqual(['magic', 'frame']);
  });

  it('frames a stream and reports its end', async () => {
    const s = scripted();
    const p = new BasePath(s.t, 65536);
    const c = collector();
    p.start(c);
    const bytes = cat(
      new Uint8Array(XMUX_MAGIC),
      encodeFrame(data(1, 'a')),
      encodeFrame(data(3, 'b')),
    );
    s.push(bytes.subarray(0, 7));
    s.push(bytes.subarray(7));
    s.end();
    await waitUntil(() => c.ends.length === 1);
    expect(c.frames.map((f) => td.decode(f.payload))).toEqual(['a', 'b']);
    expect(c.ends).toEqual([{}]);
  });

  it('reports read errors and parse errors apart', async () => {
    const s = scripted();
    const p = new BasePath(s.t, 65536);
    const c = collector();
    p.start(c);
    const boom = new Error('ECONNRESET');
    s.end(boom);
    await waitUntil(() => c.ends.length === 1);
    expect(c.ends[0]).toEqual({ readError: boom });

    const m = scripted('message');
    const pm = new BasePath(m.t, 65536);
    const cm = collector();
    pm.start(cm);
    m.push(new Uint8Array(3));
    await waitUntil(() => cm.ends.length === 1);
    expect((cm.ends[0] as { parseError: unknown }).parseError).toBeInstanceOf(XumuxProtocolError);
  });

  it('applies the limit the session sets', async () => {
    const m = scripted('message');
    const p = new BasePath(m.t, 65536);
    p.setMaxFramePayload(4096);
    const c = collector();
    p.start(c);
    m.push(encodeFrame({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(5000) }));
    await waitUntil(() => c.ends.length === 1);
    expect((c.ends[0] as { parseError: XumuxProtocolError }).parseError.code).toBe(
      CloseCode.MESSAGE_TOO_LARGE,
    );
    const s = scripted('stream');
    const ps = new BasePath(s.t, 65536);
    ps.setMaxFramePayload(4096);
    const cs = collector();
    ps.start(cs);
    s.push(new Uint8Array(XMUX_MAGIC));
    s.push(encodeFrame({ channel: 1, type: 1, flags: 0, payload: new Uint8Array(5000) }));
    await waitUntil(() => cs.ends.length === 1);
    expect(cs.ends[0]).toHaveProperty('parseError');
  });

  it('pauses mid-chunk and hands everything read, including a partial frame, to the next sink', async () => {
    const s = scripted();
    const p = new BasePath(s.t, 65536);
    const first: Frame[] = [];
    const second = collector();
    p.start({
      frame: (f) => {
        first.push(f);
        p.pause(); // e.g. an extension-defined first frame: stop here
      },
      end: () => undefined,
    });
    const tail = encodeFrame(data(5, 'c'));
    s.push(
      cat(
        new Uint8Array(XMUX_MAGIC),
        encodeFrame(data(1, 'a')),
        encodeFrame(data(3, 'b')),
        tail.subarray(0, 4),
      ),
    );
    await waitUntil(() => first.length === 1);
    await new Promise((r) => setTimeout(r, 5));
    expect(first).toHaveLength(1);
    p.start(second);
    s.push(tail.subarray(4));
    await waitUntil(() => second.frames.length === 2);
    expect(second.frames.map((f) => td.decode(f.payload))).toEqual(['b', 'c']);
  });

  it('a paused path keeps a pending read for the next sink, and an end it saw', async () => {
    const s = scripted();
    const p = new BasePath(s.t, 65536);
    const a = collector();
    p.start(a);
    await new Promise((r) => setTimeout(r, 1)); // the read is now pending
    p.pause();
    s.push(cat(new Uint8Array(XMUX_MAGIC), encodeFrame(data(1, 'x'))));
    s.end();
    await new Promise((r) => setTimeout(r, 5));
    expect(a.frames).toHaveLength(0);
    const b = collector();
    p.start(b);
    await waitUntil(() => b.ends.length === 1);
    expect(b.frames.map((f) => td.decode(f.payload))).toEqual(['x']);
    expect(a.ends).toHaveLength(0);
  });

  it('close() closes the transport once, stops delivering and releases the reader', async () => {
    const s = scripted();
    const ret = vi.fn(() => Promise.resolve({ done: true as const, value: undefined }));
    const it0 = s.t.receive()[Symbol.asyncIterator]();
    const t: TransportAdapter = {
      ...s.t,
      receive: () => ({ [Symbol.asyncIterator]: () => ({ next: () => it0.next(), return: ret }) }),
    };
    const p = new BasePath(t, 65536);
    const c = collector();
    p.start(c);
    await new Promise((r) => setTimeout(r, 1));
    p.close();
    p.close();
    expect(p.closed).toBe(true);
    expect(s.closes()).toBe(1);
    s.push(cat(new Uint8Array(XMUX_MAGIC), encodeFrame(data(1, 'late'))));
    await waitUntil(() => ret.mock.calls.length === 1);
    expect(c.frames).toHaveLength(0);
    p.start(c); // no effect once closed
    expect(c.ends).toHaveLength(0);
  });

  it('tolerates a transport whose close throws', () => {
    const s = scripted();
    const p = new BasePath(
      {
        ...s.t,
        close: () => {
          throw new Error('already closed');
        },
      },
      65536,
    );
    expect(() => p.close()).not.toThrow();
  });
});

// ─── The session seam ───────────────────────────────────────────────────────────

/** Sessions that keep running when the base path is lost, as an extension can make them. */
class KeepingClient extends XumuxClient {
  keep = true;
  protected override keepOnPathLoss(): boolean {
    return this.keep;
  }
  attach(t: TransportAdapter): void {
    this.attachBasePath(new BasePath(t, 0));
  }
  drop(reason: string): void {
    this.detachBasePath(reason);
  }
}
class KeepingServer extends XumuxServer {
  protected override keepOnPathLoss(): boolean {
    return true;
  }
  attach(t: TransportAdapter): void {
    this.attachBasePath(new BasePath(t, 0));
  }
}

const opts: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 200, onChannelRequest: () => true };

async function keepingPair(kind: 'message' | 'stream' = 'stream') {
  const [ct, st] = createInMemoryPair(kind);
  const client = new KeepingClient(ct, opts);
  const server = new KeepingServer(st, opts);
  const [c, s] = await Promise.all([
    client.connect({ channels: [{ name: 'x', reliable: true, ordered: true }] }),
    server.accept(),
  ]);
  return { client, server, ct, st, cch: c.channels[0]!, sch: s.channels[0]! };
}

const detachEvent = (peer: EventTarget): Promise<DetachInfo> =>
  new Promise((r) =>
    peer.addEventListener('detach', (e) => r((e as CustomEvent<DetachInfo>).detail), {
      once: true,
    }),
  );

describe('session seam: detaching and attaching the base path', () => {
  it('keeps the session when the transport ends, and carries on on a new one', async () => {
    const { client, server, ct, cch, sch } = await keepingPair();
    const [cd, sd] = [detachEvent(client), detachEvent(server)];
    const pings = client.ping(10_000).catch((e: unknown) => e);
    ct.close();
    expect((await cd).reason).toBe('transport closed');
    expect((await sd).reason).toBe('transport closed');
    expect(client.detached && server.detached).toBe(true);
    expect(await pings).toBeInstanceOf(Error);
    expect(client.stats.probesLost).toBe(0); // abandoned, not lost
    expect(cch.state).toBe('open');
    expect(() => client.attach(ct)).not.toThrow(); // the old one is closed: nothing comes from it
    client.drop('again');
    expect(client.detached).toBe(true);

    // Sends made while detached wait in the queue for the next path.
    const sent = cch.send(1, te.encode('while detached'));
    const back = sch.send(2, te.encode('reply'));
    const [ct2, st2] = createInMemoryPair('stream');
    client.attach(ct2);
    server.attach(st2);
    expect(client.detached).toBe(false);
    expect(() => client.attach(ct2)).toThrow(/already attached/);
    await Promise.all([sent, back]);
    expect(td.decode((await sch.receive())!.payload)).toBe('while detached');
    expect(td.decode((await cch.receive())!.payload)).toBe('reply');
    expect(await client.ping()).toBeGreaterThanOrEqual(0);
    const closing = server.closed;
    expect((await client.close(1000, 'done')).code).toBe(1000);
    expect((await closing).remote).toBe(true);
  });

  it('close() while detached ends the session at once without sending', async () => {
    const { client, ct, cch } = await keepingPair('message');
    const cd = detachEvent(client);
    ct.close();
    await cd;
    const info = await client.close(1000, 'bye');
    expect(info).toEqual({ code: 1000, reason: 'bye', remote: false });
    await expect(cch.closed).resolves.toMatchObject({ code: 1000 });
    expect(() => client.attach(createInMemoryPair()[0])).toThrow(/while closed/);
  });

  it('detaches on a transport read error instead of closing with 1002', async () => {
    const s = scripted('message');
    const client = new KeepingClient(s.t, opts);
    const p = client.connect();
    await waitUntil(() => s.written.length === 1);
    s.push(encodeFrame(ctrl(ControlType.WELCOME, { version: [0, 3, 0] })));
    await p;
    const cd = detachEvent(client);
    s.end(new Error('ECONNRESET'));
    expect((await cd).reason).toBe('transport failed: ECONNRESET');
    expect(s.closes()).toBe(1);
  });

  it('detaches on a write error, keeping the frames still queued', async () => {
    let fail = false;
    const [ct, st] = createInMemoryPair('message');
    const flaky: TransportAdapter = {
      kind: 'message',
      send: (d) => {
        if (fail) throw new Error('EPIPE');
        return ct.send(d);
      },
      receive: () => ct.receive(),
      close: () => ct.close(),
    };
    const client = new KeepingClient(flaky, opts);
    const server = new KeepingServer(st, opts);
    const [c, s] = await Promise.all([
      client.connect({ channels: [{ name: 'x', reliable: true, ordered: true }] }),
      server.accept(),
    ]);
    const [cd, sd] = [detachEvent(client), detachEvent(server)];
    fail = true;
    const lost = c.channels[0]!.send(1, te.encode('lost with the transport'));
    const kept = c.channels[0]!.send(1, te.encode('kept'));
    await expect(lost).rejects.toThrow('EPIPE');
    expect((await cd).reason).toBe('transport failed: EPIPE');
    await sd;
    const [ct2, st2] = createInMemoryPair('message');
    client.attach(ct2);
    server.attach(st2);
    await kept;
    expect(td.decode((await s.channels[0]!.receive())!.payload)).toBe('kept');
    await client.close();
  });

  it('a keepalive timeout detaches without sending CLOSE 4008', async () => {
    const [ct, st] = createInMemoryPair('message');
    const client = new KeepingClient(ct, { ...opts, pingInterval: 0.02, pingTimeout: 0.02 });
    // A server that answers HELLO but never anything else (no PONG).
    const sent: number[] = [];
    void (async () => {
      for await (const m of st.receive()) {
        sent.push(m[2]!);
        if (m[2] === ControlType.HELLO)
          void st.send(
            encodeFrame(
              ctrl(ControlType.WELCOME, {
                version: [0, 3, 0],
                pingInterval: 0.02,
                pingTimeout: 0.02,
              }),
            ),
          );
      }
    })();
    await client.connect();
    expect((await detachEvent(client)).reason).toBe('keepalive timeout');
    await new Promise((r) => setTimeout(r, 10));
    expect(sent).not.toContain(ControlType.CLOSE);
    client.keep = false;
    await client.close();
  });

  it('without the extension a lost transport still closes the connection with 1001', async () => {
    const { client, server, ct } = await keepingPair();
    client.keep = false;
    const closed = client.closed;
    ct.close();
    expect(await closed).toEqual({
      code: CloseCode.GOING_AWAY,
      reason: 'transport closed',
      remote: true,
    });
    expect(server.detached).toBe(true);
    await server.close();
  });

  it('a lost transport during the handshake closes as before', async () => {
    const [ct] = createInMemoryPair('message');
    const client = new KeepingClient(ct, opts);
    const p = client.connect();
    ct.close();
    await expect(p).rejects.toBeInstanceOf(XumuxClosedError);
    expect(client.detached).toBe(false);
  });
});
