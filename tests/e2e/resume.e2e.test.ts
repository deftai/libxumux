/**
 * E2E: session-resume/1 over real TCP. The client's socket is destroyed in the middle of a
 * transfer in both directions (bytes in socket buffers are lost), the client reconnects on a
 * new socket and resumes, and both sides must receive every byte exactly once, in order.
 *
 * With LIBXUMUX_GO_DIR set, the same against libxumux-go in both directions
 * (tests/interop/go-resume-peer).
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import * as net from 'node:net';
import * as readline from 'node:readline';
import { spawn, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { createHash, randomBytes, randomInt } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { SessionResumeStore } from '../../src/resume.js';
import { SESSION_RESUME } from '../../src/codec/constants.js';
import { TcpAdapter } from '../../src/transports/tcp-adapter.js';
import type { XumuxChannel } from '../../src/channel.js';
import { XumuxResumeError } from '../../src/errors.js';
import { buildGoPeer, haveGo } from './helpers.js';

let tcpServer: net.Server;
let port: number;
const store = new SessionResumeStore({ resumeTimeout: 30 });
const opts = { pingInterval: 0, onChannelRequest: () => true as const };

/** Sessions the TCP server started (not resumed ones). */
const sessions: Array<{ server: XumuxServer; up: XumuxChannel; down: XumuxChannel }> = [];
let resumedCount = 0;

beforeAll(async () => {
  tcpServer = net.createServer((socket) => {
    socket.on('error', () => undefined);
    const server = new XumuxServer(new TcpAdapter(socket), opts);
    void server.accept({ sessionResume: store }).then(
      (r) => {
        if (r.resumed) {
          resumedCount++;
          return;
        }
        const [up, down] = r.channels as [XumuxChannel, XumuxChannel];
        sessions.push({ server, up, down });
      },
      () => undefined,
    );
  });
  await new Promise<void>((resolve) => tcpServer.listen(0, '127.0.0.1', resolve));
  port = (tcpServer.address() as AddressInfo).port;
});

afterAll(async () => {
  await new Promise<void>((resolve) => tcpServer.close(() => resolve()));
});

function dial(): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const s = net.createConnection({ port, host: '127.0.0.1' }, () => resolve(s));
    s.once('error', reject);
  });
}

/** Deterministic message `i` of a stream, 1 byte to ~150 KB (so many are fragmented). */
function message(seed: number, i: number): Uint8Array {
  const size = 1 + ((i * 2_654_435_761 + seed) % 150_000);
  const b = new Uint8Array(size);
  let x = (seed * 7919 + i) | 0;
  for (let k = 0; k < size; k++) {
    x = (x * 1_103_515_245 + 12_345) | 0;
    b[k] = x >>> 24;
  }
  return b;
}

/** Hash of every message received on `ch` until `count` have arrived. */
async function receiveAll(
  ch: XumuxChannel,
  count: number,
  onBytes?: (total: number) => void,
): Promise<{ digest: string; bytes: number }> {
  const h = createHash('sha256');
  let bytes = 0;
  for (let n = 0; n < count; n++) {
    const m = await ch.receive();
    if (!m) throw new Error(`channel ended after ${n} messages`);
    const len = new Uint8Array(4);
    new DataView(len.buffer).setUint32(0, m.payload.length);
    h.update(len).update(m.payload);
    bytes += m.payload.length;
    onBytes?.(bytes);
  }
  return { digest: h.digest('hex'), bytes };
}

function expected(seed: number, count: number): { digest: string; bytes: number } {
  const h = createHash('sha256');
  let bytes = 0;
  for (let i = 0; i < count; i++) {
    const m = message(seed, i);
    const len = new Uint8Array(4);
    new DataView(len.buffer).setUint32(0, m.length);
    h.update(len).update(m);
    bytes += m.length;
  }
  return { digest: h.digest('hex'), bytes };
}

describe('session-resume/1 over TCP', () => {
  it('delivers every byte exactly once in both directions across a dropped socket', async () => {
    const UP = 120;
    const DOWN = 120;
    let socket = await dial();
    const client = new XumuxClient(new TcpAdapter(socket), { pingInterval: 0 });
    const c = await client.connect({
      extensions: [SESSION_RESUME],
      channels: [
        { name: 'up', reliable: true, ordered: true },
        { name: 'down', reliable: true, ordered: true },
      ],
    });
    expect(c.extensions).toEqual([SESSION_RESUME]);
    const [cUp, cDown] = c.channels as [XumuxChannel, XumuxChannel];
    await new Promise<void>((r) => {
      const t = setInterval(() => {
        if (sessions.length === 1 && client.resumable) {
          clearInterval(t);
          r();
        }
      }, 5);
    });
    const { server, up: sUp, down: sDown } = sessions[0]!;

    // Drop the client's socket once both directions are well under way.
    let drops = 0;
    let upBytes = 0;
    let downBytes = 0;
    const maybeDrop = (): void => {
      if (drops === 0 && upBytes > 2_000_000 && downBytes > 2_000_000) {
        drops++;
        socket.destroy(); // abrupt: no CLOSE, unsent and unread bytes are lost
      }
    };
    const detached = new Promise<void>((r) =>
      client.addEventListener('detach', () => r(), { once: true }),
    );
    const upDone = receiveAll(sUp, UP, (n) => ((upBytes = n), maybeDrop()));
    const downDone = receiveAll(cDown, DOWN, (n) => ((downBytes = n), maybeDrop()));
    const sending = Promise.all([
      (async () => {
        for (let i = 0; i < UP; i++) await cUp.send(1, message(1, i));
      })(),
      (async () => {
        for (let i = 0; i < DOWN; i++) await sDown.send(2, message(2, i));
      })(),
    ]);

    await detached;
    expect(client.detached).toBe(true);
    socket = await dial();
    await client.resume(new TcpAdapter(socket));
    expect(client.detached).toBe(false);

    const [gotUp, gotDown] = await Promise.all([upDone, downDone, sending]);
    expect(drops).toBe(1);
    expect(gotUp).toEqual(expected(1, UP));
    expect(gotDown).toEqual(expected(2, DOWN));
    expect(resumedCount).toBe(1);

    // Still one connection: a new channel and a clean close.
    const extra = await client.openChannel({ name: 'extra', reliable: true, ordered: true });
    await extra.send(3, new TextEncoder().encode('after resume'));
    const closed = server.closed;
    expect((await client.close(1000, 'done')).code).toBe(1000);
    expect((await closed).code).toBe(1000);
  });
});

// ─── Interop with libxumux-go ──────────────────────────────────────────────────

/**
 * The bulk exchange of tests/interop/go-resume-peer on a channel named bulk:<bytes>: both
 * sides send <bytes> random bytes (type 7, then an empty 8), answer the peer's end with the
 * SHA-256 of what they received (9), and the peer's hash with "ok" or "mismatch" (10).
 * Resolves once both verdicts are "ok"; `onBytes` sees every received payload length.
 */
async function bulk(ch: XumuxChannel, onBytes: (n: number) => void = () => undefined) {
  const total = Number(ch.name.split(':')[1]);
  const enc = new TextEncoder();
  const dec = new TextDecoder();
  const recvHash = createHash('sha256');
  let received = 0;
  let mine: string | undefined;
  let theirs: string | undefined;
  const sending = (async () => {
    const h = createHash('sha256');
    for (let sent = 0; sent < total;) {
      let n = Math.min(1 + randomInt(70_000), total - sent);
      if (randomInt(10) === 0) n = Math.min(randomInt(64), total - sent);
      const b = randomBytes(n);
      h.update(b);
      await ch.send(7, new Uint8Array(b.buffer, b.byteOffset, n));
      sent += n;
    }
    await ch.send(8, new Uint8Array(0));
    return h.digest('hex');
  })();
  while (mine === undefined || theirs === undefined) {
    const m = await ch.receive();
    if (!m) throw new Error(`${ch.name}: channel ended`);
    if (m.type === 7) {
      recvHash.update(m.payload);
      received += m.payload.length;
      onBytes(m.payload.length);
    } else if (m.type === 8) {
      await ch.send(9, enc.encode(recvHash.digest('hex')));
    } else if (m.type === 9) {
      mine = dec.decode(m.payload) === (await sending) ? 'ok' : 'mismatch';
      await ch.send(10, enc.encode(mine));
    } else if (m.type === 10) {
      theirs = dec.decode(m.payload);
    } else throw new Error(`${ch.name}: unexpected type ${m.type}`);
  }
  expect({ mine, theirs, received }).toEqual({ mine: 'ok', theirs: 'ok', received: total });
}

/** Lines a child process prints on stdout, awaited one by one. */
function lines(child: ChildProcess): (match: (l: string) => boolean) => Promise<string> {
  const seen: string[] = [];
  const waiters: Array<() => void> = [];
  readline.createInterface({ input: child.stdout! }).on('line', (l) => {
    seen.push(l);
    for (const w of waiters.splice(0)) w();
  });
  return async (match) => {
    for (;;) {
      const i = seen.findIndex(match);
      if (i >= 0) return seen.splice(i, 1)[0]!;
      await new Promise<void>((r) => waiters.push(r));
    }
  };
}

async function until(cond: () => boolean, what: string): Promise<void> {
  for (let i = 0; !cond(); i++) {
    if (i > 2000) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

function dialPort(p: number): Promise<net.Socket> {
  return new Promise((resolve, reject) => {
    const s = net.createConnection({ port: p, host: '127.0.0.1' }, () => resolve(s));
    s.once('error', reject);
  });
}

describe.skipIf(!haveGo)('session-resume/1 interop with libxumux-go', () => {
  let bin = '';
  const children: ChildProcess[] = [];
  beforeAll(() => {
    bin = buildGoPeer('go-resume-peer');
  }, 120_000);
  afterAll(() => {
    for (const c of children) c.kill();
  });

  it(
    'TS client, Go server: drops mid-transfer, rotating tokens, a refused resume',
    { timeout: 120_000 },
    async () => {
      const child = spawn(bin, ['server'], { stdio: ['ignore', 'pipe', 'inherit'] });
      children.push(child);
      const next = lines(child);
      const goPort = Number((await next((l) => l.startsWith('port '))).split(' ')[1]);

      let socket = await dialPort(goPort);
      const client = new XumuxClient(new TcpAdapter(socket), opts);
      const c = await client.connect({
        extensions: [SESSION_RESUME],
        channels: [{ name: 'bulk:6000000', reliable: true, ordered: true }],
      });
      expect(c.extensions).toEqual([SESSION_RESUME]);
      await until(() => client.resumable, 'RESUME_TOKEN');
      let auto = true;
      let resumes = 0;
      client.addEventListener('detach', () => {
        if (!auto) return;
        void (async () => {
          for (;;) {
            try {
              socket = await dialPort(goPort);
              await client.resume(new TcpAdapter(socket));
              resumes++;
              return;
            } catch (e) {
              if (e instanceof XumuxResumeError && e.sessionLost) throw e;
              await new Promise((r) => setTimeout(r, 20));
            }
          }
        })();
      });

      // Requested properties do not matter on TCP: every channel is reliable and ordered, and
      // replayed (session-resume/1 §4).
      const chs = [c.channels[0]!];
      for (const [name, reliable, ordered] of [
        ['bulk:12000000', true, true],
        ['bulk:8000000', false, false],
      ] as const) {
        const ch = await client.openChannel({ name, reliable, ordered });
        expect(ch.properties).toMatchObject({ reliable: true, ordered: true });
        chs.push(ch);
      }
      // Drop the socket (abruptly: unsent and unread bytes are lost) as the bytes received
      // pass each mark, from the receive loop itself, so the transfer is certainly under way.
      const total = 26_000_000;
      const drops = 3;
      const droppedAt: number[] = [];
      let got = 0;
      const onBytes = (n: number): void => {
        got += n;
        const k = droppedAt.length;
        if (k < drops && resumes === k && got >= ((k + 1) * total) / (drops + 1)) {
          droppedAt.push(got);
          socket.destroy();
        }
      };
      await Promise.all(chs.map((ch) => bulk(ch, onBytes)));
      expect(droppedAt).toHaveLength(drops);
      for (const n of droppedAt) expect(n).toBeLessThan(total);
      expect(resumes).toBe(drops);
      for (let i = 0; i < chs.length; i++)
        expect(await next((l) => l.startsWith('bulk '))).toMatch(/^bulk ok /);

      // A refused resume: unknown token (4100), the session is lost, and HELLO works again.
      auto = false;
      (client as unknown as { rs: { token: string } }).rs.token = 'A'.repeat(44);
      const detached = once(client, 'detach');
      socket.destroy();
      await detached;
      socket = await dialPort(goPort);
      const err = await client.resume(new TcpAdapter(socket)).catch((e: unknown) => e);
      expect(err).toBeInstanceOf(XumuxResumeError);
      expect(err).toMatchObject({ code: 4100, sessionLost: true });
      expect(client.state).toBe('closed');

      const again = new XumuxClient(new TcpAdapter(await dialPort(goPort)), opts);
      await again.connect({ extensions: [SESSION_RESUME] });
      await bulk(await again.openChannel({ name: 'bulk:100000', reliable: true, ordered: true }));
      expect(await next((l) => l.startsWith('bulk '))).toMatch(/^bulk ok /);
      expect((await again.close(1000, 'done')).code).toBe(1000);
    },
  );

  it(
    'Go client, TS server: drops mid-transfer, rotating tokens',
    { timeout: 120_000 },
    async () => {
      const resumeStore = new SessionResumeStore({ resumeTimeout: 30 });
      const sockets: net.Socket[] = [];
      let session: XumuxServer | undefined;
      let resumed = 0;
      // Drop the socket from the receive loop as the bytes received pass each mark.
      const total = 26_000_000;
      const drops = 3;
      const droppedAt: number[] = [];
      let got = 0;
      const onBytes = (n: number): void => {
        got += n;
        const k = droppedAt.length;
        if (k < drops && resumed === k && got >= ((k + 1) * total) / (drops + 1)) {
          droppedAt.push(got);
          sockets.at(-1)!.destroy();
        }
      };
      const exchanges: Array<Promise<void>> = [];
      const run = (ch: XumuxChannel): void => {
        if (ch.name.startsWith('bulk:')) exchanges.push(bulk(ch, onBytes));
      };
      // Keepalive: a socket destroyed here may not be noticed by the Go client's kernel at once.
      const serverOpts = { ...opts, pingInterval: 1, pingTimeout: 1 };
      const tcp = net.createServer((sock) => {
        sock.on('error', () => undefined);
        sockets.push(sock);
        const server = new XumuxServer(new TcpAdapter(sock), serverOpts);
        void server.accept({ sessionResume: resumeStore }).then(
          (r) => {
            if (r.resumed) {
              resumed++;
              return;
            }
            session = server;
            server.addEventListener('channel', (e) => run((e as CustomEvent<XumuxChannel>).detail));
            for (const ch of r.channels) run(ch);
          },
          () => undefined,
        );
      });
      await new Promise<void>((r) => tcp.listen(0, '127.0.0.1', r));
      const tsPort = (tcp.address() as AddressInfo).port;
      const rotate = setInterval(() => {
        if (session?.state === 'open' && !session.detached) session.rotateResumeToken();
      }, 20);
      try {
        const child = spawn(bin, ['client', String(tsPort)], {
          stdio: ['ignore', 'inherit', 'inherit'],
        });
        children.push(child);
        const exited = once(child, 'exit');
        await until(() => exchanges.length === 3, 'three channels');
        await Promise.all(exchanges);
        expect(droppedAt).toHaveLength(drops);
        for (const n of droppedAt) expect(n).toBeLessThan(total);
        expect(resumed).toBe(drops);
        expect((await session!.close(1000, 'done')).code).toBe(1000);
        const [code] = await exited;
        expect(code).toBe(0);
      } finally {
        clearInterval(rotate);
        await new Promise<void>((r) => tcp.close(() => r()));
      }
    },
  );
});
