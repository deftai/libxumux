/**
 * E2E: session-resume/1 over real TCP. The client's socket is destroyed in the middle of a
 * transfer in both directions (bytes in socket buffers are lost), the client reconnects on a
 * new socket and resumes, and both sides must receive every byte exactly once, in order.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import * as net from 'node:net';
import { createHash } from 'node:crypto';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { SessionResumeStore } from '../../src/resume.js';
import { SESSION_RESUME } from '../../src/codec/constants.js';
import { TcpAdapter } from '../../src/transports/tcp-adapter.js';
import type { XumuxChannel } from '../../src/channel.js';

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
    void server.accept({ sessionResume: store }).then((r) => {
      if (r.resumed) {
        resumedCount++;
        return;
      }
      const [up, down] = r.channels as [XumuxChannel, XumuxChannel];
      sessions.push({ server, up, down });
    }, () => undefined);
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
