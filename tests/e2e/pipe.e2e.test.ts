/**
 * E2E tests for pipe/1: xumux over real TCP, a pipe forwarded to a TCP target, TLS over a
 * pipe, and (with LIBXUMUX_GO_DIR set) interop with libxumux-go's pipe package.
 */
import { describe, it, expect, afterEach } from 'vitest';
import * as net from 'node:net';
import * as tls from 'node:tls';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { once } from 'node:events';
import { pipeline } from 'node:stream/promises';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { TcpAdapter } from '../../src/transports/tcp-adapter.js';
import { StdioAdapter } from '../../src/transports/stdio-adapter.js';
import { CloseCode } from '../../src/codec/constants.js';
import { buildGoPeer, haveGo } from './helpers.js';
import {
  dialPipe,
  PipeListener,
  PipeResetError,
  pipeTarget,
  TARGET_UNREACHABLE,
  type PipeSocket,
} from '../../src/pipe/index.js';

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0).reverse()) await c();
});

async function listen(server: net.Server): Promise<number> {
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
  cleanups.push(() => new Promise((r) => server.close(r)));
  return (server.address() as AddressInfo).port;
}

function collect(s: NodeJS.ReadableStream): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const parts: Buffer[] = [];
    s.on('data', (d: Buffer) => parts.push(d));
    s.once('end', () => resolve(Buffer.concat(parts)));
    s.once('error', reject);
  });
}

/**
 * A xumux server on TCP that forwards each pipe to its target (`tcp:host:port`), as a pipe/1
 * responder does: connect first, then accept or refuse with 4500.
 */
async function forwarder(allowedPort: number) {
  const listener = new PipeListener({
    check: async (req) => {
      const t = pipeTarget(req);
      if (t !== `tcp:127.0.0.1:${allowedPort}` && t !== 'tcp:127.0.0.1:1')
        return { code: CloseCode.CHANNEL_REFUSED, reason: 'target not allowed' };
      const port = Number(t.split(':')[2]);
      const ok = await new Promise<boolean>((resolve) => {
        const probe = net.connect(port, '127.0.0.1', () => {
          probe.destroy();
          resolve(true);
        });
        probe.on('error', () => resolve(false));
      });
      return ok || { code: TARGET_UNREACHABLE, reason: 'connection refused' };
    },
  });
  listener.on('connection', (p) => {
    const port = Number(p.target!.split(':')[2]);
    const sock = net.connect(port, '127.0.0.1');
    // Socket end -> EOF, EOF -> socket end (half-close both ways).
    void pipeline(p, sock).catch(() => undefined);
    void pipeline(sock, p).catch(() => undefined);
  });
  const tcp = net.createServer((s) => {
    const server = new XumuxServer(new TcpAdapter(s), {
      pingInterval: 0,
      onChannelRequest: listener.policy,
    });
    listener.attach(server);
    void server.accept().catch(() => undefined);
  });
  const port = await listen(tcp);
  const sock = net.connect(port, '127.0.0.1');
  await once(sock, 'connect');
  const client = new XumuxClient(new TcpAdapter(sock), { pingInterval: 0 });
  await client.connect();
  cleanups.push(() => client.close());
  cleanups.push(() => listener.close());
  return client;
}

const haveOpenssl = spawnSync('openssl', ['version']).status === 0;

describe('pipe/1 over TCP', () => {
  it('forwards a pipe to a TCP target, with half-close in both directions', async () => {
    // Target: hashes what it receives until EOF, then answers with the hash and closes.
    const target = net.createServer((s) => {
      const h = createHash('sha256');
      s.on('data', (d: Buffer) => h.update(d));
      s.on('end', () => s.end(h.digest('hex')));
    });
    const targetPort = await listen(target);
    const client = await forwarder(targetPort);

    const p = await dialPipe(client, { name: 'fwd', target: `tcp:127.0.0.1:${targetPort}` });
    const body = randomBytes(5 << 20);
    const reply = collect(p);
    p.end(body);
    expect((await reply).toString()).toBe(createHash('sha256').update(body).digest('hex'));
    expect((await p.channel.closed).code).toBe(CloseCode.NORMAL);
  });

  it('refuses targets that are not allowed (4010) or cannot be reached (4500)', async () => {
    const client = await forwarder(65000);
    await expect(dialPipe(client, { target: 'tcp:10.0.0.1:22' })).rejects.toMatchObject({
      code: 4010,
    });
    await expect(dialPipe(client, { target: 'tcp:127.0.0.1:1' })).rejects.toMatchObject({
      code: TARGET_UNREACHABLE,
    });
  });

  it.skipIf(!haveOpenssl)('runs TLS over a pipe', async () => {
    const dir = mkdtempSync(path.join(tmpdir(), 'xumux-tls-'));
    const key = path.join(dir, 'key.pem');
    const cert = path.join(dir, 'cert.pem');
    const gen = spawnSync('openssl', [
      'req',
      '-x509',
      '-newkey',
      'ec',
      '-pkeyopt',
      'ec_paramgen_curve:prime256v1',
      '-nodes',
      '-keyout',
      key,
      '-out',
      cert,
      '-days',
      '1',
      '-subj',
      '/CN=xumux.test',
      '-addext',
      'subjectAltName=DNS:xumux.test',
    ]);
    expect(gen.status).toBe(0);
    const creds = { key: readFileSync(key), cert: readFileSync(cert) };

    const listener = new PipeListener();
    const tcp = net.createServer((s) => {
      const server = new XumuxServer(new TcpAdapter(s), {
        pingInterval: 0,
        onChannelRequest: listener.policy,
      });
      listener.attach(server);
      void server.accept().catch(() => undefined);
    });
    const port = await listen(tcp);
    // TLS server on each pipe: echoes upper-cased.
    listener.on('connection', (p: PipeSocket) => {
      const t = new tls.TLSSocket(p, { isServer: true, ...creds });
      t.on('data', (d: Buffer) => t.write(d.toString().toUpperCase()));
      t.on('end', () => t.end());
      t.on('error', () => undefined);
    });
    const sock = net.connect(port, '127.0.0.1');
    await once(sock, 'connect');
    const client = new XumuxClient(new TcpAdapter(sock), { pingInterval: 0 });
    await client.connect();
    cleanups.push(() => client.close());
    cleanups.push(() => listener.close());

    const p = await dialPipe(client, { name: 'tls' });
    const t = tls.connect({ socket: p, servername: 'xumux.test', ca: creds.cert });
    await once(t, 'secureConnect');
    expect(t.authorized).toBe(true);
    const reply = collect(t);
    t.end('hello over tls');
    expect((await reply).toString()).toBe('HELLO OVER TLS');
  });
});

// ─── Interop with libxumux-go ──────────────────────────────────────────────────

describe.skipIf(!haveGo)('pipe/1 interop with libxumux-go', () => {
  it(
    'half-closes, echoes, resets and accepts pipes opened by Go',
    { timeout: 120_000 },
    async () => {
      const bin = buildGoPeer('go-pipe-peer');
      const child: ChildProcess = spawn(bin, [], { stdio: ['pipe', 'pipe', 'inherit'] });
      const exited = once(child, 'exit');
      const listener = new PipeListener();
      const client = new XumuxClient(new StdioAdapter(child.stdout!, child.stdin!), {
        pingInterval: 0,
        onChannelRequest: listener.policy,
      });
      listener.attach(client);
      await client.connect();

      // TS -> Go echo, 1 MiB, ended with EOF; Go echoes and half-closes; channel closes 1000.
      const echo = await dialPipe(client, { name: 'e', target: 'echo' });
      const body = randomBytes(1 << 20);
      const back = collect(echo);
      echo.end(body);
      expect((await back).equals(body)).toBe(true);
      expect((await echo.channel.closed).code).toBe(CloseCode.NORMAL);

      // Go RESET reaches TS as a PipeResetError.
      const r = await dialPipe(client, { name: 'r', target: 'reset' });
      const got = collect(r).catch((e: unknown) => e);
      r.write('hello');
      const err = await got;
      expect(err).toBeInstanceOf(PipeResetError);
      expect(err).toMatchObject({ code: 4501, reason: 'boom', remote: true });

      // Go opens a pipe to TS, which echoes it; Go checks the bytes.
      listener.on('connection', (p) => void pipeline(p, p).catch(() => undefined));
      const db = await dialPipe(client, { name: 'd', target: 'dial-back' });
      const verdict = collect(db);
      db.end();
      expect((await verdict).toString()).toBe('ok');

      await client.close();
      const [code] = await exited;
      expect(code).toBe(0);
    },
  );
});
