import { describe, it, expect, afterEach } from 'vitest';
import { once } from 'node:events';
import http from 'node:http';
import { createHash, randomBytes } from 'node:crypto';
import { Readable, Writable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { XumuxClient, XumuxServer, type XumuxOptions } from '../session.js';
import { createInMemoryPair } from '../transports/in-memory-adapter.js';
import { CloseCode, DEFAULT_INITIAL_WINDOW } from '../codec/constants.js';
import { XumuxClosedError } from '../errors.js';
import type { XumuxChannel } from '../channel.js';
import type { TransportAdapter } from '../types.js';
import {
  checkPipeRequest,
  dialPipe,
  PIPE_PROFILE,
  PipeListener,
  PipeMessageType,
  PipeResetError,
  PipeSocket,
  pipeRequest,
  pipeTarget,
  type PipeListenerOptions,
} from './index.js';

const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 300 };
const later = (ms: number) => new Promise((r) => setTimeout(r, ms));
const te = new TextEncoder();

/** A transport whose sends can be held, like one whose peer has stopped reading. */
function stallable(t: TransportAdapter) {
  let gate: Promise<void> | null = null;
  let open: () => void = () => undefined;
  const adapter: TransportAdapter = {
    kind: t.kind,
    magic: t.magic,
    send: async (d) => {
      const copy = new Uint8Array(d);
      while (gate) await gate;
      await t.send(copy);
    },
    receive: () => t.receive(),
    close: () => t.close(),
  };
  return {
    adapter,
    stall: () => {
      gate = new Promise((r) => (open = r));
    },
    resume: () => {
      gate = null;
      open();
    },
  };
}

const cleanups: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of cleanups.splice(0)) await c();
});

/** A client session, and a server session whose pipes go to a listener. */
async function sessions(
  lopts: PipeListenerOptions = {},
  sopts: XumuxOptions = {},
  stallServer = false,
) {
  const [ct, st] = createInMemoryPair('stream');
  const s = stallable(st);
  const listener = new PipeListener(lopts);
  const client = new XumuxClient(ct, base);
  const server = new XumuxServer(stallServer ? s.adapter : st, {
    ...base,
    ...sopts,
    onChannelRequest: listener.policy,
  });
  listener.attach(server);
  await Promise.all([client.connect(), server.accept()]);
  cleanups.push(async () => {
    listener.close();
    s.resume();
    await Promise.all([client.close(), server.close()]);
  });
  return { client, server, listener, stall: s.stall, resume: s.resume };
}

/** Both ends of one pipe: a dialed from the client, b accepted on the server. */
async function pipePair(sopts: XumuxOptions = {}) {
  const ss = await sessions({}, sopts);
  const [a, b] = (
    await Promise.all([dialPipe(ss.client, { name: 'test' }), ss.listener.accept()])
  ).map(quiet) as [PipeSocket, PipeSocket];
  cleanups.push(() => {
    a.destroy();
    b.destroy();
  });
  return { ...ss, a, b };
}

/**
 * Read a socket to its end: the bytes, and the error that ended it, if any. (Not for-await:
 * a finished async iterator destroys a duplex whose writable side is still open.)
 */
function drain(s: PipeSocket): Promise<{ data: Buffer; err?: Error }> {
  return new Promise((resolve) => {
    const parts: Buffer[] = [];
    s.on('data', (d: Buffer) => parts.push(d));
    s.once('end', () => resolve({ data: Buffer.concat(parts) }));
    s.once('error', (err) => resolve({ data: Buffer.concat(parts), err }));
  });
}

/** Ignore a socket's 'error' event (a test that checks the error another way). */
const quiet = <T extends PipeSocket>(s: T): T => s.on('error', () => undefined);

const write = (s: PipeSocket, d: string | Uint8Array) =>
  new Promise<void>((res, rej) => s.write(d, (e) => (e ? rej(e) : res())));

const closedWithin = (ch: XumuxChannel, ms: number) =>
  Promise.race([ch.closed, later(ms).then(() => null)]);

describe('pipe/1 requests', () => {
  it('builds and checks requests', () => {
    const req = { id: 1, ...pipeRequest('ssh', 'tcp:localhost:22') };
    expect(req).toMatchObject({
      reliable: true,
      ordered: true,
      metadata: { profile: PIPE_PROFILE, target: 'tcp:localhost:22' },
    });
    expect(pipeTarget(req)).toBe('tcp:localhost:22');
    expect(pipeRequest('x').metadata).toEqual({ profile: 'pipe/1' });
    expect(checkPipeRequest(req)).toBe(true);
    expect(checkPipeRequest({ ...req, ordered: false })).toMatchObject({ code: 4010 });
    expect(checkPipeRequest({ ...req, metadata: { profile: 'other/1' } })).toMatchObject({
      code: 4010,
      reason: 'unsupported profile "other/1"',
    });
    expect(checkPipeRequest({ ...req, metadata: undefined })).toMatchObject({ code: 4010 });
    expect(pipeTarget({ metadata: { target: 3 } })).toBeUndefined();
  });
});

describe('PipeSocket', () => {
  it('half-closes, keeps reading after EOF, and closes the channel after both EOFs', async () => {
    const { a, b } = await pipePair();
    a.end('ping');
    expect((await drain(b)).data.toString()).toBe('ping');
    expect(a.writableFinished).toBe(true);

    // b can still answer after a's EOF.
    b.end('pong');
    expect((await drain(a)).data.toString()).toBe('pong');

    for (const s of [a, b]) {
      const info = await closedWithin(s.channel, 2000);
      expect(info?.code).toBe(CloseCode.NORMAL);
    }
    // Ended both ways: autoDestroy.
    if (!a.closed) await once(a, 'close');
    expect(a.destroyed).toBe(true);
  });

  it('does not close the channel after only one EOF', async () => {
    const { a, b } = await pipePair();
    a.end();
    await drain(b);
    expect(await closedWithin(b.channel, 100)).toBeNull();
    await write(b, 'still open');
    expect((await new Promise<Buffer>((r) => a.once('data', r))).toString()).toBe('still open');
  });

  it('applies backpressure at exactly the peer window, returning credit as bytes are read', async () => {
    const { a, b } = await pipePair({ maxChannelWindow: -1 });
    let done = false;
    const big = randomBytes(4 << 20);
    a.write(big, () => (done = true));
    await later(100);
    expect(done).toBe(false);
    expect(b.readableLength).toBe(DEFAULT_INITIAL_WINDOW); // nobody is reading b

    // Reading 1000 bytes lets exactly 1000 more in.
    expect((b.read(1000) as Buffer).equals(big.subarray(0, 1000))).toBe(true);
    await later(50);
    expect(b.readableLength).toBe(DEFAULT_INITIAL_WINDOW);

    // Reading everything lets the writer finish.
    const hash = createHash('sha256');
    hash.update(big.subarray(0, 1000));
    b.on('data', (d: Buffer) => hash.update(d));
    a.end();
    await once(b, 'end');
    expect(done).toBe(true);
    expect(hash.digest('hex')).toBe(createHash('sha256').update(big).digest('hex'));
  });

  it('with pipe(), a stalled destination stops the writer at the window', async () => {
    const { a, b } = await pipePair({ maxChannelWindow: -1 });
    // A destination that takes one chunk and then never completes the write.
    const taken: Buffer[] = [];
    let release: () => void = () => undefined;
    const slow = new Writable({
      highWaterMark: 1,
      write(chunk: Buffer, _e, cb) {
        taken.push(chunk);
        release = cb;
      },
    });
    b.pipe(slow);
    let done = false;
    a.write(Buffer.alloc(4 << 20), () => (done = true));
    await later(100);
    // Credit went back for the chunk the destination took, and no more.
    expect(taken).toHaveLength(1);
    expect(b.readableLength).toBe(DEFAULT_INITIAL_WINDOW);
    expect(done).toBe(false);
    slow.on('drain', () => undefined);
    b.unpipe(slow);
    release();
    b.resume();
    a.end();
    await once(b, 'end');
    expect(done).toBe(true);
  });

  it('runs stream.pipeline both ways (an echo)', async () => {
    const { a, b } = await pipePair();
    const echo = pipeline(b, b);
    const input = randomBytes(1 << 20);
    const chunks = Array.from({ length: 64 }, (_, i) => input.subarray(i * 16384, (i + 1) * 16384));
    const [, got] = await Promise.all([pipeline(Readable.from(chunks), a), drain(a), echo]);
    expect(got.err).toBeUndefined();
    expect(got.data.equals(input)).toBe(true);
  });

  it('writeTo() returns credit only once the destination has written each chunk', async () => {
    const { a, b } = await pipePair({ maxChannelWindow: -1 });
    const taken: Buffer[] = [];
    let release: () => void = () => undefined;
    let stalled = true;
    const slow = new Writable({
      write(chunk: Buffer, _e, cb) {
        taken.push(chunk);
        if (stalled) release = cb;
        else cb();
      },
    });
    const copied = b.writeTo(slow);
    const big = randomBytes(4 << 20);
    let done = false;
    a.write(big, () => (done = true));
    await later(100);
    // The destination took one chunk and has not written it: no credit came back for it.
    expect(taken).toHaveLength(1);
    expect(b.readableLength).toBe(DEFAULT_INITIAL_WINDOW - taken[0]!.length);
    expect(done).toBe(false);

    stalled = false;
    release();
    a.end();
    expect(await copied).toBe(big.length);
    expect(Buffer.concat(taken).equals(big)).toBe(true);
    expect(done).toBe(true);
  });

  it('writeTo() itself echoes, then end() completes the half-close', async () => {
    const { a, b } = await pipePair();
    const echo = b.writeTo(b).then((n) => (b.end(), n));
    const input = randomBytes(1 << 20);
    a.end(input);
    const got = await drain(a);
    expect(got.err).toBeUndefined();
    expect(got.data.equals(input)).toBe(true);
    expect(await echo).toBe(input.length);
  });

  it("writeTo() rejects with the peer's RESET, and refuses a socket with an encoding", async () => {
    const { a, b } = await pipePair();
    const copied = b.writeTo(new Writable({ write: (_c, _e, cb) => cb() }));
    await write(a, 'x');
    a.destroy(new PipeResetError(4600, 'gone'));
    await expect(copied).rejects.toBeInstanceOf(PipeResetError);
    const { b: c } = await pipePair();
    c.setEncoding('utf8');
    await expect(c.writeTo(new Writable())).rejects.toThrow(/encoding/);
  });

  it('never sends DATA larger than one frame, even for big writes', async () => {
    const { a, b } = await pipePair();
    const sizes: number[] = [];
    // Observe the messages b's channel receives, through the socket's own reads.
    const orig = b.channel.receive.bind(b.channel);
    b.channel.receive = async () => {
      const m = await orig();
      if (m) sizes.push(m.payload.length);
      return m;
    };
    a.end(Buffer.alloc(200_000));
    expect((await drain(b)).data.length).toBe(200_000);
    expect(Math.max(...sizes)).toBeLessThanOrEqual(16384);
  });

  it('delivers data written before a RESET, then the reset as a typed error', async () => {
    const { a, b } = await pipePair();
    await write(a, 'partial');
    a.resetAndDestroy(CloseCode.GOING_AWAY, 'upstream died');
    // b isn't reading yet: the RESET arrives and closes the channel while 'partial' is buffered.
    expect((await closedWithin(b.channel, 2000))?.code).toBe(CloseCode.GOING_AWAY);
    expect(b.destroyed).toBe(false);
    const { data, err } = await drain(b);
    expect(data.toString()).toBe('partial');
    expect(err).toBeInstanceOf(PipeResetError);
    expect(err).toMatchObject({ code: 1001, reason: 'upstream died', remote: true });
    expect(a.destroyed).toBe(true);
  });

  it('sends a RESET whole even with little credit left', async () => {
    const { a, b } = await pipePair({ maxChannelWindow: -1 });
    await write(a, Buffer.alloc(DEFAULT_INITIAL_WINDOW - 5)); // b isn't reading
    const reason = 'r'.repeat(500);
    a.resetAndDestroy(4501, reason);
    await later(100); // let the RESET take the 5 bytes of credit left, if it would
    const { data, err } = await drain(b); // reading returns credit, so the RESET can go
    expect(data.length).toBe(DEFAULT_INITIAL_WINDOW - 5);
    expect(err).toMatchObject({ name: 'PipeResetError', code: 4501, reason });
  });

  it('destroy(err) sends a RESET with the error message', async () => {
    const { a, b } = await pipePair();
    const errored = once(a, 'error');
    const got = drain(b);
    a.destroy(new Error('disk on fire'));
    expect((await errored)[0]).toMatchObject({ message: 'disk on fire' });
    expect((await got).err).toMatchObject({ code: 1001, reason: 'disk on fire' });
    // A PipeResetError keeps its code, so a relay can pass a RESET along.
    const p2 = await pipePair();
    const got2 = drain(p2.b);
    p2.a.destroy(new PipeResetError(4500, 'refused'));
    expect((await got2).err).toMatchObject({ code: 4500, reason: 'refused' });
  });

  it('destroy() sends EOF and closes the channel, like closing a socket', async () => {
    const { a, b } = await pipePair();
    await write(a, 'bye');
    a.destroy();
    const { data, err } = await drain(b);
    expect(err).toBeUndefined();
    expect(data.toString()).toBe('bye');
    expect((await a.channel.closed).code).toBe(CloseCode.NORMAL);
    // b's writes now fail: the channel is gone.
    const e = await write(b, 'x').catch((e: unknown) => e);
    expect(e).toBeInstanceOf(Error);
  });

  it('a message type pipe/1 does not define closes the channel with 1002', async () => {
    const { client, listener } = await sessions();
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    await raw.send(0x09, te.encode('?'));
    const { err } = await drain(b);
    expect(err?.message).toBe('unknown pipe/1 message type 0x9');
    expect((await raw.closed).code).toBe(CloseCode.PROTOCOL_ERROR);
  });

  it('treats DATA or EOF after EOF as a protocol error', async () => {
    for (const type of [PipeMessageType.DATA, PipeMessageType.EOF]) {
      const { client, listener } = await sessions();
      const raw = await client.openChannel(pipeRequest('raw'));
      const b = await listener.accept();
      await raw.send(PipeMessageType.EOF, new Uint8Array(0));
      await raw.send(type, te.encode('x'));
      const { err } = await drain(b).then(async (r) => (r.err ? r : await errorOf(b)));
      void quiet(b);
      expect(err?.message).toContain('after EOF');
      expect((await raw.closed).code).toBe(CloseCode.PROTOCOL_ERROR);
    }
  });

  it('a failed pipe releases a blocked writer and closes at once', async () => {
    const { client, listener } = await sessions({}, { closeTimeoutMs: 30_000 });
    const raw = await client.openChannel(pipeRequest('raw')); // never reads: no credit for b
    const b = quiet(await listener.accept());
    const wrote = write(b, Buffer.alloc(4 << 20)).catch((e: unknown) => e);
    await later(50); // the write is now waiting for credit
    await raw.send(0x09, new Uint8Array(0));
    expect((await closedWithin(b.channel, 2000))?.code).toBe(CloseCode.PROTOCOL_ERROR);
    const e = await Promise.race([wrote, later(2000).then(() => 'still blocked')]);
    expect(e).toMatchObject({ message: 'unknown pipe/1 message type 0x9' });
  });

  it('a failed pipe with unread data still closes at once, and fails the write once read', async () => {
    const { client, listener } = await sessions({}, { closeTimeoutMs: 30_000 });
    const raw = await client.openChannel(pipeRequest('raw')); // never reads: no credit for b
    const b = quiet(await listener.accept());
    let wrote: unknown = 'pending';
    b.write(Buffer.alloc(4 << 20), (e) => (wrote = e));
    await later(50);
    await raw.send(PipeMessageType.DATA, te.encode('unread'));
    await raw.send(0x09, new Uint8Array(0));
    // The blocked write must not hold up the CLOSE_CHANNEL, though b hasn't read yet.
    expect((await closedWithin(b.channel, 2000))?.code).toBe(CloseCode.PROTOCOL_ERROR);
    expect(wrote).toBe('pending');
    const { data, err } = await drain(b);
    expect(data.toString()).toBe('unread');
    expect(err?.message).toBe('unknown pipe/1 message type 0x9');
    expect(wrote).toMatchObject({ message: 'unknown pipe/1 message type 0x9' });
  });

  it('closes the channel after both EOFs even before the consumer reads them', async () => {
    // EOF received after ours was sent, then EOF sent after theirs arrived: b closes the
    // channel itself both times (raw, the other end, never closes it).
    for (const bFirst of [true, false]) {
      const { client, listener } = await sessions();
      const raw = await client.openChannel(pipeRequest('raw'));
      const b = await listener.accept();
      if (bFirst) {
        b.end();
        await once(b, 'finish');
      }
      await raw.send(PipeMessageType.DATA, te.encode('unread'));
      await raw.send(PipeMessageType.EOF, new Uint8Array(0));
      await later(20);
      if (!bFirst) b.end();
      expect((await closedWithin(raw, 1000))?.code).toBe(CloseCode.NORMAL);
      expect(b.readableLength).toBe(6); // nobody read: not autoDestroy's doing
    }
  });

  it('a RESET received while a write is blocked fails the write once read', async () => {
    const { a, b } = await pipePair({ closeTimeoutMs: 30_000 });
    const wrote = write(b, Buffer.alloc(4 << 20)).catch((e: unknown) => e);
    await later(50);
    a.resetAndDestroy(4501, 'gone');
    expect((await drain(b)).err).toMatchObject({ code: 4501 });
    expect(await wrote).toMatchObject({ code: 4501 });
  });

  it('sends one EOF, and nothing after it, when destroyed while the EOF is queued', async () => {
    const { client, listener, stall, resume } = await sessions({}, {}, true);
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    stall();
    b.end();
    await later(20);
    b.destroy();
    resume();
    const types: number[] = [];
    for await (const m of raw) types.push(m.type);
    expect(types).toEqual([PipeMessageType.EOF]);
  });

  it('sends EOF when end() and destroy() come in the same turn', async () => {
    const { client, listener } = await sessions();
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    b.end();
    b.destroy();
    const types: number[] = [];
    for await (const m of raw) types.push(m.type);
    expect(types).toEqual([PipeMessageType.EOF]);
  });

  it('ends the readable side when the channel closes without an EOF', async () => {
    const { client, listener } = await sessions();
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    await raw.send(PipeMessageType.DATA, te.encode('last'));
    await raw.close();
    const { data, err } = await drain(b);
    expect(err).toBeUndefined();
    expect(data.toString()).toBe('last');
  });

  it('fails with the close error when the channel closes abnormally', async () => {
    const { client, listener } = await sessions();
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    await raw.close(4321, 'boom');
    const { err } = await drain(b);
    expect(err).toBeInstanceOf(XumuxClosedError);
    expect(err).toMatchObject({ code: 4321 });
  });

  it('reads a RESET with a malformed body as code 0', async () => {
    const { client, listener } = await sessions();
    const raw = await client.openChannel(pipeRequest('raw'));
    const b = await listener.accept();
    await raw.send(PipeMessageType.RESET, te.encode('not json'));
    expect((await drain(b)).err).toMatchObject({ code: 0, reason: undefined });
    const raw2 = await client.openChannel(pipeRequest('raw'));
    const b2 = await listener.accept();
    await raw2.send(PipeMessageType.RESET, te.encode('{"code":"x","reason":7}'));
    expect((await drain(b2)).err).toMatchObject({ code: 0, reason: undefined });
  });

  it('caps the RESET reason at 1024 bytes', async () => {
    const { a, b } = await pipePair();
    a.resetAndDestroy(4501, 'x'.repeat(5000));
    expect(((await drain(b)).err as PipeResetError).reason).toHaveLength(1024);
  });

  it('caps the reason in UTF-8 bytes, at a character boundary', async () => {
    const { a, b } = await pipePair();
    a.resetAndDestroy(4501, 'é'.repeat(1000)); // 2000 bytes
    const reason = ((await drain(b)).err as PipeResetError).reason!;
    expect(reason).toBe('é'.repeat(512));
  });

  it('keeps a RESET within the smallest maxMessageSize, however the reason escapes', async () => {
    // A peer may declare maxMessageSize 4096; 1024 NULs escape to over 6 KiB of JSON.
    const ss = await sessions(
      {},
      { maxFramePayload: 4096, maxMessageSize: 4096, initialWindow: 4096 },
    );
    const [a, b] = await Promise.all([dialPipe(ss.client), ss.listener.accept()]);
    quiet(a);
    a.resetAndDestroy(4501, '\0'.repeat(1024));
    const { err } = await drain(b);
    expect(err).toMatchObject({ code: 4501 });
    expect((err as PipeResetError).reason).toMatch(/^\0+$/);
  });

  it("emits 'timeout' when idle, like net.Socket", async () => {
    const { a } = await pipePair();
    expect(a.setNoDelay(true).setKeepAlive(true).ref().unref()).toBe(a);
    let fired = 0;
    const cb = () => fired++;
    a.setTimeout(30, cb);
    await later(80);
    expect(fired).toBe(1);
    a.setTimeout(0, cb);
    await later(60);
    expect(fired).toBe(1);
  });

  it('exposes the target', async () => {
    const { client, listener } = await sessions();
    const [a, b] = await Promise.all([
      dialPipe(client, { target: 'tcp:db:5432' }),
      listener.accept(),
    ]);
    expect(a.target).toBe('tcp:db:5432');
    expect(b.target).toBe('tcp:db:5432');
    expect(b.channel.name).toBe('pipe');
    a.destroy();
    b.destroy();
  });

  it('with allowHalfOpen false, the peer EOF ends this side too', async () => {
    const ss = await sessions({ socket: { allowHalfOpen: false } });
    const [a, b] = await Promise.all([dialPipe(ss.client), ss.listener.accept()]);
    a.end('x');
    await drain(b);
    await once(b, 'finish');
    expect((await drain(a)).data.length).toBe(0);
    expect((await a.channel.closed).code).toBe(CloseCode.NORMAL);
  });
});

/** The error a socket was destroyed with (after its readable side ended cleanly). */
async function errorOf(s: PipeSocket): Promise<{ err?: Error }> {
  if (s.errored) return { err: s.errored };
  const [err] = (await once(s, 'error')) as [Error];
  return { err };
}

describe('PipeListener', () => {
  it('checks profile, properties and target before accepting', async () => {
    const { client, listener } = await sessions({
      check: (req) =>
        pipeTarget(req) === 'tcp:allowed:1' || { code: 4010, reason: 'target not allowed' },
    });
    const refused = await dialPipe(client, { target: 'tcp:elsewhere:1' }).catch((e: unknown) => e);
    expect(refused).toMatchObject({ code: 4010, reason: 'target not allowed' });

    const other = await client
      .openChannel({ name: 'x', reliable: true, ordered: true, metadata: { profile: 'other/1' } })
      .catch((e: unknown) => e);
    expect(other).toMatchObject({ code: 4010, reason: 'unsupported profile "other/1"' });

    const unordered = await client
      .openChannel({ ...pipeRequest('x', 'tcp:allowed:1'), ordered: false })
      .catch((e: unknown) => e);
    expect(unordered).toMatchObject({ code: 4010 });

    const [c, s] = await Promise.all([
      dialPipe(client, { target: 'tcp:allowed:1' }),
      listener.accept(),
    ]);
    expect(s.target).toBe('tcp:allowed:1');
    c.destroy();
  });

  it('a RESET before accept() does not crash; the accepted socket shows it', async () => {
    const { client, listener } = await sessions();
    const a = await dialPipe(client);
    a.resetAndDestroy(4501, 'early');
    await later(30); // the RESET arrives and destroys the backlogged socket meanwhile
    const s = await listener.accept();
    expect(s.destroyed).toBe(true);
    expect(s.errored).toMatchObject({ code: 4501, reason: 'early' });
  });

  it('refuses with 4010 when check throws', async () => {
    const { client } = await sessions({
      check: () => {
        throw new Error('no');
      },
    });
    expect(await dialPipe(client).catch((e: unknown) => e)).toMatchObject({
      code: 4010,
      reason: 'no',
    });
  });

  it("hands pipes to 'connection' listeners, or to for-await", async () => {
    const { client, listener } = await sessions();
    const viaIter = (async () => {
      for await (const s of listener) return s;
    })();
    const a = await dialPipe(client, { name: 'one' });
    expect((await viaIter)!.channel.name).toBe('one');

    const got = once(listener, 'connection');
    await dialPipe(client, { name: 'two' });
    expect(((await got)[0] as PipeSocket).channel.name).toBe('two');
    a.destroy();
  });

  it('ignores channels of other profiles that the session accepts', async () => {
    const [ct, st] = createInMemoryPair('stream');
    const listener = new PipeListener();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, {
      ...base,
      onChannelRequest: (req) =>
        req.metadata?.profile === PIPE_PROFILE ? listener.policy(req) : true,
    });
    const detach = listener.attach(server);
    await Promise.all([client.connect(), server.accept()]);
    const others: XumuxChannel[] = [];
    server.addEventListener('channel', (e) => {
      const ch = (e as CustomEvent<XumuxChannel>).detail;
      if (ch.metadata?.profile !== PIPE_PROFILE) others.push(ch);
    });
    await client.openChannel({ name: 'plain', reliable: true, ordered: true });
    await dialPipe(client, { name: 'p' });
    expect((await listener.accept()).channel.name).toBe('p');
    expect(others.map((c) => c.name)).toEqual(['plain']);

    detach();
    await dialPipe(client, { name: 'after-detach' }); // accepted, but no longer ours
    const pending = listener.accept();
    expect(await Promise.race([pending, later(50).then(() => 'none')])).toBe('none');
    listener.close();
    await expect(pending).rejects.toMatchObject({ code: 'ERR_SERVER_NOT_RUNNING' });
    await Promise.all([client.close(), server.close()]);
  });

  it('close() refuses new pipes, closes unaccepted ones with 1001, and ends accept()', async () => {
    const { client, listener } = await sessions();
    const waiting = quiet(await dialPipe(client, { name: 'unaccepted' }));
    await later(20);
    let closedEvent = false;
    listener.on('close', () => (closedEvent = true));
    listener.close();
    listener.close(); // harmless
    expect(closedEvent).toBe(true);
    expect(listener.listening).toBe(false);
    expect((await waiting.channel.closed).code).toBe(CloseCode.GOING_AWAY);
    expect(await dialPipe(client).catch((e: unknown) => e)).toMatchObject({
      code: 4010,
      reason: 'not accepting channels',
    });
    await expect(listener.accept()).rejects.toThrow('pipe listener closed');
  });

  it('refuses a request whose check was still running when close() came', async () => {
    let release: () => void = () => undefined;
    const { client, listener } = await sessions({
      check: () => new Promise<true>((r) => (release = () => r(true))),
    });
    const dialing = dialPipe(client).catch((e: unknown) => e);
    await later(20);
    listener.close();
    release();
    expect(await dialing).toMatchObject({ code: 4010, reason: 'not accepting channels' });
  });

  it('closes pipes that a closed listener is still attached for, with 1001', async () => {
    // The session's own policy accepts everything; the listener only takes the channels.
    const [ct, st] = createInMemoryPair('stream');
    const listener = new PipeListener();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, { ...base, onChannelRequest: () => true });
    listener.attach(server);
    await Promise.all([client.connect(), server.accept()]);
    listener.close();
    const s = quiet(await dialPipe(client));
    expect((await s.channel.closed).code).toBe(CloseCode.GOING_AWAY);
    await Promise.all([client.close(), server.close()]);
  });

  it('ends a for-await loop on close()', async () => {
    const { listener } = await sessions();
    const loop = (async () => {
      for await (const s of listener) s.destroy();
      return 'ended';
    })();
    await later(10);
    listener.close();
    expect(await loop).toBe('ended');
  });
});

describe('HTTP over pipes', () => {
  it('serves http on a listener to a client whose agent dials pipes', async () => {
    const { client, listener } = await sessions();
    const web = http.createServer((req, res) => {
      if (req.method === 'POST') {
        const hash = createHash('sha256');
        req.on('data', (d: Buffer) => hash.update(d));
        req.on('end', () => res.end(hash.digest('hex')));
        return;
      }
      res.end(`hello ${Number(req.url!.slice(1))}`);
    });
    listener.on('connection', (s) => web.emit('connection', s));
    cleanups.push(() => web.close());

    for (const keepAlive of [false, true]) {
      const agent = new http.Agent({ keepAlive, maxSockets: 8 });
      // createConnection with a callback: the agent waits for the pipe to open.
      (agent as unknown as { createConnection: unknown }).createConnection = (
        _opts: unknown,
        cb: (err: Error | null, s?: PipeSocket) => void,
      ) => {
        dialPipe(client, { name: 'http' }).then((s) => cb(null, s), cb);
      };
      const get = (path: string) =>
        new Promise<string>((resolve, reject) => {
          http
            .get({ host: 'xumux', path, agent }, (res) => {
              let body = '';
              res.setEncoding('utf8');
              res.on('data', (d: string) => (body += d));
              res.on('end', () => resolve(body));
            })
            .on('error', reject);
        });
      const bodies = await Promise.all(Array.from({ length: 20 }, (_, i) => get(`/${i}`)));
      expect(bodies).toEqual(Array.from({ length: 20 }, (_, i) => `hello ${i}`));

      // A body bigger than the window exercises backpressure in both directions.
      const upload = randomBytes(3 << 20);
      const digest = await new Promise<string>((resolve, reject) => {
        const req = http.request({ host: 'xumux', method: 'POST', path: '/up', agent }, (res) => {
          let body = '';
          res.on('data', (d: Buffer) => (body += d.toString()));
          res.on('end', () => resolve(body));
        });
        req.on('error', reject);
        req.end(upload);
      });
      expect(digest).toBe(createHash('sha256').update(upload).digest('hex'));
      agent.destroy();
    }
  });
});
