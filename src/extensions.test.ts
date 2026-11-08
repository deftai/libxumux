/**
 * xumux 0.3 extension hooks: extensionParams (core §3.8), extension-defined first frames
 * (core §3.1), and the 0.2 / 0.3 version split (core §3.5).
 */
import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer, type XumuxOptions } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import {
  CloseCode,
  CONTROL_MAX_PAYLOAD,
  ControlType,
  PROTOCOL_VERSION,
} from './codec/constants.js';
import { XumuxClosedError, XumuxProtocolError } from './errors.js';
import { rawPair, ctrl, json } from '../tests/raw-peer.js';
import { XUMUX_VERSION } from './types.js';

const base: XumuxOptions = { pingInterval: 0, closeTimeoutMs: 100 };

describe('version 0.3', () => {
  it('speaks 0.3', () => {
    expect(PROTOCOL_VERSION).toEqual([0, 3, 0]);
    expect(XUMUX_VERSION).toBe('0.3.0-draft');
  });

  it('server refuses a 0.2 HELLO with 4006', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server.accept().catch((e: unknown) => e);
    raw.send(ctrl(ControlType.HELLO, { version: [0, 2, 0] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.VERSION_MISMATCH);
    expect(((await p) as XumuxClosedError).code).toBe(CloseCode.VERSION_MISMATCH);
  });

  it('client closes on a 0.2 WELCOME', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect().catch((e: unknown) => e);
    await raw.control(ControlType.HELLO);
    raw.send(ctrl(ControlType.WELCOME, { version: [0, 2, 0] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.PROTOCOL_ERROR);
    expect(await p).toBeInstanceOf(XumuxProtocolError);
  });
});

describe('extensionParams (core §3.8)', () => {
  it('negotiates parameters for accepted extensions only', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const seen: Array<[string, unknown]> = [];
    const [c, s] = await Promise.all([
      client.connect({
        extensions: ['a/1', 'b/1', 'c/1', 'd/1'],
        extensionParams: { 'a/1': { level: 3 }, 'b/1': { x: 1 }, 'c/1': 'bad', 'z/1': { y: 2 } },
      }),
      server.accept({
        extensions: ['a/1', 'b/1', 'c/1', 'd/1'],
        negotiateExtension: (ext, params) => {
          seen.push([ext, params]);
          if (ext === 'a/1') return { params: { level: 2 } };
          if (ext === 'c/1') return typeof params === 'object';
          if (ext === 'd/1') throw new Error('nope');
          return true;
        },
      }),
    ]);
    // z/1 was not requested, so the client never sends its key.
    expect(seen).toEqual([
      ['a/1', { level: 3 }],
      ['b/1', { x: 1 }],
      ['c/1', 'bad'],
      ['d/1', undefined],
    ]);
    expect(s.hello.extensionParams).toEqual({ 'a/1': { level: 3 }, 'b/1': { x: 1 }, 'c/1': 'bad' });
    expect(s.extensions).toEqual(['a/1', 'b/1']);
    expect(s.extensionParams).toEqual({ 'a/1': { level: 3 }, 'b/1': { x: 1 } });
    expect(c.extensions).toEqual(['a/1', 'b/1']);
    expect(c.extensionParams).toEqual({ 'a/1': { level: 2 } });
    expect(c.welcome.extensionParams).toEqual({ 'a/1': { level: 2 } });
    await client.close();
  });

  it('server ignores keys for extensions it does not accept or that were not requested', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const accepted = server.accept({ extensions: ['a/1'] });
    const welcome = json(
      await raw.helloAndWelcome({
        extensions: ['a/1', 'x-unknown/1'],
        extensionParams: { 'a/1': {}, 'x-unknown/1': { level: 3 }, 'x-not-requested/1': true },
      }),
    );
    expect(welcome.extensions).toEqual(['a/1']);
    expect(welcome.extensionParams).toBeUndefined();
    const s = await accepted;
    expect(s.extensionParams).toEqual({ 'a/1': {} });
    await server.close();
  });

  it('server never sends keys for an extension it leaves out', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const accepted = server.accept({
      extensions: ['a/1'],
      negotiateExtension: () => false,
    });
    const welcome = json(
      await raw.helloAndWelcome({ extensions: ['a/1'], extensionParams: { 'a/1': { v: 1 } } }),
    );
    expect(welcome.extensions).toBeUndefined();
    expect(welcome.extensionParams).toBeUndefined();
    expect((await accepted).extensions).toEqual([]);
    await server.close();
  });

  it('keeps a key named __proto__ as data', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const [c, s] = await Promise.all([
      client.connect({
        extensions: ['__proto__'],
        extensionParams: JSON.parse('{"__proto__":{"polluted":true}}') as Record<string, unknown>,
      }),
      server.accept({
        extensions: ['__proto__'],
        negotiateExtension: () => ({ params: { v: 1 } }),
      }),
    ]);
    expect(Object.hasOwn(s.extensionParams, '__proto__')).toBe(true);
    expect(Object.getPrototypeOf(s.extensionParams)).toBe(Object.prototype);
    expect(Object.hasOwn(c.extensionParams, '__proto__')).toBe(true);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
    await client.close();
  });

  it('passes undefined, not an inherited value, for a missing __proto__ entry', async () => {
    const [ct, st] = createInMemoryPair();
    const client = new XumuxClient(ct, base);
    const server = new XumuxServer(st, base);
    const got: unknown[] = [];
    await Promise.all([
      client.connect({
        extensions: ['__proto__'],
        checkExtensionParams: (_e, p) => (got.push(p), true),
      }),
      server.accept({
        extensions: ['__proto__'],
        negotiateExtension: (_e, p) => (got.push(p), true),
      }),
    ]);
    expect(got).toEqual([undefined, undefined]);
    await client.close();
  });

  it('client checks a duplicated WELCOME extension once', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    let calls = 0;
    const p = client.connect({ extensions: ['a/1'], checkExtensionParams: () => (calls++, true) });
    await raw.awaitHelloSendWelcome({ extensions: ['a/1', 'a/1'] });
    expect((await p).extensions).toEqual(['a/1']);
    expect(calls).toBe(1);
    await client.close();
  });

  it('server refuses when its WELCOME params cannot be encoded', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server
      .accept({ extensions: ['a/1'], negotiateExtension: () => ({ params: 1n }) })
      .catch((e: unknown) => e);
    raw.send(ctrl(ControlType.HELLO, { version: [0, 3, 0], extensions: ['a/1'] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.GOING_AWAY);
    expect(await p).toBeInstanceOf(XumuxClosedError);
    expect((await server.closed).code).toBe(CloseCode.GOING_AWAY);
  });

  it('server closes 4001 when HELLO extensionParams is not an object', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server.accept().catch((e: unknown) => e);
    raw.send(ctrl(ControlType.HELLO, { version: [0, 3, 0], extensionParams: ['x'] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
    expect(await p).toBeInstanceOf(Error);
  });

  it('client closes 4001 when WELCOME extensionParams is not an object', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const p = client.connect().catch((e: unknown) => e);
    await raw.awaitHelloSendWelcome({ extensionParams: 7 });
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
    expect(await p).toBeInstanceOf(Error);
  });

  it('client ignores WELCOME keys for extensions that were not accepted', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    const checked: string[] = [];
    const p = client.connect({
      extensions: ['a/1', 'b/1'],
      checkExtensionParams: (ext) => (checked.push(ext), true),
    });
    await raw.awaitHelloSendWelcome({
      extensions: ['a/1'],
      extensionParams: { 'a/1': { ok: true }, 'b/1': 'junk', 'q/1': 1 },
    });
    const c = await p;
    expect(checked).toEqual(['a/1']);
    expect(c.extensionParams).toEqual({ 'a/1': { ok: true } });
    await client.close();
  });

  for (const [what, check] of [
    ['returns false', () => false],
    [
      'throws',
      () => {
        throw new Error('bad');
      },
    ],
  ] as const) {
    it(`client closes 4001 when checkExtensionParams ${what}`, async () => {
      const { transport, raw } = rawPair();
      const client = new XumuxClient(transport, base);
      const p = client
        .connect({ extensions: ['a/1'], checkExtensionParams: check })
        .catch((e: unknown) => e);
      await raw.awaitHelloSendWelcome({ extensions: ['a/1'], extensionParams: { 'a/1': 5 } });
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.INVALID_MESSAGE);
      expect(((await p) as XumuxProtocolError).code).toBe(CloseCode.INVALID_MESSAGE);
    });
  }

  it('client refuses to send a HELLO over 65536 bytes', async () => {
    const { transport, raw } = rawPair();
    const client = new XumuxClient(transport, base);
    await expect(
      client.connect({
        extensions: ['a/1'],
        extensionParams: { 'a/1': 'x'.repeat(CONTROL_MAX_PAYLOAD) },
      }),
    ).rejects.toThrow(RangeError);
    expect(raw.frames).toHaveLength(0);
  });

  it('server refuses when its WELCOME would exceed 65536 bytes', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server
      .accept({
        extensions: ['a/1'],
        negotiateExtension: () => ({ params: 'x'.repeat(CONTROL_MAX_PAYLOAD) }),
      })
      .catch((e: unknown) => e);
    raw.send(ctrl(ControlType.HELLO, { version: [0, 3, 0], extensions: ['a/1'] }));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.GOING_AWAY);
    expect(await p).toBeInstanceOf(XumuxClosedError);
    expect(raw.frames.some((f) => f.type === ControlType.WELCOME)).toBe(false);
  });
});

describe('extension-defined first frames (core §3.1)', () => {
  const RESUME = 0x31;

  it('a claimed first frame goes to its handler, which refuses with CLOSE', async () => {
    for (const kind of ['message', 'stream'] as const) {
      const { transport, raw } = rawPair(kind);
      const server = new XumuxServer(transport, base);
      const got: Array<[number, unknown]> = [];
      const p = server
        .accept({
          firstFrames: {
            [RESUME]: (f) => {
              got.push([f.type, JSON.parse(new TextDecoder().decode(f.payload))]);
              return { code: 4100, reason: 'unknown token' };
            },
          },
        })
        .catch((e: unknown) => e);
      raw.send(ctrl(RESUME, { token: 't' }));
      const close = json(await raw.control(ControlType.CLOSE));
      expect(close).toEqual({ code: 4100, reason: 'unknown token' });
      expect(got).toEqual([[RESUME, { token: 't' }]]);
      const err = (await p) as XumuxClosedError;
      expect(err).toBeInstanceOf(XumuxClosedError);
      expect(err.code).toBe(4100);
    }
  });

  it('maps handler exceptions to a refusal', async () => {
    for (const [thrown, code] of [
      [new XumuxProtocolError(CloseCode.INVALID_MESSAGE, 'bad RESUME'), 4001],
      [new Error('boom'), 1002],
    ] as const) {
      const { transport, raw } = rawPair();
      const server = new XumuxServer(transport, base);
      const p = server
        .accept({
          firstFrames: {
            [RESUME]: () => {
              throw thrown;
            },
          },
        })
        .catch((e: unknown) => e);
      raw.send(ctrl(RESUME, {}));
      expect(json(await raw.control(ControlType.CLOSE)).code).toBe(code);
      expect(await p).toBeInstanceOf(XumuxClosedError);
    }
  });

  it('a first frame of an unclaimed type still closes with 1002', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server
      .accept({ firstFrames: { [RESUME]: () => ({ code: 4100 }) } })
      .catch((e: unknown) => e);
    raw.send(ctrl(0x32, {}));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.PROTOCOL_ERROR);
    expect(await p).toBeInstanceOf(Error);
  });

  it('without handlers an extension first frame closes with 1002', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const p = server.accept().catch((e: unknown) => e);
    raw.send(ctrl(RESUME, {}));
    expect(json(await raw.control(ControlType.CLOSE)).code).toBe(CloseCode.PROTOCOL_ERROR);
    expect(await p).toBeInstanceOf(Error);
  });

  it('a claimed type is not accepted after HELLO', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    const accepted = server.accept({ firstFrames: { [RESUME]: () => ({ code: 4100 }) } });
    await raw.helloAndWelcome();
    await accepted;
    raw.send(ctrl(RESUME, {}));
    // After the handshake it is just an unknown control type (core §4.1).
    expect(json(await raw.control(ControlType.ERROR)).code).toBe(CloseCode.UNSUPPORTED);
    await server.close();
  });

  it('a handler that outlives the connection does not send CLOSE twice', async () => {
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, base);
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const p = server
      .accept({ firstFrames: { [RESUME]: async () => (await gate, { code: 4100 }) } })
      .catch((e: unknown) => e);
    raw.send(ctrl(RESUME, {}));
    await new Promise((r) => setTimeout(r, 10));
    void server.close(CloseCode.GOING_AWAY);
    release();
    expect(((await p) as XumuxClosedError).code).toBe(CloseCode.GOING_AWAY);
    const closes = raw.frames.filter((f) => f.type === ControlType.CLOSE);
    expect(closes.map((f) => json(f).code)).toEqual([CloseCode.GOING_AWAY]);
  });

  it('rejects types outside 0x30-0x7f', async () => {
    for (const t of [0x01, 0x2f, 0x80, 1.5]) {
      const { transport } = rawPair();
      const server = new XumuxServer(transport, base);
      await expect(server.accept({ firstFrames: { [t]: () => ({ code: 1 }) } })).rejects.toThrow(
        RangeError,
      );
    }
  });
});
