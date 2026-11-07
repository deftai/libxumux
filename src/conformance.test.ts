/**
 * Runs the shared xumux conformance vectors (tests/fixtures/xumux-vectors.json,
 * copied from deftai/xumux conformance/vectors.json).
 */
import { describe, it, expect } from 'vitest';
import { readFileSync } from 'node:fs';
import { decodeFrame, StreamFramer } from './codec/decode-frame.js';
import { encodeFrame } from './codec/encode-frame.js';
import { ControlType } from './codec/constants.js';
import { XumuxClient, XumuxServer } from './session.js';
import { rawPair, json } from '../tests/raw-peer.js';

interface FrameJson {
  channel: number;
  type: number;
  flags: number;
  payloadHex?: string;
  payloadJson?: unknown;
}
interface Valid {
  name: string;
  hex: string;
  decoded: FrameJson & { frames?: FrameJson[]; reassembledHex?: string; magic?: string };
}
interface Invalid {
  name: string;
  hex: string;
  state: 'start' | 'awaiting-hello' | 'connected';
  expect: {
    action: 'close' | 'close-transport';
    code?: number;
    bindings?: string[];
    receiver?: 'client' | 'server';
  };
}

const vectors = JSON.parse(
  readFileSync(new URL('../tests/fixtures/xumux-vectors.json', import.meta.url), 'utf8'),
) as {
  valid: Valid[];
  invalid: Invalid[];
};

const fromHex = (h: string): Uint8Array =>
  new Uint8Array(
    h
      .split(' ')
      .filter(Boolean)
      .map((x) => parseInt(x, 16)),
  );
const toHex = (b: Uint8Array): string =>
  Array.from(b, (x) => x.toString(16).toUpperCase().padStart(2, '0')).join(' ');

function payloadOf(f: FrameJson): Uint8Array {
  return f.payloadJson !== undefined
    ? new TextEncoder().encode(JSON.stringify(f.payloadJson))
    : fromHex(f.payloadHex ?? '');
}

describe('conformance: valid vectors', () => {
  for (const v of vectors.valid) {
    it(v.name, () => {
      const bytes = fromHex(v.hex);
      if (v.decoded.magic) {
        expect(new TextDecoder().decode(bytes)).toBe('XMUX');
        return;
      }
      const frameJsons = v.decoded.frames ?? [v.decoded];
      // Decode: a stream framer (no magic) must yield exactly these frames.
      const frames = new StreamFramer(0x7fffffff, false).push(bytes);
      expect(frames).toHaveLength(frameJsons.length);
      frames.forEach((f, i) => {
        const want = frameJsons[i]!;
        expect([f.channel, f.type, f.flags]).toEqual([want.channel, want.type, want.flags]);
        if (want.payloadJson !== undefined)
          expect(JSON.parse(new TextDecoder().decode(f.payload))).toEqual(want.payloadJson);
        else expect(toHex(f.payload)).toBe(want.payloadHex ?? '');
      });
      // Encode: the frames must produce exactly these bytes.
      const encoded = frameJsons.map((f) =>
        encodeFrame({ channel: f.channel, type: f.type, flags: f.flags, payload: payloadOf(f) }),
      );
      expect(toHex(new Uint8Array(encoded.flatMap((e) => Array.from(e))))).toBe(v.hex);
      if (frameJsons.length === 1)
        expect(decodeFrame(bytes).payload).toEqual(payloadOf(frameJsons[0]!));
    });
  }
});

describe('conformance: invalid vectors', () => {
  for (const v of vectors.invalid) {
    const binding = v.expect.bindings?.[0] ?? 'websocket';
    const kind =
      binding === 'tcp' || binding === 'stdio' || v.state === 'start' ? 'stream' : 'message';
    const receiver = v.expect.receiver ?? 'server';

    it(`${v.name} (${v.state}, ${receiver}, ${kind})`, async () => {
      const { transport, raw } = rawPair(kind);
      const opts = { pingInterval: 0, closeTimeoutMs: 200, onChannelRequest: () => true as const };
      const bytes = fromHex(v.hex);

      if (receiver === 'server') {
        const server = new XumuxServer(transport, opts);
        const accepted = server.accept().catch(() => undefined);
        if (v.state === 'connected') {
          await raw.helloAndWelcome();
          await accepted;
        }
        raw.sendBytes(bytes, v.state === 'start');
        if (v.expect.action === 'close-transport') {
          await raw.waitEnded();
          expect(raw.frames).toHaveLength(0);
          expect((await server.closed).code).toBe(1002);
        } else {
          const close = await raw.control(ControlType.CLOSE);
          expect(json(close).code).toBe(v.expect.code);
        }
      } else {
        const client = new XumuxClient(transport, opts);
        const connected = client.connect().catch(() => undefined);
        await raw.awaitHelloSendWelcome();
        await connected;
        raw.sendBytes(bytes);
        const close = await raw.control(ControlType.CLOSE);
        expect(json(close).code).toBe(v.expect.code);
      }
    });
  }
});
