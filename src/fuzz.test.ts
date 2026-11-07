import { describe, it, expect } from 'vitest';
import fc from 'fast-check';
import { StreamFramer } from './codec/decode-frame.js';
import { encodeFrame } from './codec/encode-frame.js';
import { XMUX_MAGIC } from './codec/constants.js';
import { XumuxServer } from './session.js';
import { rawPair } from '../tests/raw-peer.js';

const frameArb = fc.record({
  channel: fc.integer({ min: 0, max: 0xfffe }),
  type: fc.integer({ min: 0, max: 255 }),
  flags: fc.integer({ min: 0, max: 255 }),
  payload: fc.uint8Array({ maxLength: 300 }),
});

describe('fuzz', () => {
  it('StreamFramer yields the same frames however the stream is split', () => {
    fc.assert(
      fc.property(
        fc.array(frameArb, { maxLength: 8 }),
        fc.array(fc.integer({ min: 1, max: 50 }), { maxLength: 40 }),
        (frames, cuts) => {
          const bytes = new Uint8Array([
            ...XMUX_MAGIC,
            ...frames.flatMap((f) => Array.from(encodeFrame(f))),
          ]);
          const fr = new StreamFramer(0x7fffffff);
          const out = [];
          let off = 0;
          for (const c of cuts) {
            out.push(...fr.push(bytes.subarray(off, off + c)));
            off += c;
          }
          out.push(...fr.push(bytes.subarray(off)));
          expect(out.map((f) => [f.channel, f.type, f.flags, Array.from(f.payload)])).toEqual(
            frames.map((f) => [f.channel, f.type, f.flags, Array.from(f.payload)]),
          );
        },
      ),
    );
  });

  it('a server fed arbitrary frames after the handshake never throws and always ends closed or open', async () => {
    await fc.assert(
      fc.asyncProperty(fc.array(frameArb, { minLength: 1, maxLength: 6 }), async (frames) => {
        const { transport, raw } = rawPair();
        const server = new XumuxServer(transport, {
          pingInterval: 0,
          closeTimeoutMs: 20,
          onChannelRequest: () => true,
        });
        const acc = server.accept();
        await raw.helloAndWelcome({
          channels: [{ id: 1, name: 'd', reliable: true, ordered: true }],
        });
        await acc;
        for (const f of frames) raw.send(f);
        await new Promise((r) => setTimeout(r, 5));
        await server.close();
        const info = await server.closed;
        expect(typeof info.code).toBe('number');
      }),
      { numRuns: 150 },
    );
  });

  it('a server fed random bytes before HELLO closes cleanly', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.uint8Array({ minLength: 1, maxLength: 64 }),
        fc.constantFrom('message' as const, 'stream' as const),
        async (bytes, kind) => {
          const { transport, raw } = rawPair(kind);
          const server = new XumuxServer(transport, {
            pingInterval: 0,
            closeTimeoutMs: 20,
            handshakeTimeoutMs: 20,
          });
          const acc = server.accept().catch(() => 'refused');
          raw.sendBytes(bytes, true);
          expect(await acc).toBe('refused');
          await server.closed;
        },
      ),
      { numRuns: 150 },
    );
  });
});
