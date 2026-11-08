/**
 * E2E tests: the WebRTC binding over a standard RTCPeerConnection / RTCDataChannel.
 *
 * `node-datachannel/polyfill` provides the W3C WebRTC API in Node (backed by
 * libdatachannel), so the adapter runs here exactly as it would in a browser:
 * negotiated DataChannel created before the offer, `bufferedamountlow`
 * backpressure, and the DataChannel closing after the xumux CLOSE exchange.
 * Signalling is done in-process.
 */
import { describe, it, expect, afterAll } from 'vitest';
import nodeDataChannel from 'node-datachannel';
import { RTCPeerConnection } from 'node-datachannel/polyfill';
import { createHash } from 'node:crypto';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import type { XumuxChannel } from '../../src/channel.js';
import {
  WebRTCAdapter,
  createXumuxDataChannel,
  fragmentSizeFor,
} from '../../src/transports/webrtc-adapter.js';

afterAll(() => nodeDataChannel.cleanup());

type PC = InstanceType<typeof RTCPeerConnection>;

/** Offer/answer and trickled candidates, wired directly between the two peers. */
async function signal(a: PC, b: PC): Promise<void> {
  a.onicecandidate = (e) => void (e.candidate && b.addIceCandidate(e.candidate));
  b.onicecandidate = (e) => void (e.candidate && a.addIceCandidate(e.candidate));
  const offer = await a.createOffer();
  await a.setLocalDescription(offer);
  await b.setRemoteDescription(offer);
  const answer = await b.createAnswer();
  await b.setLocalDescription(answer);
  await a.setRemoteDescription(answer);
}

interface Pair {
  client: { pc: PC; dc: RTCDataChannel; adapter: WebRTCAdapter };
  server: { pc: PC; dc: RTCDataChannel; adapter: WebRTCAdapter };
  close(): void;
}

/** Two peers with the negotiated xumux DataChannel, adapters created before signalling. */
async function pair(highWaterMark?: number): Promise<Pair> {
  const pcs = [new RTCPeerConnection(), new RTCPeerConnection()] as const;
  const side = (pc: PC) => {
    const dc = createXumuxDataChannel(pc) as RTCDataChannel;
    return { pc, dc, adapter: new WebRTCAdapter(dc, { highWaterMark }) };
  };
  const p = {
    client: side(pcs[0]),
    server: side(pcs[1]),
    close: () => pcs.forEach((x) => x.close()),
  };
  await signal(pcs[0], pcs[1]);
  return p;
}

const closed = (dc: RTCDataChannel): Promise<void> =>
  dc.readyState === 'closed'
    ? Promise.resolve()
    : new Promise((r) => dc.addEventListener('close', () => r(), { once: true }));

describe('WebRTC binding over the standard RTCDataChannel API (node-datachannel polyfill)', () => {
  it('handshake, channels, probes and a normal close that closes the DataChannel', async () => {
    const p = await pair();
    try {
      const client = new XumuxClient(p.client.adapter, { pingInterval: 0 });
      const server = new XumuxServer(p.server.adapter, {
        pingInterval: 0,
        onChannelRequest: () => true,
      });
      const echo = (ch: XumuxChannel): void =>
        void (async () => {
          for (let m = await ch.receive(); m; m = await ch.receive())
            await ch.send(m.type, m.payload);
        })();
      server.addEventListener('channel', (e) => echo((e as CustomEvent<XumuxChannel>).detail));

      // connect() runs while the DataChannel may still be connecting: HELLO waits for open.
      const [c, s] = await Promise.all([
        client.connect({
          application: 'webrtc-e2e/1',
          channels: [{ name: 'hello', reliable: true, ordered: true }],
        }),
        server.accept(),
      ]);
      expect(s.hello.application).toBe('webrtc-e2e/1');
      expect(c.channels[0]!.properties).toMatchObject({ reliable: true, ordered: true });
      echo(s.channels[0]!);

      const more = await client.openChannel({ name: 'more', reliable: true, ordered: true });
      for (const [i, ch] of [c.channels[0]!, more].entries()) {
        const msg = new Uint8Array(100_000).fill(i + 1); // fragmented
        await ch.send(0x07, msg);
        const m = await ch.receive();
        expect(m?.type).toBe(0x07);
        expect(m?.payload).toEqual(msg);
        expect(await ch.ping()).toBeGreaterThanOrEqual(0);
      }

      const info = await client.close(1000, 'bye');
      const sInfo = await server.closed;
      expect(info.code).toBe(1000);
      expect(sInfo).toMatchObject({ code: 1000, remote: true });
      await Promise.all([closed(p.client.dc), closed(p.server.dc)]);
    } finally {
      p.close();
    }
  });

  it('a bulk transfer is held back by the DataChannel send buffer, and arrives intact', async () => {
    const high = 64 << 10;
    const p = await pair(high);
    try {
      // Record the send buffer after every send.
      let peak = 0;
      const dc = p.client.dc;
      const send = dc.send.bind(dc) as (d: Uint8Array) => void;
      (dc as { send: (d: Uint8Array) => void }).send = (d) => {
        send(d);
        peak = Math.max(peak, dc.bufferedAmount);
      };

      const total = 16 << 20;
      const client = new XumuxClient(p.client.adapter, { pingInterval: 0 });
      // A large window, so only the DataChannel's buffer can hold the sender back.
      const server = new XumuxServer(p.server.adapter, {
        pingInterval: 0,
        initialWindow: 8 << 20,
        onChannelRequest: () => true,
      });
      const received = new Promise<string>((resolve) =>
        server.addEventListener('channel', (e) => {
          const ch = (e as CustomEvent<XumuxChannel>).detail;
          void (async () => {
            const h = createHash('sha256');
            for (let n = 0; n < total; ) {
              const m = await ch.receive();
              if (!m) break;
              h.update(m.payload);
              n += m.payload.length;
            }
            resolve(h.digest('hex'));
          })();
        }),
      );
      await Promise.all([client.connect(), server.accept()]);
      const sctp = p.client.pc.sctp;
      expect(fragmentSizeFor(sctp?.maxMessageSize)).toBeGreaterThan(0);

      const ch = await client.openChannel({ name: 'bulk', reliable: true, ordered: true });
      const h = createHash('sha256');
      const chunk = new Uint8Array(64 << 10);
      for (let i = 0; i < total / chunk.length; i++) {
        for (let j = 0; j < chunk.length; j++) chunk[j] = (i * 7 + j) & 0xff;
        h.update(chunk);
        await ch.send(0x01, chunk);
      }
      expect(await received).toBe(h.digest('hex'));
      // At most the mark plus one 16 KiB frame (+ 8-byte header) is ever buffered.
      expect(peak).toBeLessThanOrEqual(high + 16384 + 8);
      expect(peak).toBeGreaterThan(high / 2);

      await client.close(1000);
      await server.closed;
    } finally {
      p.close();
    }
  }, 60_000);

  it('the session ends as lost if the DataChannel closes without a CLOSE exchange', async () => {
    const p = await pair();
    try {
      const client = new XumuxClient(p.client.adapter, { pingInterval: 0 });
      const server = new XumuxServer(p.server.adapter, { pingInterval: 0 });
      await Promise.all([client.connect(), server.accept()]);
      p.server.dc.close(); // behind the server session's back
      const info = await client.closed;
      expect(info.code).toBe(1001);
      expect(info.remote).toBe(true);
    } finally {
      p.close();
    }
  });
});
