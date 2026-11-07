/**
 * E2E tests: WebRTC DataChannel via node-datachannel (real DTLS/SCTP).
 *
 * Two PeerConnection instances are created in the same process and ICE
 * negotiation is wired in-process — no STUN/TURN server required for loopback.
 * The DataChannels are wrapped with NodeDCShim → WebRTCAdapter → XumuxClient/Server.
 */
import { describe, it, expect, afterAll } from 'vitest';
import dc from 'node-datachannel';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { WebRTCAdapter } from '../../src/transports/webrtc-adapter.js';
import { NodeDCShim, waitForChannelOpen } from './node-dc-shim.js';
import { runProtocolScenarios, expectAll, E2E_CHANNELS, E2E_OPTIONS } from './helpers.js';

afterAll(() => {
  // Release native resources (threads, sockets) after all tests complete.
  dc.cleanup();
});

describe('WebRTC node-datachannel — real DTLS/SCTP — all 5 protocol scenarios', () => {
  it('connects over real DTLS/SCTP loopback and passes all 5 scenarios', async () => {
    // Two in-process PeerConnections — loopback ICE, no STUN/TURN needed
    const pc1 = new dc.PeerConnection('pc1', { iceServers: [] });
    const pc2 = new dc.PeerConnection('pc2', { iceServers: [] });

    try {
      // ── Wire ICE negotiation in-process ─────────────────────────────────────
      pc1.onLocalDescription((sdp, type) => {
        pc2.setRemoteDescription(sdp, type);
      });
      pc2.onLocalDescription((sdp, type) => {
        pc1.setRemoteDescription(sdp, type);
      });
      pc1.onLocalCandidate((cand, mid) => {
        pc2.addRemoteCandidate(cand, mid);
      });
      pc2.onLocalCandidate((cand, mid) => {
        pc1.addRemoteCandidate(cand, mid);
      });

      // ── Create DataChannel on pc1 (triggers ICE negotiation) ───────────────
      // pc2 receives the channel via onDataChannel
      const dc2Promise = new Promise<dc.DataChannel>((resolve) => {
        pc2.onDataChannel((channel) => resolve(channel));
      });

      const dc1 = pc1.createDataChannel('xumux', { protocol: 'xumux' });
      const dc2 = await dc2Promise;

      // ── Wait for both DataChannels to open ─────────────────────────────────
      await Promise.all([waitForChannelOpen(dc1), waitForChannelOpen(dc2)]);

      // ── Wrap with NodeDCShim → WebRTCAdapter ───────────────────────────────
      // Shims are created AFTER open so native callbacks (onMessage, etc.) are
      // registered before any xumux frames arrive.
      const shim1 = new NodeDCShim(dc1);
      const shim2 = new NodeDCShim(dc2);

      const clientTransport = new WebRTCAdapter(shim1);
      const serverTransport = new WebRTCAdapter(shim2);

      const client = new XumuxClient(clientTransport, { pingInterval: 0 });
      const server = new XumuxServer(serverTransport, E2E_OPTIONS);

      const [clientCh, serverCh] = await Promise.all([
        client.connect({ channels: E2E_CHANNELS }),
        server.accept(),
      ]);

      // ── Run the 5 standard E2E protocol scenarios ──────────────────────────
      const results = await runProtocolScenarios(client, server, clientCh, serverCh);

      expectAll(results, expect);
    } finally {
      // Always clean up PeerConnections regardless of test outcome
      try {
        pc1.close();
      } catch {
        /* ignore */
      }
      try {
        pc2.close();
      } catch {
        /* ignore */
      }
    }
  });
});
