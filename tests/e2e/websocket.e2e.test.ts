/**
 * E2E tests: real WebSocket transport (ws package).
 * Spins up a real ws.WebSocketServer on a random port and connects via WebSocketAdapter.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { WebSocketServer, WebSocket as NodeWebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/session.js';
import { WebSocketAdapter } from '../../src/transports/websocket-adapter.js';
import { runProtocolScenarios, expectAll, E2E_CHANNELS, E2E_OPTIONS } from './helpers.js';

let wss: WebSocketServer;
let port: number;

beforeAll(async () => {
  wss = new WebSocketServer({
    port: 0,
    handleProtocols: (protocols) => (protocols.has('xumux') ? 'xumux' : false),
  });
  port = (wss.address() as AddressInfo).port;
});

afterAll(async () => {
  await new Promise<void>((resolve) => wss.close(() => resolve()));
});

describe('WebSocket E2E — all 5 protocol scenarios', () => {
  it('completes handshake, data exchange, fragmentation, dynamic channel, and graceful close', async () => {
    // Server side: accept one connection
    const serverReady = new Promise<{
      server: XumuxServer;
      serverCh: import('../../src/session.js').AcceptResult;
    }>((resolve) => {
      wss.once('connection', (ws) => {
        const transport = new WebSocketAdapter(
          ws as unknown as import('../../src/transports/websocket-adapter.js').WebSocketLike,
        );
        const server = new XumuxServer(transport, E2E_OPTIONS);
        void server.accept().then((serverCh) => {
          resolve({ server, serverCh });
        });
      });
    });

    // Client side: connect
    const clientWs = new NodeWebSocket(`ws://localhost:${port}/xumux`, 'xumux');
    await new Promise<void>((resolve, reject) => {
      clientWs.once('open', resolve);
      clientWs.once('error', reject);
    });
    const clientTransport = new WebSocketAdapter(
      clientWs as unknown as import('../../src/transports/websocket-adapter.js').WebSocketLike,
    );
    expect(clientWs.protocol).toBe('xumux');
    const client = new XumuxClient(clientTransport, { pingInterval: 0 });
    const clientCh = await client.connect({ channels: E2E_CHANNELS });

    const { server, serverCh } = await serverReady;

    const results = await runProtocolScenarios(client, server, clientCh, serverCh);

    expectAll(results, expect);
  });
});
