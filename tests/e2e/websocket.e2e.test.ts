/**
 * E2E tests: real WebSocket transport (ws package).
 * Spins up a real ws.WebSocketServer on a random port and connects via WebSocketAdapter.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { WebSocketServer, WebSocket as NodeWebSocket } from 'ws';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/connection.js';
import { WebSocketAdapter } from '../../src/transports/websocket-adapter.js';
import { runProtocolScenarios, E2E_CHANNELS } from './helpers.js';

let wss: WebSocketServer;
let port: number;

beforeAll(async () => {
  wss = new WebSocketServer({ port: 0 });
  port = (wss.address() as AddressInfo).port;
});

afterAll(async () => {
  await new Promise<void>((resolve) => wss.close(() => resolve()));
});

describe('WebSocket E2E — all 5 protocol scenarios', () => {
  it('completes handshake, data exchange, fragmentation, dynamic channel, and graceful close', async () => {
    // Server side: accept one connection
    const serverReady = new Promise<{ server: XumuxServer; serverCh: Map<string, import('../../src/channel.js').XumuxChannel> }>((resolve) => {
      wss.once('connection', (ws) => {
        const transport = new WebSocketAdapter(ws as unknown as import('../../src/transports/websocket-adapter.js').WebSocketLike);
        const server = new XumuxServer(transport, { pingInterval: 0 });
        void server.accept().then((serverCh) => {
          resolve({ server, serverCh });
        });
      });
    });

    // Client side: connect
    const clientWs = new NodeWebSocket(`ws://localhost:${port}`);
    await new Promise<void>((resolve, reject) => {
      clientWs.once('open', resolve);
      clientWs.once('error', reject);
    });
    const clientTransport = new WebSocketAdapter(clientWs as unknown as import('../../src/transports/websocket-adapter.js').WebSocketLike);
    const client = new XumuxClient(clientTransport, { pingInterval: 0 });
    const clientCh = await client.connect({ channels: E2E_CHANNELS });

    const { server, serverCh } = await serverReady;

    const results = await runProtocolScenarios(client, server, clientCh, serverCh);

    expect(results.handshakeCompleted).toBe(true);
    expect(results.dataExchangeWorks).toBe(true);
    expect(results.fragmentationWorks).toBe(true);
    expect(results.dynamicChannelWorks).toBe(true);
    expect(results.gracefulCloseWorks).toBe(true);
  });
});
