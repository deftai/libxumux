/**
 * E2E tests: real TCP transport (net.Socket).
 * Spins up a net.createServer and connects via TcpAdapter.
 * Both sides send/validate OMUX magic bytes.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import * as net from 'node:net';
import type { AddressInfo } from 'node:net';
import { XumuxClient, XumuxServer } from '../../src/connection.js';
import { TcpAdapter } from '../../src/transports/tcp-adapter.js';
import { runProtocolScenarios, E2E_CHANNELS } from './helpers.js';

let tcpServer: net.Server;
let port: number;

beforeAll(async () => {
  tcpServer = net.createServer();
  await new Promise<void>((resolve) => tcpServer.listen(0, '127.0.0.1', resolve));
  port = (tcpServer.address() as AddressInfo).port;
});

afterAll(async () => {
  await new Promise<void>((resolve) => tcpServer.close(() => resolve()));
});

describe('TCP E2E — all 5 protocol scenarios', () => {
  it('completes handshake, data exchange, fragmentation, dynamic channel, and graceful close', async () => {
    // Server side: accept one connection
    const serverReady = new Promise<{ server: XumuxServer; serverCh: Map<string, import('../../src/channel.js').XumuxChannel> }>((resolve) => {
      tcpServer.once('connection', (socket) => {
        const transport = new TcpAdapter(socket);
        const server = new XumuxServer(transport, { pingInterval: 0 });
        void server.accept().then((serverCh) => {
          resolve({ server, serverCh });
        });
      });
    });

    // Client side: connect
    const clientSocket = await new Promise<net.Socket>((resolve, reject) => {
      const s = net.createConnection({ port, host: '127.0.0.1' }, () => resolve(s));
      s.once('error', reject);
    });
    const clientTransport = new TcpAdapter(clientSocket);
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
