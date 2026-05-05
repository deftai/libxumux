import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer } from './connection.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { CloseCode } from './codec/constants.js';

/** Helper: create a connected client+server pair (both handshake complete). */
async function createConnectedPair(options?: {
  helloChannels?: Array<{ name: string; reliable: boolean; ordered: boolean }>;
}) {
  const [clientT, serverT] = createInMemoryPair();
  const client = new XumuxClient(clientT, { pingInterval: 0 });
  const server = new XumuxServer(serverT, { pingInterval: 0 });

  const [clientChannels, serverChannels] = await Promise.all([
    client.connect({ channels: options?.helloChannels }),
    server.accept(),
  ]);

  return { client, server, clientChannels, serverChannels };
}

describe('XumuxClient + XumuxServer — basic handshake', () => {
  it('connect() and accept() complete the HELLO/WELCOME handshake', async () => {
    const { client, server } = await createConnectedPair();
    expect(client.channels.size).toBe(0);
    expect(server.channels.size).toBe(0);
    await client.close();
  });

  it('channels requested in HELLO are available post-connect', async () => {
    const { clientChannels, serverChannels } = await createConnectedPair({
      helloChannels: [{ name: 'data', reliable: true, ordered: true }],
    });

    expect(clientChannels.has('data')).toBe(true);
    expect(serverChannels.has('data')).toBe(true);
    expect(clientChannels.get('data')!.name).toBe('data');
  });
});

describe('XumuxClient + XumuxServer — data exchange', () => {
  it('data written on client channel arrives at server channel', async () => {
    const { client, clientChannels, serverChannels } = await createConnectedPair({
      helloChannels: [{ name: 'pipe', reliable: true, ordered: true }],
    });

    const clientCh = clientChannels.get('pipe')!;
    const serverCh = serverChannels.get('pipe')!;

    // Write on client
    const writer = clientCh.writable.getWriter();
    await writer.write(new TextEncoder().encode('hello world'));
    writer.releaseLock();

    // Read on server
    const reader = serverCh.readable.getReader();
    const { value } = await reader.read();
    reader.releaseLock();

    expect(new TextDecoder().decode(value ?? new Uint8Array())).toBe('hello world');
    await client.close();
  });

  it('bidirectional data exchange works', async () => {
    const { client, server, clientChannels, serverChannels } = await createConnectedPair({
      helloChannels: [{ name: 'echo', reliable: true, ordered: true }],
    });

    const clientCh = clientChannels.get('echo')!;
    const serverCh = serverChannels.get('echo')!;

    // Client → Server
    const clientWriter = clientCh.writable.getWriter();
    await clientWriter.write(new TextEncoder().encode('ping'));
    clientWriter.releaseLock();

    // Server reads and echoes back
    const serverReader = serverCh.readable.getReader();
    const { value: fromClient } = await serverReader.read();
    serverReader.releaseLock();

    const serverWriter = serverCh.writable.getWriter();
    await serverWriter.write(fromClient ?? new Uint8Array());
    serverWriter.releaseLock();

    // Client reads echo
    const clientReader = clientCh.readable.getReader();
    const { value: echo } = await clientReader.read();
    clientReader.releaseLock();

    expect(new TextDecoder().decode(echo)).toBe('ping');
    await client.close();
    void server; // suppress unused
  });
});

describe('XumuxClient + XumuxServer — dynamic channel', () => {
  it('client can open a dynamic channel after handshake', async () => {
    const { client, server } = await createConnectedPair();

    const openedOnServer: unknown[] = [];
    server.addEventListener('channel-open', (e) => {
      openedOnServer.push((e as CustomEvent).detail);
    });

    const clientCh = await client.openChannel({ name: 'dynamic', reliable: true, ordered: true });

    expect(clientCh.name).toBe('dynamic');
    expect(openedOnServer).toHaveLength(1);
    await client.close();
  });
});

describe('XumuxClient + XumuxServer — graceful close', () => {
  it('client.close() gracefully closes the connection', async () => {
    const { client, server } = await createConnectedPair();

    const closeEvents: unknown[] = [];
    server.addEventListener('close', (e) => closeEvents.push((e as CustomEvent).detail));

    await client.close(CloseCode.NORMAL, 'done');

    expect(closeEvents).toHaveLength(1);
    expect((closeEvents[0] as { code: number }).code).toBe(CloseCode.NORMAL);
  });
});

describe('XumuxPeer — error paths', () => {
  it('data frame on unknown channel causes client to receive ERROR message', async () => {
    const { createInMemoryPair: pair } = await import('./transports/in-memory-adapter.js');
    const { encodeFrame: ef } = await import('./codec/encode-frame.js');
    const [clientT2, serverT2] = pair();
    const client2 = new XumuxClient(clientT2, { pingInterval: 0 });
    const server2 = new XumuxServer(serverT2, { pingInterval: 0 });

    // Server sends ERROR back to client when it receives data on an unknown channel.
    // The ERROR frame arrives at the client and triggers client's 'error' event.
    const errorsOnClient2: unknown[] = [];
    client2.addEventListener('error', (e) => errorsOnClient2.push((e as CustomEvent).detail));

    await Promise.all([client2.connect(), server2.accept()]);

    // Send a data frame for channel 99 (not open) from client to server
    clientT2.send(ef({ channel: 99, type: 1, flags: 0, payload: new Uint8Array([1]) }));

    // Wait for both receive loops to process (server processes frame, sends ERROR;
    // client receives ERROR and dispatches 'error' event)
    await new Promise((r) => setTimeout(r, 30));

    expect(errorsOnClient2.length).toBeGreaterThan(0);
    await client2.close();
  });

  it('closeChannel() on a non-existent channel ID does not throw', async () => {
    const { client } = await createConnectedPair();
    expect(() => client.closeChannel(99)).not.toThrow();
    await client.close();
  });

  it('closeChannel() terminates an existing channel', async () => {
    const { client, clientChannels } = await createConnectedPair({
      helloChannels: [{ name: 'c', reliable: true, ordered: true }],
    });
    const ch = clientChannels.get('c')!;
    expect(ch.state).toBe('open');
    client.closeChannel(ch.id);
    expect(client.channels.size).toBe(0);
    await client.close();
  });
});

describe('XumuxChannel', () => {
  it('channel state is "open" after connect', async () => {
    const { clientChannels, client } = await createConnectedPair({
      helloChannels: [{ name: 'ch', reliable: true, ordered: true }],
    });
    expect(clientChannels.get('ch')!.state).toBe('open');
    await client.close();
  });
});
