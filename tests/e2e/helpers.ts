/**
 * Shared helpers for E2E tests.
 */
import type { XumuxClient, XumuxServer } from '../../src/connection.js';
import type { XumuxChannel } from '../../src/channel.js';

/** The 5 protocol scenarios every transport E2E test must exercise. */
export interface E2EScenarioResults {
  handshakeCompleted: boolean;
  dataExchangeWorks: boolean;
  fragmentationWorks: boolean;
  dynamicChannelWorks: boolean;
  gracefulCloseWorks: boolean;
}

/**
 * Run the 5 standard E2E protocol scenarios over an already-connected client+server.
 *
 * @param client - Connected XumuxClient (connect() already called)
 * @param server - Connected XumuxServer (accept() already called)
 * @param clientChannels - Channels returned by client.connect()
 * @param serverChannels - Channels returned by server.accept()
 * @param largePayloadSize - Size in bytes for fragmentation test (default 70000, > 65535 for EXT frames)
 */
export async function runProtocolScenarios(
  client: XumuxClient,
  server: XumuxServer,
  clientChannels: Map<string, XumuxChannel>,
  serverChannels: Map<string, XumuxChannel>,
  largePayloadSize = 70_000,
): Promise<E2EScenarioResults> {
  const results: E2EScenarioResults = {
    handshakeCompleted: false,
    dataExchangeWorks: false,
    fragmentationWorks: false,
    dynamicChannelWorks: false,
    gracefulCloseWorks: false,
  };

  // 1. Handshake: checked implicitly — if we're here, it completed
  results.handshakeCompleted = true;

  // 2. Data exchange on a pre-negotiated channel
  const clientPipe = clientChannels.get('pipe');
  const serverPipe = serverChannels.get('pipe');
  if (clientPipe && serverPipe) {
    const payload = new TextEncoder().encode('hello E2E');
    const writer = clientPipe.writable.getWriter();
    await writer.write(payload);
    writer.releaseLock();

    const reader = serverPipe.readable.getReader();
    const { value } = await reader.read();
    reader.releaseLock();

    results.dataExchangeWorks = new TextDecoder().decode(value) === 'hello E2E';
  }

  // 3. Fragmentation: send a large message (>65535 bytes for EXT header path)
  const clientFrag = clientChannels.get('frag');
  const serverFrag = serverChannels.get('frag');
  if (clientFrag && serverFrag) {
    const large = new Uint8Array(largePayloadSize).fill(0xab);
    const writer = clientFrag.writable.getWriter();
    await writer.write(large);
    writer.releaseLock();

    // Read all chunks until we have the full payload
    const reader = serverFrag.readable.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (total < largePayloadSize) {
      const { value, done } = await reader.read();
      if (done || !value) break;
      chunks.push(value);
      total += value.length;
    }
    reader.releaseLock();

    const assembled = new Uint8Array(total);
    let off = 0;
    for (const chunk of chunks) { assembled.set(chunk, off); off += chunk.length; }

    results.fragmentationWorks = total === largePayloadSize && assembled.every((b) => b === 0xab);
  }

  // 4. Dynamic channel (OPEN_CHANNEL / CHANNEL_ACK)
  let dynamicChannelOpenedOnServer = false;
  server.addEventListener('channel-open', () => { dynamicChannelOpenedOnServer = true; });
  const dynamicCh = await client.openChannel({ name: 'dynamic', reliable: true, ordered: true });
  results.dynamicChannelWorks = dynamicCh.name === 'dynamic' && dynamicChannelOpenedOnServer;

  // 5. Graceful close
  const closeReceived = new Promise<boolean>((resolve) => {
    server.addEventListener('close', () => resolve(true));
    setTimeout(() => resolve(false), 5000);
  });
  await client.close(1000, 'e2e done');
  results.gracefulCloseWorks = await closeReceived;

  return results;
}

/** Default HELLO channel list for E2E tests. */
export const E2E_CHANNELS = [
  { name: 'pipe', reliable: true, ordered: true },
  { name: 'frag', reliable: true, ordered: true },
];
