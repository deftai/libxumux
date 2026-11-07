/**
 * Shared helpers for E2E tests.
 */
import type { XumuxClient, XumuxServer, ConnectResult, AcceptResult } from '../../src/session.js';
import type { XumuxChannel } from '../../src/channel.js';

export interface E2EScenarioResults {
  handshakeCompleted: boolean;
  dataExchangeWorks: boolean;
  fragmentationWorks: boolean;
  dynamicChannelWorks: boolean;
  probeWorks: boolean;
  gracefulCloseWorks: boolean;
}

/** Server options for E2E: accept every channel, no keepalive. */
export const E2E_OPTIONS = { pingInterval: 0, onChannelRequest: () => true as const };

/** HELLO channels for E2E tests. */
export const E2E_CHANNELS = [
  { name: 'pipe', reliable: true, ordered: true },
  { name: 'frag', reliable: true, ordered: true },
];

const byName = (chs: XumuxChannel[], name: string): XumuxChannel | undefined =>
  chs.find((c) => c.name === name);

/** Run the standard scenarios over an already-connected client and server. */
export async function runProtocolScenarios(
  client: XumuxClient,
  server: XumuxServer,
  c: ConnectResult,
  s: AcceptResult,
  largePayloadSize = 200_000,
): Promise<E2EScenarioResults> {
  const results: E2EScenarioResults = {
    handshakeCompleted: c.channels.length === 2 && s.channels.length === 2,
    dataExchangeWorks: false,
    fragmentationWorks: false,
    dynamicChannelWorks: false,
    probeWorks: false,
    gracefulCloseWorks: false,
  };

  const [cp, sp] = [byName(c.channels, 'pipe')!, byName(s.channels, 'pipe')!];
  await cp.send(0x01, new TextEncoder().encode('hello E2E'));
  const m = await sp.receive();
  results.dataExchangeWorks =
    m?.type === 0x01 && new TextDecoder().decode(m.payload) === 'hello E2E';

  const [cf, sf] = [byName(c.channels, 'frag')!, byName(s.channels, 'frag')!];
  const large = new Uint8Array(largePayloadSize).map((_, i) => i & 0xff);
  const sent = cf.send(0x02, large);
  const big = await sf.receive();
  await sent;
  results.fragmentationWorks =
    big?.payload.length === largePayloadSize && big.payload.every((b, i) => b === (i & 0xff));

  const opened = new Promise<XumuxChannel>((r) =>
    server.addEventListener('channel', (e) => r((e as CustomEvent<XumuxChannel>).detail)),
  );
  const dyn = await client.openChannel({ name: 'dynamic', reliable: true, ordered: true });
  results.dynamicChannelWorks = (await opened).id === dyn.id && dyn.id % 2 === 1;

  results.probeWorks = (await dyn.ping()) >= 0 && (await client.ping()) >= 0;

  const info = await client.close(1000, 'e2e done');
  const serverInfo = await server.closed;
  results.gracefulCloseWorks = info.code === 1000 && serverInfo.code === 1000 && serverInfo.remote;
  return results;
}

export function expectAll(
  r: E2EScenarioResults,
  expect: (v: unknown) => { toEqual: (x: unknown) => void },
): void {
  expect(r).toEqual({
    handshakeCompleted: true,
    dataExchangeWorks: true,
    fragmentationWorks: true,
    dynamicChannelWorks: true,
    probeWorks: true,
    gracefulCloseWorks: true,
  });
}
