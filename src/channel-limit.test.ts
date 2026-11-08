import { describe, it, expect } from 'vitest';
import { XumuxClient, XumuxServer } from './session.js';
import { createInMemoryPair } from './transports/in-memory-adapter.js';
import { CloseCode, CONTROL_MAX_PAYLOAD, DEFAULT_MAX_CHANNELS } from './codec/constants.js';
import { rawPair } from '../tests/raw-peer.js';

const spec = (name: string) => ({ name, reliable: true, ordered: true });

async function pair(maxChannels?: number, initial = 0) {
  const [ct, st] = createInMemoryPair('message');
  const client = new XumuxClient(ct, { pingInterval: 0 });
  const server = new XumuxServer(st, {
    pingInterval: 0,
    onChannelRequest: () => true,
    ...(maxChannels !== undefined && { maxChannels }),
  });
  const [c] = await Promise.all([
    client.connect({ channels: Array.from({ length: initial }, (_, i) => spec(`h${i}`)) }),
    server.accept(),
  ]);
  return { client, server, hello: c };
}

describe('maxChannels', () => {
  it('refuses channels past the limit with 4002 and keeps the connection open', async () => {
    const { client, server } = await pair(3);
    const open = await Promise.all([0, 1, 2].map((i) => client.openChannel(spec(`c${i}`))));
    await expect(client.openChannel(spec('over'))).rejects.toMatchObject({
      code: CloseCode.CHANNEL_LIMIT,
    });
    expect(server.state).toBe('open');
    // Closing one frees its slot.
    await open[0]!.close();
    await open[0]!.closed;
    await expect(client.openChannel(spec('again'))).resolves.toBeDefined();
    await client.close();
  });

  it('applies to HELLO channels too', async () => {
    const { client, hello } = await pair(2, 3);
    expect(hello.channels).toHaveLength(2);
    expect(hello.rejected.map((r) => r.code)).toEqual([CloseCode.CHANNEL_LIMIT]);
    await client.close();
  });

  it('defaults to 1024 and must be a positive integer', () => {
    expect(DEFAULT_MAX_CHANNELS).toBe(1024);
    const [t] = createInMemoryPair('message');
    for (const bad of [0, -1, 1.5, Number.NaN])
      expect(() => new XumuxServer(t, { maxChannels: bad })).toThrow(RangeError);
  });

  it('still fits WELCOME when a full HELLO is mostly refused', async () => {
    // As many channels as fit in one HELLO, against maxChannels: 1.
    const { transport, raw } = rawPair();
    const server = new XumuxServer(transport, {
      pingInterval: 0,
      closeTimeoutMs: 50, // the raw peer never answers CLOSE
      maxChannels: 1,
      onChannelRequest: () => true,
    });
    const accepted = server.accept();
    const channels = [];
    for (let id = 1; ; id += 2) {
      channels.push({ id, name: '', reliable: true, ordered: true });
      const size = JSON.stringify({ version: [0, 2, 0], channels }).length;
      if (size > CONTROL_MAX_PAYLOAD - 100) {
        channels.pop();
        break;
      }
    }
    const welcome = await raw.helloAndWelcome({ channels });
    const body = JSON.parse(new TextDecoder().decode(welcome.payload)) as {
      channels: unknown[];
      rejected: { code: number; reason?: string }[];
    };
    expect(body.channels).toHaveLength(1);
    expect(body.rejected).toHaveLength(channels.length - 1);
    expect(body.rejected.every((r) => r.code === CloseCode.CHANNEL_LIMIT && !r.reason)).toBe(true);
    await accepted;
    await server.close();
  });
});
