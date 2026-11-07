/**
 * E2E: stdio transport. The parent runs XumuxClient on a spawned child's pipes;
 * the child (child-server.mjs, built from dist/) runs XumuxServer on its own stdio and echoes.
 */
import { describe, it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { XumuxClient } from '../../src/session.js';
import { StdioAdapter } from '../../src/transports/stdio-adapter.js';
import { E2E_CHANNELS } from './helpers.js';

const childScript = path.join(path.dirname(fileURLToPath(import.meta.url)), 'child-server.mjs');

describe('stdio E2E', () => {
  it('client (parent) <-> server (child) over stdio pipes', async () => {
    const child = spawn(process.execPath, [childScript], { stdio: ['pipe', 'pipe', 'inherit'] });
    const exited = new Promise<void>((r) => child.once('exit', () => r()));
    const client = new XumuxClient(new StdioAdapter(child.stdout, child.stdin), {
      pingInterval: 0,
    });
    const { channels } = await client.connect({ channels: E2E_CHANNELS });
    expect(channels.map((c) => c.name)).toEqual(['pipe', 'frag']);

    const [pipe, frag] = channels as [(typeof channels)[0], (typeof channels)[0]];
    await pipe.send(3, new TextEncoder().encode('stdio hello'));
    const echo = await pipe.receive();
    expect([echo?.type, new TextDecoder().decode(echo?.payload)]).toEqual([3, 'stdio hello']);

    const large = new Uint8Array(200_000).fill(0xcd);
    const sending = frag.send(4, large);
    const back = await frag.receive();
    await sending;
    expect(back?.payload.length).toBe(200_000);

    expect(await pipe.ping()).toBeGreaterThanOrEqual(0);
    expect((await client.close(1000, 'stdio done')).code).toBe(1000);
    await exited;
  });
});
