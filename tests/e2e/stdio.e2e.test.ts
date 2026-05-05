/**
 * E2E tests: stdio transport.
 * Spawns a child Node.js process. Parent uses StdioAdapter on child's stdio pipes.
 * Child runs XumuxServer on its own stdin/stdout.
 */
import { describe, it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { XumuxClient } from '../../src/connection.js';
import { StdioAdapter } from '../../src/transports/stdio-adapter.js';
import { E2E_CHANNELS } from './helpers.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const childScript = path.join(__dirname, 'child-server.mjs');

describe('stdio E2E — handshake and data exchange via spawned child process', () => {
  it('client (parent) ↔ server (child) over stdio pipes', async () => {
    const child = spawn(process.execPath, [childScript], {
      stdio: ['pipe', 'pipe', 'inherit'],
    });

    const parentAdapter = new StdioAdapter(
      child.stdout!,
      child.stdin!,
    );
    const client = new XumuxClient(parentAdapter, { pingInterval: 0 });
    const clientCh = await client.connect({ channels: E2E_CHANNELS });

    // 1. Handshake
    expect(clientCh.has('pipe')).toBe(true);

    // 2. Data exchange
    const pipeCh = clientCh.get('pipe')!;
    const writer = pipeCh.writable.getWriter();
    await writer.write(new TextEncoder().encode('stdio hello'));
    writer.releaseLock();

    const reader = pipeCh.readable.getReader();
    // The child echoes back whatever it receives
    const { value } = await reader.read();
    reader.releaseLock();
    expect(new TextDecoder().decode(value)).toBe('stdio hello');

    // 3. Fragmentation — 70KB message
    const fragCh = clientCh.get('frag')!;
    const large = new Uint8Array(70_000).fill(0xcd);
    const fw = fragCh.writable.getWriter();
    await fw.write(large);
    fw.releaseLock();

    const fr = fragCh.readable.getReader();
    const chunks: Uint8Array[] = [];
    let total = 0;
    while (total < 70_000) {
      const { value: chunk, done } = await fr.read();
      if (done || !chunk) break;
      chunks.push(chunk);
      total += chunk.length;
    }
    fr.releaseLock();
    expect(total).toBe(70_000);

    // 4. Graceful close
    await client.close(1000, 'stdio done');

    await new Promise<void>((resolve) => child.once('exit', () => resolve()));
  });
});
