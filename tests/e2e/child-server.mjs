/**
 * Child process stdio server for E2E testing.
 * Runs XumuxServer on process.stdin/stdout, echoes data back on all channels.
 * Imports from dist/ (requires `task build` before running E2E tests).
 */
import { fileURLToPath } from 'url';
import path from 'path';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Import from built output (dist/)
const { XumuxServer } = await import(path.join(__dirname, '../../dist/index.js'));
const { StdioAdapter } = await import(path.join(__dirname, '../../dist/transports/stdio-adapter.js'));

const transport = new StdioAdapter(process.stdin, process.stdout);
const server = new XumuxServer(transport, { pingInterval: 0 });

const channels = await server.accept();

// Echo mode: read from each channel and write back
for (const [, ch] of channels) {
  const reader = ch.readable.getReader();
  const writer = ch.writable.getWriter();
  // Pump in the background
  (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done || !value) break;
      await writer.write(value);
    }
  })().catch(() => {});
}

// Also handle dynamically opened channels
server.addEventListener('channel-open', (e) => {
  const ch = e.detail;
  const reader = ch.readable.getReader();
  const writer = ch.writable.getWriter();
  (async () => {
    for (;;) {
      const { value, done } = await reader.read();
      if (done || !value) break;
      await writer.write(value);
    }
  })().catch(() => {});
});

// Server close event — exit
server.addEventListener('close', () => {
  process.exit(0);
});
