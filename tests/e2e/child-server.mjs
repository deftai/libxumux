/**
 * stdio E2E child: XumuxServer on process.stdin/stdout that echoes every message.
 * Imports from dist/ (run `npm run build` first).
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const dir = path.dirname(fileURLToPath(import.meta.url));
const { XumuxServer } = await import(path.join(dir, '../../dist/index.js'));
const { StdioAdapter } = await import(path.join(dir, '../../dist/transports/stdio-adapter.js'));

const echo = async (ch) => {
  try {
    for await (const m of ch) await ch.send(m.type, m.payload);
  } catch {
    /* channel closed */
  }
};

const server = new XumuxServer(new StdioAdapter(process.stdin, process.stdout), {
  pingInterval: 0,
  onChannelRequest: () => true,
});
server.addEventListener('channel', (e) => void echo(e.detail));
server.addEventListener('close', () => process.exit(0));
const { channels } = await server.accept();
for (const ch of channels) void echo(ch);
