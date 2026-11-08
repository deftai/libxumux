/**
 * Runs libxumux as the server under test for the xumux behavioural conformance suite
 * (deftai/xumux conformance/behaviour). Speaks xumux on stdin/stdout; its one argument is a
 * JSON config of limits. Channels behave according to their name: echo, sink, hold, refuse.
 * Imports from dist/ (run `npm run build` first).
 *
 *   python3 -I run.py --features core -- node conformance/adapter.mjs
 */
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const dir = path.dirname(fileURLToPath(import.meta.url));
const { XumuxServer, CloseCode } = await import(path.join(dir, '../dist/index.js'));
const { StdioAdapter } = await import(path.join(dir, '../dist/transports/stdio-adapter.js'));

const cfg = process.argv[2] ? JSON.parse(process.argv[2]) : {};

const behaviours = {
  async echo(ch) {
    for await (const m of ch) await ch.send(m.type, m.payload);
  },
  async sink(ch) {
    for await (const m of ch) void m;
  },
  async hold() {
    /* never read, so credit is never returned */
  },
};

const run = (ch) =>
  void behaviours[ch.name](ch).catch(() => {
    /* channel or connection closed */
  });

const server = new XumuxServer(new StdioAdapter(process.stdin, process.stdout), {
  pingInterval: 0,
  maxFramePayload: cfg.maxFramePayload,
  maxMessageSize: cfg.maxMessageSize,
  initialWindow: cfg.initialWindow,
  maxChannels: cfg.maxChannels,
  handshakeTimeoutMs: cfg.handshakeTimeoutMs,
  onChannelRequest: (req) =>
    req.name in behaviours ? true : { code: CloseCode.CHANNEL_REFUSED, reason: 'refused' },
});
server.addEventListener('channel', (e) => run(e.detail));
// Let pending stdout writes drain, then stop reading so the process can exit.
server.addEventListener('close', () => {
  process.stdin.destroy();
  setTimeout(() => process.exit(0), 2000).unref();
});
try {
  const { channels } = await server.accept();
  for (const ch of channels) run(ch);
} catch {
  /* handshake refused; the close handler ends the process */
}
