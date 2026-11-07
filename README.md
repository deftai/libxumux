# libxumux

TypeScript reference implementation of [xumux](https://github.com/deftai/xumux) **0.2**: named, typed channels with per-channel flow control and probes, over any transport.

> **Status**: implements the xumux 0.2.0-draft core ([spec/core.md](https://github.com/deftai/xumux/blob/main/spec/core.md)) and passes its [conformance vectors](tests/fixtures/xumux-vectors.json). Not yet implemented: the `webrtc-multichannel/1` and `session-resume/1` extensions, and multi-stream QUIC.

## Install

```sh
npm install libxumux
```

## Quick start

```ts
import { XumuxClient, XumuxServer } from 'libxumux';
import { WebSocketAdapter } from 'libxumux/transports/websocket';

// Server (Node, with the `ws` package): accept every channel the client asks for.
wss.on('connection', async (ws) => {
  const server = new XumuxServer(new WebSocketAdapter(ws), { onChannelRequest: () => true });
  const { channels } = await server.accept({
    authorize: (hello) =>
      hello.auth?.token === secret ? undefined : { code: 4000, reason: 'bad token' },
  });
  for (const ch of channels) {
    for await (const msg of ch) await ch.send(msg.type, msg.payload); // echo
  }
});

// Client (browser or Node)
const ws = new WebSocket('wss://example.com/xumux', 'xumux');
await new Promise((r) => (ws.onopen = r));
const client = new XumuxClient(new WebSocketAdapter(ws));
const {
  channels: [data],
} = await client.connect({
  auth: { token },
  channels: [{ name: 'data', reliable: true, ordered: true }],
});

await data.send(0x01, new TextEncoder().encode('hello')); // type byte + payload
const reply = await data.receive(); // { type, payload } | null
const rtt = await data.ping(); // round-trip time on this channel, ms

const files = await client.openChannel({
  name: 'file',
  reliable: true,
  ordered: true,
  metadata: { path: '/tmp/x' },
});
await files.close();
await client.close();
```

## Concepts

- **Messages, not byte streams.** `channel.send(type, payload)` sends one message. The type byte is yours, and large payloads are fragmented and reassembled for you. Read with `await channel.receive()` or `for await (const msg of channel)`.
- **Backpressure.** `send()` resolves once the frames are handed to the transport. It waits when the peer's flow-control window is used up, or when the transport's buffer is full. Reading a message returns its credit to the sender, so a channel nobody reads from eventually pauses its sender. Other channels are not affected.
- **Peer-opened channels need a policy.** Pass `onChannelRequest: (req) => true | { code, reason }`. Without it, every channel the peer opens (including HELLO channels on a server) is refused with `1003`. Accepted dynamic channels arrive on the `'channel'` event.
- **Probes.** `channel.ping()` and `peer.ping()` measure round-trip time. `channel.stats` and `peer.stats` give `{ rtt, srtt, rttvar, probesSent, probesLost }`. Keepalive pings only when the connection is idle (`pingInterval` and `pingTimeout`, in seconds; the server's values apply).
- **Closing.** `channel.close(code?, reason?)` and `peer.close(code?, reason?)` resolve when the close is complete. `peer.closed` and `channel.closed` are promises. When a channel closes with a code other than `1000`, a pending `receive()` rejects with `XumuxClosedError`.
- **Events** (`addEventListener`): `'channel'`, `'channel-close'`, `'peer-error'` (the peer sent ERROR) and `'close'`.

### Options

| Option                         | Default   |                                                                                                        |
| ------------------------------ | --------- | ------------------------------------------------------------------------------------------------------ |
| `maxFramePayload`              | 65536     | Largest frame payload this side accepts                                                                |
| `maxMessageSize`               | 262144    | Largest reassembled message this side accepts                                                          |
| `initialWindow`                | 262144    | Initial credit granted per channel (must be ≥ `maxMessageSize`)                                        |
| `maxChannelWindow`             | 16 MiB    | How far auto-tuning may grow one channel's receive window. Negative, or ≤ `initialWindow`, disables it |
| `connectionWindow`             | 64 MiB    | Stop growing windows once their sum on the connection reaches this                                     |
| `pingInterval` / `pingTimeout` | 30 / 10 s | Keepalive. `pingInterval: 0` disables it                                                               |
| `fragmentSize`                 | 16384     | Fragment size for fair interleaving between channels                                                   |
| `handshakeTimeoutMs`           | 10000     | HELLO timeout (server) / WELCOME timeout (client)                                                      |
| `closeTimeoutMs`               | 5000      | How long CLOSE waits for the peer's CLOSE                                                              |
| `onChannelRequest`             | refuse    | Policy for channels the peer opens                                                                     |

### Receive windows

A channel can have at most one window of data in flight, so with a fixed window it can't go faster than window ÷ round-trip time. The 256 KiB initial window allows about 2.6 MB/s at 100 ms. To keep long links fast, each side auto-tunes its own receive windows:

- When the peer uses a whole window in under two round trips, the window doubles. The extra credit goes out with the next WINDOW_UPDATE.
- The round-trip time comes from probes. When none has completed yet, one is sent.
- A channel's window grows up to `maxChannelWindow`. No window grows once the sum of all windows would pass `connectionWindow`. Initial windows are always granted, even on top of windows that already grew, so the total is at most `connectionWindow` + (open channels × `initialWindow`).
- Windows don't shrink. A channel's window leaves the total when it closes, though data it received that the application hasn't read yet stays buffered.
- The round-trip time is re-measured, at most once a second, while growth checks fail. Growth compares against the larger of the smoothed and latest RTT, so a path that gets slower is noticed straight away.
- `channel.receiveWindow` reports a channel's current window.

This is local policy, with no wire change, so it works with any xumux peer.

## Transports

Adapters are plain byte pipes that declare whether they carry whole messages or a byte stream. The session handles magic bytes, framing, size limits and backpressure for all of them.

```ts
// Browser + Node.js
import { WebSocketAdapter } from 'libxumux/transports/websocket'; // subprotocol "xumux"
import { WebRTCAdapter, XUMUX_DATACHANNEL_INIT } from 'libxumux/transports/webrtc';
import { QuicAdapter } from 'libxumux/transports/quic'; // experimental

// Node.js only
import { TcpAdapter } from 'libxumux/transports/tcp'; // also Unix sockets
import { StdioAdapter } from 'libxumux/transports/stdio';

// Tests and in-process use
import { createInMemoryPair } from 'libxumux';
```

WebRTC: create the DataChannel with `pc.createDataChannel('xumux', XUMUX_DATACHANNEL_INIT)` on both peers. That sets `negotiated: true, id: 0`.

A custom transport implements `TransportAdapter`: `kind: 'message' | 'stream'`, `send(bytes): void | Promise<void>` (return a promise to apply backpressure), `receive(): AsyncIterable<Uint8Array>` and `close()`.

## Migrating from 0.1

xumux 0.2 is a breaking wire change. See the [spec's change table](https://github.com/deftai/xumux/blob/main/spec/core.md#appendix-a-changes-from-01) and [CHANGELOG.md](CHANGELOG.md). The API changes most code will hit:

- `connect()` and `accept()` return `{ channels: XumuxChannel[], ... }` instead of a `Map`.
- Channels expose `send(type, payload)` and `receive()` instead of `readable`/`writable` byte streams.
- Servers must set `onChannelRequest` to accept channels.
- Transport adapters no longer handle magic bytes or framing; custom adapters must declare `kind`.

## Development

```sh
task build          # ESM + CJS + .d.ts
task test           # unit + conformance tests
task test:coverage  # with coverage thresholds
task e2e            # WebSocket, TCP, stdio, WebRTC (needs `task build` first, for stdio)
task fmt            # Prettier
```

`tests/fixtures/xumux-vectors.json` is copied from [`deftai/xumux/conformance/vectors.json`](https://github.com/deftai/xumux/tree/main/conformance). Update it when the spec changes.

## License

MIT
