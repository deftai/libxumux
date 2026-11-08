# libxumux

TypeScript reference implementation of [xumux](https://github.com/deftai/xumux) **0.3**: named, typed channels with per-channel flow control and probes, over any transport.

> **Status**: implements the xumux 0.3.0-draft core ([spec/core.md](https://github.com/deftai/xumux/blob/v0.3.0-draft/spec/core.md)) and passes its [conformance vectors](tests/fixtures/xumux-vectors.json), plus the [pipe/1 profile](#pipes-pipe1). Not yet implemented: the `webrtc-multichannel/1` and `session-resume/1` extensions, and multi-stream QUIC. A server refuses a HELLO from a 0.2 client with `4006`, and a 0.2 server refuses this library's HELLO the same way.

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

| Option                         | Default   |                                                                                                                  |
| ------------------------------ | --------- | ---------------------------------------------------------------------------------------------------------------- |
| `maxFramePayload`              | 65536     | Largest frame payload this side accepts                                                                          |
| `maxMessageSize`               | 262144    | Largest reassembled message this side accepts                                                                    |
| `initialWindow`                | 262144    | Initial credit granted per channel (must be ≥ `maxMessageSize`)                                                  |
| `maxChannelWindow`             | 16 MiB    | How far auto-tuning may grow one channel's receive window. Negative, or ≤ `initialWindow`, disables it           |
| `connectionWindow`             | 64 MiB    | Stop growing windows once their sum on the connection reaches this                                               |
| `maxChannels`                  | 1024      | Channels open or being opened on the connection; the peer's opens past this get 4002                             |
| `maxControlQueue`              | 1 MiB     | Bytes of queued control/probe replies before a peer that isn't reading is disconnected with 4004 (negative: off) |
| `pingInterval` / `pingTimeout` | 30 / 10 s | Keepalive. `pingInterval: 0` disables it                                                                         |
| `fragmentSize`                 | 16384     | Fragment size for fair interleaving between channels                                                             |
| `handshakeTimeoutMs`           | 10000     | HELLO timeout (server) / WELCOME timeout (client)                                                                |
| `closeTimeoutMs`               | 5000      | How long CLOSE waits for the peer's CLOSE                                                                        |
| `onChannelRequest`             | refuse    | Policy for channels the peer opens                                                                               |

### Receive windows

A channel can have at most one window of data in flight, so with a fixed window it can't go faster than window ÷ round-trip time. The 256 KiB initial window allows about 2.6 MB/s at 100 ms. To keep long links fast, each side auto-tunes its own receive windows:

- When the peer uses a whole window in under two round trips, the window doubles. The extra credit goes out with the next WINDOW_UPDATE.
- The round-trip time comes from probes. When none has completed yet, one is sent.
- A channel's window grows up to `maxChannelWindow`. No window grows once the sum of all windows would pass `connectionWindow`. Initial windows are always granted, even on top of windows that already grew, so the total is at most `connectionWindow` + (open channels × `initialWindow`).
- Windows don't shrink. A channel's window leaves the total when it closes, though data it received that the application hasn't read yet stays buffered.
- The round-trip time is re-measured, at most once a second, while growth checks fail. Growth compares against the larger of the smoothed and latest RTT, so a path that gets slower is noticed straight away.
- `channel.receiveWindow` reports a channel's current window.

This is local policy, with no wire change, so it works with any xumux peer.

## Pipes (`pipe/1`)

`libxumux/pipe` implements the [pipe/1 profile](https://github.com/deftai/xumux/blob/main/spec/profiles/pipe.md): one bidirectional byte stream per channel, with half-close. A pipe is a Node `stream.Duplex` (`PipeSocket`), the analogue of a `net.Socket`, so `stream.pipeline()`, `http` and `tls` run over it. Node.js only.

```ts
import { dialPipe, PipeListener, pipeTarget } from 'libxumux/pipe';

// Responder: accept pipe/1 channels, checking each target against an allow-list.
const listener = new PipeListener({
  check: (req) => allowed.has(pipeTarget(req)) || { code: 4010, reason: 'target not allowed' },
});
const server = new XumuxServer(transport, { onChannelRequest: listener.policy });
listener.attach(server); // one listener can serve many sessions
listener.on('connection', (socket) => {
  const upstream = net.connect(/* from socket.target */);
  pipeline(socket, upstream).catch(() => {});
  pipeline(upstream, socket).catch(() => {});
});
// or: for await (const socket of listener) { ... }, or await listener.accept()

// Opener: resolves once the peer accepts (rejects with XumuxClosedError 4010/4500 if refused).
const socket = await dialPipe(client, { name: 'ssh', target: 'tcp:localhost:22' });
```

| pipe/1        | `PipeSocket`                                                                                                                                                                                                     |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DATA          | `write()`. Never fragmented: each message fits in one frame and the free credit. The callback runs once the bytes are on the transport.                                                                          |
| EOF           | `end()` sends it (half-close; reading goes on, `allowHalfOpen: true`). The peer's EOF ends the readable side.                                                                                                    |
| RESET         | `destroy(err)` or `resetAndDestroy(code, reason)` sends it. The peer's RESET destroys the socket with a `PipeResetError` (`code`, `reason`, `remote: true`), after the bytes that came before it have been read. |
| Other types   | The channel closes with 1002, and the socket is destroyed with an error.                                                                                                                                         |
| Channel close | 1000 once both EOFs are sent and received; 1001 after a RESET. `destroy()` sends EOF and closes at once, like closing a socket.                                                                                  |

Flow control follows the consumer, as pipe/1 requires: credit goes back to the peer only as bytes are consumed, so a consumer that stops stops the peer's writer once the window is full. How a byte counts as consumed depends on how the socket is read:

- `pipe()` and `stream.pipeline()` (which calls `pipe()`): when the destination has written the byte (its write callback), not when the byte reaches the destination's buffer. A slow destination slows the peer exactly, with no buffering in between.
- `socket.writeTo(dest)`: the same, as a promise that resolves at the peer's EOF and leaves `dest` open.
- `read()`, `'data'` and async iteration: when the byte leaves the readable side. Nothing is written downstream there, so taking the byte is the consumption.

A socket pipes to one destination at a time. With `setEncoding()`, `pipe()` falls back to `Readable.pipe()`, which returns credit as chunks are read. `setTimeout()`, `setNoDelay()`, `setKeepAlive()`, `ref()` and `unref()` exist for `net.Socket` compatibility.

HTTP over pipes: emit the listener's sockets into an `http.Server`, and give the client's agent a `createConnection` that dials a pipe:

```ts
const web = http.createServer(handler);
listener.on('connection', (socket) => web.emit('connection', socket));

const agent = new http.Agent({ keepAlive: true });
agent.createConnection = (_opts, cb) => {
  dialPipe(client, { name: 'http' }).then((s) => cb(null, s), cb);
};
http.get({ host: 'xumux', path: '/', agent }, (res) => {
  /* ... */
});
```

To serve pipe/1 next to other profiles, call `listener.policy(req)` from your own `onChannelRequest` for requests where `req.metadata?.profile === 'pipe/1'`; `checkPipeRequest(req)` tells whether a request is a valid pipe/1 request. The listener only takes pipe/1 channels.

Two channel primitives underpin this, and are available to any byte-stream profile: `channel.sendPrefix(type, bytes, signal?)` sends as much as fits in one unfragmented frame and the free credit, and resolves with the count; `channel.setManualCredit(true)` with `channel.returnCredit(n)` returns credit when the application has really used the bytes rather than when `receive()` hands them over.

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

WebRTC ([binding](https://github.com/deftai/xumux/blob/main/spec/bindings/webrtc.md)): the whole connection runs over one reliable, ordered DataChannel. Signalling is the application's. Create the DataChannel on both peers before the offer, then wrap it in an adapter:

```ts
import { WebRTCAdapter, createXumuxDataChannel } from 'libxumux/transports/webrtc';

const dc = createXumuxDataChannel(pc); // label/protocol "xumux", negotiated: true, id: 0, ordered
const client = new XumuxClient(new WebRTCAdapter(dc));
// ... exchange offer/answer and ICE candidates ...
await client.connect(); // HELLO waits for the DataChannel to open
```

- `WebRTCAdapter` takes any standard `RTCDataChannel`: from a browser, or from `node-datachannel/polyfill` in Node. It checks the label, protocol and reliability. A DataChannel that is still connecting is fine, and `adapter.opened` resolves once it opens.
- Backpressure: once `bufferedAmount` is above `highWaterMark` (default 1 MiB), sends wait for `bufferedamountlow` at half of it.
- `client.close()` runs the xumux CLOSE exchange and then closes the DataChannel. The PeerConnection is yours to close, since it may also carry media tracks. If the DataChannel closes unexpectedly, the session ends with `1001`.
- Frames must fit in one SCTP message. The default 16 KiB fragments always do. If you raise `fragmentSize`, cap it with `fragmentSizeFor(pc.sctp?.maxMessageSize, size)`.
- If the DataChannel hasn't opened about 10 s after the offer, the binding recommends falling back to WebSocket.

A custom transport implements `TransportAdapter`: `kind: 'message' | 'stream'`, `send(bytes): void | Promise<void>` (return a promise to apply backpressure), `receive(): AsyncIterable<Uint8Array>` and `close()`.

## Extensions

The core's extension hooks are exposed for code that implements an extension:

- **`extensionParams`** (core §3.8). The client sends parameters with `connect({ extensions, extensionParams })`; only keys for requested extensions are sent. The server decides per extension with `accept({ extensions, negotiateExtension })`, returning `true`, `{ params }` (sent in WELCOME) or `false` (leave the extension out). Keys for extensions that are not accepted are ignored on both sides and never sent in WELCOME. The client checks WELCOME parameters with `connect({ checkExtensionParams })`; `false` or a throw closes with `4001`. Both results carry `extensions` and the peer's `extensionParams` for the accepted extensions.
- **First frames** (core §3.1). `accept({ firstFrames: { [type]: handler } })` lets an extension claim a control type in `0x30`–`0x7F` as the first frame of a transport, instead of HELLO. The handler answers with a refusal (`{ code, reason }`, sent as CLOSE). With no handler for the type, the server closes with `1002`.

```ts
await server.accept({
  extensions: ['example/1'],
  negotiateExtension: (ext, params) =>
    (params as { level?: number } | undefined)?.level === 3 ? { params: { level: 2 } } : false,
});
```

## Migrating from 0.2

xumux 0.3 changes no frame layouts, but the version is `[0, 3, 0]` and 0.x minors are incompatible, so a server of either version refuses a HELLO of the other with CLOSE `4006` ([spec §3.5](https://github.com/deftai/xumux/blob/v0.3.0-draft/spec/core.md#35-version-negotiation)). A client that nonetheless receives a WELCOME of another version closes with `1002`. Upgrade both ends together. The API is backwards compatible; `ConnectResult` and `AcceptResult` gain `extensions` and `extensionParams`.

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

The behavioural conformance suite in [`deftai/xumux/conformance/behaviour`](https://github.com/deftai/xumux/tree/main/conformance/behaviour) runs against `conformance/adapter.mjs`, covering core and pipe/1 (needs `python3`):

```sh
XUMUX_DIR=path/to/xumux npm run test:conformance
```

`tests/fixtures/xumux-vectors.json` is copied from [`deftai/xumux/conformance/vectors.json`](https://github.com/deftai/xumux/tree/main/conformance). Update it when the spec changes.

## License

MIT
