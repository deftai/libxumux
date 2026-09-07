# libxumux

TypeScript implementation of the [xumux](https://xumux.org) transport-agnostic channel multiplexing protocol.

> **Status**: In development — implementing xumux v0.2.0-draft

## What is xumux?

xumux lets you run multiple independent typed channels over a single connection — WebSocket, WebRTC DataChannel, TCP, stdio, or QUIC — with a standard binary framing format and handshake.

## Install

```sh
npm install libxumux
```

## Quick start

```ts
import { XumuxClient } from 'libxumux';
import { WebSocketAdapter } from 'libxumux/transports/websocket';

// Connect as client over WebSocket
const transport = new WebSocketAdapter('wss://example.com/xumux');
const client = new XumuxClient(transport);

// Open a named channel
const channel = await client.openChannel('my-channel');

// Write data
const writer = channel.writable.getWriter();
await writer.write(new TextEncoder().encode('hello'));

// Read data
const reader = channel.readable.getReader();
const { value } = await reader.read();
console.log(new TextDecoder().decode(value));
```

## Architecture / I/O pipeline

Folders map to a Subspace-style stack: **ByteTransport → Framer → Codec → Connection**, with [VROOM](https://github.com/deftai/vroom) (or any app codec) sitting **above** `XumuxChannel` byte streams.

| Layer | Where |
| --- | --- |
| ByteTransport | `src/transports/*` (`TransportAdapter`) |
| Framer / mux codec (frame ↔ bytes) | `src/codec/*` |
| Control codec | `src/control/*` |
| Connection + channels | `src/connection.ts`, `src/channel.ts` |
| VROOM / app messages | out of scope — encode on `channel.readable` / `channel.writable` |

Adapters **attach** to an already-open underlay (they do not dial). `createInMemoryPair()` is the in-process / test analogue of a MessageTransport.

Full mapping, inbound/outbound path, and a layer diagram: [docs/io-stack.md](docs/io-stack.md).

## Transport adapters

Each transport is a separate entry point — import only what you use:

```ts
// Browser + Node.js
import { WebSocketAdapter } from 'libxumux/transports/websocket';
import { WebRTCAdapter }   from 'libxumux/transports/webrtc';
import { QuicAdapter }     from 'libxumux/transports/quic';

// Node.js only
import { TcpAdapter }   from 'libxumux/transports/tcp';
import { StdioAdapter } from 'libxumux/transports/stdio';
```

## Development

```sh
task build          # Build ESM + CJS + .d.ts
task test           # Run tests
task test:coverage  # Run tests + coverage (≥85% required)
task check          # lint + typecheck + coverage (pre-commit gate)
task fmt            # Format with Prettier
task clean          # Remove dist/ and coverage/
```

## Protocol

libxumux implements [xumux v0.2.0-draft](https://xumux.org):

- 8-byte fixed binary frame header: `[Channel:2][Type:1][Flags:1][Length:4]`
- 2-byte channel IDs (up to 65,534 application channels)
- 4-byte payload length (up to ~4GB per frame)
- Fragmentation / reassembly for HOL blocking prevention and transport MTU compliance
- Control channel (0x0000) with HELLO/WELCOME handshake, keepalive, graceful close
- Dynamic channel lifecycle (OPEN_CHANNEL / CHANNEL_ACK / CLOSE_CHANNEL)

## License

MIT
