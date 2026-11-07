# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### xumux 0.2 core (breaking)

Implements the xumux 0.2.0-draft core specification (deftai/xumux `spec/core.md`).

#### Wire format
- Magic bytes are `XMUX` (was `OMUX`). The WebSocket subprotocol and WebRTC DataChannel label and protocol are `xumux`.
- Flags: `MORE` (0x01) and `PROBE` (0x02) replace `FRAGMENT`/`FRAGMENT_END`.
- Channel IDs are chosen by the opener (client odd, server even). `requestId` is gone.
- CLOSE_CHANNEL is a two-step exchange, and IDs are reused only after both sides have closed.
- New WINDOW_UPDATE (0x07): per-channel credit flow control on reliable channels.
- PING/PONG are probe frames on any channel. PONG echoes a 4-byte token.
- Receiver-declared `maxFramePayload` / `maxMessageSize` / `initialWindow` replace `maxMessageSize`.
- WELCOME reports effective channel properties and can reject individual HELLO channels.

#### API
- New `XumuxChannel` with `send(type, payload)`, `receive()`, async iteration, `ping()`, `stats`, `close()` and `closed`. The `readable`/`writable` byte streams are removed.
- `XumuxClient.connect()` returns `{ welcome, channels, rejected }`, and `XumuxServer.accept()` returns `{ hello, channels }`.
- `onChannelRequest` policy for peer-opened channels (the default refuses with 1003). `accept({ authorize, extensions })`.
- Events: `'channel'`, `'channel-close'`, `'peer-error'`, `'close'`.
- `peer.ping()`, `peer.rtt`, `peer.stats`, `peer.closed`, `peer.channel(name)`.
- `XumuxProtocolError` and `XumuxClosedError`.

#### Behaviour
- HELLO timeout, version negotiation (0.x minors must match), auth refusal and limit validation are enforced.
- Malformed frames and control messages close with the codes the spec gives (4001, 4005, 1002, ...) instead of ending the receive loop.
- The outbound scheduler sends control and probe frames first, then interleaves channels round-robin in 16 KiB fragments.
- Backpressure from flow control and from the transport (`socket.write()`/`drain`, `bufferedAmount`, `writer.ready`).
- Keepalive pings only when the connection is idle. A silent peer is closed with 4008.
- Graceful close flushes queued data first, then waits for the peer's CLOSE.

#### Transports
- Adapters are byte pipes with `kind: 'message' | 'stream'`. Magic bytes, framing and size limits moved into the session's `StreamFramer`. This fixes `QuicAdapter`, which previously treated each read chunk as a whole frame.
- `createInMemoryPair(kind)` can also simulate a stream transport.

#### Tests
- Shared conformance vectors (valid and invalid) in `tests/fixtures/xumux-vectors.json`.
- E2E tests now also cover probes, and use 200 KB messages.

### Previously unreleased

### Changed
- **BREAKING**: Frame header upgraded from 6-byte variable to 8-byte fixed format `[Channel:2][Type:1][Flags:1][Length:4]`
- **BREAKING**: Channel ID field widened from 1 byte (max 254) to 2 bytes big-endian (max 65534)
- **BREAKING**: Payload Length field is always 4 bytes (max ~4GB); `EXTENDED_LENGTH` flag removed
- **BREAKING**: `XUMUX_VERSION` bumped to `0.2.0-draft`; HELLO/WELCOME now send version `[0, 2, 0]`

### Removed
- `FrameFlags.EXTENDED_LENGTH` constant
- `STANDARD_HEADER_SIZE`, `EXTENDED_HEADER_SIZE`, `HEADER_RESERVED_BYTE`, `MAX_STANDARD_PAYLOAD` constants

### Added
- `HEADER_SIZE` constant (8)
- Initial project scaffold (package.json, tsconfig.json, tsup, Vitest, ESLint, Prettier, Taskfile)
