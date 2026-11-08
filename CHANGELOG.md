# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `conformance/adapter.mjs` runs libxumux as the server under test for the xumux behavioural conformance suite (deftai/xumux `conformance/behaviour`): `XUMUX_DIR=path/to/xumux npm run test:conformance`. All 48 core scenarios pass; the pipe/1 ones are skipped until pipe/1 support lands.

## [0.3.1] - 2026-11-08

### Security

- `maxChannels` (default 1024) limits the channels open or being opened on a connection. Past it, the peer's OPEN_CHANNEL or HELLO channels are refused with `4002`. Before, a peer could open channels until the IDs ran out, about 32,000, each with its own receive window.
- `maxControlQueue` (default 1 MiB) limits the queue of control and probe frames waiting to be sent. A peer that keeps sending frames that need replies but never reads them is disconnected with `4004`, instead of growing the queue without limit. A negative value turns the cap off.
- `DEFAULT_MAX_CHANNELS` and `DEFAULT_MAX_CONTROL_QUEUE` are exported.

## [0.3.0] - 2026-11-07

### Added

- Receive-window auto-tuning. A channel's window doubles when the peer uses a whole window in under two round trips, up to `maxChannelWindow` (default 16 MiB). No window grows once the sum of all windows would pass `connectionWindow` (default 64 MiB). A single channel at 40 ms RTT now runs at more than 4× the 6.6 MB/s that the fixed 256 KiB window allowed. `maxChannelWindow: -1` turns it off.
- Limits and the window options are validated when a session is created. Non-integer values, such as `NaN` from a malformed setting, throw `RangeError` instead of producing a zero WINDOW_UPDATE increment that the peer rejects.
- `XumuxChannel.receiveWindow`, and the exported `DEFAULT_MAX_CHANNEL_WINDOW` and `DEFAULT_CONNECTION_WINDOW` constants.

### Fixed

- A channel close now completes as soon as both CLOSE_CHANNELs are sent and received (core §5.4). Before, the reply side waited for its own send to finish. Under backpressure, the peer could reuse the ID first, and the connection then closed with `1002 OPEN_CHANNEL for ID n, which is in use` (#8).

## [0.2.0] - 2026-11-07

### Packaging

- Published to npm as `libxumux`, and as `deft-libxumux`, an alias package (`packages/deft-libxumux`) that re-exports `libxumux` and its `transports/*` subpaths.
- Added the MIT `LICENSE` file.

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
- The CLOSE reply is sent even when the peer shuts its write side right after its CLOSE (#5).

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

[Unreleased]: https://github.com/deftai/libxumux/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/deftai/libxumux/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/deftai/libxumux/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/deftai/libxumux/releases/tag/v0.2.0
