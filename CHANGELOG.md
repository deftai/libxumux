# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### xumux 0.3 core (breaking on the wire)

Implements the xumux 0.3.0-draft core specification (deftai/xumux `spec/core.md` at `v0.3.0-draft`).

- **BREAKING**: `PROTOCOL_VERSION` is `[0, 3, 0]` and `XUMUX_VERSION` is `0.3.0-draft`. 0.x minors are incompatible (core §3.5): a 0.2 HELLO is refused with CLOSE `4006`, and a 0.2 WELCOME makes the client close with `1002`. Frame layouts are unchanged.
- `extensionParams` in HELLO and WELCOME (core §3.8), typed as `ExtensionParams` on `HelloMessage` and `WelcomeMessage`. A value that is not a JSON object closes with `4001`. Keys for extensions that are not listed or not accepted are ignored.
  - Client: `ConnectOptions.extensionParams` (only keys for requested extensions are sent) and `ConnectOptions.checkExtensionParams(extension, params)`, which closes with `4001` when it returns false or throws for an accepted extension.
  - Server: `AcceptOptions.negotiateExtension(extension, params, hello)` returns an `ExtensionDecision`: `true`, `{ params }` for WELCOME, or `false` to leave the extension out. WELCOME never carries a key for an extension it does not accept.
  - `ConnectResult` and `AcceptResult` gain `extensions` (accepted) and `extensionParams` (the peer's, for accepted extensions only).
  - HELLO and WELCOME, with their `extensionParams`, must fit in 65536 bytes: `connect()` rejects with `RangeError` before sending a larger HELLO, and a server whose WELCOME would be larger refuses the connection with `1001`.
- Extension-defined first frames (core §3.1): `AcceptOptions.firstFrames` maps a control type in `0x30`–`0x7F` to a `FirstFrameHandler`, which gets the `FirstFrame` (type and payload) that arrived instead of HELLO and returns a `FirstFrameDecision` refusal, sent as CLOSE. A first frame of any other type still closes with `1002`. `EXTENSION_CONTROL_TYPE_MIN` and `EXTENSION_CONTROL_TYPE_MAX` are exported.
- Conformance: `tests/fixtures/xumux-vectors.json` is the 0.3 `conformance/vectors.json`, and CI runs the behavioural suite from the spec's `v0.3.0-draft` tag (57 of 57 pass).
- The pipe/1 interop e2e test needs a libxumux-go that speaks 0.3.

### Changed

- Development tooling: vitest 5 and `@vitest/coverage-v8` 5 (were 3), tsup 9 (was 8), and in-range updates of the rest, which clears every `npm audit` advisory (all were in dev dependencies; the package itself has none). An npm override moves typescript-eslint's `minimatch` to 11.0.5. Building and testing now need Node 22.12 or later; the published package still supports Node 20. No library changes, apart from sharing one `ignore` rejection handler, which the newer coverage tool counts once instead of once per call site.

## [0.4.0] - 2026-11-08

### Added

- `libxumux/pipe`: the pipe/1 profile (one bidirectional byte stream, with half-close, per channel) as a Node `stream.Duplex`, so `stream.pipeline()`, `http` and `tls` run over a channel. `PipeSocket` maps `end()` to EOF, `destroy(err)` and `resetAndDestroy()` to RESET, the peer's RESET to a `PipeResetError`, and closes the channel after both EOFs (1000) or a RESET (1001). Unknown message types close the channel with 1002. Credit goes back to the peer only as the consumer reads, and DATA is never fragmented. `dialPipe()` opens a pipe; `PipeListener` accepts them, checking profile, reliable+ordered and an optional `check` hook for target allow-lists, with `'connection'` events, `accept()` and `for await`. Also `pipeRequest()`, `checkPipeRequest()`, `pipeTarget()` and the pipe/1 constants. Tested against libxumux-go's `pipe` package (set `LIBXUMUX_GO_DIR` to run the interop e2e test). `deft-libxumux` re-exports it as `deft-libxumux/pipe`.
- `XumuxChannel.sendPrefix(type, payload, signal?)`: sends as much of `payload` as fits in one unfragmented frame and the free send credit, and resolves with the number of bytes sent. An abort while it waits for credit sends nothing.
- `XumuxChannel.setManualCredit()` and `returnCredit()`: with manual credit, `receive()` no longer returns a message's credit; the application returns it once it has used the bytes.
- `conformance/adapter.mjs` runs libxumux as the server under test for the xumux behavioural conformance suite (deftai/xumux `conformance/behaviour`): `XUMUX_DIR=path/to/xumux npm run test:conformance`. It implements the `pipe-echo` behaviour too, and `test:conformance` now runs the pipe/1 scenarios: all 54 pass.
- `CloseCode.BUSY` (4011): at capacity, try again later (core §10, §3.7). A server at its connection limit refuses HELLO with it; a responder short of a shared resource rejects a channel with it.
- `PipeSocket.writeTo(dest)`: copies the readable side to `dest` until the peer's EOF, returning credit for each chunk only once `dest` has written it, as pipe/1 requires of a relay ("credit follows writes to the local end"). It is the promise-returning form of `pipe()`.
- CI (`.github/workflows/ci.yml`): typecheck, lint, format check, unit tests, build, e2e tests and the behavioural conformance suite on every push to `main` and every pull request.
- WebRTC binding: `createXumuxDataChannel(pc)` creates the negotiated `xumux` DataChannel, `checkXumuxDataChannel` checks a DataChannel against the binding (label and protocol `xumux`, ordered, reliable), and `fragmentSizeFor(pc.sctp?.maxMessageSize)` keeps outgoing frames within the peer's SCTP maximum message size.
- `WebRTCAdapter.opened` resolves when the DataChannel opens and rejects if it closes first.

### Changed

- `PipeSocket.pipe()`, and so `stream.pipeline()` from a `PipeSocket`, return credit for each chunk only once the destination has written it, instead of when the chunk reaches the destination's buffer (pipe/1: credit follows writes to the local end). This is a behaviour change. `pipe()` honours `{ end }` and `pause()`/`resume()`, emits `'pipe'`, and emits `'unpipe'` once however it stops. It leaves errors to the streams' listeners, as `Readable.pipe()` does. `unpipe()` stops the copy and leaves unread bytes unread, without credit. A socket pipes to one destination at a time. With an encoding set, `pipe()` is `Readable.pipe()`. `read()`, `'data'` and async iteration still return credit as bytes leave the readable side. The conformance adapter's `pipe-echo` is `socket.pipe(socket)`.
- `WebRTCAdapter` is typed against the standard `RTCDataChannel`, so a browser DataChannel (or the `node-datachannel/polyfill` one) can be passed straight in. A compile-time check keeps the two compatible.
- `WebRTCAdapter` accepts a DataChannel that is still connecting. Sends wait for it to open instead of throwing.
- Send backpressure now waits for the `bufferedamountlow` event, with the threshold at half of `highWaterMark`, instead of polling every 5 ms. It still polls for DataChannel wrappers that have no `bufferedAmountLowThreshold`.
- `WebRTCAdapter` throws a `TypeError` for a DataChannel that breaks the binding: the wrong label or protocol, unordered, or with `maxRetransmits` or `maxPacketLifeTime`.
- Blob messages, from a DataChannel whose `binaryType` cannot be changed, are accepted and kept in order.

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

[Unreleased]: https://github.com/deftai/libxumux/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/deftai/libxumux/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/deftai/libxumux/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/deftai/libxumux/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/deftai/libxumux/releases/tag/v0.2.0
