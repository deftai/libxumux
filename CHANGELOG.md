# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
