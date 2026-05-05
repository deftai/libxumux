/**
 * libxumux — TypeScript implementation of the xumux multiplexing protocol.
 *
 * @see https://xumux.org
 */

// Core types
export type { TransportAdapter } from './types.js';
export { XUMUX_VERSION, OMUX_MAGIC } from './types.js';

// Channel
export type { XumuxChannel, ChannelState } from './channel.js';

// Connection
export { XumuxClient, XumuxServer, XumuxPeer } from './connection.js';
export type { XumuxOptions, ChannelOpenEvent, CloseEvent, ErrorEvent } from './connection.js';

// Codec (public-facing constants and types)
export {
  FrameFlags,
  ControlType,
  CloseCode,
  CONTROL_CHANNEL,
  MAX_CHANNEL_ID,
} from './codec/constants.js';
export type { Frame } from './codec/frame.js';
export { encodeFrame } from './codec/encode-frame.js';
export { decodeFrame, FrameDecoder } from './codec/decode-frame.js';
export { fragment, Reassembler } from './codec/fragment.js';
export { getMagicBytes, validateMagicBytes } from './codec/magic.js';

// Control messages
export type {
  HelloMessage,
  WelcomeMessage,
  OpenChannelMessage,
  ChannelAckMessage,
  CloseChannelMessage,
  ChannelRejectMessage,
  PingPayload,
  PongPayload,
  CloseMessage,
  ErrorMessage,
} from './control/messages.js';
