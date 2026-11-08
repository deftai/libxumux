/**
 * libxumux: TypeScript reference implementation of xumux 0.3.
 *
 * @see https://github.com/deftai/xumux/blob/main/spec/core.md
 */

export type { TransportAdapter } from './types.js';
export { XUMUX_VERSION } from './types.js';

export { XumuxPeer, XumuxClient, XumuxServer, versionsCompatible } from './session.js';
export type {
  XumuxOptions,
  ConnectOptions,
  ConnectResult,
  AcceptOptions,
  AcceptResult,
  ChannelDecision,
  ChannelPolicy,
  ExtensionDecision,
  FirstFrame,
  FirstFrameDecision,
  FirstFrameHandler,
  CloseInfo,
  ChannelCloseEventDetail,
  DetachInfo,
} from './session.js';
export { BasePath } from './base-path.js';
export type { BasePathSink } from './base-path.js';

export { XumuxChannel } from './channel.js';
export type {
  ChannelState,
  ChannelMessage,
  ChannelProperties,
  ChannelCloseInfo,
  ProbeStats,
} from './channel.js';

export { XumuxProtocolError, XumuxClosedError } from './errors.js';

export {
  PROTOCOL_VERSION,
  FrameFlags,
  ControlType,
  ProbeType,
  CloseCode,
  CONTROL_CHANNEL,
  MAX_CHANNEL_ID,
  XMUX_MAGIC,
  DEFAULT_MAX_CHANNEL_WINDOW,
  DEFAULT_CONNECTION_WINDOW,
  DEFAULT_MAX_CONTROL_QUEUE,
  DEFAULT_MAX_CHANNELS,
  EXTENSION_CONTROL_TYPE_MIN,
  EXTENSION_CONTROL_TYPE_MAX,
} from './codec/constants.js';
export type { Frame } from './codec/frame.js';
export { encodeFrame } from './codec/encode-frame.js';
export { decodeFrame, validateFrame, StreamFramer } from './codec/decode-frame.js';

export type {
  Version,
  ExtensionParams,
  HelloMessage,
  WelcomeMessage,
  ChannelRequest,
  ChannelAccept,
  ChannelRefusal,
  CloseChannelMessage,
  WindowUpdateEntry,
  CloseMessage,
  ErrorMessage,
} from './control/messages.js';

export { createInMemoryPair } from './transports/in-memory-adapter.js';
