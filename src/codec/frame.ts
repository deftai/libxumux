/**
 * Core Frame type for the xumux binary protocol.
 * @module codec/frame
 */

/**
 * A decoded xumux frame.
 *
 * Channel 0x0000 is always the control channel.
 * Channels 1–65534 (0x0001–0xFFFE) are application channels assigned during HELLO/WELCOME or OPEN_CHANNEL.
 * Channel 0xFFFF is reserved.
 */
export interface Frame {
  /** Logical channel ID (0x0000 = control), big-endian on the wire. */
  channel: number;
  /**
   * Message type byte, scoped to the channel.
   * On channel 0: see ControlType constants.
   * On application channels: application-defined.
   */
  type: number;
  /** Flags bitfield — FRAGMENT and FRAGMENT_END are the only defined flags. Bit 0 is reserved. */
  flags: number;
  /** Frame payload bytes. */
  payload: Uint8Array;
}
