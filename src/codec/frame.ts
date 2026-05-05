/**
 * Core Frame type for the xumux binary protocol.
 * @module codec/frame
 */

/**
 * A decoded xumux frame.
 *
 * Channel 0 is always the control channel.
 * Channels 1-254 are application channels assigned during HELLO/WELCOME or OPEN_CHANNEL.
 * Channel 255 is reserved.
 */
export interface Frame {
  /** Logical channel ID (0 = control). */
  channel: number;
  /**
   * Message type byte, scoped to the channel.
   * On channel 0: see ControlType constants.
   * On application channels: application-defined.
   */
  type: number;
  /**
   * Flags bitfield — FRAGMENT and FRAGMENT_END are the only application-visible flags.
   * EXTENDED_LENGTH is stripped during decode (it is a wire-level encoding detail).
   */
  flags: number;
  /** Frame payload bytes. */
  payload: Uint8Array;
}
