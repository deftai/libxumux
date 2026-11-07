/**
 * Core Frame type.
 * @module codec/frame
 */

/** A decoded xumux frame. */
export interface Frame {
  /** Channel ID. 0 = control, 0xFFFF reserved. */
  channel: number;
  /** Message type. Control type on channel 0, probe type when PROBE is set, else application-defined. */
  type: number;
  /** Flag bits: MORE (0x01), PROBE (0x02). Others reserved. */
  flags: number;
  /** Payload bytes. */
  payload: Uint8Array;
}
