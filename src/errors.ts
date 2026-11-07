/**
 * Error types.
 * @module errors
 */

/**
 * A violation of the protocol detected while processing input.
 *
 * `scope: 'connection'` closes the whole connection with `code`;
 * `scope: 'channel'` closes only `channel` with CLOSE_CHANNEL `code`;
 * `scope: 'transport'` closes the transport without sending anything (bad magic).
 */
export class XumuxProtocolError extends Error {
  override readonly name = 'XumuxProtocolError';
  constructor(
    readonly code: number,
    message: string,
    readonly scope: 'connection' | 'channel' | 'transport' = 'connection',
    readonly channel?: number,
  ) {
    super(message);
  }
}

/** The connection or a channel was closed (by either side) with a code and reason. */
export class XumuxClosedError extends Error {
  override readonly name = 'XumuxClosedError';
  constructor(
    readonly code: number,
    readonly reason?: string,
    /** True when the peer initiated the close. */
    readonly remote = false,
  ) {
    super(`closed [${code}]${reason ? ` ${reason}` : ''}`);
  }
}
