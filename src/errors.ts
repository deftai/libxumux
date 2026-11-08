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

/**
 * `XumuxClient.resume()` failed (session-resume/1). With `sessionLost`, the earlier session is
 * gone (CLOSE 4100–4103, 1002 or 4006 from the server, a RESUME too large to send, or
 * inconsistent counts in RESUMED): the client has closed, and the application falls back to a
 * new connection with HELLO. Otherwise the session is still detached and `resume()` may be
 * tried again on another transport, until `resumeTimeout`.
 */
export class XumuxResumeError extends XumuxClosedError {
  constructor(
    code: number,
    reason: string | undefined,
    remote: boolean,
    /** The earlier session is gone; reconnect with HELLO. */
    readonly sessionLost: boolean,
  ) {
    super(code, reason, remote);
  }
}
