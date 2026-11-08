/**
 * pipe/1 constants, channel-request helpers and errors.
 * @see https://github.com/deftai/xumux/blob/main/spec/profiles/pipe.md
 * @module pipe/profile
 */

import { CloseCode } from '../codec/constants.js';
import type { ChannelRequest } from '../control/messages.js';
import type { ChannelDecision } from '../session.js';

/** The `metadata.profile` value of a pipe/1 channel. */
export const PIPE_PROFILE = 'pipe/1';

/** pipe/1 message types. */
export const PipeMessageType = {
  /** The next bytes of the stream. Never fragmented. */
  DATA: 0x01,
  /** The sender will send no more DATA (half-close). */
  EOF: 0x02,
  /** The sender's end failed. JSON `{code, reason?}`. */
  RESET: 0x03,
} as const;

/** CHANNEL_REJECT code for a pipe whose target cannot be reached. */
export const TARGET_UNREACHABLE = 4500;

/** A pipe/1 channel request (for `openChannel`). Without `target`, the responder chooses. */
export function pipeRequest(name: string, target?: string): Omit<ChannelRequest, 'id'> {
  const metadata: Record<string, unknown> = { profile: PIPE_PROFILE };
  if (target !== undefined) metadata.target = target;
  return { name, reliable: true, ordered: true, metadata };
}

/**
 * Whether `req` asks for a pipe/1 channel: `true`, or a 4010 refusal saying why not. Use it
 * in a channel policy that serves several profiles.
 */
export function checkPipeRequest(req: ChannelRequest): ChannelDecision {
  const profile = req.metadata?.profile;
  if (profile !== PIPE_PROFILE)
    return {
      code: CloseCode.CHANNEL_REFUSED,
      reason: `unsupported profile ${JSON.stringify(profile ?? null)}`,
    };
  if (!req.reliable || !req.ordered)
    return {
      code: CloseCode.CHANNEL_REFUSED,
      reason: 'pipe/1 channels must be reliable and ordered',
    };
  return true;
}

/** The target a pipe/1 request or channel names, if any. */
export function pipeTarget(req: { metadata?: Record<string, unknown> }): string | undefined {
  const t = req.metadata?.target;
  return typeof t === 'string' ? t : undefined;
}

/**
 * A pipe/1 RESET: one end of the pipe failed. A socket destroyed by the peer's RESET emits
 * this error with `remote: true`.
 */
export class PipeResetError extends Error {
  override readonly name = 'PipeResetError';
  constructor(
    readonly code: number,
    readonly reason?: string,
    /** True when the peer sent the RESET. */
    readonly remote = false,
  ) {
    super(`pipe reset${remote ? ' by peer' : ''} [${code}]${reason ? ` ${reason}` : ''}`);
  }
}
