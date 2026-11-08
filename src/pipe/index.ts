/**
 * The xumux pipe/1 profile for Node: one bidirectional byte stream, with half-close, per
 * channel, as a `stream.Duplex`.
 *
 * ```ts
 * import { dialPipe, PipeListener } from 'libxumux/pipe';
 *
 * // Opener
 * const socket = await dialPipe(client, { name: 'ssh', target: 'tcp:localhost:22' });
 * pipeline(process.stdin, socket, process.stdout, done);
 *
 * // Responder
 * const listener = new PipeListener({ check: (req) => allowed(pipeTarget(req)) || refusal });
 * const server = new XumuxServer(transport, { onChannelRequest: listener.policy });
 * listener.attach(server);
 * for await (const socket of listener) handle(socket);
 * ```
 *
 * @see https://github.com/deftai/xumux/blob/main/spec/profiles/pipe.md
 * @module pipe
 */

import type { XumuxPeer } from '../session.js';
import { pipeRequest } from './profile.js';
import { PipeSocket, type PipeSocketOptions } from './socket.js';

export {
  PIPE_PROFILE,
  PipeMessageType,
  TARGET_UNREACHABLE,
  PipeResetError,
  pipeRequest,
  checkPipeRequest,
  pipeTarget,
} from './profile.js';
export { PipeSocket, type PipeSocketOptions } from './socket.js';
export { PipeListener, type PipeListenerOptions } from './listener.js';

export interface DialPipeOptions {
  /** Channel name. Default `'pipe'`. */
  name?: string;
  /** Endpoint for the peer to connect the pipe to. Without it, the peer chooses. */
  target?: string;
  /** Options for the socket. */
  socket?: PipeSocketOptions;
}

/**
 * Open a pipe/1 channel on `peer` and resolve with it as a {@link PipeSocket} once the peer
 * accepts. Rejects with `XumuxClosedError` if the peer refuses (4010 for a target it doesn't
 * allow, 4500 for one it cannot reach).
 */
export async function dialPipe(
  peer: XumuxPeer,
  options: DialPipeOptions = {},
): Promise<PipeSocket> {
  const ch = await peer.openChannel(pipeRequest(options.name ?? 'pipe', options.target));
  return new PipeSocket(ch, options.socket);
}
