/**
 * XumuxChannel — application channel abstraction.
 *
 * Each open channel exposes a `{ readable, writable }` Web Streams pair.
 * The readable stream delivers incoming data payloads; the writable stream
 * sends data payloads over the connection (with fragmentation when needed).
 *
 * @module channel
 */

import type { Frame } from './codec/frame.js';
import { fragment } from './codec/fragment.js';
import { MAX_STANDARD_PAYLOAD } from './codec/constants.js';

/** Channel lifecycle states. */
export type ChannelState = 'opening' | 'open' | 'closing' | 'closed';

/**
 * An open xumux application channel.
 *
 * Consumers read from `readable` and write to `writable`.
 * Closing the `writable` side does NOT automatically close the channel —
 * call `XumuxPeer.closeChannel(channel.id)` to send a `CLOSE_CHANNEL` frame.
 */
export interface XumuxChannel {
  /** Numeric channel ID (1-254). */
  readonly id: number;
  /** Channel name as requested during HELLO or OPEN_CHANNEL. */
  readonly name: string;
  /** Incoming data stream. */
  readonly readable: ReadableStream<Uint8Array>;
  /** Outgoing data stream. */
  readonly writable: WritableStream<Uint8Array>;
  /** Current channel state. */
  readonly state: ChannelState;
}

/** Internal channel controller (holds the ReadableStream controller reference). */
export interface ChannelController {
  channel: XumuxChannel;
  enqueue(data: Uint8Array): void;
  terminate(reason?: unknown): void;
}

/**
 * Create a new `XumuxChannel` bound to a send function.
 *
 * @param id - Assigned channel ID (1-254).
 * @param name - Channel name.
 * @param sendFrame - Function that encodes and sends a frame on the connection.
 * @param maxPayloadSize - Max payload per frame (fragments are used above this).
 * @returns A `ChannelController` that exposes both the public `XumuxChannel` and
 *   internal `enqueue` / `terminate` methods used by `XumuxPeer`.
 */
export function createChannel(
  id: number,
  name: string,
  sendFrame: (frame: Frame) => void,
  maxPayloadSize: number = MAX_STANDARD_PAYLOAD,
): ChannelController {
  let readableController!: ReadableStreamDefaultController<Uint8Array>;
  let state: ChannelState = 'open';

  const readable = new ReadableStream<Uint8Array>({
    start(controller) {
      readableController = controller;
    },
  });

  const writable = new WritableStream<Uint8Array>({
    write(chunk) {
      if (state !== 'open') return;
      const frames = fragment({ channel: id, type: 0x01, flags: 0, payload: chunk }, maxPayloadSize);
      for (const frame of frames) {
        sendFrame(frame);
      }
    },
    close() {
      state = 'closing';
    },
    abort() {
      state = 'closed';
    },
  });

  const xumuxChannel: XumuxChannel = {
    id,
    name,
    readable,
    writable,
    get state() {
      return state;
    },
  };

  return {
    channel: xumuxChannel,
    enqueue(data: Uint8Array) {
      if (state === 'open') {
        readableController.enqueue(data);
      }
    },
    terminate(reason?: unknown) {
      state = 'closed';
      try {
        readableController.error(reason ?? new Error('Channel closed'));
      } catch {
        // Already closed
      }
    },
  };
}
