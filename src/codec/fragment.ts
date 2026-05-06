/**
 * xumux fragmentation and reassembly.
 *
 * Fragmentation splits large messages into frames that fit within the transport MTU.
 * Reassembly accumulates FRAGMENT frames until the FRAGMENT_END flag arrives.
 *
 * @module codec/fragment
 */

import { FrameFlags, DEFAULT_MAX_MESSAGE_SIZE } from './constants.js';
import type { Frame } from './frame.js';

/**
 * Split a Frame into one or more fragment frames if its payload exceeds `maxPayloadSize`.
 *
 * If the payload fits in a single frame, a single-element array is returned unchanged.
 *
 * **Restriction**: MUST NOT fragment on channels declared as `unordered` or `unreliable`.
 * MUST NOT fragment on QUIC datagrams.
 * Only one fragmented message may be in-flight per channel at a time.
 *
 * @param frame - The frame to potentially fragment.
 * @param maxPayloadSize - Maximum payload bytes per fragment. Defaults to 65535.
 * @returns One or more frames.
 */
export function fragment(
  frame: Frame,
  maxPayloadSize: number = DEFAULT_MAX_MESSAGE_SIZE,
): Frame[] {
  if (frame.payload.length <= maxPayloadSize) {
    return [frame];
  }

  const fragments: Frame[] = [];
  let offset = 0;

  while (offset < frame.payload.length) {
    const end = Math.min(offset + maxPayloadSize, frame.payload.length);
    const isLast = end >= frame.payload.length;
    const flags = FrameFlags.FRAGMENT | (isLast ? FrameFlags.FRAGMENT_END : 0);

    fragments.push({
      channel: frame.channel,
      type: frame.type,
      flags,
      payload: frame.payload.slice(offset, end),
    });

    offset = end;
  }

  return fragments;
}

/**
 * Stateful per-channel fragment reassembler.
 *
 * Create one `Reassembler` per channel. Call `push(frame)` for each incoming frame:
 * - Non-fragment frames are passed through immediately (returns their payload).
 * - Fragment frames accumulate until `FRAGMENT_END` arrives (returns assembled payload).
 * - Returns `null` if more fragments are still expected.
 *
 * Throws a protocol-error `Error` on:
 * - A non-fragment frame interleaved with an in-progress fragmented sequence.
 * - A `FRAGMENT_END` frame with no preceding `FRAGMENT`.
 * - A fragment whose channel or type differs from the current sequence.
 */
export class Reassembler {
  private accumulator: Uint8Array[] = [];
  private inProgress = false;
  private expectedChannel: number | null = null;
  private expectedType: number | null = null;

  /** Push a frame into the reassembler. Returns the reassembled payload or `null`. */
  push(frame: Frame): Uint8Array | null {
    const isFragment = (frame.flags & FrameFlags.FRAGMENT) !== 0;
    const isFragmentEnd = (frame.flags & FrameFlags.FRAGMENT_END) !== 0;

    if (!isFragment && !isFragmentEnd) {
      // Normal non-fragmented frame
      if (this.inProgress) {
        throw new Error(
          'Protocol error: non-fragment frame interleaved with in-progress fragmented message',
        );
      }
      return frame.payload;
    }

    if (!isFragment && isFragmentEnd) {
      // FRAGMENT_END alone (without FRAGMENT) is only valid if we're already assembling
      if (!this.inProgress) {
        throw new Error('Protocol error: FRAGMENT_END received without a preceding FRAGMENT');
      }
    }

    if (!this.inProgress) {
      // Start of a new fragmented sequence
      this.inProgress = true;
      this.expectedChannel = frame.channel;
      this.expectedType = frame.type;
    } else {
      if (frame.channel !== this.expectedChannel || frame.type !== this.expectedType) {
        throw new Error(
          `Protocol error: fragment channel/type mismatch. Expected ch=${String(
            this.expectedChannel,
          )} type=${String(this.expectedType)}, got ch=${frame.channel} type=${frame.type}`,
        );
      }
    }

    this.accumulator.push(frame.payload);

    if (isFragmentEnd) {
      return this.flush();
    }

    return null;
  }

  /** Whether a fragment sequence is currently being assembled. */
  get isAssembling(): boolean {
    return this.inProgress;
  }

  /** Discard any partially assembled message and reset to idle state. */
  abort(): void {
    this.accumulator = [];
    this.inProgress = false;
    this.expectedChannel = null;
    this.expectedType = null;
  }

  private flush(): Uint8Array {
    const totalLength = this.accumulator.reduce((sum, chunk) => sum + chunk.length, 0);
    const result = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of this.accumulator) {
      result.set(chunk, offset);
      offset += chunk.length;
    }
    this.accumulator = [];
    this.inProgress = false;
    this.expectedChannel = null;
    this.expectedType = null;
    return result;
  }
}
