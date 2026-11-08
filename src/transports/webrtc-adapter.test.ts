import { describe, it, expect, vi } from 'vitest';
import {
  WebRTCAdapter,
  XUMUX_DATACHANNEL_INIT,
  checkXumuxDataChannel,
  createXumuxDataChannel,
  fragmentSizeFor,
  type DataChannelLike,
} from './webrtc-adapter.js';
import { XumuxProtocolError } from '../errors.js';
import { DEFAULT_FRAGMENT_SIZE } from '../codec/constants.js';

/** A standards-shaped RTCDataChannel stand-in, with bufferedamountlow support. */
class FakeRTCDataChannel extends EventTarget {
  label = 'xumux';
  protocol = 'xumux';
  ordered = true;
  maxRetransmits: number | null = null;
  maxPacketLifeTime: number | null = null;
  readyState: RTCDataChannelState = 'open';
  binaryType = 'blob';
  bufferedAmount = 0;
  bufferedAmountLowThreshold = 0;
  sent: Uint8Array[] = [];
  close = vi.fn(() => {
    this.readyState = 'closed';
    this.dispatchEvent(new Event('close'));
  });
  send(d: Uint8Array): void {
    if (this.readyState !== 'open') throw new Error('InvalidStateError');
    this.sent.push(d);
    this.bufferedAmount += d.length;
  }
  /** Drain the send buffer, firing bufferedamountlow on crossing the threshold, as browsers do. */
  drain(to: number): void {
    const was = this.bufferedAmount;
    this.bufferedAmount = to;
    if (was > this.bufferedAmountLowThreshold && to <= this.bufferedAmountLowThreshold)
      this.dispatchEvent(new Event('bufferedamountlow'));
  }
  open(): void {
    this.readyState = 'open';
    this.dispatchEvent(new Event('open'));
  }
  message(data: unknown): void {
    this.dispatchEvent(Object.assign(new Event('message'), { data }));
  }
}

function fake(over: Partial<FakeRTCDataChannel> = {}): FakeRTCDataChannel {
  return Object.assign(new FakeRTCDataChannel(), over);
}

const asDc = (f: FakeRTCDataChannel): DataChannelLike => f as unknown as DataChannelLike;

async function collect(it: AsyncIterable<Uint8Array>): Promise<number[][]> {
  const out: number[][] = [];
  for await (const x of it) out.push([...x]);
  return out;
}

const tick = (ms = 0): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe('WebRTC binding helpers', () => {
  it('createXumuxDataChannel uses the binding label and init', () => {
    const pc = { createDataChannel: vi.fn(() => 'dc') };
    expect(createXumuxDataChannel(pc)).toBe('dc');
    expect(pc.createDataChannel).toHaveBeenCalledWith('xumux', {
      negotiated: true,
      id: 0,
      ordered: true,
      protocol: 'xumux',
    });
    expect(XUMUX_DATACHANNEL_INIT).not.toHaveProperty('maxRetransmits');
    expect(XUMUX_DATACHANNEL_INIT).not.toHaveProperty('maxPacketLifeTime');
  });

  it.each([
    ['label', { label: 'chat' }],
    ['protocol', { protocol: '' }],
    ['unordered', { ordered: false }],
    ['maxRetransmits', { maxRetransmits: 0 }],
    ['maxPacketLifeTime', { maxPacketLifeTime: 500 }],
  ])('rejects a DataChannel with the wrong %s', (_n, over) => {
    expect(() => checkXumuxDataChannel(asDc(fake(over)))).toThrow(TypeError);
    expect(() => new WebRTCAdapter(asDc(fake(over)))).toThrow(TypeError);
  });

  it('accepts a valid DataChannel, and one that does not report its options', () => {
    expect(() => checkXumuxDataChannel(asDc(fake()))).not.toThrow();
    expect(() =>
      checkXumuxDataChannel({ readyState: 'open' } as unknown as DataChannelLike),
    ).not.toThrow();
  });

  it('fragmentSizeFor keeps frames within the SCTP max message size', () => {
    expect(fragmentSizeFor(262144)).toBe(DEFAULT_FRAGMENT_SIZE);
    expect(fragmentSizeFor(undefined, 1 << 20)).toBe(65536 - 8);
    expect(fragmentSizeFor(null, 1 << 20)).toBe(65536 - 8);
    expect(fragmentSizeFor(Infinity, 1 << 20)).toBe(65536 - 8);
    expect(fragmentSizeFor(8192)).toBe(8192 - 8);
  });
});

describe('WebRTCAdapter', () => {
  it('sets binaryType and the buffered-amount-low threshold', () => {
    const f = fake();
    new WebRTCAdapter(asDc(f), { highWaterMark: 1000 });
    expect(f.binaryType).toBe('arraybuffer');
    expect(f.bufferedAmountLowThreshold).toBe(500);
  });

  it('waits for bufferedamountlow, not polling, once over the high-water mark', async () => {
    const f = fake();
    const a = new WebRTCAdapter(asDc(f), { highWaterMark: 100 });
    expect(a.send(new Uint8Array(60))).toBeUndefined();
    const p = a.send(new Uint8Array(60));
    expect(p).toBeInstanceOf(Promise);
    let done = false;
    void p!.then(() => (done = true));
    // Drained to between the marks, without the event: still waiting.
    f.bufferedAmount = 70;
    await tick(20);
    expect(done).toBe(false);
    f.drain(10);
    await tick();
    expect(done).toBe(true);
  });

  it('a stale bufferedamountlow does not release a sender above the low-water mark', async () => {
    const f = fake();
    const a = new WebRTCAdapter(asDc(f), { highWaterMark: 100 });
    const p = a.send(new Uint8Array(150));
    let done = false;
    void p!.then(() => (done = true));
    f.dispatchEvent(new Event('bufferedamountlow'));
    await tick();
    expect(done).toBe(false);
    f.drain(0);
    await p;
  });

  it('releases a waiting sender when the DataChannel closes', async () => {
    const f = fake();
    const a = new WebRTCAdapter(asDc(f), { highWaterMark: 10 });
    const p = a.send(new Uint8Array(50));
    expect(p).toBeInstanceOf(Promise);
    f.close();
    await p;
  });

  it('falls back to polling when bufferedamountlow is unsupported', async () => {
    const f = fake();
    delete (f as Partial<FakeRTCDataChannel>).bufferedAmountLowThreshold;
    Object.defineProperty(f, 'bufferedAmountLowThreshold', { value: undefined });
    const a = new WebRTCAdapter(asDc(f), { highWaterMark: 10 });
    const p = a.send(new Uint8Array(50));
    f.bufferedAmount = 0;
    await p;
  });

  it('sends wait for a connecting DataChannel to open', async () => {
    const f = fake({ readyState: 'connecting' });
    const a = new WebRTCAdapter(asDc(f));
    const p = a.send(new Uint8Array([1]));
    expect(p).toBeInstanceOf(Promise);
    await tick();
    expect(f.sent).toHaveLength(0);
    f.open();
    await p;
    await a.opened;
    expect(f.sent).toHaveLength(1);
  });

  it('a send waiting for open fails if the DataChannel closes first', async () => {
    const f = fake({ readyState: 'connecting' });
    const a = new WebRTCAdapter(asDc(f));
    const p = a.send(new Uint8Array([1]));
    f.close();
    await expect(p).rejects.toThrow('closed before it opened');
    await expect(a.opened).rejects.toThrow('closed before it opened');
    expect(await collect(a.receive())).toEqual([]);
  });

  it('opened rejects for a DataChannel that is already closed', async () => {
    const a = new WebRTCAdapter(asDc(fake({ readyState: 'closed' })));
    await expect(a.opened).rejects.toThrow();
  });

  it('yields binary messages in order, Blobs included, then ends on close', async () => {
    const f = fake();
    const a = new WebRTCAdapter(asDc(f));
    const got = collect(a.receive());
    f.message(new Uint8Array([1]).buffer);
    f.message(new Blob([new Uint8Array([2, 2])]));
    f.message(new Uint8Array([9, 3, 9]).subarray(1, 2));
    f.message(new Blob([new Uint8Array([4])]));
    f.close();
    expect(await got).toEqual([[1], [2, 2], [3], [4]]);
  });

  it('treats a text message as a protocol error', async () => {
    const f = fake();
    const a = new WebRTCAdapter(asDc(f));
    const got = collect(a.receive());
    f.message('HELLO');
    await expect(got).rejects.toBeInstanceOf(XumuxProtocolError);
  });

  it('ends on error and detaches its listeners', async () => {
    const f = fake();
    const remove = vi.spyOn(f, 'removeEventListener');
    const a = new WebRTCAdapter(asDc(f));
    const got = collect(a.receive());
    f.dispatchEvent(new Event('error'));
    expect(await got).toEqual([]);
    expect(remove.mock.calls.map((c) => c[0]).sort()).toEqual([
      'bufferedamountlow',
      'close',
      'error',
      'message',
      'open',
    ]);
  });

  it('close() closes the DataChannel', () => {
    const f = fake();
    new WebRTCAdapter(asDc(f)).close();
    expect(f.close).toHaveBeenCalled();
  });
});
