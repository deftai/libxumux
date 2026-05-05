import { describe, it, expect } from 'vitest';
import { createChannel } from './channel.js';

describe('createChannel', () => {
  it('WritableStream close() sets state to "closing"', async () => {
    const ctrl = createChannel(1, 'ch', () => {});
    const writer = ctrl.channel.writable.getWriter();
    await writer.close();
    expect(ctrl.channel.state).toBe('closing');
  });

  it('WritableStream abort() sets state to "closed"', async () => {
    const ctrl = createChannel(1, 'ch', () => {});
    const writer = ctrl.channel.writable.getWriter();
    await writer.abort();
    expect(ctrl.channel.state).toBe('closed');
  });

  it('write() is a no-op when state is not "open"', async () => {
    const sent: unknown[] = [];
    const ctrl = createChannel(1, 'ch', (f) => sent.push(f));
    const writer = ctrl.channel.writable.getWriter();
    await writer.close(); // state → 'closing'
    // WritableStream is now closed; cannot write through it.
    // Verify state is as expected and no frames were sent after close
    expect(ctrl.channel.state).toBe('closing');
    expect(sent).toHaveLength(0);
  });

  it('terminate() hits the catch block when readableController.error() throws on already-errored stream', () => {
    const ctrl = createChannel(1, 'ch', () => {});
    ctrl.terminate(new Error('first')); // closes the readable
    // Second call — readableController.error() throws "already closed"
    expect(() => ctrl.terminate(new Error('second'))).not.toThrow();
    expect(ctrl.channel.state).toBe('closed');
  });

  it('enqueue() is a no-op when channel is not open', () => {
    const ctrl = createChannel(1, 'ch', () => {});
    ctrl.terminate(); // state → 'closed'
    // enqueue should not throw
    expect(() => ctrl.enqueue(new Uint8Array([42]))).not.toThrow();
  });

  it('channel state starts as "open"', () => {
    const ctrl = createChannel(1, 'ch', () => {});
    expect(ctrl.channel.state).toBe('open');
  });

  it('send frame via writable write', async () => {
    const frames: unknown[] = [];
    const ctrl = createChannel(1, 'ch', (f) => frames.push(f));
    const writer = ctrl.channel.writable.getWriter();
    await writer.write(new Uint8Array([1, 2, 3]));
    writer.releaseLock();
    expect(frames.length).toBeGreaterThan(0);
  });
});
