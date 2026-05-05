import { describe, it, expect } from 'vitest';
import { PassThrough } from 'node:stream';
import { StdioAdapter } from './stdio-adapter.js';
import { encodeFrame } from '../codec/encode-frame.js';
import { decodeFrame } from '../codec/decode-frame.js';
import { getMagicBytes } from '../codec/magic.js';

/** Creates a pair of PassThrough streams for stdio testing. */
function makeMockStdio(): {
  input: PassThrough;
  output: PassThrough;
  writtenToOutput: () => Buffer;
} {
  const input = new PassThrough();
  const output = new PassThrough();
  const chunks: Buffer[] = [];
  output.on('data', (c: Buffer) => chunks.push(c));
  return {
    input,
    output,
    writtenToOutput: () => Buffer.concat(chunks),
  };
}

describe('StdioAdapter', () => {
  it('send() prepends OMUX magic bytes on the first call only', () => {
    const { input, output, writtenToOutput } = makeMockStdio();
    const adapter = new StdioAdapter(input, output);
    const frame = encodeFrame({ channel: 0, type: 1, flags: 0, payload: new Uint8Array([1]) });

    adapter.send(frame);
    adapter.send(frame);

    const written = writtenToOutput();
    // First 4 bytes = magic, then frame, then frame again
    expect(new Uint8Array(written.subarray(0, 4))).toEqual(getMagicBytes());
    expect(written.length).toBe(4 + frame.length + frame.length);
  });

  it('receive() validates OMUX magic and yields frames', async () => {
    const { input, output } = makeMockStdio();
    const adapter = new StdioAdapter(input, output);

    const frameBytes = encodeFrame({ channel: 1, type: 2, flags: 0, payload: new Uint8Array([55]) });
    const magic = getMagicBytes();

    // Write magic + frame into input
    const combined = Buffer.concat([Buffer.from(magic), Buffer.from(frameBytes)]);
    input.write(combined);
    input.end();

    const frames: Uint8Array[] = [];
    for await (const f of adapter.receive()) {
      frames.push(f);
    }

    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).channel).toBe(1);
    expect(decodeFrame(frames[0]!).payload[0]).toBe(55);
  });

  it('receive() handles magic split across multiple writes', async () => {
    const { input, output } = makeMockStdio();
    const adapter = new StdioAdapter(input, output);
    const frameBytes = encodeFrame({ channel: 3, type: 1, flags: 0, payload: new Uint8Array([7]) });
    const magic = getMagicBytes();

    // Write magic in pieces
    input.write(Buffer.from(magic.slice(0, 2)));
    input.write(Buffer.from(magic.slice(2, 4)));
    input.write(Buffer.from(frameBytes));
    input.end();

    const frames: Uint8Array[] = [];
    for await (const f of adapter.receive()) frames.push(f);
    expect(frames).toHaveLength(1);
    expect(decodeFrame(frames[0]!).channel).toBe(3);
  });

  it('receive() throws when magic bytes are invalid', async () => {
    const { input, output } = makeMockStdio();
    const adapter = new StdioAdapter(input, output);

    input.write(Buffer.from([0x00, 0x00, 0x00, 0x00]));
    input.end();

    await expect(async () => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      for await (const _frame of adapter.receive()) { /* consume */ }
    }).rejects.toThrow();
  });

  it('close() calls output.end()', () => {
    const { input, output } = makeMockStdio();
    const adapter = new StdioAdapter(input, output);
    expect(output.writableEnded).toBe(false);
    adapter.close();
    // writableEnded is set synchronously when end() is called
    expect(output.writableEnded).toBe(true);
  });
});
