import { describe, it, expect } from 'vitest';
import { getMagicBytes, validateMagicBytes } from './magic.js';

describe('getMagicBytes', () => {
  it('returns 4 bytes', () => {
    expect(getMagicBytes()).toHaveLength(4);
  });

  it('spells OMUX', () => {
    const bytes = getMagicBytes();
    expect(bytes[0]).toBe(0x4f); // O
    expect(bytes[1]).toBe(0x4d); // M
    expect(bytes[2]).toBe(0x55); // U
    expect(bytes[3]).toBe(0x58); // X
  });

  it('returns a fresh copy each time (not shared reference)', () => {
    const a = getMagicBytes();
    const b = getMagicBytes();
    expect(a).not.toBe(b);
  });
});

describe('validateMagicBytes', () => {
  it('accepts correct OMUX magic', () => {
    expect(() => validateMagicBytes(getMagicBytes())).not.toThrow();
  });

  it('accepts magic bytes embedded in a longer buffer', () => {
    const buf = new Uint8Array([0x4f, 0x4d, 0x55, 0x58, 0x00, 0x01]);
    expect(() => validateMagicBytes(buf)).not.toThrow();
  });

  it('rejects wrong magic bytes', () => {
    expect(() => validateMagicBytes(new Uint8Array([0x00, 0x00, 0x00, 0x00]))).toThrow();
  });

  it('rejects OMUX with wrong case', () => {
    // Lowercase 'omux' = 0x6F 0x6D 0x75 0x78
    expect(() =>
      validateMagicBytes(new Uint8Array([0x6f, 0x6d, 0x75, 0x78])),
    ).toThrow();
  });

  it('throws on buffer shorter than 4 bytes', () => {
    expect(() => validateMagicBytes(new Uint8Array([0x4f, 0x4d, 0x55]))).toThrow(
      /need 4 bytes/,
    );
  });
});
