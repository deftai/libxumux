import { describe, it, expect } from 'vitest';
import { XUMUX_VERSION, OMUX_MAGIC } from './types.js';

describe('XUMUX_VERSION', () => {
  it('matches the spec version', () => {
    expect(XUMUX_VERSION).toBe('0.2.0-draft');
  });
});

describe('OMUX_MAGIC', () => {
  it('is 4 bytes', () => {
    expect(OMUX_MAGIC).toHaveLength(4);
  });

  it('spells OMUX in ASCII', () => {
    expect(OMUX_MAGIC[0]).toBe(0x4f); // O
    expect(OMUX_MAGIC[1]).toBe(0x4d); // M
    expect(OMUX_MAGIC[2]).toBe(0x55); // U
    expect(OMUX_MAGIC[3]).toBe(0x58); // X
  });
});
