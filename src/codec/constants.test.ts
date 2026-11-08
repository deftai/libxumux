import { describe, it, expect } from 'vitest';
import { CloseCode } from './constants.js';

describe('CloseCode', () => {
  it('matches the code table in core §10', () => {
    expect(CloseCode).toEqual({
      NORMAL: 1000,
      GOING_AWAY: 1001,
      PROTOCOL_ERROR: 1002,
      UNSUPPORTED: 1003,
      AUTH_FAILED: 4000,
      INVALID_MESSAGE: 4001,
      CHANNEL_LIMIT: 4002,
      CHANNEL_NOT_FOUND: 4003,
      RATE_LIMITED: 4004,
      MESSAGE_TOO_LARGE: 4005,
      VERSION_MISMATCH: 4006,
      HELLO_TIMEOUT: 4007,
      KEEPALIVE_TIMEOUT: 4008,
      FLOW_CONTROL_ERROR: 4009,
      CHANNEL_REFUSED: 4010,
      BUSY: 4011,
    });
  });
});
