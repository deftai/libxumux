import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { ControlHandler } from './handler.js';
import { decodeFrame } from '../codec/decode-frame.js';
import { ControlType, CloseCode } from '../codec/constants.js';
import { encodePong } from './codec.js';
import type { ControlHandlerCallbacks } from './handler.js';
import type { CloseMessage, ErrorMessage, ChannelAckMessage } from './messages.js';

function makeCallbacks(): ControlHandlerCallbacks & {
  closes: CloseMessage[];
  errors: ErrorMessage[];
  channelOpens: ChannelAckMessage[];
  transportCloses: number;
} {
  const closes: CloseMessage[] = [];
  const errors: ErrorMessage[] = [];
  const channelOpens: ChannelAckMessage[] = [];
  let transportCloses = 0;
  return {
    closes, errors, channelOpens,
    get transportCloses() { return transportCloses; },
    onChannelOpen(msg) { channelOpens.push(msg); },
    onChannelClose(_id, _reason) { /* no-op for now */ },
    onError(msg) { errors.push(msg); },
    onClose(msg) { closes.push(msg); },
    onTransportClose() { transportCloses++; },
  };
}

function makePair() {
  const sentA: Uint8Array[] = [];
  const sentB: Uint8Array[] = [];
  const cbA = makeCallbacks();
  const cbB = makeCallbacks();

  const handlerA = new ControlHandler(
    (bytes) => { sentA.push(bytes); handlerB.handleFrame(decodeFrame(bytes)); },
    cbA,
    { pingInterval: 0 }, // disable keepalive for tests
  );
  const handlerB = new ControlHandler(
    (bytes) => { sentB.push(bytes); handlerA.handleFrame(decodeFrame(bytes)); },
    cbB,
    { pingInterval: 0 },
  );

  return { handlerA, handlerB, sentA, sentB, cbA, cbB };
}

describe('ControlHandler — handshake', () => {
  it('client sends HELLO, server receives it and sends WELCOME', async () => {
    const { handlerA, handlerB } = makePair();

    const welcomePromise = handlerA.sendHello({
      version: [0, 1, 0],
      channels: [],
    });
    const hello = await handlerB.awaitHello();
    expect(hello.version).toEqual([0, 1, 0]);

    handlerB.sendWelcome({
      version: [0, 1, 0],
      channels: [],
    });

    const welcome = await welcomePromise;
    expect(welcome.version).toEqual([0, 1, 0]);
    expect(handlerA.connectionState).toBe('connected');
    expect(handlerB.connectionState).toBe('connected');
  });
});

describe('ControlHandler — dynamic channel management', () => {
  it('OPEN_CHANNEL → CHANNEL_ACK resolves the promise', async () => {
    const { handlerA, handlerB, cbB } = makePair();

    // Complete handshake
    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;

    // A opens a channel; B receives the request and acknowledges
    const ackPromise = handlerA.openChannel({ name: 'data', reliable: true, ordered: true });

    // B should have received the OPEN_CHANNEL (via onChannelOpen callback)
    expect(cbB.channelOpens).toHaveLength(1);
    const openMsg = cbB.channelOpens[0]!;

    // B sends the ACK
    handlerB.sendChannelAck({ requestId: openMsg.requestId, id: 2, name: 'data' });

    const ack = await ackPromise;
    expect(ack.id).toBe(2);
    expect(ack.name).toBe('data');
  });

  it('CHANNEL_REJECT rejects the promise', async () => {
    const { handlerA, handlerB, cbB } = makePair();
    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;

    const ackPromise = handlerA.openChannel({ name: 'restricted', reliable: true, ordered: true });
    const openMsg = cbB.channelOpens[0]!;
    handlerB.sendChannelReject({ requestId: openMsg.requestId, code: 4000, reason: 'denied' });

    await expect(ackPromise).rejects.toThrow(/denied/);
  });
});

describe('ControlHandler — PING / PONG', () => {
  it('responds to a PING with a PONG', () => {
    const { handlerA, sentB } = makePair();
    // Simulate A sending a PING directly into B
    const pingFrame = {
      channel: 0,
      type: ControlType.PING,
      flags: 0,
      payload: new Uint8Array([0x00, 0x00, 0x03, 0xe8]), // ts=1000
    };
    const { handlerB } = makePair();
    const pongsReceived: Uint8Array[] = [];
    const b = new ControlHandler(
      (bytes) => pongsReceived.push(bytes),
      makeCallbacks(),
      { pingInterval: 0 },
    );
    b.handleFrame(pingFrame);
    expect(pongsReceived).toHaveLength(1);
    const pongFrame = decodeFrame(pongsReceived[0]!);
    expect(pongFrame.type).toBe(ControlType.PONG);
    // Echo timestamp should match
    const echoTs = ((pongFrame.payload[0]! << 24) | (pongFrame.payload[1]! << 16) |
      (pongFrame.payload[2]! << 8) | pongFrame.payload[3]!) >>> 0;
    expect(echoTs).toBe(1000);

    // suppress unused var
    void handlerA; void sentB; void handlerB;
  });
});

describe('ControlHandler — graceful close', () => {
  it('initiating close waits for echo before resolving', async () => {
    const { handlerA, handlerB, cbA } = makePair();

    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;

    const closedPromise = handlerA.close(CloseCode.NORMAL, 'bye');
    // B should have received CLOSE and echoed back
    await closedPromise;
    expect(handlerA.connectionState).toBe('closed');
    expect(cbA.transportCloses).toBe(1);
  });

  it('receiving CLOSE from remote triggers onClose callback', async () => {
    const { handlerA, handlerB, cbA } = makePair();

    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;

    // B initiates close
    void handlerB.close(CloseCode.NORMAL, 'from B');
    // A should have received CLOSE and emitted onClose
    expect(cbA.closes).toHaveLength(1);
    expect(cbA.closes[0]!.code).toBe(CloseCode.NORMAL);
  });
});

describe('ControlHandler — close idempotency', () => {
  it('close() on an already-closed connection resolves immediately', async () => {
    const { handlerA, handlerB } = makePair();
    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;
    await handlerA.close();
    // Second close on already-closed state
    await expect(handlerA.close()).resolves.toBeUndefined();
  });
});

describe('ControlHandler — destroy', () => {
  it('destroy() rejects all pending openChannel promises', async () => {
    const { handlerA, handlerB } = makePair();
    const welcomeP = handlerA.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerB.awaitHello();
    handlerB.sendWelcome({ version: [0, 1, 0], channels: [] });
    await welcomeP;

    // Start openChannel — B receives request but won't ACK (test just captures callback)
    const ackPromise = handlerA.openChannel({ name: 'x', reliable: true, ordered: true });
    // Destroy before ACK arrives
    handlerA.destroy();
    await expect(ackPromise).rejects.toThrow('Connection closed');
  });
});

describe('ControlHandler — keepalive (fake timers)', () => {
  beforeEach(() => { vi.useFakeTimers(); });
  afterEach(() => { vi.useRealTimers(); });

  it('sends PING after interval and closes on PONG timeout', async () => {
    const pingsSent: Uint8Array[] = [];

    // A has keepalive; PINGs are intercepted so B never PONGs → timeout fires
    const handlerAka = new ControlHandler(
      (bytes) => {
        const frame = decodeFrame(bytes);
        if (frame.type === ControlType.PING) {
          pingsSent.push(bytes);
          return; // drop PING — B never sees it, so no PONG comes back
        }
        handlerBka.handleFrame(frame);
      },
      makeCallbacks(),
      { pingInterval: 1, pingTimeout: 5 },
    );
    const handlerBka = new ControlHandler(
      (bytes) => handlerAka.handleFrame(decodeFrame(bytes)),
      makeCallbacks(),
      { pingInterval: 0 },
    );

    const wp = handlerAka.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerBka.awaitHello();
    handlerBka.sendWelcome({ version: [0, 1, 0], channels: [] });
    await wp;

    vi.advanceTimersByTime(1100); // trigger PING interval
    expect(pingsSent.length).toBeGreaterThan(0);

    vi.advanceTimersByTime(5500); // trigger PONG timeout → connection closes
    expect(handlerAka.connectionState).toBe('closed');
  });

  it('PONG clears the pong deadline so connection stays alive', async () => {
    // A has keepalive; PINGs go to B which auto-PONGs back
    const handlerAkb = new ControlHandler(
      (bytes) => handlerBkb.handleFrame(decodeFrame(bytes)),
      makeCallbacks(),
      { pingInterval: 1, pingTimeout: 5 },
    );
    const handlerBkb = new ControlHandler(
      (bytes) => handlerAkb.handleFrame(decodeFrame(bytes)),
      makeCallbacks(),
      { pingInterval: 0 },
    );

    const wp = handlerAkb.sendHello({ version: [0, 1, 0], channels: [] });
    await handlerBkb.awaitHello();
    handlerBkb.sendWelcome({ version: [0, 1, 0], channels: [] });
    await wp;

    vi.advanceTimersByTime(1100); // PING fires → B receives it → PONG back → A clears deadline
    vi.advanceTimersByTime(5500); // past timeout window
    // Still connected because PONG cleared the deadline
    expect(handlerAkb.connectionState).toBe('connected');
    handlerAkb.destroy();
  });

  it('onPong when no pongDeadline is set (does not throw)', () => {
    const handler = new ControlHandler(
      () => {},
      makeCallbacks(),
      { pingInterval: 0 },
    );
    const pongBytes = encodePong(1000, 500);
    const pongFrame = decodeFrame(pongBytes);
    // No pongDeadline set, but handling PONG should not throw
    expect(() => handler.handleFrame(pongFrame)).not.toThrow();
  });
});

describe('ControlHandler — onErrorFrame', () => {
  it('routes received ERROR frame to onError callback', async () => {
    const cb = makeCallbacks();
    const handler = new ControlHandler(() => {}, cb, { pingInterval: 0 });
    const { encodeError } = await import('./codec.js');
    handler.handleFrame(decodeFrame(encodeError({ code: 4001, reason: 'bad frame' })));
    expect(cb.errors).toHaveLength(1);
    expect(cb.errors[0]!.code).toBe(4001);
  });
});

describe('ControlHandler — ERROR', () => {
  it('sends an ERROR for unknown control type', () => {
    const errors: Uint8Array[] = [];
    const handler = new ControlHandler(
      (bytes) => errors.push(bytes),
      makeCallbacks(),
      { pingInterval: 0 },
    );
    handler.handleFrame({ channel: 0, type: 0xfe, flags: 0, payload: new Uint8Array(0) });
    expect(errors).toHaveLength(1);
    const frame = decodeFrame(errors[0]!);
    expect(frame.type).toBe(ControlType.ERROR);
  });

  it('CLOSE_CHANNEL for channel 0 triggers ERROR', async () => {
    const errors: Uint8Array[] = [];
    const handler = new ControlHandler(
      (bytes) => errors.push(bytes),
      makeCallbacks(),
      { pingInterval: 0 },
    );
    const { encodeCloseChannel } = await import('./codec.js');
    const ccFrame = decodeFrame(encodeCloseChannel({ id: 0, reason: 'test' }));
    handler.handleFrame(ccFrame);
    expect(errors).toHaveLength(1);
    const errorFrame = decodeFrame(errors[0]!);
    expect(errorFrame.type).toBe(ControlType.ERROR);
  });
});
