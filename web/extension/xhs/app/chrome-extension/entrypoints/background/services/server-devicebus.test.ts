// Vitest unit tests for the proxy-only local devicebus WS client.
//
// Coverage:
//   - WS URL construction (local proxy endpoint only)
//   - happy path: inbound DeviceFrame{to_device, payload:Command} →
//     handler → outbound DeviceFrame{from_device, payload:Callback}
//   - unknown_cmd / handler throw → error Callback frame on the wire
//   - non-command payload → synthetic invalid_payload Callback
//   - outbox: WS down at postCallback time → enqueue; on reconnect open
//     → drain over WS, clear on success
//   - GC: outbox entries older than OUTBOX_MAX_AGE_MS dropped at drain
//   - connectOnce open+error race settles with success:false (parity
//     with the legacy module's Fix-T4 §7)

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

vi.mock('coagent-xhs-shared', () => ({
  COAGENT_SERVER_DEVICEBUS_PROTOCOL: {
    DEVICEBUS_PATH: '/devicebus',
    DIRECTION_TO_DEVICE: 'to_device',
    DIRECTION_FROM_DEVICE: 'from_device',
    COMMAND_TYPE: 'command',
    STATUS_OK: 'ok',
    STATUS_ERROR: 'error',
    RECONNECT_BASE_MS: 1_000,
    RECONNECT_MAX_MS: 30_000,
    // Small budget so the reconnect-loop-cap test doesn't need to
    // simulate 10+ failed handshakes.
    RECONNECT_MAX_ATTEMPTS_AFTER_DIRTY_CLOSE: 3,
    WS_CLOSE_CODE_NORMAL: 1000,
    WS_CLOSE_CODE_GOING_AWAY: 1001,
    WS_CLOSE_CODE_AUTH_FAILED: 4401,
    WS_CLOSE_CODE_SESSION_REVOKED: 4403,
    OUTBOX_STORAGE_KEY: 'coagent_server_device_outbox',
    OUTBOX_MAX_SIZE: 200,
    OUTBOX_MAX_AGE_MS: 7 * 24 * 60 * 60 * 1000,
  },
  ERROR_MESSAGES: {
    DEVICE_NOT_CONFIGURED: 'Device not configured',
  },
}));

vi.mock('../connection-state', () => ({
  saveConnectionStatus: vi.fn(async () => undefined),
}));

vi.mock('./cmd-handlers', () => ({
  getCommandHandler: vi.fn(),
  isKnownCommand: vi.fn().mockReturnValue(true),
}));

import {
  CoagentServerDeviceClientForTest,
  buildDeviceBusUrl,
  readOutbox,
  type DeviceFrame,
  type OutboxEntry,
  type ServerDeviceConfig,
} from './server-devicebus';

const OUTBOX_KEY = 'coagent_server_device_outbox';

function installFakeChromeStorage(): { store: Record<string, unknown> } {
  const store: Record<string, unknown> = {};
  (globalThis as any).chrome = {
    storage: {
      local: {
        get: vi.fn(async (key: string) => (key in store ? { [key]: store[key] } : {})),
        set: vi.fn(async (items: Record<string, unknown>) => {
          Object.assign(store, items);
        }),
        remove: vi.fn(async (key: string) => {
          delete store[key];
        }),
      },
    },
  };
  return { store };
}

function makeConfig(over: Partial<ServerDeviceConfig> = {}): ServerDeviceConfig {
  return {
    mode: 'proxy',
    wsEndpoint: 'ws://127.0.0.1:10387',
    actorId: 'tool:xhs',
    channelId: '',
    autoReconnect: true,
    userId: 'user-1',
    deviceId: 'dev-1',
    ...over,
  };
}

function makeClient(over: Partial<ServerDeviceConfig> = {}) {
  const client = new CoagentServerDeviceClientForTest();
  client.updateConfig(makeConfig(over));
  return client;
}

class FakeWebSocket {
  static OPEN = 1;
  static CLOSED = 3;
  readyState = 0;
  url: string;
  /** Subprotocols offered by the client at construct time. Proxy mode
   *  should not offer any. */
  protocols: string[];
  sent: string[] = [];
  private listeners = new Map<string, ((ev: any) => void)[]>();
  constructor(url: string, protocols?: string | string[]) {
    this.url = url;
    this.protocols = Array.isArray(protocols)
      ? protocols.slice()
      : typeof protocols === 'string'
        ? [protocols]
        : [];
  }
  addEventListener(type: string, fn: (ev: any) => void) {
    const list = this.listeners.get(type) ?? [];
    list.push(fn);
    this.listeners.set(type, list);
  }
  send(data: string) {
    if (this.readyState !== FakeWebSocket.OPEN) {
      throw new Error(`send while readyState=${this.readyState}`);
    }
    this.sent.push(data);
  }
  close() {
    this.readyState = FakeWebSocket.CLOSED;
    this.fire('close', { code: 1000, reason: 'test', wasClean: true });
  }
  fire(type: string, ev: any) {
    for (const fn of this.listeners.get(type) ?? []) fn(ev);
  }
  triggerOpen() {
    this.readyState = FakeWebSocket.OPEN;
    this.fire('open', {});
  }
}

async function openClient(
  client: ReturnType<typeof makeClient>,
  opts: { keepInitialSent?: boolean } = {},
): Promise<FakeWebSocket> {
  let captured: FakeWebSocket | null = null;
  const SocketCtor: any = function (this: any, url: string, protocols?: string | string[]) {
    const ws = new FakeWebSocket(url, protocols);
    captured = ws;
    return ws;
  };
  SocketCtor.OPEN = 1;
  client.setWebSocketImpl(SocketCtor as unknown as typeof WebSocket);
  const p = client.connect();
  await Promise.resolve();
  captured!.triggerOpen();
  await p;
  // Allow drainOutbox microtask to flush.
  await new Promise((r) => setTimeout(r, 0));
  if (!opts.keepInitialSent) captured!.sent = [];
  return captured!;
}

beforeEach(() => {
  vi.useRealTimers();
});

afterEach(() => {
  delete (globalThis as any).chrome;
});

// ─── URL builder ─────────────────────────────────────────────────────────

describe('buildDeviceBusUrl', () => {
  it('returns the local proxy endpoint without actor or token query state', () => {
    const url = buildDeviceBusUrl(makeConfig());
    expect(url).toBe('ws://127.0.0.1:10387/');
    expect(url).not.toContain('actor_id=');
    expect(url).not.toContain('token=');
  });

  it('returns empty when wsEndpoint is missing', () => {
    expect(buildDeviceBusUrl(makeConfig({ wsEndpoint: '' }))).toBe('');
  });

  it('returns empty when actorId is missing', () => {
    expect(buildDeviceBusUrl(makeConfig({ actorId: '' }))).toBe('');
  });

  it('returns empty when wsEndpoint is not a valid URL', () => {
    expect(buildDeviceBusUrl(makeConfig({ wsEndpoint: 'not a url' }))).toBe('');
  });
});

describe('proxy mode local hello', () => {
  it('connects without subprotocols and sends actor hello frame on open', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client, { keepInitialSent: true });
    expect(socket.url).toBe('ws://127.0.0.1:10387/');
    expect(socket.protocols).toEqual([]);
    expect(socket.sent).toHaveLength(1);
    expect(JSON.parse(socket.sent[0])).toEqual({
      frame_type: 'hello',
      actor_id: 'tool:xhs',
    });
  });
});

// ─── Happy path: inbound command → outbound callback ─────────────────────

describe('handleToDeviceFrame', () => {
  it('dispatches command and replies with status=ok callback frame', async () => {
    const cmdMod = await import('./cmd-handlers');
    (cmdMod.isKnownCommand as any).mockReturnValueOnce(true);
    (cmdMod.getCommandHandler as any).mockReturnValueOnce(async () => ({
      result: { url: 'https://xhs.com/x', note_id: 'x' },
    }));

    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    const inbound: DeviceFrame = {
      direction: 'to_device',
      actor_id: 'sess-A',
      channel_id: 'ch-1',
      request_id: 'req-1',
      correlation_id: 'req-1',
      payload: {
        type: 'command',
        correlation_id: 'req-1',
        cmd: 'publish',
        params: { title: 'hi' },
      },
    };
    socket.fire('message', { data: JSON.stringify(inbound) });
    await new Promise((r) => setTimeout(r, 5));

    expect(socket.sent).toHaveLength(1);
    const out = JSON.parse(socket.sent[0]) as DeviceFrame;
    expect(out.direction).toBe('from_device');
    expect(out.actor_id).toBe('sess-A');
    expect(out.channel_id).toBe('ch-1');
    expect(out.request_id).toBe('req-1');
    expect(out.correlation_id).toBe('req-1');
    const cb = out.payload as any;
    expect(cb.correlation_id).toBe('req-1');
    expect(cb.status).toBe('ok');
    expect(cb.result).toEqual({ url: 'https://xhs.com/x', note_id: 'x' });
  });

  it('unknown command → error callback frame on the wire', async () => {
    const cmdMod = await import('./cmd-handlers');
    (cmdMod.isKnownCommand as any).mockReturnValueOnce(false);

    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    socket.fire('message', {
      data: JSON.stringify({
        direction: 'to_device',
        actor_id: 'sess-A',
        channel_id: 'ch-1',
        request_id: 'req-2',
        correlation_id: 'req-2',
        payload: {
          type: 'command',
          correlation_id: 'req-2',
          cmd: 'mystery',
          params: {},
        },
      }),
    });
    await new Promise((r) => setTimeout(r, 5));

    expect(socket.sent).toHaveLength(1);
    const cb = (JSON.parse(socket.sent[0]) as DeviceFrame).payload as any;
    expect(cb.status).toBe('error');
    expect(cb.error.code).toBe('unknown_cmd');
  });

  it('handler throw → error callback with structured code/message', async () => {
    const cmdMod = await import('./cmd-handlers');
    (cmdMod.isKnownCommand as any).mockReturnValueOnce(true);
    (cmdMod.getCommandHandler as any).mockReturnValueOnce(async () => {
      const err = new Error('publish wait timed out');
      (err as any).code = 'publish_timeout';
      throw err;
    });

    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    socket.fire('message', {
      data: JSON.stringify({
        direction: 'to_device',
        actor_id: 'sess-A',
        channel_id: 'ch-1',
        request_id: 'req-3',
        correlation_id: 'req-3',
        payload: {
          type: 'command',
          correlation_id: 'req-3',
          cmd: 'publish',
          params: {},
        },
      }),
    });
    await new Promise((r) => setTimeout(r, 5));

    expect(socket.sent).toHaveLength(1);
    const cb = (JSON.parse(socket.sent[0]) as DeviceFrame).payload as any;
    expect(cb.status).toBe('error');
    expect(cb.error.code).toBe('publish_timeout');
    expect(cb.error.message).toBe('publish wait timed out');
  });

  it('non-command payload → invalid_payload error callback', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    socket.fire('message', {
      data: JSON.stringify({
        direction: 'to_device',
        actor_id: 'sess-A',
        channel_id: 'ch-1',
        request_id: 'req-4',
        correlation_id: 'req-4',
        payload: { random: 'not a command' },
      }),
    });
    await new Promise((r) => setTimeout(r, 5));

    expect(socket.sent).toHaveLength(1);
    const cb = (JSON.parse(socket.sent[0]) as DeviceFrame).payload as any;
    expect(cb.status).toBe('error');
    expect(cb.error.code).toBe('invalid_payload');
  });

  it('drops frames whose direction is not to_device', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    socket.fire('message', {
      data: JSON.stringify({
        direction: 'from_device',
        actor_id: 'sess-A',
        channel_id: 'ch-1',
        payload: { ignored: true },
      }),
    });
    await new Promise((r) => setTimeout(r, 5));
    expect(socket.sent).toEqual([]);
  });
});

// ─── postCallback (recovery path) ────────────────────────────────────────

describe('postCallback', () => {
  it('sends from_device frame straight over WS when open', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);

    await client.postCallback('corr-A', {
      status: 'ok',
      result: { ok: 1 },
      error: null,
    });

    expect(socket.sent).toHaveLength(1);
    const out = JSON.parse(socket.sent[0]) as DeviceFrame;
    expect(out.direction).toBe('from_device');
    expect(out.actor_id).toBe('tool:xhs');
    expect(out.channel_id).toBe('');
    expect(out.correlation_id).toBe('corr-A');
    expect((out.payload as any).status).toBe('ok');
  });
});

// ─── Outbox: WS-down enqueue + reconnect drain ────────────────────────────

describe('outbox enqueue + drain on reconnect', () => {
  it('postCallback while WS down enqueues; reconnect open drains', async () => {
    installFakeChromeStorage();
    const client = makeClient();

    // No connect yet → WS down. postCallback should enqueue.
    await client.postCallback('corr-1', {
      status: 'ok',
      result: { ok: 1 },
      error: null,
    });
    const pending = await readOutbox();
    expect(pending).toHaveLength(1);
    expect(pending[0].frame.correlation_id).toBe('corr-1');
    expect(pending[0].frame.direction).toBe('from_device');

    // Now connect → open should drain.
    const socket = await openClient(client, { keepInitialSent: true });
    expect(socket.sent).toHaveLength(2);
    const drained = JSON.parse(socket.sent[1]) as DeviceFrame;
    expect(drained.correlation_id).toBe('corr-1');
    // Successful drain clears outbox.
    expect(await readOutbox()).toEqual([]);
  });

  it('outbox dedupes by correlation_id (last-writer-wins)', async () => {
    installFakeChromeStorage();
    const client = makeClient();

    await client.postCallback('dup', {
      status: 'error',
      result: null,
      error: { code: 'first', message: 'first' },
    });
    await client.postCallback('dup', {
      status: 'ok',
      result: { winner: true },
      error: null,
    });

    const pending = await readOutbox();
    expect(pending).toHaveLength(1);
    expect((pending[0].frame.payload as any).status).toBe('ok');
    expect((pending[0].frame.payload as any).result).toEqual({ winner: true });
  });

  it('GC drops entries older than OUTBOX_MAX_AGE_MS at drain time', async () => {
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => undefined);
    try {
      const { store } = installFakeChromeStorage();
      const eightDaysAgo = Date.now() - 8 * 24 * 60 * 60 * 1000;
      const stale: OutboxEntry = {
        frame: {
          direction: 'from_device',
          actor_id: 'sess-A',
          channel_id: 'ch-1',
          request_id: 'old',
          correlation_id: 'old',
          payload: { correlation_id: 'old', status: 'ok' },
        },
        enqueued_at: eightDaysAgo,
      };
      const fresh: OutboxEntry = {
        frame: {
          direction: 'from_device',
          actor_id: 'sess-A',
          channel_id: 'ch-1',
          request_id: 'fresh',
          correlation_id: 'fresh',
          payload: { correlation_id: 'fresh', status: 'ok' },
        },
        enqueued_at: Date.now(),
      };
      store[OUTBOX_KEY] = [stale, fresh];

      const client = makeClient();
      const socket = await openClient(client, { keepInitialSent: true });

      // Only the fresh frame should have gone out.
      expect(socket.sent).toHaveLength(2);
      const out = JSON.parse(socket.sent[1]) as DeviceFrame;
      expect(out.correlation_id).toBe('fresh');

      // Outbox cleared (fresh sent + clear; stale GC'd).
      expect(await readOutbox()).toEqual([]);

      // GC log line for the stale entry.
      const gcLog = warn.mock.calls.find((args) => args[0] === '[ServerDeviceBus] outbox.dropped');
      expect(gcLog).toBeDefined();
      expect(gcLog![1]).toMatchObject({
        correlation_id: 'old',
        reason: 'gc_max_age_exceeded',
      });
    } finally {
      warn.mockRestore();
    }
  });
});

// ─── connectOnce open+error race (Fix-T4 §7 parity) ──────────────────────

describe('connectOnce open+error race', () => {
  it('error before open settles connect() with success:false', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    let captured: FakeWebSocket | null = null;
    const SocketCtor: any = function (this: any, url: string) {
      const ws = new FakeWebSocket(url);
      captured = ws;
      return ws;
    };
    SocketCtor.OPEN = 1;
    client.setWebSocketImpl(SocketCtor as unknown as typeof WebSocket);

    const p = client.connect();
    await Promise.resolve();
    captured!.fire('error', { message: 'ECONNREFUSED' });
    const res = await p;
    expect(res.success).toBe(false);
    expect(res.error).toContain('ECONNREFUSED');
  });

  it('returns DEVICE_NOT_CONFIGURED when wsEndpoint missing', async () => {
    installFakeChromeStorage();
    const client = new CoagentServerDeviceClientForTest();
    client.updateConfig(makeConfig({ wsEndpoint: '' }));
    const res = await client.connect();
    expect(res.success).toBe(false);
    expect(res.error).toContain('Device not configured');
  });
});

// ─── Reconnect-loop bug fixes (bind/unbind/identity-swap) ────────────────
//
// These tests pin the four scenarios called out in the device-bind bug
// report: proxy identity swap, unbindDevice full cleanup, dirty-close
// reconnect budget, and the race where a close handler fires AFTER
// hardReset / identity swap and tries to respawn under the old actor.

function multiSocketCtor(): {
  ctor: typeof WebSocket;
  sockets: FakeWebSocket[];
} {
  const sockets: FakeWebSocket[] = [];
  const Ctor: any = function (this: any, url: string, protocols?: string | string[]) {
    const ws = new FakeWebSocket(url, protocols);
    sockets.push(ws);
    return ws;
  };
  Ctor.OPEN = 1;
  return { ctor: Ctor as unknown as typeof WebSocket, sockets };
}

describe('connect idempotency', () => {
  it('reuses the in-flight connect attempt when connect() is called twice before open', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    const first = client.connect();
    const second = client.connect();
    await Promise.resolve();

    expect(sockets).toHaveLength(1);
    sockets[0].triggerOpen();
    const [firstResult, secondResult] = await Promise.all([first, second]);
    expect(firstResult.success).toBe(true);
    expect(secondResult.success).toBe(true);
    expect(sockets).toHaveLength(1);
  });

  it('returns success without opening a new socket when already connected to the same URL', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    const first = client.connect();
    await Promise.resolve();
    sockets[0].triggerOpen();
    await first;

    const second = await client.connect();

    expect(second.success).toBe(true);
    expect(sockets).toHaveLength(1);
  });
});

describe('updateConfig identity swap (bind / rebind)', () => {
  it('new proxy actor identity cancels in-flight connect + closes prior socket before new opens', async () => {
    installFakeChromeStorage();
    const client = new CoagentServerDeviceClientForTest();
    client.updateConfig(makeConfig({ actorId: 'tool:old' }));
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    // Kick off connect with OLD identity; do NOT fire open yet so the
    // promise stays pending — simulates the local proxy stalling the
    // handshake while UI is mid-rebind.
    const pOld = client.connect();
    await Promise.resolve();
    expect(sockets).toHaveLength(1);
    expect(sockets[0].url).toBe('ws://127.0.0.1:10387/');
    expect(sockets[0].protocols).toEqual([]);

    // Now swap identity via updateConfig — mirrors what applyClients(cfg)
    // does after the popup selects a new proxy actor.
    client.updateConfig(makeConfig({ actorId: 'tool:new' }));

    // The in-flight OLD promise must resolve (canceled) — without
    // the hard-reset fix, this would hang forever and the next
    // connect() awaiting connectInFlight would deadlock.
    const oldRes = await pOld;
    expect(oldRes.success).toBe(false);
    expect(oldRes.error).toMatch(/canceled/);

    // The OLD socket must have been closed by the reset (readyState
    // = CLOSED), so even if the proxy close frame arrives late
    // it hits the dropped-stale-socket path.
    expect(sockets[0].readyState).toBe(FakeWebSocket.CLOSED);

    // Now connect with NEW identity and verify the hello frame.
    const pNew = client.connect();
    await Promise.resolve();
    expect(sockets).toHaveLength(2);
    expect(sockets[1].url).toBe('ws://127.0.0.1:10387/');
    expect(sockets[1].protocols).toEqual([]);
    sockets[1].triggerOpen();
    const newRes = await pNew;
    expect(newRes.success).toBe(true);
    expect(JSON.parse(sockets[1].sent[0])).toEqual({
      frame_type: 'hello',
      actor_id: 'tool:new',
    });
  });

  it('same-identity updateConfig (autoReconnect flip) does not bounce the socket', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const socket = await openClient(client);
    expect(socket.readyState).toBe(FakeWebSocket.OPEN);

    // Flipping autoReconnect alone is NOT an identity change — must
    // not trigger hardReset (otherwise routine config refreshes would
    // tear down a perfectly good WS).
    client.updateConfig(makeConfig({ autoReconnect: false }));
    expect(socket.readyState).toBe(FakeWebSocket.OPEN);
  });
});

describe('unbindDevice full cleanup', () => {
  it('disconnect() cancels pending in-flight connect promise (no hang on subsequent connect)', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    // Start a connect; leave handshake stalled.
    const p = client.connect();
    await Promise.resolve();
    expect(sockets).toHaveLength(1);
    expect(sockets[0].readyState).toBe(0); // not yet open

    // Unbind disconnect path must settle the pending
    // promise + clear connectInFlight + clear currentUrl. Without
    // the hard-reset fix, this connect promise would hang forever.
    client.disconnect();
    const res = await p;
    expect(res.success).toBe(false);
    expect(res.error).toMatch(/canceled/);

    // Subsequent connect() must NOT short-circuit via the stale
    // connectInFlight; it must build a fresh WS attempt instead.
    // Restore the config so connect() doesn't bail on DEVICE_NOT_CONFIGURED.
    client.updateConfig(makeConfig({ actorId: 'tool:fresh' }));
    const p2 = client.connect();
    await Promise.resolve();
    // A new socket must have been created (no hang).
    expect(sockets.length).toBeGreaterThanOrEqual(2);
    expect(sockets[sockets.length - 1].url).toBe('ws://127.0.0.1:10387/');
    sockets[sockets.length - 1].triggerOpen();
    const res2 = await p2;
    expect(res2.success).toBe(true);
  });

  it('disconnect() prevents any further reconnect even after a stray close event fires', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);
    // Open the WS.
    const p = client.connect();
    await Promise.resolve();
    sockets[0].triggerOpen();
    await p;

    // User unbinds.
    client.disconnect();

    // Now simulate a stray late close event from the proxy side
    // (e.g. WS close frame arriving after the local hardReset).
    // The close handler must NOT schedule a reconnect, MUST NOT
    // create a new socket, and MUST not flip shouldReconnect back on.
    sockets[0].fire('close', { code: 1006, reason: 'late close', wasClean: false });
    // Give any micro / macrotasks a chance to run.
    await new Promise((r) => setTimeout(r, 50));

    expect(sockets).toHaveLength(1);
  });

  it('disconnect() then reconnect with new config opens new WS within attempt budget', async () => {
    // Sanity guard: ensure the cap doesn't carry across identity
    // swaps. After a fresh actor the budget MUST be reset, otherwise
    // a user with a flaky network in session A could hit "stuck"
    // when they bind session B.
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    // Burn 2 dirty closes under identity A (budget = 3 in test mock).
    for (let i = 0; i < 2; i++) {
      const p = client.connect();
      await Promise.resolve();
      sockets[sockets.length - 1].fire('close', {
        code: 1006,
        reason: 'flaky',
        wasClean: false,
      });
      await p;
      // Cancel any scheduled reconnect timer so we control attempts.
      client.disconnect();
      client.updateConfig(makeConfig());
    }

    // Now bind a brand new identity — budget should be reset.
    client.updateConfig(makeConfig({ actorId: 'tool:new' }));
    const p = client.connect();
    await Promise.resolve();
    sockets[sockets.length - 1].triggerOpen();
    const res = await p;
    expect(res.success).toBe(true);
  });
});

describe('reconnect budget for dirty-close (1006) loop', () => {
  it('stops reconnecting after RECONNECT_MAX_ATTEMPTS_AFTER_DIRTY_CLOSE consecutive non-clean closes', async () => {
    vi.useFakeTimers();
    try {
      installFakeChromeStorage();
      const client = makeClient(); // mock budget = 3
      const { ctor, sockets } = multiSocketCtor();
      client.setWebSocketImpl(ctor);

      // First connect.
      const p = client.connect();
      await Promise.resolve();
      sockets[0].fire('close', { code: 1006, reason: 'denied', wasClean: false });
      await p;

      // Drive the reconnect timer forward repeatedly. Each scheduled
      // reconnect fires, builds a new socket, that socket gets a 1006.
      // After 3 attempts (budget) we expect shouldReconnect=false and
      // no further sockets to be created on subsequent ticks.
      for (let i = 0; i < 10; i++) {
        await vi.advanceTimersByTimeAsync(60_000);
        // If a new socket was created, immediately close it 1006.
        const last = sockets[sockets.length - 1];
        if (last.readyState === 0) {
          last.fire('close', { code: 1006, reason: 'denied', wasClean: false });
        }
      }
      // We allow at most budget + 1 sockets total (initial + budget
      // reconnects). Without the cap this loop would create 11+.
      expect(sockets.length).toBeLessThanOrEqual(4);
    } finally {
      vi.useRealTimers();
    }
  });

  it('auth-failure close code stops reconnect immediately', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    const p = client.connect();
    await Promise.resolve();
    // Server explicitly signals revoked-session via 4403.
    sockets[0].fire('close', { code: 4403, reason: 'session revoked', wasClean: true });
    await p;

    // Drive any scheduled timer — there must be none, so this is a no-op.
    await new Promise((r) => setTimeout(r, 50));
    expect(sockets).toHaveLength(1);
  });

  it('successful open resets the dirty-close attempt counter', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    // Burn 2 dirty closes (budget = 3 in mock).
    for (let i = 0; i < 2; i++) {
      const p = client.connect();
      await Promise.resolve();
      sockets[sockets.length - 1].fire('close', {
        code: 1006,
        reason: 'flaky',
        wasClean: false,
      });
      await p;
    }
    // Manually clear any scheduled reconnect to keep this test
    // deterministic — we want to drive connect() explicitly.
    client.disconnect();
    client.updateConfig(makeConfig());

    // Successful connect resets the counter.
    const pOk = client.connect();
    await Promise.resolve();
    sockets[sockets.length - 1].triggerOpen();
    await pOk;

    // Now we should be able to fail more times than the original
    // budget without hitting the cap (because the open reset it).
    for (let i = 0; i < 3; i++) {
      sockets[sockets.length - 1].fire('close', {
        code: 1006,
        reason: 'flaky again',
        wasClean: false,
      });
      const pNext = client.connect();
      await Promise.resolve();
      // Don't open — just verify a new socket was attempted.
      if (sockets[sockets.length - 1].readyState === 0) {
        sockets[sockets.length - 1].fire('error', { message: 'still flaky' });
        await pNext;
      }
    }
    // The budget reset means we got more attempts than the initial
    // exhaust. Without reset-on-open this would be capped at 3.
    expect(sockets.length).toBeGreaterThan(4);
  });
});

describe('stale socket close after generation bump', () => {
  it('close event from a pre-reset socket must not schedule a reconnect', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const { ctor, sockets } = multiSocketCtor();
    client.setWebSocketImpl(ctor);

    // Start a connect and capture the stale socket.
    const p = client.connect();
    await Promise.resolve();
    const stale = sockets[0];

    // Identity swap (new proxy actor). This bumps generation and
    // closes the stale socket via hardReset.
    client.updateConfig(makeConfig({ actorId: 'tool:b' }));
    const pCanceled = await p;
    expect(pCanceled.success).toBe(false);

    // Now the proxy close frame arrives late on the stale socket
    // with a dirty code. The close handler must self-cancel via
    // generation guard and MUST NOT schedule a new socket using the
    // stale actor. After waiting a beat there should still be only
    // one extra socket from the identity-swap reconnect path (or
    // zero if no auto-connect was triggered).
    stale.fire('close', { code: 1006, reason: 'late', wasClean: false });
    await new Promise((r) => setTimeout(r, 50));

    expect(sockets).toHaveLength(1);
  });
});
