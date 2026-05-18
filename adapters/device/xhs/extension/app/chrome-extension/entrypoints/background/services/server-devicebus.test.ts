// Vitest unit tests for the v4 server-devicebus WS client (T147 §A-E).
//
// Coverage:
//   - WS URL construction (`session_id` + `token` query, redactToken)
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
  redactToken,
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
    wsEndpoint: 'wss://coagent.example.com/devicebus',
    sessionId: 'sess-A',
    token: 'tok-secret',
    channelId: 'ch-1',
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
  sent: string[] = [];
  private listeners = new Map<string, ((ev: any) => void)[]>();
  constructor(url: string) {
    this.url = url;
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
): Promise<FakeWebSocket> {
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
  captured!.triggerOpen();
  await p;
  // Allow drainOutbox microtask to flush.
  await new Promise((r) => setTimeout(r, 0));
  return captured!;
}

beforeEach(() => {
  vi.useRealTimers();
});

afterEach(() => {
  delete (globalThis as any).chrome;
});

// ─── URL builder + token redact ──────────────────────────────────────────

describe('buildDeviceBusUrl', () => {
  it('appends session_id and token query params', () => {
    const url = buildDeviceBusUrl(makeConfig());
    expect(url).toContain('wss://coagent.example.com/devicebus');
    expect(url).toContain('session_id=sess-A');
    expect(url).toContain('token=tok-secret');
  });

  it('returns empty when wsEndpoint is missing', () => {
    expect(buildDeviceBusUrl(makeConfig({ wsEndpoint: '' }))).toBe('');
  });

  it('returns empty when sessionId is missing', () => {
    expect(buildDeviceBusUrl(makeConfig({ sessionId: '' }))).toBe('');
  });

  it('returns empty when token is missing', () => {
    expect(buildDeviceBusUrl(makeConfig({ token: '' }))).toBe('');
  });

  it('returns empty when wsEndpoint is not a valid URL', () => {
    expect(buildDeviceBusUrl(makeConfig({ wsEndpoint: 'not a url' }))).toBe('');
  });
});

describe('redactToken', () => {
  it('masks ?token=… to ?token=*** ', () => {
    expect(redactToken('wss://h/devicebus?session_id=s&token=secret')).toBe(
      'wss://h/devicebus?session_id=s&token=***',
    );
  });

  it('masks &token=… mid-query without touching session_id', () => {
    expect(redactToken('wss://h/devicebus?token=secret&session_id=s')).toBe(
      'wss://h/devicebus?token=***&session_id=s',
    );
  });

  it('passes through urls without token', () => {
    expect(redactToken('wss://h/devicebus')).toBe('wss://h/devicebus');
    expect(redactToken('')).toBe('');
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
      device_session_id: 'sess-A',
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
    expect(out.device_session_id).toBe('sess-A');
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
        device_session_id: 'sess-A',
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
        device_session_id: 'sess-A',
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
        device_session_id: 'sess-A',
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
        device_session_id: 'sess-A',
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
    expect(out.device_session_id).toBe('sess-A');
    expect(out.channel_id).toBe('ch-1');
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
    const socket = await openClient(client);
    expect(socket.sent).toHaveLength(1);
    const drained = JSON.parse(socket.sent[0]) as DeviceFrame;
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
          device_session_id: 'sess-A',
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
          device_session_id: 'sess-A',
          channel_id: 'ch-1',
          request_id: 'fresh',
          correlation_id: 'fresh',
          payload: { correlation_id: 'fresh', status: 'ok' },
        },
        enqueued_at: Date.now(),
      };
      store[OUTBOX_KEY] = [stale, fresh];

      const client = makeClient();
      const socket = await openClient(client);

      // Only the fresh frame should have gone out.
      expect(socket.sent).toHaveLength(1);
      const out = JSON.parse(socket.sent[0]) as DeviceFrame;
      expect(out.correlation_id).toBe('fresh');

      // Outbox cleared (fresh sent + clear; stale GC'd).
      expect(await readOutbox()).toEqual([]);

      // GC log line for the stale entry.
      const gcLog = warn.mock.calls.find(
        (args) => args[0] === '[ServerDeviceBus] outbox.dropped',
      );
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
