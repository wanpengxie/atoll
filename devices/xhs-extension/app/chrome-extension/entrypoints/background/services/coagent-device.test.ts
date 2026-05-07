// Vitest unit tests for the postCallback retry / timeout / outbox path and
// WS callback_replay drain (M1.1 Fix-T3).
//
// What we *don't* test:
//   - the full WS connect lifecycle — that is fundamentally Chrome WebSocket
//     behavior; we drive the open hook directly via setWebSocketImpl.
//   - real Chrome storage — replaced by an in-memory shim attached to
//     globalThis.chrome before each test.

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';

// In tests we don't go through the workspace package — easier to inline the
// constants we need so vitest doesn't need a build step from the shared pkg.
vi.mock('xiaohongshu-mcp-shared', () => ({
  COAGENT_DEVICE_PROTOCOL: {
    COMMAND_FRAME_TYPE: 'command',
    ACK_FRAME_TYPE: 'ack',
    CALLBACK_REPLAY_FRAME_TYPE: 'callback_replay',
    CALLBACK_PATH_PREFIX: '/api/device/',
    CALLBACK_PATH_SUFFIX_CALLBACK: '/callback',
    CALLBACK_PATH_SUFFIX_SESSION: '/session',
    RECONNECT_BASE_MS: 1_000,
    RECONNECT_MAX_MS: 30_000,
    COMMAND_DISPATCH_TIMEOUT_MS: 180_000,
    CALLBACK_RETRY_TIMEOUT_MS: 50, // shorter so timeout test runs fast
    CALLBACK_RETRY_BACKOFF_MS_LIST: [10, 20, 40] as readonly number[],
    CALLBACK_OUTBOX_STORAGE_KEY: 'coagent_device_pending_callbacks',
    CALLBACK_OUTBOX_MAX_SIZE: 200,
  },
  ERROR_MESSAGES: {
    DEVICE_NOT_CONFIGURED: 'Device not configured',
    DEVICE_CALLBACK_FAILED: 'Device callback POST to daemon failed',
  },
  EXTENSION_CONSTANTS: {
    NATIVE_HOST_NAME: 'ai.coagent.xhs.device',
    STORAGE_KEYS: {
      SERVER_STATUS: 'coagent_device_status',
      CONNECTION_CONFIG: 'coagent_device_config',
    },
    DEFAULT_DAEMON_HTTP_PORT: 9501,
    DEFAULT_DEVICE_WS_PATH_PREFIX: '/device/',
  },
}));

// connection-state hits chrome.storage; stub it out — postCallback only needs
// it for `saveConnectionStatus`, which we don't assert on here.
vi.mock('../connection-state', () => ({
  saveConnectionStatus: vi.fn(async () => undefined),
  getConnectionConfig: vi.fn(),
  getStoredConnectionStatus: vi.fn(),
  saveConnectionConfig: vi.fn(),
  broadcastConnectionStatus: vi.fn(),
  getDefaultDaemonHttpBase: vi.fn(() => 'http://127.0.0.1:9501'),
  getDefaultWebSocketUrl: vi.fn(() => ''),
}));

// cmd-handlers stub: tests that exercise handleCommand override these per-case.
vi.mock('./cmd-handlers', () => ({
  getCommandHandler: vi.fn(),
  isKnownCommand: vi.fn().mockReturnValue(true),
}));

import {
  CoagentDeviceClientForTest,
  readPendingCallbacks,
  writePendingCallbacks,
  redactKey,
  type PendingCallbackEntry,
} from './coagent-device';

const STORAGE_KEY = 'coagent_device_pending_callbacks';

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

function makeClient() {
  const client = new CoagentDeviceClientForTest();
  client.updateConfig({
    serverUrl: 'ws://127.0.0.1:9501/device/dev-A',
    autoReconnect: true,
    apiKey: 'k1',
    daemonHttpBase: 'http://127.0.0.1:9501',
    deviceId: 'dev-A',
    userId: 'user-001',
  });
  return client;
}

function fakeResponse(status: number, body: unknown = {}): Response {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: vi.fn(async () => (typeof body === 'string' ? body : JSON.stringify(body))),
    json: vi.fn(async () => (typeof body === 'string' ? body : body)),
  } as unknown as Response;
}

beforeEach(() => {
  vi.useRealTimers();
});

afterEach(() => {
  delete (globalThis as any).chrome;
});

// ── postCallback retry path ───────────────────────────────────────────────

describe('postCallback retry', () => {
  it('returns after the first 2xx without retrying', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const fetchImpl = vi.fn(async () => fakeResponse(200, { ok: true })) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-1', {
      status: 'ok',
      result: { url: 'https://xhs.com/note/x', note_id: 'x' },
      error: null,
    });

    expect(fetchImpl).toHaveBeenCalledTimes(1);
    const callArgs = (fetchImpl as any).mock.calls[0];
    expect(callArgs[0]).toBe('http://127.0.0.1:9501/api/device/dev-A/callback');
    expect(callArgs[1]?.headers?.Authorization).toBe('Bearer k1');
    const body = JSON.parse(callArgs[1].body);
    expect(body.correlation_id).toBe('corr-1');
    expect(body.status).toBe('ok');

    // No outbox entry on success.
    expect(await readPendingCallbacks()).toEqual([]);
  });

  it('retries 5xx responses up to 3 times then succeeds (4 total attempts when last 2xx)', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const responses = [
      fakeResponse(503),
      fakeResponse(502),
      fakeResponse(500),
      fakeResponse(200),
    ];
    const fetchImpl = vi.fn(async () => responses.shift()!) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-2', {
      status: 'ok',
      result: { ok: 1 },
      error: null,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    expect(await readPendingCallbacks()).toEqual([]);
  });

  it('exhausts retries on persistent network failure and queues to outbox', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const fetchImpl = vi.fn(async () => {
      throw new TypeError('NetworkError');
    }) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-3', {
      status: 'error',
      result: null,
      error: { code: 'auth_expired', message: 'login required' },
    });

    // Initial + 3 retries = 4 attempts.
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    const pending = await readPendingCallbacks();
    expect(pending).toHaveLength(1);
    expect(pending[0].body.correlation_id).toBe('corr-3');
    expect(pending[0].body.status).toBe('error');
    expect(typeof pending[0].enqueued_at).toBe('number');
  });

  it('AbortController fires when fetch hangs past CALLBACK_RETRY_TIMEOUT_MS', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    let abortObserved = false;
    const fetchImpl = vi.fn(async (_url: any, init: any) => {
      // Simulate a hang that resolves after the timeout has fired by waiting
      // for the abort signal.
      return await new Promise<Response>((_resolve, reject) => {
        const signal = init?.signal as AbortSignal | undefined;
        if (signal) {
          signal.addEventListener('abort', () => {
            abortObserved = true;
            const err = new Error('aborted');
            (err as any).name = 'AbortError';
            reject(err);
          });
        }
      });
    }) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-timeout', {
      status: 'ok',
      result: { ok: 1 },
      error: null,
    });

    expect(abortObserved).toBe(true);
    // Timed out + retried 3 times → 4 attempts total → outbox.
    expect(fetchImpl).toHaveBeenCalledTimes(4);
    const pending = await readPendingCallbacks();
    expect(pending).toHaveLength(1);
    expect(pending[0].body.correlation_id).toBe('corr-timeout');
  });

  it('treats 4xx (non-429) as terminal — no retry, no outbox', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const fetchImpl = vi.fn(async () => fakeResponse(400, 'bad correlation')) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-bad', {
      status: 'ok',
      result: {},
      error: null,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(1);
    expect(await readPendingCallbacks()).toEqual([]);
  });

  it('429 is treated as retryable', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const responses = [fakeResponse(429), fakeResponse(200)];
    const fetchImpl = vi.fn(async () => responses.shift()!) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-429', {
      status: 'ok',
      result: {},
      error: null,
    });
    expect(fetchImpl).toHaveBeenCalledTimes(2);
  });
});

// ── storage outbox primitives ─────────────────────────────────────────────

describe('outbox storage helpers', () => {
  it('readPendingCallbacks returns [] when storage missing', async () => {
    delete (globalThis as any).chrome;
    expect(await readPendingCallbacks()).toEqual([]);
  });

  it('readPendingCallbacks filters malformed entries', async () => {
    const { store } = installFakeChromeStorage();
    store[STORAGE_KEY] = [
      { body: { correlation_id: 'a', status: 'ok' } },
      'garbage',
      { body: { correlation_id: 'b', status: 'invalid' } },
      null,
      { body: { correlation_id: 'c', status: 'error' } },
    ];
    const out = await readPendingCallbacks();
    expect(out.map((e) => e.body.correlation_id)).toEqual(['a', 'c']);
  });

  it('writePendingCallbacks trims to CALLBACK_OUTBOX_MAX_SIZE (last-N)', async () => {
    installFakeChromeStorage();
    const entries: PendingCallbackEntry[] = [];
    for (let i = 0; i < 250; i += 1) {
      entries.push({
        body: { correlation_id: `c-${i}`, status: 'ok', result: {}, error: null },
        enqueued_at: i,
      });
    }
    await writePendingCallbacks(entries);
    const out = await readPendingCallbacks();
    expect(out).toHaveLength(200);
    // FIFO trim → newest preserved
    expect(out[0].body.correlation_id).toBe('c-50');
    expect(out[out.length - 1].body.correlation_id).toBe('c-249');
  });

  it('outbox dedupes by correlation_id on enqueue (last-writer-wins)', async () => {
    installFakeChromeStorage();
    const client = makeClient();
    const fetchImpl = vi.fn(async () => {
      throw new Error('net');
    }) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    await client.postCallback('corr-dup', {
      status: 'error',
      result: null,
      error: { code: 'first', message: 'first' },
    });
    await client.postCallback('corr-dup', {
      status: 'ok',
      result: { winner: true },
      error: null,
    });

    const pending = await readPendingCallbacks();
    expect(pending).toHaveLength(1);
    expect(pending[0].body.status).toBe('ok');
    expect((pending[0].body.result as any).winner).toBe(true);
  });
});

// ── WS callback_replay drain ──────────────────────────────────────────────

class FakeWebSocket {
  static OPEN = 1;
  static CLOSED = 3;
  readyState = 0; // CONNECTING
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

describe('WS callback_replay drain on reconnect', () => {
  it('open event sends one callback_replay frame containing all pending payloads', async () => {
    const { store } = installFakeChromeStorage();
    // Pre-populate outbox with 2 entries.
    const seed: PendingCallbackEntry[] = [
      {
        body: { correlation_id: 'p1', status: 'ok', result: { a: 1 }, error: null },
        enqueued_at: 1,
      },
      {
        body: {
          correlation_id: 'p2',
          status: 'error',
          result: null,
          error: { code: 'x', message: 'y' },
        },
        enqueued_at: 2,
      },
    ];
    store[STORAGE_KEY] = seed;

    const client = makeClient();
    let captured: FakeWebSocket | null = null;
    const SocketCtor: any = function (this: any, url: string) {
      const ws = new FakeWebSocket(url);
      captured = ws;
      return ws;
    };
    SocketCtor.OPEN = 1;
    client.setWebSocketImpl(SocketCtor as unknown as typeof WebSocket);

    const connectPromise = client.connect();
    // The fake socket created synchronously inside connectOnce — give micro
    // tasks a chance to run before triggering open.
    await Promise.resolve();
    expect(captured).not.toBeNull();
    captured!.triggerOpen();
    await connectPromise;

    // Allow the async drain to complete.
    await new Promise((r) => setTimeout(r, 0));

    expect(captured!.sent).toHaveLength(1);
    const frame = JSON.parse(captured!.sent[0]);
    expect(frame.type).toBe('callback_replay');
    expect(frame.payloads).toHaveLength(2);
    expect(frame.payloads[0].correlation_id).toBe('p1');
    expect(frame.payloads[1].correlation_id).toBe('p2');

    // Outbox cleared after a successful drain.
    const pending = await readPendingCallbacks();
    expect(pending).toEqual([]);
  });

  it('skips drain when outbox empty (no WS frame sent)', async () => {
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

    const connectPromise = client.connect();
    await Promise.resolve();
    captured!.triggerOpen();
    await connectPromise;
    await new Promise((r) => setTimeout(r, 0));

    expect(captured!.sent).toEqual([]);
  });
});

// ── M1.1 Fix-T4 §4: WS URL key redact ────────────────────────────────────

describe('redactKey helper', () => {
  it('replaces ?key=… with ?key=*** ', () => {
    expect(redactKey('ws://127.0.0.1:9501/device/dev-A?key=secret-123')).toBe(
      'ws://127.0.0.1:9501/device/dev-A?key=***'
    );
  });

  it('replaces &key=… mid-query without touching other params', () => {
    expect(redactKey('ws://h:1/device/A?foo=bar&key=secret&baz=qux')).toBe(
      'ws://h:1/device/A?foo=bar&key=***&baz=qux'
    );
  });

  it('returns the original URL when no key= present', () => {
    expect(redactKey('ws://h:1/device/A')).toBe('ws://h:1/device/A');
    expect(redactKey('')).toBe('');
  });
});

// ── M1.1 Fix-T4 §7: connectOnce open+error race ──────────────────────────

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

    const connectPromise = client.connect();
    await Promise.resolve();
    expect(captured).not.toBeNull();
    // Fire error before open — must settle false (was prone to hang before fix).
    captured!.fire('error', { message: 'ECONNREFUSED' });
    const result = await connectPromise;
    expect(result.success).toBe(false);
    expect(result.error).toContain('ECONNREFUSED');
  });
});

// ── M1.1 Fix-T4 §7: handleCommand unknown_cmd → error callback ───────────

describe('handleCommand unknown_cmd path', () => {
  it('posts error envelope with code=unknown_cmd', async () => {
    const cmdMod = await import('./cmd-handlers');
    (cmdMod.isKnownCommand as any).mockReturnValueOnce(false);

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

    const fetchImpl = vi.fn(async () => fakeResponse(200, { ok: true })) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    const connectPromise = client.connect();
    await Promise.resolve();
    captured!.triggerOpen();
    await connectPromise;

    // Drive a fake command frame through onMessage by firing a 'message' event.
    captured!.fire('message', {
      data: JSON.stringify({
        type: 'command',
        correlation_id: 'corr-unk-1',
        cmd: 'mystery-cmd',
        params: {},
      }),
    });

    // Allow async dispatch + fetch to flush.
    await new Promise((r) => setTimeout(r, 5));

    expect(fetchImpl).toHaveBeenCalled();
    const args = (fetchImpl as any).mock.calls[0];
    expect(args[0]).toBe('http://127.0.0.1:9501/api/device/dev-A/callback');
    const body = JSON.parse(args[1].body);
    expect(body.correlation_id).toBe('corr-unk-1');
    expect(body.status).toBe('error');
    expect(body.error.code).toBe('unknown_cmd');
  });

  it('handler throw is propagated with structured code', async () => {
    const cmdMod = await import('./cmd-handlers');
    (cmdMod.isKnownCommand as any).mockReturnValueOnce(true);
    (cmdMod.getCommandHandler as any).mockReturnValueOnce(async () => {
      const err = new Error('publish wait timed out');
      (err as any).code = 'publish_timeout';
      throw err;
    });

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

    const fetchImpl = vi.fn(async () => fakeResponse(200, { ok: true })) as unknown as typeof fetch;
    client.setFetchImpl(fetchImpl);

    const connectPromise = client.connect();
    await Promise.resolve();
    captured!.triggerOpen();
    await connectPromise;

    captured!.fire('message', {
      data: JSON.stringify({
        type: 'command',
        correlation_id: 'corr-pubt',
        cmd: 'publish',
        params: {},
      }),
    });
    await new Promise((r) => setTimeout(r, 5));

    expect(fetchImpl).toHaveBeenCalled();
    const args = (fetchImpl as any).mock.calls[0];
    const body = JSON.parse(args[1].body);
    expect(body.correlation_id).toBe('corr-pubt');
    expect(body.status).toBe('error');
    expect(body.error.code).toBe('publish_timeout');
    expect(body.error.message).toBe('publish wait timed out');
  });
});
