// Vitest unit tests for entrypoints/background/external-bind.ts (T148 / M1.6-T6).
//
// Coverage:
//   - isAllowedSenderOrigin: wildcard host, exact host, scheme mismatch,
//                            missing origin, malformed URL, multiple patterns
//   - getDeviceInfo:         first call generates + persists a device_id;
//                            second call returns the same id; surface
//                            bound snapshot when v4 fields present.
//   - setDeviceToken:        happy path persists + connects + responds
//                            "connected"; missing field → invalid_payload;
//                            bad URL → invalid_payload; connect() failure
//                            maps to ws_connect_failed / ws_connect_timeout.
//   - unbindDevice:          disconnects + clears v4 fields, keeps device_id.
//   - top-level handler:     unknown action → invalid_payload; origin reject
//                            → origin_not_allowed (no side effects).
//
// All side effects are routed through ExternalBindDeps so we don't need
// chrome APIs — the deps adapter is the seam.

import { describe, it, expect, vi } from 'vitest';

import type { ConnectionConfig } from './connection-state';
import {
  handleExternalMessage,
  isAllowedSenderOrigin,
  parseAllowedOriginsEnv,
  type ExternalBindDeps,
  type ExternalBindMessage,
  type ExternalSender,
} from './external-bind';

// ────────────────────────────────────────────────────────────────────────
// Test harness helpers
// ────────────────────────────────────────────────────────────────────────

const ALLOWED = ['https://*.coagent.dev/*', 'http://localhost:*/*'];

function makeConfig(over: Partial<ConnectionConfig> = {}): ConnectionConfig {
  return {
    serverUrl: '',
    autoReconnect: true,
    apiKey: '',
    daemonHttpBase: 'http://127.0.0.1:9501',
    deviceId: '',
    userId: '',
    coagentServerUrl: 'https://coagent-server',
    wsUrl: '',
    httpBase: 'http://127.0.0.1:9501',
    channelId: '',
    daemonId: '',
    serverWsEndpoint: '',
    deviceActorId: '',
    deviceActorToken: '',
    ...over,
  };
}

/**
 * Build a fake deps adapter backed by an in-memory ConnectionConfig
 * store. Each call to `saveConfig` merges the patch + records it so
 * tests can assert what was written.
 */
function makeDeps(initial: Partial<ConnectionConfig> = {}, opts: {
  connectResult?: { success: boolean; error?: string };
  uuidSequence?: string[];
  waitForOpen?: ExternalBindDeps['waitForOpen'];
} = {}): {
  deps: ExternalBindDeps;
  state: { config: ConnectionConfig; saves: Partial<ConnectionConfig>[] };
  disconnectAll: ReturnType<typeof vi.fn>;
  applyClients: ReturnType<typeof vi.fn>;
  connect: ReturnType<typeof vi.fn>;
} {
  const state = {
    config: makeConfig(initial),
    saves: [] as Partial<ConnectionConfig>[],
  };
  const disconnectAll = vi.fn();
  const applyClients = vi.fn();
  const connect = vi.fn(async () => opts.connectResult ?? { success: true });
  const uuids = (opts.uuidSequence ?? ['uuid-1', 'uuid-2']).slice();
  const deps: ExternalBindDeps = {
    getConfig: async () => state.config,
    saveConfig: async (patch) => {
      state.saves.push(patch);
      state.config = { ...state.config, ...patch };
      return state.config;
    },
    disconnectAll,
    applyClients,
    connect,
    extensionVersion: '1.2.3-test',
    allowedOrigins: ALLOWED,
    generateDeviceID: () => `xhs-${uuids.shift() ?? 'fallback'}`,
    waitForOpen: opts.waitForOpen,
  };
  return { deps, state, disconnectAll, applyClients, connect };
}

function senderOk(origin = 'https://app.coagent.dev'): ExternalSender {
  return { origin, url: origin + '/', id: 'webpage' };
}

// ────────────────────────────────────────────────────────────────────────
// isAllowedSenderOrigin
// ────────────────────────────────────────────────────────────────────────

describe('isAllowedSenderOrigin', () => {
  it('accepts wildcard host suffix match', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://app.coagent.dev' }, ALLOWED)).toBe(true);
    expect(isAllowedSenderOrigin({ origin: 'https://www.coagent.dev' }, ALLOWED)).toBe(true);
    // suffix host itself (no leading subdomain) matches *.host per Chrome semantics
    expect(isAllowedSenderOrigin({ origin: 'https://coagent.dev' }, ALLOWED)).toBe(true);
  });

  it('rejects non-matching host', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://evil.com' }, ALLOWED)).toBe(false);
    // Look-alike that doesn't end with allowed suffix.
    expect(isAllowedSenderOrigin({ origin: 'https://coagent.dev.evil.com' }, ALLOWED)).toBe(false);
  });

  it('respects scheme: http://localhost matches, https://localhost does not (allowlist is http only)', () => {
    expect(isAllowedSenderOrigin({ origin: 'http://localhost:5173' }, ALLOWED)).toBe(true);
    expect(isAllowedSenderOrigin({ origin: 'https://localhost:5173' }, ALLOWED)).toBe(false);
  });

  it('falls back to sender.url when origin is missing', () => {
    expect(isAllowedSenderOrigin({ url: 'https://app.coagent.dev/page' }, ALLOWED)).toBe(true);
    expect(isAllowedSenderOrigin({ url: 'https://nope.com/page' }, ALLOWED)).toBe(false);
  });

  it('rejects sender with neither origin nor url', () => {
    expect(isAllowedSenderOrigin({}, ALLOWED)).toBe(false);
    expect(isAllowedSenderOrigin(undefined, ALLOWED)).toBe(false);
  });

  it('rejects malformed URL strings', () => {
    expect(isAllowedSenderOrigin({ origin: 'not-a-url' }, ALLOWED)).toBe(false);
    expect(isAllowedSenderOrigin({ origin: '' }, ALLOWED)).toBe(false);
  });

  it('returns false when allowlist is empty (fail closed)', () => {
    expect(isAllowedSenderOrigin({ origin: 'https://app.coagent.dev' }, [])).toBe(false);
  });

  it('supports exact host (no wildcard) patterns', () => {
    const list = ['https://app.coagent.dev/*'];
    expect(isAllowedSenderOrigin({ origin: 'https://app.coagent.dev' }, list)).toBe(true);
    expect(isAllowedSenderOrigin({ origin: 'https://other.coagent.dev' }, list)).toBe(false);
  });
});

// ────────────────────────────────────────────────────────────────────────
// parseAllowedOriginsEnv
// ────────────────────────────────────────────────────────────────────────

describe('parseAllowedOriginsEnv', () => {
  it('returns empty array for missing / empty env', () => {
    expect(parseAllowedOriginsEnv(undefined)).toEqual([]);
    expect(parseAllowedOriginsEnv('')).toEqual([]);
    expect(parseAllowedOriginsEnv('  ')).toEqual([]);
  });

  it('splits on comma + trims whitespace', () => {
    expect(parseAllowedOriginsEnv('https://a.com/*, http://localhost:*/*')).toEqual([
      'https://a.com/*',
      'http://localhost:*/*',
    ]);
  });

  it('drops empty fragments', () => {
    expect(parseAllowedOriginsEnv('a,,b,')).toEqual(['a', 'b']);
  });
});

// ────────────────────────────────────────────────────────────────────────
// handleExternalMessage — top-level dispatch
// ────────────────────────────────────────────────────────────────────────

describe('handleExternalMessage — top-level dispatch', () => {
  it('rejects when sender origin is not on allowlist (no side effects)', async () => {
    const { deps, state, disconnectAll, connect } = makeDeps();
    const res = await handleExternalMessage(
      { action: 'getDeviceInfo' },
      { origin: 'https://evil.com' },
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('origin_not_allowed');
      expect(res.detail).toBe('https://evil.com');
    }
    // crucially: no storage write, no WS connect
    expect(state.saves).toEqual([]);
    expect(disconnectAll).not.toHaveBeenCalled();
    expect(connect).not.toHaveBeenCalled();
  });

  it('rejects unknown action with invalid_payload', async () => {
    const { deps } = makeDeps();
    const res = await handleExternalMessage(
      { action: 'mystery' as unknown as ExternalBindMessage['action'] },
      senderOk(),
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('invalid_payload');
    }
  });

  it('rejects non-object message', async () => {
    const { deps } = makeDeps();
    const res = await handleExternalMessage(
      null as unknown as ExternalBindMessage,
      senderOk(),
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('invalid_payload');
    }
  });
});

// ────────────────────────────────────────────────────────────────────────
// getDeviceInfo
// ────────────────────────────────────────────────────────────────────────

describe('getDeviceInfo', () => {
  it('generates + persists a new device_id when storage is empty', async () => {
    const { deps, state } = makeDeps({}, { uuidSequence: ['abc'] });
    const res = await handleExternalMessage({ action: 'getDeviceInfo' }, senderOk(), deps);
    expect(res.status).toBe('ok');
    if (res.status === 'ok') {
      expect(res.device_id).toBe('xhs-abc');
      expect(res.version).toBe('1.2.3-test');
      expect(res.bound?.channel_id).toBeUndefined();
    }
    // saveConfig was called with the generated id
    expect(state.saves).toEqual([{ deviceId: 'xhs-abc' }]);
    expect(state.config.deviceId).toBe('xhs-abc');
  });

  it('returns the existing device_id without regenerating', async () => {
    const { deps, state } = makeDeps({ deviceId: 'xhs-existing' });
    const res = await handleExternalMessage({ action: 'getDeviceInfo' }, senderOk(), deps);
    expect(res.status).toBe('ok');
    if (res.status === 'ok') {
      expect(res.device_id).toBe('xhs-existing');
    }
    // no save: already persisted
    expect(state.saves).toEqual([]);
  });

  it('surfaces bound snapshot when v4 fields are populated', async () => {
    const { deps } = makeDeps({
      deviceId: 'xhs-dev',
      channelId: 'ch-1',
      userId: 'user-A',
      deviceActorId: 'sess-X',
      serverWsEndpoint: 'wss://app.coagent.dev/devicebus',
    });
    const res = await handleExternalMessage({ action: 'getDeviceInfo' }, senderOk(), deps);
    expect(res.status).toBe('ok');
    if (res.status === 'ok') {
      expect(res.bound).toEqual({
        channel_id: 'ch-1',
        user_id: 'user-A',
        actor_id: 'sess-X',
        server_ws_url: 'wss://app.coagent.dev/devicebus',
      });
    }
  });
});

// ────────────────────────────────────────────────────────────────────────
// setDeviceToken
// ────────────────────────────────────────────────────────────────────────

describe('setDeviceToken', () => {
  const baseMsg: ExternalBindMessage = {
    action: 'setDeviceToken',
    server_ws_url: 'wss://app.coagent.dev/devicebus',
    actor_id: 'sess-A',
    token: 'tok-A',
    channel_id: 'ch-1',
    user_id: 'user-1',
    device_id: 'xhs-dev-A',
    expires_at: Date.now() + 3600_000,
  };

  it('happy path: persists, calls disconnectAll/applyClients/connect, returns connected', async () => {
    const { deps, state, disconnectAll, applyClients, connect } = makeDeps();
    const res = await handleExternalMessage(baseMsg, senderOk(), deps);
    expect(res.status).toBe('connected');
    if (res.status === 'connected') {
      expect(res.actor_id).toBe('sess-A');
      expect(res.channel_id).toBe('ch-1');
      expect(res.user_id).toBe('user-1');
    }
    // Persisted v4 bundle + cleared legacy fields
    expect(state.saves).toEqual([
      {
        serverWsEndpoint: 'wss://app.coagent.dev/devicebus',
        deviceActorId: 'sess-A',
        deviceActorToken: 'tok-A',
        channelId: 'ch-1',
        deviceId: 'xhs-dev-A',
        connectionMode: 'server',
        autoReconnect: true,
        userId: 'user-1',
        serverUrl: '',
        wsUrl: '',
      },
    ]);
    expect(disconnectAll).toHaveBeenCalledOnce();
    expect(applyClients).toHaveBeenCalledOnce();
    expect(connect).toHaveBeenCalledOnce();
  });

  it('proxy mode: setDeviceToken is a no-op and does not require token fields', async () => {
    const { deps, state, disconnectAll, applyClients, connect } = makeDeps({
      connectionMode: 'proxy',
      proxyEndpoint: 'ws://127.0.0.1:10387',
      deviceActorId: 'tool:xhs',
      channelId: 'ch-proxy',
      userId: 'user-proxy',
    });
    const res = await handleExternalMessage(
      { action: 'setDeviceToken' } as ExternalBindMessage,
      senderOk(),
      deps,
    );
    expect(res.status).toBe('connected');
    if (res.status === 'connected') {
      expect(res.actor_id).toBe('tool:xhs');
      expect(res.channel_id).toBe('ch-proxy');
      expect(res.user_id).toBe('user-proxy');
    }
    expect(state.saves).toEqual([]);
    expect(disconnectAll).not.toHaveBeenCalled();
    expect(applyClients).not.toHaveBeenCalled();
    expect(connect).not.toHaveBeenCalled();
  });

  it('missing server_ws_url returns invalid_payload', async () => {
    const { deps, disconnectAll, connect } = makeDeps();
    const msg = { ...baseMsg, server_ws_url: '' };
    const res = await handleExternalMessage(msg, senderOk(), deps);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('invalid_payload');
      expect(res.detail).toContain('server_ws_url');
    }
    expect(disconnectAll).not.toHaveBeenCalled();
    expect(connect).not.toHaveBeenCalled();
  });

  it.each([
    ['actor_id', { actor_id: '' }],
    ['token', { token: '' }],
    ['channel_id', { channel_id: '' }],
    ['device_id', { device_id: '' }],
  ])('missing %s returns invalid_payload', async (_label, override) => {
    const { deps } = makeDeps();
    const res = await handleExternalMessage({ ...baseMsg, ...override }, senderOk(), deps);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') expect(res.reason).toBe('invalid_payload');
  });

  it('rejects non-ws[s] schemes', async () => {
    const { deps } = makeDeps();
    const res = await handleExternalMessage(
      { ...baseMsg, server_ws_url: 'https://app.coagent.dev/devicebus' },
      senderOk(),
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('invalid_payload');
      expect(res.detail).toContain('ws[s]');
    }
  });

  it('rejects malformed URL', async () => {
    const { deps } = makeDeps();
    const res = await handleExternalMessage(
      { ...baseMsg, server_ws_url: 'not-a-url' },
      senderOk(),
      deps,
    );
    expect(res.status).toBe('failed');
    if (res.status === 'failed') expect(res.reason).toBe('invalid_payload');
  });

  it('maps connect() failure to ws_connect_failed', async () => {
    const { deps } = makeDeps({}, {
      connectResult: { success: false, error: 'handshake refused' },
    });
    const res = await handleExternalMessage(baseMsg, senderOk(), deps);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('ws_connect_failed');
      expect(res.detail).toContain('handshake refused');
    }
  });

  it('maps connect() timeout error to ws_connect_timeout', async () => {
    const { deps } = makeDeps({}, {
      connectResult: { success: false, error: 'WS connect timeout' },
    });
    const res = await handleExternalMessage(baseMsg, senderOk(), deps);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('ws_connect_timeout');
    }
  });

  it('waitForOpen=false → ws_connect_timeout (even when connect succeeded)', async () => {
    const { deps } = makeDeps({}, {
      connectResult: { success: true },
      waitForOpen: async () => ({ open: false, error: 'open event never arrived' }),
    });
    const res = await handleExternalMessage(baseMsg, senderOk(), deps);
    expect(res.status).toBe('failed');
    if (res.status === 'failed') {
      expect(res.reason).toBe('ws_connect_timeout');
      expect(res.detail).toContain('open event never arrived');
    }
  });

  it('waitForOpen=true → returns connected', async () => {
    const { deps } = makeDeps({}, {
      connectResult: { success: true },
      waitForOpen: async () => ({ open: true }),
    });
    const res = await handleExternalMessage(baseMsg, senderOk(), deps);
    expect(res.status).toBe('connected');
  });
});

// ────────────────────────────────────────────────────────────────────────
// unbindDevice
// ────────────────────────────────────────────────────────────────────────

describe('unbindDevice', () => {
  it('clears v4 fields, calls disconnectAll, keeps device_id', async () => {
    const { deps, state, disconnectAll } = makeDeps({
      deviceId: 'xhs-keep',
      deviceActorId: 'sess-A',
      deviceActorToken: 'tok-A',
      channelId: 'ch-1',
      serverWsEndpoint: 'wss://app.coagent.dev/devicebus',
    });
    const res = await handleExternalMessage({ action: 'unbindDevice' }, senderOk(), deps);
    expect(res.status).toBe('unbound');
    expect(disconnectAll).toHaveBeenCalledOnce();
    expect(state.saves).toEqual([
      {
        serverWsEndpoint: '',
        deviceActorId: '',
        deviceActorToken: '',
        channelId: '',
        autoReconnect: false,
      },
    ]);
    // device_id intentionally retained
    expect(state.config.deviceId).toBe('xhs-keep');
  });
});
