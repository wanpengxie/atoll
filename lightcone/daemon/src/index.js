#!/usr/bin/env node
import 'dotenv/config';
import os from 'os';
import { randomUUID } from 'crypto';
import { createRequire } from 'module';
import { DaemonConnection } from './connection.js';
import { AgentManager } from './agent-manager.js';
import { ChannelManager } from './channel-manager.js';
import { releaseProfileLocksForProcess } from './profile-lock.js';
import { RpcServer } from './rpc-server.js';
import { daemonSocketPath, machineKeyPath, normalizeProjectKey, readMachineKeyFile } from './paths.js';
import { missingMachineApiKeyErrorLines, resolveMachineApiKey } from './machine-api-key.js';
import { DeviceWsServer, parseDeviceKeysEnv } from './devices/ws-server.js';
import { DeviceStore } from './devices/device-store.js';
import { registerDaemon, fetchDevices } from './devices/registrar.js';

const { version } = createRequire(import.meta.url)('../package.json');

// ── CLI args ──────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let cliServerUrl = '';
let cliApiKey    = '';
let cliProjectKey = '';

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--server-url' && args[i + 1]) cliServerUrl = args[++i];
  if (args[i] === '--api-key'    && args[i + 1]) cliApiKey    = args[++i];
  if (args[i] === '--project-key' && args[i + 1]) cliProjectKey = args[++i];
  if (args[i] === '--help' || args[i] === '-h') {
    console.log('Usage: lightcone-daemon --server-url <url> [--api-key <key>] [--project-key <key>]');
    process.exit(0);
  }
}

const PROJECT_KEY      = normalizeProjectKey(cliProjectKey || process.env.COAGENT_PROJECT_KEY);
const SERVER_URL      = cliServerUrl || process.env.SERVER_URL      || 'http://localhost:8779';
const MACHINE_KEY_PATH = machineKeyPath(PROJECT_KEY);
const { value: MACHINE_API_KEY } = resolveMachineApiKey({
  cliApiKey,
  env: process.env,
  projectKey: PROJECT_KEY,
  readMachineKeyFileImpl: readMachineKeyFile,
});
const DAEMON_SOCKET   = daemonSocketPath(PROJECT_KEY);
const HTTP_PORT_RAW   = process.env.COAGENT_DAEMON_HTTP_PORT ?? '';
const HTTP_PORT       = HTTP_PORT_RAW ? Number(HTTP_PORT_RAW) : null;
const DAEMON_HTTP_URL = Number.isInteger(HTTP_PORT) ? `http://127.0.0.1:${HTTP_PORT}` : '';
const DAEMON_TOKEN    = process.env.COAGENT_DAEMON_TOKEN || randomUUID();

if (!MACHINE_API_KEY) {
  for (const line of missingMachineApiKeyErrorLines(MACHINE_KEY_PATH)) {
    console.error(line);
  }
  process.exit(1);
}

console.error(`[Daemon] v${version} Server: ${SERVER_URL} project=${PROJECT_KEY}`);

// ── Device endpoint config ────────────────────────────────────────────────────
// Device api-key sources (T74):
//   - server (default+preferred): pulled from /api/daemon/{id}/devices on every
//     ws-open, mutated in-place by daemon-WS device.created/revoked/updated push.
//   - env (dev fallback):         COAGENT_DEVICE_KEYS = "id1:key1,..."
//                                 (csv or JSON object form). Server entries
//                                 shadow env entries on shared deviceId.
// COAGENT_DEVICE_SOURCE selects the active source:
//   server → only server pull, register failure leaves the map empty (warn);
//   env    → skip register/pull entirely, use env exclusively;
//   both   → default; on register/pull failure fall back to env-only.
// MACHINE_API_KEY is **never** silently reused as a device key (Fix-T2 §1).
// COAGENT_DEVICE_DEV_FALLBACK_KEY (single shared dev key) deprecated by the
// device-store; left advertised via parseDeviceKeysEnv only.
const DEVICE_SOURCE_RAW = String(process.env.COAGENT_DEVICE_SOURCE ?? 'both').toLowerCase().trim();
const DEVICE_SOURCE = ['server', 'env', 'both'].includes(DEVICE_SOURCE_RAW) ? DEVICE_SOURCE_RAW : 'both';
const ENV_DEVICE_KEYS = parseDeviceKeysEnv(process.env.COAGENT_DEVICE_KEYS ?? '');
// device-store: env entries seeded only when the source allows it; server map
// fills in at ws-open via the registrar bootstrap below.
const deviceStoreEnvSeed = (DEVICE_SOURCE === 'env' || DEVICE_SOURCE === 'both') ? ENV_DEVICE_KEYS : new Map();
const deviceStore = new DeviceStore({
  envEntries: deviceStoreEnvSeed,
  // onRevoke is wired post-construction (deviceWsServer not built yet).
});
const verifyDeviceKey = (args) => deviceStore.verifyKey(args ?? {});
console.error(`[Daemon] device source=${DEVICE_SOURCE} env-entries=${ENV_DEVICE_KEYS.size}`);

// ── Start ─────────────────────────────────────────────────────────────────────
const agentManager = new AgentManager({ serverUrl: SERVER_URL, machineApiKey: MACHINE_API_KEY });
// `channelManager` is referenced inside the deviceWsServer onMessage closure
// below (callback_replay routing — M1.1 Fix-T3); hoist via let so the closure
// reads the live binding.
let channelManager;
const deviceWsServer = new DeviceWsServer({
  verifyKey: verifyDeviceKey,
  // Inbound frames from extension:
  //   - `ack`             — fire-and-forget heartbeat ack (drop)
  //   - `callback_replay` — extension drained its chrome.storage.local outbox
  //                         after WS reconnect; payloads must be replayed via
  //                         channelManager.deviceCallback (with dedupe).
  //   - any other         — log only (forward-compat for future frames)
  onMessage: ({ deviceId, frame }) => {
    if (frame?.type === 'ack') return;
    if (frame?.type === 'callback_replay' && Array.isArray(frame?.payloads)) {
      if (!channelManager || typeof channelManager.handleCallbackReplay !== 'function') {
        console.error(`[Daemon] callback_replay dropped — channelManager not ready (device=${deviceId})`);
        return;
      }
      Promise.resolve(channelManager.handleCallbackReplay({ deviceId, payloads: frame.payloads }))
        .catch((err) => {
          console.error(`[Daemon] callback_replay handler failed for ${deviceId}: ${err?.message ?? err}`);
        });
      return;
    }
    console.error(`[Daemon] device ${deviceId} → ${frame?.type ?? '<unknown>'} frame`);
  },
});
channelManager = new ChannelManager({
  serverUrl: SERVER_URL,
  machineApiKey: MACHINE_API_KEY,
  daemonSocketPath: DAEMON_SOCKET,
  daemonHttpUrl: DAEMON_HTTP_URL,
  daemonToken: DAEMON_TOKEN,
  projectKey: PROJECT_KEY,
  deviceWsServer,
});
const rpcServer = new RpcServer({
  channelManager,
  socketPath: DAEMON_SOCKET,
  httpPort: Number.isInteger(HTTP_PORT) ? HTTP_PORT : null,
  authToken: DAEMON_TOKEN,
  authTokens: [DAEMON_TOKEN, MACHINE_API_KEY],
  deviceWsServer,
  verifyDeviceKey,
  defaultUserId: process.env.COAGENT_DEFAULT_USER_ID || '',
});

// Wire onRevoke now that deviceWsServer exists. Server `device.revoked` events
// (and replaceServer drop-diffs after re-pull) force already-connected
// extensions off the daemon endpoint (T74 §2 contract).
deviceStore._onRevoke = (deviceId) => {
  if (deviceWsServer.disconnect(deviceId, 'revoked')) {
    console.error(`[Daemon] device.revoked → forcibly disconnected ${deviceId}`);
  }
};

// device.* WS event routing (T74 §2): daemon ↔ server WS may push device CRUD
// events; mutate the in-memory store before falling through to channel/agent
// dispatch. Returns true when the event was consumed.
function handleDeviceEvent(msg) {
  if (!msg || typeof msg.type !== 'string' || !msg.type.startsWith('device.')) return false;
  const payload = msg.payload ?? {};
  switch (msg.type) {
    case 'device.created': {
      const ok = deviceStore.upsert(payload);
      console.error(`[Daemon] device.created device_id=${payload?.device_id ?? '?'} ok=${ok} total=${deviceStore.size()}`);
      return true;
    }
    case 'device.updated': {
      const ok = deviceStore.update(payload);
      console.error(`[Daemon] device.updated device_id=${payload?.device_id ?? '?'} ok=${ok}`);
      return true;
    }
    case 'device.revoked': {
      const id = payload?.device_id ?? payload?.id ?? '';
      const removed = deviceStore.remove(id);
      console.error(`[Daemon] device.revoked device_id=${id} removed=${removed} total=${deviceStore.size()}`);
      return true;
    }
    default:
      console.error(`[Daemon] unknown device event ${msg.type}`);
      return true;
  }
}

// Boot/reconnect register + pull (T74 §1, §3). Runs on every WS open. On
// failure the daemon logs and continues — env fallback is already populated.
// `resolvedDaemonId` is cached after the first successful register; the server
// handler is idempotent so re-registering on every reconnect is intentional
// (refreshes daemon_host / capabilities / last_heartbeat).
let resolvedDaemonId = '';
async function bootstrapDeviceSync() {
  if (DEVICE_SOURCE === 'env') {
    console.error(`[Daemon] device source=env — skipping register+pull (env-entries=${ENV_DEVICE_KEYS.size})`);
    return;
  }
  try {
    const reg = await registerDaemon({
      serverUrl: SERVER_URL,
      machineApiKey: MACHINE_API_KEY,
      daemonId: resolvedDaemonId || '',
      host: os.hostname(),
      capabilities: ['xhs-creator'],
    });
    const daemonId = String(reg?.daemon_id ?? '').trim();
    if (!daemonId) throw new Error('register response missing daemon_id');
    resolvedDaemonId = daemonId;
    const devices = await fetchDevices({
      serverUrl: SERVER_URL,
      machineApiKey: MACHINE_API_KEY,
      daemonId,
    });
    deviceStore.replaceServer(devices);
    console.error(`[Daemon] register+pull ok — daemon_id=${daemonId} devices=${devices.length} total=${deviceStore.size()}`);
  } catch (err) {
    if (DEVICE_SOURCE === 'server') {
      console.error(`[Daemon] register+pull failed (source=server, no env fallback): ${err?.message ?? err}`);
    } else {
      console.error(`[Daemon] register+pull failed — falling back to env (${ENV_DEVICE_KEYS.size} entries): ${err?.message ?? err}`);
    }
  }
}

const connection = new DaemonConnection({
  serverUrl: SERVER_URL,
  machineApiKey: MACHINE_API_KEY,
  onMessage: (msg) => {
    if (handleDeviceEvent(msg)) return;
    const handler = channelManager.canHandle(msg)
      ? channelManager.handle(msg, connection)
      : agentManager.handle(msg, connection);
    Promise.resolve(handler).catch((err) => {
      console.error(`[Daemon] Failed to handle ${msg.type}:`, err.message);
    });
  },
  onReady: () => bootstrapDeviceSync(),
});

let shuttingDown = false;
async function shutdown(signal, exitCode = 0) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.error(`[Daemon] Shutting down (${signal})`);
  connection.stop();
  try { await rpcServer.stop(); } catch (err) { console.error('[Daemon] RPC shutdown error:', err.message); }
  try { await channelManager.stopAll(); } catch (err) { console.error('[Daemon] Channel shutdown error:', err.message); }
  try { await agentManager.stopAll(); } catch (err) { console.error('[Daemon] Agent shutdown error:', err.message); }
  releaseProfileLocksForProcess();
  process.exit(exitCode);
}

async function main() {
  await channelManager.start();
  await rpcServer.start();
  channelManager.setConnection(connection);
  connection.connect();
  console.error(`[Daemon] RPC ready at unix://${DAEMON_SOCKET}${DAEMON_HTTP_URL ? ` and ${DAEMON_HTTP_URL}` : ''}`);
}

main().catch(async (err) => {
  console.error('[Daemon] Startup failed:', err.message);
  await shutdown('startup_error', 1);
});

process.on('SIGINT',  () => { shutdown('SIGINT'); });
process.on('SIGTERM', () => { shutdown('SIGTERM'); });
process.on('SIGHUP',  () => { shutdown('SIGHUP'); });
process.on('exit', () => { releaseProfileLocksForProcess(); });
