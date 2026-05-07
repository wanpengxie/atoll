#!/usr/bin/env node
import 'dotenv/config';
import { randomUUID } from 'crypto';
import { createRequire } from 'module';
import { DaemonConnection } from './connection.js';
import { AgentManager } from './agent-manager.js';
import { ChannelManager } from './channel-manager.js';
import { releaseProfileLocksForProcess } from './profile-lock.js';
import { RpcServer } from './rpc-server.js';
import { daemonSocketPath, machineKeyPath, normalizeProjectKey, readMachineKeyFile } from './paths.js';
import { missingMachineApiKeyErrorLines, resolveMachineApiKey } from './machine-api-key.js';
import { DeviceWsServer, makeKeyVerifier, parseDeviceKeysEnv } from './devices/ws-server.js';

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
// Per-device api keys: env COAGENT_DEVICE_KEYS = "id1:key1,id2:key2" (or JSON
// object form). MACHINE_API_KEY is **never** silently reused as a device key —
// the daemon admin credential must remain disjoint from device-side trust
// boundary (see Fix-T2 §1, round-1 review codex#2 / claude-C1).
// For local-dev convenience an explicit opt-in env can be supplied:
//   COAGENT_DEVICE_DEV_FALLBACK_KEY=<key>
// When set, any device id authenticates with that key (dev only — never set in
// production deployments).
const DEVICE_KEYS = parseDeviceKeysEnv(process.env.COAGENT_DEVICE_KEYS ?? '');
const DEVICE_FALLBACK_KEY = String(process.env.COAGENT_DEVICE_DEV_FALLBACK_KEY ?? '').trim();
const verifyDeviceKey = makeKeyVerifier({ deviceKeys: DEVICE_KEYS, fallbackKey: DEVICE_FALLBACK_KEY });
if (DEVICE_KEYS.size === 0 && !DEVICE_FALLBACK_KEY) {
  console.error('[Daemon] WARN device endpoints disabled — no device keys configured (set COAGENT_DEVICE_KEYS or COAGENT_DEVICE_DEV_FALLBACK_KEY)');
}
if (DEVICE_FALLBACK_KEY) {
  console.error('[Daemon] WARN COAGENT_DEVICE_DEV_FALLBACK_KEY is set — every device id authenticates with the same key (dev-only)');
}

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

const connection = new DaemonConnection({
  serverUrl: SERVER_URL,
  machineApiKey: MACHINE_API_KEY,
  onMessage: (msg) => {
    const handler = channelManager.canHandle(msg)
      ? channelManager.handle(msg, connection)
      : agentManager.handle(msg, connection);
    Promise.resolve(handler).catch((err) => {
      console.error(`[Daemon] Failed to handle ${msg.type}:`, err.message);
    });
  },
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
