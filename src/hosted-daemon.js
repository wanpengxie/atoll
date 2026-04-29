// ─── Platform-hosted daemon ──────────────────────────────────────────────────
// Supports two modes:
// 1. Embedded: server runs daemon locally (dev / self-hosted VPS)
// 2. Remote:   server only creates the platform machine record;
//              a separate VPS runs the daemon with that machine's API key
//
// Controlled by HOSTED_DAEMON env var:
//   "off"    → don't create platform machine, don't start daemon
//   "remote" → create platform machine record only (for Railway + remote VPS)
//   anything else / unset → create + start embedded daemon (default)

import crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { getDb, insertMachine } from './db/index.js';

const DEFAULT_SERVER_ID = process.env.DEFAULT_SERVER_ID ?? 'server-001';

let connection = null;
let agentManager = null;

/**
 * Ensure the platform machine record exists. Returns its API key.
 * Idempotent — safe to call on every server startup.
 */
export async function ensurePlatformMachine() {
  const db = getDb();
  const [existing] = await db.execute(
    `SELECT * FROM machines WHERE server_id = ? AND is_platform = 1 LIMIT 1`,
    [DEFAULT_SERVER_ID]
  );

  if (existing.length > 0) {
    console.log(`[HostedDaemon] Platform machine exists: ${existing[0].id}`);
    return existing[0].api_key;
  }

  const apiKey = 'sk_machine_' + crypto.randomBytes(32).toString('hex');
  const machine = await insertMachine(db, {
    id: uuidv4(),
    serverId: DEFAULT_SERVER_ID,
    ownerId: null,
    name: 'Platform',
    apiKey,
    apiKeyPrefix: apiKey.slice(0, 18),
    isPlatform: true,
  });
  console.log(`[HostedDaemon] Created platform machine: ${machine.id}`);
  console.log(`[HostedDaemon] Platform machine API key: ${apiKey}`);
  return apiKey;
}

/**
 * Start the platform-hosted daemon.
 * - "remote" mode: only creates the machine record (daemon runs elsewhere)
 * - default mode: creates record + starts embedded daemon on this process
 */
export async function startHostedDaemon(port) {
  const mode = process.env.HOSTED_DAEMON ?? '';

  const apiKey = await ensurePlatformMachine();

  if (mode === 'remote') {
    console.log('[HostedDaemon] Remote mode — platform machine ready, daemon runs on external VPS');
    return;
  }

  // Embedded mode: start daemon in this process
  const { DaemonConnection } = await import('../daemon/src/connection.js');
  const { AgentManager } = await import('../daemon/src/agent-manager.js');

  const serverUrl = `http://localhost:${port}`;
  agentManager = new AgentManager({ serverUrl, machineApiKey: apiKey });

  connection = new DaemonConnection({
    serverUrl,
    machineApiKey: apiKey,
    onMessage: (msg) => agentManager.handle(msg, connection),
  });

  connection.connect();
  console.log('[HostedDaemon] Embedded daemon started');
}

export function stopHostedDaemon() {
  if (connection) {
    connection.stop();
    connection = null;
  }
  if (agentManager) {
    agentManager.stopAll();
    agentManager = null;
  }
}
