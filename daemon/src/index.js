#!/usr/bin/env node
import 'dotenv/config';
import { createRequire } from 'module';
import { DaemonConnection } from './connection.js';
import { AgentManager } from './agent-manager.js';
import { releaseProfileLocksForProcess } from './profile-lock.js';

const { version } = createRequire(import.meta.url)('../package.json');

// ── CLI args ──────────────────────────────────────────────────────────────────
const args = process.argv.slice(2);
let cliServerUrl = '';
let cliApiKey    = '';

for (let i = 0; i < args.length; i++) {
  if (args[i] === '--server-url' && args[i + 1]) cliServerUrl = args[++i];
  if (args[i] === '--api-key'    && args[i + 1]) cliApiKey    = args[++i];
  if (args[i] === '--help' || args[i] === '-h') {
    console.log('Usage: lightcone-daemon --server-url <url> --api-key <key>');
    process.exit(0);
  }
}

const SERVER_URL      = cliServerUrl || process.env.SERVER_URL      || 'http://localhost:8779';
const MACHINE_API_KEY = cliApiKey    || process.env.MACHINE_API_KEY || '';

if (!MACHINE_API_KEY) {
  console.error('Error: API key is required.');
  console.error('Usage: lightcone-daemon --server-url <url> --api-key <key>');
  process.exit(1);
}

console.log(`[Daemon] v${version} Server: ${SERVER_URL}`);

// ── Start ─────────────────────────────────────────────────────────────────────
const agentManager = new AgentManager({ serverUrl: SERVER_URL, machineApiKey: MACHINE_API_KEY });

const connection = new DaemonConnection({
  serverUrl: SERVER_URL,
  machineApiKey: MACHINE_API_KEY,
  onMessage: (msg) => agentManager.handle(msg, connection),
});

connection.connect();

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[Daemon] Shutting down (${signal})`);
  connection.stop();
  try { await agentManager.stopAll(); } catch (err) { console.error('[Daemon] Shutdown error:', err.message); }
  releaseProfileLocksForProcess();
  process.exit(0);
}

process.on('SIGINT',  () => { shutdown('SIGINT'); });
process.on('SIGTERM', () => { shutdown('SIGTERM'); });
process.on('SIGHUP',  () => { shutdown('SIGHUP'); });
process.on('exit', () => { releaseProfileLocksForProcess(); });
