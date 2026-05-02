#!/usr/bin/env node
import { execFile } from 'node:child_process';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import http from 'node:http';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const offline = process.argv.includes('--offline');

function parseEnvFile(filePath) {
  if (!existsSync(filePath)) return {};
  const env = {};
  for (const rawLine of readFileSync(filePath, 'utf8').split('\n')) {
    const line = rawLine.trim();
    if (!line || line.startsWith('#')) continue;
    const index = line.indexOf('=');
    if (index < 0) continue;
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    env[key] = value;
  }
  return env;
}

const fileEnv = parseEnvFile(path.resolve('.env'));
const env = { ...fileEnv, ...process.env };
const checks = [];

function projectKey() {
  return String(env.COAGENT_PROJECT_KEY || path.basename(process.cwd()) || 'default')
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

function projectDir() {
  return path.join(homedir(), '.coagent', projectKey());
}

function machineKeyPath() {
  return env.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(env.COAGENT_MACHINE_KEY_PATH)
    : path.join(projectDir(), 'machine.key');
}

function daemonSocketPath() {
  return env.COAGENT_DAEMON_SOCKET
    ? path.resolve(env.COAGENT_DAEMON_SOCKET)
    : path.join(projectDir(), 'daemon.sock');
}

function add(name, level, ok, message, fix = '', extra = {}) {
  checks.push({ name, level, ok, message, fix, ...extra });
  return ok;
}

function skipped(name, level, blockedBy) {
  checks.push({
    name,
    level,
    ok: false,
    message: `skipped because ${blockedBy} failed`,
    fix: `Fix ${blockedBy} first, then rerun make doctor.`,
    skipped: `blocked by ${blockedBy}`,
  });
}

async function commandOk(binary, args = ['--version']) {
  try {
    const { stdout, stderr } = await execFileAsync(binary, args, { timeout: 5000 });
    return { ok: true, output: (stdout || stderr).trim().split('\n')[0] ?? '' };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

function mode(filePath) {
  return existsSync(filePath) ? (statSync(filePath).mode & 0o777) : null;
}

async function checkPathTools() {
  const node = await commandOk('node', ['--version']);
  add('node', 'blocker', node.ok && /^v(2[0-9]|[3-9][0-9])\./.test(node.output), node.ok ? `found ${node.output}` : node.error, 'Install Node.js 20+.');

  const pnpm = await commandOk('pnpm', ['--version']);
  add('pnpm', 'blocker', pnpm.ok && Number(pnpm.output.split('.')[0]) >= 9, pnpm.ok ? `found ${pnpm.output}` : pnpm.error, 'Install pnpm 9+.');

  for (const binary of ['pm2', 'claude', 'mysql']) {
    const result = await commandOk(binary, ['--version']);
    add(binary, binary === 'claude' ? 'blocker' : 'blocker', result.ok, result.ok ? `found ${result.output || binary}` : result.error, `Install ${binary} and ensure it is on PATH.`);
  }
}

function checkEnv() {
  add('.env', 'blocker', existsSync('.env'), `.env file ${existsSync('.env') ? 'present' : 'missing'}`, 'cp ops/env.example .env && chmod 600 .env');
  if (existsSync('.env')) {
    const envMode = mode('.env');
    add('.env_mode', 'warn', envMode === 0o600, `.env mode is ${envMode?.toString(8)}`, 'chmod 600 .env');
  }

  for (const key of ['SERVER_URL', 'ADMIN_TOKEN', 'DB_HOST', 'DB_PORT', 'DB_USER', 'DB_NAME', 'COAGENT_PROJECT_KEY']) {
    add(`env_${key}`, 'blocker', Boolean(String(env[key] ?? '').trim()), `${key} ${env[key] ? 'is set' : 'is missing'}`, `Set ${key} in .env.`);
  }
  add('env_ANTHROPIC_API_KEY', 'blocker', Boolean(String(env.ANTHROPIC_API_KEY ?? '').trim()), 'ANTHROPIC_API_KEY is required for --real smoke', 'Set ANTHROPIC_API_KEY in .env or shell.');
}

async function checkMysql() {
  const dbOk = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_NAME'].every((key) => String(env[key] ?? '').trim());
  if (!dbOk) {
    skipped('mysql_connectivity', 'blocker', 'env_DB_*');
    skipped('mysql_schema', 'blocker', 'mysql_connectivity');
    return;
  }

  const baseArgs = [
    '--protocol=TCP',
    '-h', env.DB_HOST,
    '-P', String(env.DB_PORT || '3306'),
    '-u', env.DB_USER,
    env.DB_NAME,
  ];
  const mysqlEnv = { ...process.env, MYSQL_PWD: env.DB_PASSWORD ?? '' };

  let connected = false;
  try {
    await execFileAsync('mysql', [...baseArgs, '-e', 'SELECT 1'], { env: mysqlEnv, timeout: 10000 });
    connected = true;
  } catch (err) {
    add('mysql_connectivity', 'blocker', false, err.message, 'Start MySQL and verify DB_HOST/PORT/USER/PASSWORD/NAME.');
  }
  if (!connected) {
    skipped('mysql_schema', 'blocker', 'mysql_connectivity');
    return;
  }
  add('mysql_connectivity', 'blocker', true, 'mysql client can connect');

  for (const table of ['users', 'machines', 'channels', 'messages']) {
    try {
      await execFileAsync('mysql', [...baseArgs, '-e', `SELECT 1 FROM ${table} LIMIT 1`], { env: mysqlEnv, timeout: 10000 });
      add(`schema_${table}`, 'blocker', true, `${table} table exists`);
    } catch (err) {
      add(`schema_${table}`, 'blocker', false, err.message, 'Start coagent-server once so lightcone initDb() creates schema.');
    }
  }
}

async function checkPm2() {
  const pm2 = await commandOk('pm2', ['jlist']);
  if (!pm2.ok) {
    add('pm2_jlist', 'blocker', false, pm2.error, 'Install pm2 and run pm2 start ecosystem.config.cjs.');
    return;
  }
  let apps = [];
  try {
    apps = JSON.parse(pm2.output.startsWith('[') ? pm2.output : (await execFileAsync('pm2', ['jlist'], { encoding: 'utf8' })).stdout);
  } catch {
    const { stdout } = await execFileAsync('pm2', ['jlist'], { encoding: 'utf8' });
    apps = JSON.parse(stdout);
  }
  for (const name of ['coagent-server', 'coagent-daemon']) {
    const app = apps.find((item) => item.name === name);
    add(`pm2_${name}`, 'blocker', app?.pm2_env?.status === 'online', app ? `${name} is ${app.pm2_env?.status}` : `${name} not found`, 'pm2 start ecosystem.config.cjs && pm2 save');
  }
}

async function checkDaemonHealth() {
  const keyPath = machineKeyPath();
  if (!existsSync(keyPath)) {
    skipped('daemon_admin_status', 'blocker', 'machine_key');
    return;
  }
  const token = readFileSync(keyPath, 'utf8').trim();
  const socketPath = daemonSocketPath();
  if (!existsSync(socketPath)) {
    add('daemon_admin_status', 'blocker', false, `${socketPath} does not exist`, 'pm2 start coagent-daemon or pm2 reload coagent-daemon');
    return;
  }

  await new Promise((resolve) => {
    const req = http.request({
      socketPath,
      path: '/admin/status',
      method: 'GET',
      headers: { Authorization: `Bearer ${token}` },
      timeout: 5000,
    }, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => { raw += chunk; });
      res.on('end', () => {
        add('daemon_admin_status', 'blocker', res.statusCode === 200, raw || `HTTP ${res.statusCode}`, 'Check pm2 logs coagent-daemon --raw --lines 200.');
        resolve();
      });
    });
    req.on('error', (err) => {
      add('daemon_admin_status', 'blocker', false, err.message, 'Check daemon socket path and pm2 daemon status.');
      resolve();
    });
    req.end();
  });
}

function checkFilesystem() {
  const dir = projectDir();
  const keyPath = machineKeyPath();
  const keyExists = add('machine_key', 'blocker', existsSync(keyPath), `${keyPath} ${existsSync(keyPath) ? 'exists' : 'missing'}`, 'make register');
  if (keyExists) {
    const keyMode = mode(keyPath);
    add('machine_key_mode', 'warn', keyMode === 0o600, `${keyPath} mode is ${keyMode?.toString(8)}`, `chmod 600 ${keyPath}`);
  }
  add('project_dir', 'warn', existsSync(dir), `${dir} ${existsSync(dir) ? 'exists' : 'missing'}`, 'make register or start the daemon');
  add('channels_dir', 'warn', existsSync(path.join(dir, 'channels')), 'channels directory check', 'Start the daemon once to create channels/.');
  add('daemon_socket_file', 'warn', existsSync(daemonSocketPath()), 'daemon socket file check', 'Start coagent-daemon.');
}

async function checkOfflineSignals() {
  try {
    const { stdout } = await execFileAsync('pm2', ['logs', 'coagent-daemon', '--raw', '--lines', '200', '--nostream'], { timeout: 10000 });
    const errorLines = stdout.split('\n').filter((line) => /error|failed|exception/i.test(line)).slice(-10);
    add('pm2_recent_errors', 'warn', errorLines.length === 0, errorLines.length ? errorLines.join('\n') : 'no recent daemon error lines');
  } catch (err) {
    add('pm2_recent_errors', 'warn', false, err.message, 'Use pm2 logs coagent-daemon --raw --lines 200.');
  }

  try {
    const { stdout } = await execFileAsync('ps', ['-ef'], { timeout: 5000 });
    const lines = stdout.split('\n').filter((line) => /coagent|lightcone/.test(line) && !/doctor\.mjs/.test(line));
    add('ps_coagent', 'warn', lines.length > 0, lines.slice(0, 10).join('\n') || 'no coagent/lightcone process found', 'Start services with pm2 start ecosystem.config.cjs.');
  } catch (err) {
    add('ps_coagent', 'warn', false, err.message, 'Run ps -ef manually.');
  }
}

async function main() {
  await checkPathTools();
  checkEnv();
  checkFilesystem();
  await checkPm2();
  if (offline) {
    await checkOfflineSignals();
  } else {
    await checkMysql();
    await checkDaemonHealth();
  }

  const summary = {
    blocker_count: checks.filter((check) => check.level === 'blocker' && !check.ok && !check.skipped).length,
    warn_count: checks.filter((check) => check.level === 'warn' && !check.ok && !check.skipped).length,
  };
  console.log(JSON.stringify({ mode: offline ? 'offline' : 'online', checks, summary }, null, 2));
  process.exitCode = summary.blocker_count > 0 ? 1 : 0;
}

main().catch((err) => {
  add('doctor_internal', 'blocker', false, err.message, 'Rerun with node --trace-warnings ops/doctor.mjs.');
  console.log(JSON.stringify({
    mode: offline ? 'offline' : 'online',
    checks,
    summary: {
      blocker_count: checks.filter((check) => check.level === 'blocker' && !check.ok && !check.skipped).length,
      warn_count: checks.filter((check) => check.level === 'warn' && !check.ok && !check.skipped).length,
    },
  }, null, 2));
  process.exit(1);
});
