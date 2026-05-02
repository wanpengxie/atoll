#!/usr/bin/env node
import { execFile } from 'node:child_process';
import { closeSync, existsSync, openSync, readFileSync, readdirSync, readSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const offline = process.argv.includes('--offline');
const expectedTables = ['users', 'machines', 'channels', 'messages'];
const pm2LogTailLineLimit = 200;
const pm2LogTailInitialBytes = 64 * 1024;
const pm2LogTailMaxBytes = 1024 * 1024;
const pm2LogTailMaxLineChars = 64 * 1024;

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

function skipped(name, blockedBy) {
  checks.push({
    name,
    level: 'skipped',
    ok: false,
    message: `blocked by ${blockedBy}`,
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
  const tools = {};

  const node = await commandOk('node', ['--version']);
  tools.node = node;
  add('node', 'blocker', node.ok && /^v(2[0-9]|[3-9][0-9])\./.test(node.output), node.ok ? `found ${node.output}` : node.error, 'Install Node.js 20+.');

  const pnpm = await commandOk('pnpm', ['--version']);
  tools.pnpm = pnpm;
  add('pnpm', 'blocker', pnpm.ok && Number(pnpm.output.split('.')[0]) >= 9, pnpm.ok ? `found ${pnpm.output}` : pnpm.error, 'Install pnpm 9+.');

  for (const binary of ['pm2', 'claude', 'mysql']) {
    const result = await commandOk(binary, ['--version']);
    tools[binary] = result;
    add(binary, 'blocker', result.ok, result.ok ? `found ${result.output || binary}` : result.error, `Install ${binary} and ensure it is on PATH.`);
  }

  return tools;
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
  add(
    'env_ANTHROPIC_API_KEY',
    offline ? 'warn' : 'blocker',
    Boolean(String(env.ANTHROPIC_API_KEY ?? '').trim()),
    offline ? 'ANTHROPIC_API_KEY is optional in --offline mode' : 'ANTHROPIC_API_KEY is required for --real smoke',
    'Set ANTHROPIC_API_KEY in .env or shell.',
  );
}

async function checkMysql(tools) {
  if (!tools.mysql?.ok) {
    skipped('mysql_connectivity', 'mysql binary');
    skipped('mysql_schema', 'mysql binary');
    return;
  }

  const dbOk = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_NAME'].every((key) => String(env[key] ?? '').trim());
  if (!dbOk) {
    skipped('mysql_connectivity', 'env_DB_*');
    skipped('mysql_schema', 'mysql_connectivity');
    return;
  }

  const baseArgs = [
    '--protocol=TCP',
    '-h', env.DB_HOST,
    '-P', String(env.DB_PORT || '3306'),
    '-u', env.DB_USER,
    '--batch',
    '--skip-column-names',
    env.DB_NAME,
  ];
  const mysqlEnv = { ...process.env, MYSQL_PWD: env.DB_PASSWORD ?? '' };

  try {
    await execFileAsync('mysql', [...baseArgs, '-e', 'SELECT 1'], { env: mysqlEnv, timeout: 10000 });
    add('mysql_connectivity', 'blocker', true, 'mysql client can connect');
  } catch (err) {
    add('mysql_connectivity', 'blocker', false, err.message, 'Start MySQL and verify DB_HOST/PORT/USER/PASSWORD/NAME.');
    skipped('mysql_schema', 'mysql_connectivity');
    return;
  }

  try {
    const { stdout } = await execFileAsync(
      'mysql',
      [...baseArgs, '-e', 'SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()'],
      { env: mysqlEnv, timeout: 10000 },
    );
    const tables = stdout.split('\n').map((line) => line.trim()).filter(Boolean).sort();
    const missing = expectedTables.filter((table) => !tables.includes(table));
    add('mysql_schema', 'warn', missing.length === 0, missing.length ? `missing tables: ${missing.join(', ')}` : 'expected tables present', 'Start coagent-server once so lightcone initDb() creates schema.', {
      schema: {
        tables,
        expected_present: expectedTables,
        missing,
      },
    });
  } catch (err) {
    add('mysql_schema', 'warn', false, err.message, 'Inspect INFORMATION_SCHEMA permissions and DB_NAME.');
  }
}

async function checkPm2(tools) {
  if (!tools.pm2?.ok) {
    skipped('pm2_processes', 'pm2 binary');
    skipped('pm2_logs', 'pm2 binary');
    return;
  }

  let apps = [];
  try {
    const { stdout } = await execFileAsync('pm2', ['jlist'], { encoding: 'utf8', timeout: 10000 });
    apps = JSON.parse(stdout);
  } catch (err) {
    add('pm2_processes', 'blocker', false, err.message, 'Run pm2 start ecosystem.config.cjs && pm2 save.');
    skipped('pm2_logs', 'pm2_processes');
    return;
  }

  const expectedApps = ['coagent-server', 'coagent-daemon'];
  const details = expectedApps.map((name) => {
    const app = apps.find((item) => item.name === name);
    return { name, status: app?.pm2_env?.status ?? 'missing' };
  });
  const ok = details.every((item) => item.status === 'online');
  add('pm2_processes', 'blocker', ok, details.map((item) => `${item.name}=${item.status}`).join(', '), 'pm2 start ecosystem.config.cjs && pm2 save', { processes: details });
}

async function checkDaemonHealth() {
  const http = await import('node:http');
  const keyPath = machineKeyPath();
  if (!existsSync(keyPath)) {
    skipped('daemon_admin_status', 'machine_key');
    return;
  }
  const token = readFileSync(keyPath, 'utf8').trim();
  const socketPath = daemonSocketPath();
  if (!existsSync(socketPath)) {
    skipped('daemon_admin_status', 'daemon_socket_file');
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

function readFileWindow(filePath, start, length) {
  const fd = openSync(filePath, 'r');
  try {
    const buffer = Buffer.allocUnsafe(length);
    let totalRead = 0;
    while (totalRead < length) {
      const bytesRead = readSync(fd, buffer, totalRead, length - totalRead, start + totalRead);
      if (bytesRead === 0) break;
      totalRead += bytesRead;
    }
    return buffer.subarray(0, totalRead).toString('utf8');
  } finally {
    closeSync(fd);
  }
}

function truncateTailLine(line) {
  if (line.length <= pm2LogTailMaxLineChars) return line;
  return `[truncated] ${line.slice(-pm2LogTailMaxLineChars)}`;
}

function tailLogLines(filePath, lineLimit = pm2LogTailLineLimit) {
  const fileSize = statSync(filePath).size;
  if (fileSize === 0) return [];

  const maxBytes = Math.min(fileSize, pm2LogTailMaxBytes);
  let windowBytes = Math.min(fileSize, pm2LogTailInitialBytes);

  while (true) {
    const start = fileSize - windowBytes;
    const text = readFileWindow(filePath, start, windowBytes);
    const rawLines = text.split('\n');
    if (start > 0 && rawLines.length === 1) {
      return [truncateTailLine(rawLines[0])];
    }
    if (start > 0) rawLines.shift();
    const lines = rawLines.filter(Boolean);
    if (lines.length >= lineLimit || start === 0 || windowBytes >= maxBytes) {
      return lines.slice(-lineLimit).map(truncateTailLine);
    }
    windowBytes = Math.min(maxBytes, windowBytes * 2);
  }
}

function tailPm2LogFiles() {
  const logDir = path.join(homedir(), '.pm2', 'logs');
  if (!existsSync(logDir)) return { lines: [], files: [], error: `${logDir} does not exist` };

  const files = readdirSync(logDir)
    .filter((fileName) => /coagent-(daemon|server).*\.log$/.test(fileName))
    .map((fileName) => path.join(logDir, fileName));
  const lines = files.flatMap((filePath) => tailLogLines(filePath)
    .map((line) => `${path.basename(filePath)}: ${line}`));
  return { lines, files };
}

async function checkOfflineSignals() {
  const { lines, files, error } = tailPm2LogFiles();
  const errorLines = lines.filter((line) => /error|failed|exception/i.test(line)).slice(-10);
  add('pm2_file_logs', 'warn', !error && errorLines.length === 0, error ?? (errorLines.length ? errorLines.join('\n') : 'no recent daemon/server error lines'), 'Inspect ~/.pm2/logs/*.log directly.', {
    log_files: files,
  });

  try {
    const { stdout } = await execFileAsync('ps', ['-ef'], { timeout: 5000 });
    const lines = stdout.split('\n').filter((line) => /node.*lightcone|coagent-daemon/.test(line) && !/doctor\.mjs/.test(line));
    add('ps_coagent', 'warn', lines.length > 0, lines.slice(0, 10).join('\n') || 'no node lightcone/coagent-daemon process found', 'Start services with pm2 start ecosystem.config.cjs.');
  } catch (err) {
    add('ps_coagent', 'warn', false, err.message, 'Run ps -ef manually.');
  }
}

function summary() {
  return {
    blocker_count: checks.filter((check) => check.level === 'blocker' && !check.ok).length,
    warn_count: checks.filter((check) => check.level === 'warn' && !check.ok).length,
  };
}

async function main() {
  const tools = await checkPathTools();
  checkEnv();
  checkFilesystem();
  if (offline) {
    await checkOfflineSignals();
  } else {
    await checkPm2(tools);
    await checkMysql(tools);
    await checkDaemonHealth();
  }

  const result = summary();
  console.log(JSON.stringify({ mode: offline ? 'offline' : 'online', checks, summary: result }, null, 2));
  process.exitCode = result.blocker_count > 0 ? 1 : 0;
}

main().catch((err) => {
  add('doctor_internal', 'blocker', false, err.message, 'Rerun with node --trace-warnings ops/doctor.mjs.');
  console.log(JSON.stringify({
    mode: offline ? 'offline' : 'online',
    checks,
    summary: summary(),
  }, null, 2));
  process.exit(1);
});
