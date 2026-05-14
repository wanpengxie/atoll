import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';

const execFileAsync = promisify(execFile);
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const doctorPath = path.join(repoRoot, 'ops', 'doctor.mjs');
const sentinelPath = path.join(repoRoot, 'ops', 'schema-sentinel.json');

function tempDir(t) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'doctor-test-'));
  t.after(() => {
    rmSync(dir, { recursive: true, force: true });
  });
  return dir;
}

function writeBin(dir, name, contents) {
  const filePath = path.join(dir, name);
  writeFileSync(filePath, contents, { mode: 0o755 });
  return filePath;
}

function installFakeTools(dir) {
  mkdirSync(dir, { recursive: true });
  writeBin(dir, 'pnpm', '#!/bin/sh\necho 9.0.0\n');
  writeBin(dir, 'claude', '#!/bin/sh\necho 1.0.0\n');
  writeBin(dir, 'pm2', `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo 5.0.0
  exit 0
fi
if [ "$1" = "jlist" ]; then
  echo '[{"name":"coagent-server","pm2_env":{"status":"online"}},{"name":"coagent-daemon","pm2_env":{"status":"online"}}]'
  exit 0
fi
echo ok
`);
  writeBin(dir, 'mysql', `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "mysql  Ver 8.0"
  exit 0
fi
args="$*"
case "$args" in
  *"SELECT 1"*) echo 1 ;;
  *"INFORMATION_SCHEMA.TABLES"*) printf '%s' "$MYSQL_TABLES" | tr ',' '\\n' ;;
  *) echo "" ;;
esac
`);
}

async function runDoctor({ cwd, homeDir, binDir, args = [], env = {} }) {
  try {
    const result = await execFileAsync(process.execPath, [doctorPath, ...args], {
      cwd,
      env: {
        ...process.env,
        HOME: homeDir,
        PATH: `${binDir}${path.delimiter}${process.env.PATH ?? ''}`,
        ...env,
      },
      encoding: 'utf8',
    });
    return { code: 0, stdout: result.stdout, stderr: result.stderr, body: JSON.parse(result.stdout) };
  } catch (err) {
    return { code: err.code, stdout: err.stdout, stderr: err.stderr, body: JSON.parse(err.stdout) };
  }
}

function checkByName(body, name) {
  return body.checks.find((check) => check.name === name);
}

test('doctor reports placeholder ADMIN_TOKEN and unsafe secret file modes as blockers by default', async (t) => {
  const cwd = tempDir(t);
  const homeDir = tempDir(t);
  const binDir = path.join(tempDir(t), 'bin');
  installFakeTools(binDir);

  const keyPath = path.join(cwd, 'machine.key');
  writeFileSync(keyPath, 'sk_machine_valid\n', { mode: 0o600 });
  chmodSync(keyPath, 0o644);
  writeFileSync(path.join(cwd, '.env'), [
    'SERVER_URL=http://localhost:8779',
    'ADMIN_TOKEN=change-me',
    'DB_HOST=127.0.0.1',
    'DB_PORT=3306',
    'DB_USER=root',
    'DB_NAME=lightcone',
    'COAGENT_PROJECT_KEY=doctor-project',
    'ANTHROPIC_API_KEY=sk-ant-test',
  ].join('\n'));
  chmodSync(path.join(cwd, '.env'), 0o644);

  const result = await runDoctor({
    cwd,
    homeDir,
    binDir,
    args: ['--offline'],
    env: {
      ADMIN_TOKEN: 'change-me',
      COAGENT_MACHINE_KEY_PATH: keyPath,
    },
  });

  assert.equal(result.code, 1);
  assert.deepEqual(
    {
      level: checkByName(result.body, 'env_ADMIN_TOKEN')?.level,
      ok: checkByName(result.body, 'env_ADMIN_TOKEN')?.ok,
    },
    { level: 'blocker', ok: false },
  );
  assert.deepEqual(
    {
      level: checkByName(result.body, '.env_mode')?.level,
      ok: checkByName(result.body, '.env_mode')?.ok,
    },
    { level: 'blocker', ok: false },
  );
  assert.deepEqual(
    {
      level: checkByName(result.body, 'machine_key_mode')?.level,
      ok: checkByName(result.body, 'machine_key_mode')?.ok,
    },
    { level: 'blocker', ok: false },
  );
});

test('doctor compares actual MySQL tables against schema sentinel', async (t) => {
  const cwd = tempDir(t);
  const homeDir = tempDir(t);
  const binDir = path.join(tempDir(t), 'bin');
  installFakeTools(binDir);

  const keyPath = path.join(cwd, 'machine.key');
  writeFileSync(keyPath, 'sk_machine_valid\n', { mode: 0o600 });
  const expectedTables = JSON.parse(readFileSync(sentinelPath, 'utf8')).tables;
  const mysqlTables = expectedTables
    .filter((table) => table !== 'pending_actions')
    .concat('legacy_table')
    .join(',');

  writeFileSync(path.join(cwd, '.env'), [
    'SERVER_URL=http://localhost:8779',
    'ADMIN_TOKEN=real-admin-token',
    'DB_HOST=127.0.0.1',
    'DB_PORT=3306',
    'DB_USER=root',
    'DB_NAME=lightcone',
    'COAGENT_PROJECT_KEY=doctor-project',
    'ANTHROPIC_API_KEY=sk-ant-test',
  ].join('\n'));
  chmodSync(path.join(cwd, '.env'), 0o600);

  const result = await runDoctor({
    cwd,
    homeDir,
    binDir,
    env: {
      ADMIN_TOKEN: 'real-admin-token',
      ANTHROPIC_API_KEY: 'sk-ant-test',
      COAGENT_MACHINE_KEY_PATH: keyPath,
      COAGENT_DAEMON_SOCKET: path.join(cwd, 'missing.sock'),
      MYSQL_TABLES: mysqlTables,
    },
  });

  assert.equal(result.code, 1);
  const missing = checkByName(result.body, 'mysql_schema_missing');
  const extra = checkByName(result.body, 'mysql_schema_extra');
  assert.equal(missing.level, 'blocker');
  assert.equal(missing.ok, false);
  assert.match(missing.message, /pending_actions/);
  assert.equal(extra.level, 'warn');
  assert.equal(extra.ok, false);
  assert.match(extra.message, /legacy_table/);
});
