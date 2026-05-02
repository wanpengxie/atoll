#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, chmodSync, writeFileSync } from 'node:fs';
import { homedir, hostname, platform } from 'node:os';
import path from 'node:path';

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

function mergedEnv() {
  return { ...parseEnvFile(path.resolve('.env')), ...process.env };
}

function projectKey(env) {
  return String(env.COAGENT_PROJECT_KEY || path.basename(process.cwd()) || 'default')
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

function machineKeyPath(env) {
  return env.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(env.COAGENT_MACHINE_KEY_PATH)
    : path.join(homedir(), '.coagent', projectKey(env), 'machine.key');
}

async function fetchJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();
  let body = {};
  if (text.trim()) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { raw: text };
    }
  }
  if (!res.ok) {
    throw new Error(`${res.status} ${res.statusText}: ${body.error ?? body.raw ?? text}`);
  }
  return body;
}

async function existingKeyLooksRegistered(env, key) {
  const serverUrl = String(env.SERVER_URL || 'http://localhost:8779').replace(/\/+$/, '');
  const serverId = String(env.DEFAULT_SERVER_ID || 'server-001');
  const adminToken = String(env.ADMIN_TOKEN || '').trim();
  if (!adminToken || adminToken === 'change-me') return false;

  try {
    const body = await fetchJson(`${serverUrl}/api/servers/${encodeURIComponent(serverId)}/machines`, {
      headers: { Authorization: `Bearer ${adminToken}` },
    });
    const prefix = key.slice(0, 18);
    return Array.isArray(body.machines) && body.machines.some((machine) => machine.apiKeyPrefix === prefix);
  } catch {
    return false;
  }
}

async function main() {
  const env = mergedEnv();
  const serverUrl = String(env.SERVER_URL || 'http://localhost:8779').replace(/\/+$/, '');
  const serverId = String(env.DEFAULT_SERVER_ID || 'server-001');
  const adminToken = String(env.ADMIN_TOKEN || '').trim();
  if (!adminToken || adminToken === 'change-me') {
    throw new Error('ADMIN_TOKEN is required in .env before registering a machine');
  }

  const keyPath = machineKeyPath(env);
  if (existsSync(keyPath)) {
    const existing = readFileSync(keyPath, 'utf8').trim();
    if (existing && await existingKeyLooksRegistered(env, existing)) {
      chmodSync(keyPath, 0o600);
      console.log(JSON.stringify({ ok: true, skipped: true, keyPath, projectKey: projectKey(env) }));
      return;
    }
  }

  const name = env.MACHINE_NAME || `${hostname()}-${platform()}`;
  const body = await fetchJson(`${serverUrl}/api/servers/${encodeURIComponent(serverId)}/machines`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${adminToken}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ name }),
  });

  if (!body.apiKey || !String(body.apiKey).startsWith('sk_machine_')) {
    throw new Error('server did not return a machine apiKey');
  }

  mkdirSync(path.dirname(keyPath), { recursive: true });
  writeFileSync(keyPath, `${body.apiKey}\n`, { mode: 0o600 });
  chmodSync(keyPath, 0o600);
  console.log(JSON.stringify({
    ok: true,
    skipped: false,
    machineId: body.id,
    apiKeyPrefix: body.apiKeyPrefix ?? body.apiKey.slice(0, 18),
    keyPath,
    projectKey: projectKey(env),
  }));
}

main().catch((err) => {
  console.error(JSON.stringify({ ok: false, error: err.message }));
  process.exit(1);
});
