#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, chmodSync, writeFileSync } from 'node:fs';
import { homedir, hostname, platform } from 'node:os';
import http from 'node:http';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

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

function normalizeServerUrl(value) {
  return String(value || 'http://localhost:8779').replace(/\/+$/, '');
}

function machineKeyPath(env) {
  return env.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(env.COAGENT_MACHINE_KEY_PATH)
    : path.join(homedir(), '.coagent', projectKey(env), 'machine.key');
}

function daemonSocketPath(env) {
  return env.COAGENT_DAEMON_SOCKET
    ? path.resolve(env.COAGENT_DAEMON_SOCKET)
    : path.join(homedir(), '.coagent', projectKey(env), 'daemon.sock');
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

export async function existingKeyLooksRegistered(env, key, {
  targetServerId = String(env.DEFAULT_SERVER_ID || 'server-001'),
  targetServerUrl = normalizeServerUrl(env.SERVER_URL),
  projectKey: targetProjectKey = projectKey(env),
} = {}) {
  const serverUrl = normalizeServerUrl(targetServerUrl);

  const socketPath = daemonSocketPath(env);
  if (existsSync(socketPath)) {
    const status = await new Promise((resolve) => {
      const req = http.request({
        socketPath,
        path: '/admin/status',
        method: 'GET',
        headers: { Authorization: `Bearer ${key}` },
        timeout: 5000,
      }, (res) => {
        let raw = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => { raw += chunk; });
        res.on('end', () => {
          resolve({ ok: res.statusCode === 200, raw });
        });
      });
      req.on('error', () => resolve({ ok: false }));
      req.end();
    });
    if (status.ok) {
      try {
        const body = JSON.parse(status.raw || '{}');
        const statusServerUrl = body.server_url ? normalizeServerUrl(body.server_url) : '';
        if (
          statusServerUrl === serverUrl
          && String(body.project_key ?? '') === String(targetProjectKey)
        ) {
          return { valid: true, via: '/admin/status' };
        }
      } catch {
        // Fall through and let the server whoami path verify the key.
      }
    }
  }

  try {
    const body = await fetchJson(`${serverUrl}/api/machines/whoami`, {
      headers: { Authorization: `Bearer ${key}` },
    });
    if (body.key_valid === true && String(body.server_id ?? '') === String(targetServerId)) {
      return { valid: true, via: '/api/machines/whoami' };
    }
  } catch {
    // Fall through and let registration create a fresh key.
  }
  return { valid: false, via: null };
}

export async function main() {
  const env = mergedEnv();
  const serverUrl = normalizeServerUrl(env.SERVER_URL);
  const serverId = String(env.DEFAULT_SERVER_ID || 'server-001');
  const currentProjectKey = projectKey(env);
  const keyPath = machineKeyPath(env);
  if (existsSync(keyPath)) {
    const existing = readFileSync(keyPath, 'utf8').trim();
    const existingStatus = existing
      ? await existingKeyLooksRegistered(env, existing, {
        targetServerId: serverId,
        targetServerUrl: serverUrl,
        projectKey: currentProjectKey,
      })
      : { valid: false };
    if (existingStatus.valid) {
      chmodSync(keyPath, 0o600);
      console.log(JSON.stringify({
        ok: true,
        skipped: true,
        reason: `already registered, key valid via ${existingStatus.via}`,
        machineId: existingStatus.machineId,
        keyPath,
        projectKey: currentProjectKey,
      }));
      return;
    }
  }

  const adminToken = String(env.ADMIN_TOKEN || '').trim();
  if (!adminToken || adminToken === 'change-me') {
    throw new Error('ADMIN_TOKEN is required in .env before registering a machine');
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
    projectKey: currentProjectKey,
  }));
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  main().catch((err) => {
    console.error(JSON.stringify({ ok: false, error: err.message }));
    process.exit(1);
  });
}
