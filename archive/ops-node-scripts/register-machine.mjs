#!/usr/bin/env node
import { existsSync, mkdirSync, readFileSync, chmodSync, unlinkSync, writeFileSync } from 'node:fs';
import { homedir, hostname, platform } from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

const PLACEHOLDERS = new Set(['', 'change-me', 'changeme', 'your-token-here', 'todo']);

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

function requestTimeoutMs(env) {
  const configured = Number(env.REGISTER_MACHINE_TIMEOUT_MS);
  return Number.isFinite(configured) && configured > 0 ? configured : 10_000;
}

function timeoutSeconds(timeoutMs) {
  return String(timeoutMs / 1000);
}

async function fetchJson(url, options = {}, { timeoutMs = 10_000, serverUrl = '' } = {}) {
  let res;
  try {
    res = await fetch(url, {
      ...options,
      signal: options.signal ?? AbortSignal.timeout(timeoutMs),
    });
  } catch (err) {
    if (err?.name === 'TimeoutError' || err?.name === 'AbortError') {
      throw new Error(`注册请求 ${timeoutSeconds(timeoutMs)} 秒超时，请检查 SERVER_URL=${serverUrl || normalizeServerUrl(url)} 是否可达`);
    }
    throw err;
  }
  const text = await res.text();
  let body = {};
  if (text.trim()) {
    try {
      body = JSON.parse(text);
    } catch {
      if (res.ok) {
        throw new Error(`server returned invalid JSON: ${text.slice(0, 200)}`);
      }
      body = { raw: text };
    }
  }
  if (!res.ok) {
    const err = new Error(`${res.status} ${res.statusText}: ${body.error ?? body.raw ?? text}`);
    err.status = res.status;
    err.statusText = res.statusText;
    throw err;
  }
  return body;
}

function errorMessage(err) {
  return err instanceof Error && err.message ? err.message : String(err);
}

function isAuthoritativeAuthFailure(err) {
  return err?.status === 401 || err?.status === 403;
}

export async function existingKeyLooksRegistered(env, key, {
  targetServerId = String(env.DEFAULT_SERVER_ID || 'server-001'),
  targetServerUrl = normalizeServerUrl(env.SERVER_URL),
  timeoutMs = requestTimeoutMs(env),
} = {}) {
  const serverUrl = normalizeServerUrl(targetServerUrl);

  try {
    const body = await fetchJson(`${serverUrl}/api/machines/whoami`, {
      headers: { Authorization: `Bearer ${key}` },
    }, {
      timeoutMs,
      serverUrl,
    });
    if (body.key_valid === true && String(body.server_id ?? '') === String(targetServerId)) {
      return { valid: true, via: '/api/machines/whoami' };
    }
    return { valid: false, via: null, reason: 'invalid_authoritative' };
  } catch (err) {
    if (isAuthoritativeAuthFailure(err)) {
      return { valid: false, via: null, reason: 'invalid_authoritative' };
    }
    return { valid: false, via: null, reason: 'unknown', error: errorMessage(err) };
  }
}

export async function main() {
  const env = mergedEnv();
  const serverUrl = normalizeServerUrl(env.SERVER_URL);
  const serverId = String(env.DEFAULT_SERVER_ID || 'server-001');
  const currentProjectKey = projectKey(env);
  const keyPath = machineKeyPath(env);
  const timeoutMs = requestTimeoutMs(env);
  if (existsSync(keyPath)) {
    const existing = readFileSync(keyPath, 'utf8').trim();
    const existingStatus = existing
      ? await existingKeyLooksRegistered(env, existing, {
        targetServerId: serverId,
        targetServerUrl: serverUrl,
        timeoutMs,
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
    if (existingStatus.reason === 'unknown') {
      throw new Error(
        `cannot validate existing machine key (${existingStatus.error}); ` +
        `keeping ${keyPath} intact. retry once server is reachable.`
      );
    }
    unlinkSync(keyPath);
  }

  const adminToken = String(env.ADMIN_TOKEN || '').trim();
  if (PLACEHOLDERS.has(adminToken.toLowerCase())) {
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
  }, {
    timeoutMs,
    serverUrl,
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
