import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';

type Env = NodeJS.ProcessEnv | Record<string, string | undefined>;

function findDotenv(startDir = process.cwd()): string {
  let current = path.resolve(startDir);
  while (true) {
    const candidate = path.join(current, '.env');
    if (existsSync(candidate)) return candidate;

    const parent = path.dirname(current);
    if (parent === current) return '';
    current = parent;
  }
}

function parseDotenv(filePath: string): Record<string, string> {
  if (!filePath) return {};
  const values: Record<string, string> = {};
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
    values[key] = value;
  }
  return values;
}

function withDotenv(env: Env = process.env): Env {
  return { ...parseDotenv(findDotenv()), ...env };
}

function sanitizeProjectKey(raw: string): string {
  return raw
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

export function resolveProjectKey(env: Env = process.env): string {
  const effectiveEnv = withDotenv(env);
  return sanitizeProjectKey(String(effectiveEnv.COAGENT_PROJECT_KEY ?? path.basename(process.cwd()) ?? 'default'));
}

export function resolveProjectDir(env: Env = process.env): string {
  return path.join(homedir(), '.coagent', resolveProjectKey(env));
}

export function resolveMachineKeyPath(env: Env = process.env): string {
  const effectiveEnv = withDotenv(env);
  return effectiveEnv.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(String(effectiveEnv.COAGENT_MACHINE_KEY_PATH))
    : path.join(resolveProjectDir(env), 'machine.key');
}

export function resolveDaemonSocketPath(env: Env = process.env): string {
  const effectiveEnv = withDotenv(env);
  return effectiveEnv.COAGENT_DAEMON_SOCKET
    ? path.resolve(String(effectiveEnv.COAGENT_DAEMON_SOCKET))
    : path.join(resolveProjectDir(env), 'daemon.sock');
}

export function readMachineKey(env: Env = process.env): string {
  const keyPath = resolveMachineKeyPath(env);
  if (!existsSync(keyPath)) return '';
  return readFileSync(keyPath, 'utf8').trim();
}

export interface DaemonRpcConfig {
  socketPath?: string;
  daemonHttp?: string;
  token?: string;
}

export function configureDaemonRpcEnv(env: Env = process.env): DaemonRpcConfig {
  const effectiveEnv = withDotenv(env);
  const daemonHttp = String(effectiveEnv.COAGENT_DAEMON_HTTP ?? '').trim();
  const configuredSocket = String(effectiveEnv.COAGENT_DAEMON_SOCKET ?? '').trim();
  const socketPath = configuredSocket || (!daemonHttp ? resolveDaemonSocketPath(effectiveEnv) : '');
  const token = String(effectiveEnv.COAGENT_DAEMON_TOKEN ?? '').trim() || readMachineKey(effectiveEnv);

  return {
    ...(socketPath ? { socketPath } : {}),
    ...(daemonHttp ? { daemonHttp } : {}),
    ...(token ? { token } : {}),
  };
}
