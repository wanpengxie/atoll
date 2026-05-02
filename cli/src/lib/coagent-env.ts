import { existsSync, readFileSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';

function sanitizeProjectKey(raw: string): string {
  return raw
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

export function resolveProjectKey(env = process.env): string {
  return sanitizeProjectKey(String(env.COAGENT_PROJECT_KEY ?? path.basename(process.cwd()) ?? 'default'));
}

export function resolveProjectDir(env = process.env): string {
  return path.join(homedir(), '.coagent', resolveProjectKey(env));
}

export function resolveMachineKeyPath(env = process.env): string {
  return env.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(String(env.COAGENT_MACHINE_KEY_PATH))
    : path.join(resolveProjectDir(env), 'machine.key');
}

export function resolveDaemonSocketPath(env = process.env): string {
  return env.COAGENT_DAEMON_SOCKET
    ? path.resolve(String(env.COAGENT_DAEMON_SOCKET))
    : path.join(resolveProjectDir(env), 'daemon.sock');
}

export function readMachineKey(env = process.env): string {
  const keyPath = resolveMachineKeyPath(env);
  if (!existsSync(keyPath)) return '';
  return readFileSync(keyPath, 'utf8').trim();
}

export interface DaemonRpcConfig {
  socketPath?: string;
  daemonHttp?: string;
  token?: string;
}

export function configureDaemonRpcEnv(env = process.env): DaemonRpcConfig {
  const daemonHttp = String(env.COAGENT_DAEMON_HTTP ?? '').trim();
  const configuredSocket = String(env.COAGENT_DAEMON_SOCKET ?? '').trim();
  const socketPath = configuredSocket || (!daemonHttp ? resolveDaemonSocketPath(env) : '');
  const token = String(env.COAGENT_DAEMON_TOKEN ?? '').trim() || readMachineKey(env);

  return {
    ...(socketPath ? { socketPath } : {}),
    ...(daemonHttp ? { daemonHttp } : {}),
    ...(token ? { token } : {}),
  };
}
