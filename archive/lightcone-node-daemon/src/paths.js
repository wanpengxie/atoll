import { existsSync, readFileSync } from 'fs';
import { homedir } from 'os';
import path from 'path';

export function normalizeProjectKey(raw = process.env.COAGENT_PROJECT_KEY || path.basename(process.cwd()) || 'default') {
  return String(raw)
    .trim()
    .replace(/[^a-zA-Z0-9._-]/g, '-')
    .replace(/^-+|-+$/g, '') || 'default';
}

export function coagentProjectDir(projectKey = normalizeProjectKey()) {
  return path.join(homedir(), '.coagent', normalizeProjectKey(projectKey));
}

export function machineKeyPath(projectKey = normalizeProjectKey()) {
  return process.env.COAGENT_MACHINE_KEY_PATH
    ? path.resolve(process.env.COAGENT_MACHINE_KEY_PATH)
    : path.join(coagentProjectDir(projectKey), 'machine.key');
}

export function daemonSocketPath(projectKey = normalizeProjectKey()) {
  return process.env.COAGENT_DAEMON_SOCKET
    ? path.resolve(process.env.COAGENT_DAEMON_SOCKET)
    : path.join(coagentProjectDir(projectKey), 'daemon.sock');
}

export function readMachineKeyFile(projectKey = normalizeProjectKey()) {
  const keyPath = machineKeyPath(projectKey);
  if (!existsSync(keyPath)) return '';
  return readFileSync(keyPath, 'utf8').trim();
}
