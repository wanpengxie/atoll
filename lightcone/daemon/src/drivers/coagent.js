import { randomUUID } from 'crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const DEFAULT_AGENT_NAME = 'channel-agent';

const INLINE_AGENT_STUB = `
  import { mkdirSync, writeFileSync } from 'fs';
  import path from 'path';
  const sessionId = process.env.SESSION_ID || process.env.COAGENT_SESSION_ID || '${randomUUID()}';
  const sessionIdPath = process.env.SESSION_ID_PATH || process.env.COAGENT_SESSION_ID_PATH || '';
  if (sessionIdPath) {
    mkdirSync(path.dirname(sessionIdPath), { recursive: true });
    writeFileSync(sessionIdPath, sessionId + '\\n', 'utf8');
  }
  process.stdin.setEncoding('utf8');
  process.stdin.resume();
  process.stdin.on('data', () => {});
  const timer = setInterval(() => {}, 60_000);
  timer.unref?.();
`;

function repoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
}

function resolveAgentRuntime() {
  const root = repoRoot();
  const distEntry = path.join(root, 'agent-binary', 'dist', 'index.js');
  if (existsSync(distEntry)) {
    return { command: 'node', args: [distEntry], entry: distEntry };
  }

  const srcTsEntry = path.join(root, 'agent-binary', 'src', 'index.ts');
  if (existsSync(srcTsEntry)) {
    return { command: 'node', args: [srcTsEntry], entry: srcTsEntry };
  }

  const srcJsEntry = path.join(root, 'agent-binary', 'src', 'index.js');
  if (existsSync(srcJsEntry)) {
    return { command: 'node', args: [srcJsEntry], entry: srcJsEntry };
  }

  return {
    command: 'node',
    args: ['--input-type=module', '-e', INLINE_AGENT_STUB],
    entry: '<inline-agent-stub>',
  };
}

function normalizeCliBinaries(capabilitySet) {
  const items = Array.isArray(capabilitySet?.cli_binaries)
    ? capabilitySet.cli_binaries
    : [];

  return [...new Set(items.map((item) => String(item).trim()).filter(Boolean))];
}

function resolveMountedCliBinaries(cliBinaries) {
  const cliBinDir = path.join(repoRoot(), 'cli', 'bin');
  const mounted = cliBinaries.filter((binary) => existsSync(path.join(cliBinDir, binary)));
  return {
    cliBinDir,
    mounted,
  };
}

function ensureSessionId(sessionIdPath) {
  mkdirSync(path.dirname(sessionIdPath), { recursive: true });

  if (existsSync(sessionIdPath)) {
    const sessionId = readFileSync(sessionIdPath, 'utf8').trim();
    if (sessionId) return sessionId;
  }

  const sessionId = randomUUID();
  writeFileSync(sessionIdPath, `${sessionId}\n`, 'utf8');
  return sessionId;
}

export function buildCoagentSpawn({
  channelId,
  channelName = '',
  workspaceId,
  workdir,
  capabilitySet,
  daemonSocketPath,
  daemonHttpUrl,
  daemonToken,
  sessionIdPath,
  agentName = DEFAULT_AGENT_NAME,
  extraEnv = {},
}) {
  const runtime = resolveAgentRuntime();
  const cliBinaries = normalizeCliBinaries(capabilitySet);
  const { cliBinDir, mounted } = resolveMountedCliBinaries(cliBinaries);
  const sessionId = ensureSessionId(sessionIdPath);
  const pathParts = mounted.length > 0
    ? [cliBinDir, process.env.PATH ?? '']
    : [process.env.PATH ?? ''];

  const env = {
    ...process.env,
    FORCE_COLOR: '0',
    NO_COLOR: '1',
    PATH: pathParts.filter(Boolean).join(path.delimiter),
    CHANNEL_ID: channelId,
    CHANNEL_NAME: channelName,
    WORKSPACE_ID: workspaceId ?? '',
    CAPABILITY_SET: JSON.stringify({ cli_binaries: cliBinaries }),
    SESSION_ID: sessionId,
    SESSION_ID_PATH: sessionIdPath,
    COAGENT_CHANNEL_ID: channelId,
    COAGENT_CHANNEL_NAME: channelName,
    COAGENT_WORKSPACE_ID: workspaceId ?? '',
    COAGENT_CAPABILITY_SET: JSON.stringify({ cli_binaries: cliBinaries }),
    COAGENT_SESSION_ID: sessionId,
    COAGENT_SESSION_ID_PATH: sessionIdPath,
    COAGENT_AGENT_NAME: agentName,
    COAGENT_DAEMON_SOCKET: daemonSocketPath ?? '',
    COAGENT_DAEMON_HTTP: daemonHttpUrl ?? '',
    COAGENT_DAEMON_TOKEN: daemonToken ?? '',
    COAGENT_WORKDIR: workdir,
    ...extraEnv,
  };

  return {
    command: runtime.command,
    args: runtime.args,
    env,
    sessionId,
    mountedCliBinaries: mounted,
    entry: runtime.entry,
  };
}
