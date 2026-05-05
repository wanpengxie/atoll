import { randomUUID } from 'crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const DEFAULT_AGENT_NAME = 'channel-agent';
const ALLOWED_ENV_KEYS = new Set(['HOME', 'USER', 'LOGNAME', 'LANG', 'PATH', 'TERM']);
const ALLOWED_SECRET_ENV_KEYS = new Set(['ANTHROPIC_API_KEY', 'DEEPSEEK_API_KEY']);
const ALLOWED_ENV_PREFIXES = ['LC_', 'ANTHROPIC_', 'DEEPSEEK_', 'CLAUDE_CODE_', 'NODE_'];

function repoRoot() {
  return path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
}

function missingBuildArtifactError({ packageName, entry, buildCommand }) {
  const err = new Error(
    `Missing build artifact for ${packageName}: ${entry}\n`
    + `Run "${buildCommand}" or "pnpm -r build" before starting the daemon.`,
  );
  err.code = 'missing_build_artifact';
  return err;
}

export function resolveAgentRuntime(root = repoRoot()) {
  const distEntry = path.join(root, 'agent-binary', 'dist', 'index.js');
  if (existsSync(distEntry)) {
    return { command: 'node', args: [distEntry], entry: distEntry };
  }
  throw missingBuildArtifactError({
    packageName: '@coagent/agent-binary',
    entry: distEntry,
    buildCommand: 'pnpm --filter @coagent/agent-binary build',
  });
}

function normalizeCliBinaries(capabilitySet) {
  const items = Array.isArray(capabilitySet?.cli_binaries)
    ? capabilitySet.cli_binaries
    : [];

  return [...new Set(items.map((item) => String(item).trim()).filter(Boolean))];
}

function resolveMountedCliBinaries(cliBinaries, root = repoRoot()) {
  const cliBinDir = path.join(root, 'cli', 'bin');
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

function isSensitiveEnvKey(key) {
  return key === 'ADMIN_TOKEN'
    || key.endsWith('_TOKEN')
    || key.endsWith('_KEY')
    || key.endsWith('_PASSWORD');
}

function shouldInheritEnvKey(key) {
  if (isSensitiveEnvKey(key) && !ALLOWED_SECRET_ENV_KEYS.has(key)) return false;
  return ALLOWED_ENV_KEYS.has(key) || ALLOWED_ENV_PREFIXES.some((prefix) => key.startsWith(prefix));
}

export function buildAgentInheritedEnv(sourceEnv = process.env) {
  const env = {};
  for (const [key, value] of Object.entries(sourceEnv)) {
    if (value == null) continue;
    if (shouldInheritEnvKey(key)) env[key] = value;
  }
  return env;
}

export function buildCoagentSpawn({
  channelId,
  channelName = '',
  channelType = 'echo',
  channelTypeConfig = null,
  workspaceId,
  workdir,
  capabilitySet,
  daemonSocketPath,
  daemonHttpUrl,
  daemonToken,
  sessionIdPath,
  agentName = DEFAULT_AGENT_NAME,
  extraEnv = {},
  repoRootDir = repoRoot(),
}) {
  const runtime = resolveAgentRuntime(repoRootDir);
  const cliBinaries = normalizeCliBinaries(capabilitySet);
  const { cliBinDir, mounted } = resolveMountedCliBinaries(cliBinaries, repoRootDir);
  const sessionId = ensureSessionId(sessionIdPath);
  const pathParts = mounted.length > 0
    ? [cliBinDir, process.env.PATH ?? '']
    : [process.env.PATH ?? ''];

  const env = {
    ...buildAgentInheritedEnv(process.env),
    ...extraEnv,
    FORCE_COLOR: '0',
    NO_COLOR: '1',
    PATH: pathParts.filter(Boolean).join(path.delimiter),
    CHANNEL_ID: channelId,
    CHANNEL_NAME: channelName,
    CHANNEL_TYPE: channelType,
    WORKSPACE_ID: workspaceId ?? '',
    CAPABILITY_SET: JSON.stringify({ cli_binaries: cliBinaries }),
    SESSION_ID: sessionId,
    SESSION_ID_PATH: sessionIdPath,
    COAGENT_CHANNEL_ID: channelId,
    COAGENT_CHANNEL_NAME: channelName,
    COAGENT_CHANNEL_TYPE: channelType,
    COAGENT_CHANNEL_TYPE_CONFIG: channelTypeConfig ? JSON.stringify(channelTypeConfig) : '',
    COAGENT_WORKSPACE_ID: workspaceId ?? '',
    COAGENT_CAPABILITY_SET: JSON.stringify({ cli_binaries: cliBinaries }),
    COAGENT_SESSION_ID: sessionId,
    COAGENT_SESSION_ID_PATH: sessionIdPath,
    COAGENT_AGENT_NAME: agentName,
    COAGENT_DAEMON_SOCKET: daemonSocketPath ?? '',
    COAGENT_DAEMON_HTTP: daemonHttpUrl ?? '',
    COAGENT_DAEMON_TOKEN: daemonToken ?? '',
    COAGENT_WORKDIR: workdir,
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
