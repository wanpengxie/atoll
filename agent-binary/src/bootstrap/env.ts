import { accessSync, constants as fsConstants } from 'node:fs';
import path from 'node:path';
import { CliError } from '../util/simple-error.js';
import type { AgentEnv, CapabilitySet } from '../types/env.js';

function parseCapabilitySet(raw: string): CapabilitySet {
  if (!raw.trim()) {
    return { cli_binaries: [] };
  }

  try {
    const value = JSON.parse(raw) as { cli_binaries?: unknown };
    return {
      cli_binaries: Array.isArray(value.cli_binaries)
        ? value.cli_binaries.map((item) => String(item).trim()).filter(Boolean)
        : [],
    };
  } catch (error) {
    throw new CliError(
      'invalid_capability_set',
      error instanceof Error ? `Invalid capability set JSON: ${error.message}` : 'Invalid capability set JSON',
    );
  }
}

function requireValue(value: string, key: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    throw new CliError('missing_env', `Missing required environment variable: ${key}`);
  }
  return trimmed;
}

function assertBinaryOnPath(binary: string): void {
  const pathValue = process.env.PATH ?? '';
  const candidates = pathValue.split(path.delimiter).filter(Boolean);
  const found = candidates.some((candidate) => {
    try {
      accessSync(path.join(candidate, binary), fsConstants.X_OK);
      return true;
    } catch {
      return false;
    }
  });

  if (!found) {
    throw new CliError('missing_binary', `Required binary is not available on PATH: ${binary}`);
  }
}

export function parseEnv(env = process.env): AgentEnv {
  const daemonSocket = String(env.COAGENT_DAEMON_SOCKET ?? '').trim();
  const daemonHttp = String(env.COAGENT_DAEMON_HTTP ?? '').trim();
  if (!daemonSocket && !daemonHttp) {
    throw new CliError('missing_env', 'Either COAGENT_DAEMON_SOCKET or COAGENT_DAEMON_HTTP is required');
  }

  const capabilitySet = parseCapabilitySet(
    String(env.COAGENT_CAPABILITY_SET ?? env.CAPABILITY_SET ?? '{"cli_binaries":[]}'),
  );
  const requiredBinaries = [...new Set(['coagent-kernel', 'coagent-msg', ...capabilitySet.cli_binaries])];
  requiredBinaries.forEach(assertBinaryOnPath);

  const workdir = path.resolve(String(env.COAGENT_WORKDIR ?? process.cwd()));

  return {
    channelId: requireValue(String(env.COAGENT_CHANNEL_ID ?? env.CHANNEL_ID ?? ''), 'COAGENT_CHANNEL_ID'),
    channelName: String(env.COAGENT_CHANNEL_NAME ?? env.CHANNEL_NAME ?? '').trim(),
    workspaceId: String(env.COAGENT_WORKSPACE_ID ?? env.WORKSPACE_ID ?? '').trim(),
    workdir,
    agentName: requireValue(String(env.COAGENT_AGENT_NAME ?? 'channel-agent'), 'COAGENT_AGENT_NAME'),
    sessionId: requireValue(String(env.COAGENT_SESSION_ID ?? env.SESSION_ID ?? ''), 'COAGENT_SESSION_ID'),
    sessionIdPath: path.resolve(requireValue(String(env.COAGENT_SESSION_ID_PATH ?? env.SESSION_ID_PATH ?? ''), 'COAGENT_SESSION_ID_PATH')),
    daemonSocket,
    daemonHttp,
    daemonToken: String(env.COAGENT_DAEMON_TOKEN ?? '').trim(),
    capabilitySet,
  };
}
