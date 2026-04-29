import { execSync } from 'child_process';
import { existsSync } from 'fs';
import path from 'path';
import { buildSystemPrompt as buildClaudeSystemPrompt } from './claude.js';
import { buildSkillMcpServers } from '../mcp-config.js';

function buildChatBridgeArgs(chatBridgePath, { agentId, teamId, serverUrl, authToken, workspaceDir }) {
  const args = [
    chatBridgePath,
    '--agent-id', agentId,
    '--server-url', serverUrl,
    '--auth-token', authToken,
    '--workspace-dir', workspaceDir,
  ];
  if (teamId) args.push('--team-id', teamId);
  return args;
}

function quote(value) {
  return JSON.stringify(value);
}

function normalizeCodexModel(model) {
  if (!model) return 'gpt-5.2';
  const normalized = String(model).trim().toLowerCase();
  if (!normalized) return 'gpt-5.2';
  if (['sonnet', 'opus', 'haiku'].includes(normalized)) return 'gpt-5.2';
  if (normalized.startsWith('claude-')) return 'gpt-5.2';
  return model;
}

function ensureGitRepo(workspaceDir) {
  const gitDir = path.join(workspaceDir, '.git');
  if (existsSync(gitDir)) return;

  execSync('git init', { cwd: workspaceDir, stdio: 'pipe' });
  execSync('git add -A && git commit --allow-empty -m "init"', {
    cwd: workspaceDir,
    stdio: 'pipe',
    env: {
      ...process.env,
      GIT_AUTHOR_NAME: 'lightcone',
      GIT_AUTHOR_EMAIL: 'lightcone@local',
      GIT_COMMITTER_NAME: 'lightcone',
      GIT_COMMITTER_EMAIL: 'lightcone@local',
    },
  });
}

export function buildCodexSystemPrompt(config, agentId) {
  let prompt = buildClaudeSystemPrompt(config, agentId)
    .replaceAll('mcp__chat__', '')
    .replace(
      '3. If there is no concrete incoming message to handle, stop and wait. New messages will be delivered to you automatically via stdin.',
      '3. If there is no concrete incoming message to handle, stop. The daemon will restart you when new messages arrive.'
    )
    .replace(
      '5. **Complete ALL your work before stopping.** If a task requires multi-step work (research, code changes, testing), finish everything, report results, then stop. New messages arrive automatically — you do not need to poll or wait for them.',
      '5. **Complete ALL your work before stopping.** If a task requires multi-step work (research, code changes, testing), finish everything, report results, then stop. Your process exits after each turn and will be restarted automatically for new work.'
    );

  prompt += '\n\nIMPORTANT: Do not wait for stdin notifications. Finish the current turn completely, then stop.';
  return prompt;
}

export function buildCodexSpawn({
  config, agentId, teamId, workspaceDir, chatBridgePath, serverUrl, machineApiKey, prompt, skills, credentialGrants,
}) {
  ensureGitRepo(workspaceDir);

  const args = ['exec'];
  if (config.sessionId) {
    args.push('resume', config.sessionId);
  }

  const bridgeArgs = buildChatBridgeArgs(chatBridgePath, {
    agentId,
    teamId,
    serverUrl,
    authToken: config.authToken || machineApiKey,
    workspaceDir,
  });

  args.push(
    '--dangerously-bypass-approvals-and-sandbox',
    '--json',
    '-c', `mcp_servers.chat.command=${quote('node')}`,
    '-c', `mcp_servers.chat.args=${quote(bridgeArgs)}`,
    '-c', 'mcp_servers.chat.enabled=true',
    '-c', 'mcp_servers.chat.required=true',
    '-c', 'mcp_servers.chat.startup_timeout_sec=30',
    '-c', 'mcp_servers.chat.tool_timeout_sec=300',
  );

  const skillMcpServers = buildSkillMcpServers({
    skills,
    credentialGrants,
    config,
    agentId,
    teamId,
    workspaceDir,
    serverUrl,
    authToken: config.authToken || machineApiKey,
  });

  for (const [serverKey, mc] of Object.entries(skillMcpServers)) {
    const envPairs = Object.entries(mc.env ?? {}).map(([k, v]) => `${k}=${v ?? ''}`);
    if (envPairs.length > 0) {
      args.push(
        '-c', `mcp_servers.${quote(serverKey)}.command=${quote('env')}`,
        '-c', `mcp_servers.${quote(serverKey)}.args=${quote([...envPairs, mc.command, ...(mc.args ?? [])])}`,
        '-c', `mcp_servers.${quote(serverKey)}.enabled=true`
      );
      continue;
    }
    args.push(
      '-c', `mcp_servers.${quote(serverKey)}.command=${quote(mc.command)}`,
      '-c', `mcp_servers.${quote(serverKey)}.args=${quote(mc.args ?? [])}`,
      '-c', `mcp_servers.${quote(serverKey)}.enabled=true`
    );
  }

  const model = normalizeCodexModel(config.model);
  if (model) {
    args.push('-m', model);
  }

  if (config.reasoningEffort) {
    args.push('-c', `model_reasoning_effort=${config.reasoningEffort}`);
  }

  args.push(prompt || buildCodexSystemPrompt(config, agentId));

  const env = { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1', ...(config.envVars ?? {}) };
  return { args, env };
}

export function parseCodexLine(line) {
  let event;
  try { event = JSON.parse(line); }
  catch { return []; }

  const events = [];

  switch (event.type) {
    case 'thread.started':
      if (event.thread_id) events.push({ kind: 'session_init', sessionId: event.thread_id });
      break;
    case 'turn.started':
      events.push({ kind: 'thinking' });
      break;
    case 'item.started':
    case 'item.updated':
    case 'item.completed': {
      const item = event.item;
      if (!item) break;
      switch (item.type) {
        case 'reasoning':
          if (item.text) events.push({ kind: 'thinking' });
          break;
        case 'agent_message':
          if (item.text && event.type === 'item.completed') {
            events.push({ kind: 'text', text: item.text });
          }
          break;
        case 'command_execution':
          if (event.type === 'item.started') {
            events.push({ kind: 'tool_call', name: 'shell' });
          }
          break;
        case 'file_change':
          if (event.type === 'item.started') {
            events.push({ kind: 'tool_call', name: 'file_change' });
          }
          break;
        case 'mcp_tool_call':
          if (event.type === 'item.started') {
            const toolName = item.server === 'chat'
              ? item.tool
              : `${item.server || 'mcp'}_${item.tool || 'tool'}`;
            events.push({ kind: 'tool_call', name: toolName });
          }
          break;
        case 'web_search':
          if (event.type === 'item.started') {
            events.push({ kind: 'tool_call', name: 'web_search' });
          }
          break;
        case 'error':
          if (item.message) events.push({ kind: 'error', message: item.message });
          break;
      }
      break;
    }
    case 'turn.completed':
      events.push({ kind: 'turn_end' });
      break;
    case 'turn.failed':
      if (event.error?.message) events.push({ kind: 'error', message: event.error.message });
      events.push({ kind: 'turn_end' });
      break;
    case 'error':
      events.push({ kind: 'error', message: event.message || 'Unknown Codex error' });
      break;
  }

  return events;
}
