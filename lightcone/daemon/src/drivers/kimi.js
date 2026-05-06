import { randomUUID } from 'crypto';
import { existsSync, writeFileSync } from 'fs';
import path from 'path';
import { buildSystemPrompt as buildClaudeSystemPrompt } from './claude.js';

const KIMI_WIRE_PROTOCOL_VERSION = '1.3';
const KIMI_SYSTEM_PROMPT_FILE = '.lightcone-kimi-system.md';
const KIMI_AGENT_FILE = '.lightcone-kimi-agent.yaml';

/**
 * Build Kimi CLI spawn args and config files.
 * Returns { args, env, setupFiles() } ready for spawn('kimi', args, { env }).
 */
export function buildKimiSpawn({ config, agentId, workspaceDir }) {
  const isResume = !!config.sessionId;
  const sessionId = config.sessionId || randomUUID();

  const systemPromptPath = path.join(workspaceDir, KIMI_SYSTEM_PROMPT_FILE);
  const agentFilePath = path.join(workspaceDir, KIMI_AGENT_FILE);

  // Build system prompt (reuse claude's prompt builder)
  const prompt = buildClaudeSystemPrompt(config, agentId);

  // Write system prompt file (skip if resuming and file exists)
  if (!isResume || !existsSync(systemPromptPath)) {
    writeFileSync(systemPromptPath, prompt, 'utf8');
  }

  // Write agent YAML config
  writeFileSync(agentFilePath, [
    'version: 1',
    'agent:',
    '  extend: default',
    `  system_prompt_path: ./${KIMI_SYSTEM_PROMPT_FILE}`,
    '',
  ].join('\n'), 'utf8');

  // Build CLI args
  const args = [
    '--wire',
    '--yolo',
    '--agent-file', agentFilePath,
    '--session', sessionId,
  ];

  if (config.model && config.model !== 'default') {
    args.push('--model', config.model);
  }

  const spawnEnv = { ...process.env, FORCE_COLOR: '0', NO_COLOR: '1', ...(config.envVars ?? {}) };

  return { args, env: spawnEnv, sessionId, isResume, prompt };
}

/**
 * Build the JSON-RPC initialize + prompt messages to send to stdin after spawn.
 */
export function buildKimiInitMessages({ sessionId, isResume, prompt }) {
  const initMsg = JSON.stringify({
    jsonrpc: '2.0',
    id: randomUUID(),
    method: 'initialize',
    params: {
      protocol_version: KIMI_WIRE_PROTOCOL_VERSION,
      client: { name: 'lightcone-daemon', version: '1.0.0' },
      capabilities: { supports_question: false, supports_plan_mode: false },
    },
  });

  const promptMsg = JSON.stringify({
    jsonrpc: '2.0',
    id: randomUUID(),
    method: 'prompt',
    params: {
      user_input: isResume
        ? prompt
        : 'Your system prompt contains your standing instructions. Follow it now and begin listening for messages.',
    },
  });

  return [initMsg, promptMsg];
}

/**
 * Parse a line of Kimi wire-protocol JSON-RPC output.
 * Returns an array of events: { kind, ... }
 */
export function parseKimiLine(line, state) {
  let message;
  try { message = JSON.parse(line); }
  catch { return []; }

  const events = [];

  // Announce session on first output
  if (!state.sessionAnnounced && state.sessionId) {
    events.push({ kind: 'session_init', sessionId: state.sessionId });
    state.sessionAnnounced = true;
  }

  if ('method' in message && message.method === 'event') {
    const eventType = message.params?.type;
    const payload = message.params?.payload || {};

    switch (eventType) {
      case 'StepBegin':
        events.push({ kind: 'thinking' });
        break;
      case 'ContentPart':
        if (payload.type === 'think') {
          events.push({ kind: 'thinking' });
        } else if (payload.type === 'text') {
          events.push({ kind: 'text', text: payload.text });
        }
        break;
      case 'ToolCall':
        events.push({ kind: 'tool_call', name: payload.function?.name || 'unknown_tool' });
        break;
      case 'TurnEnd':
        events.push({ kind: 'turn_end' });
        break;
      case 'StepInterrupted':
        events.push({ kind: 'turn_end' });
        break;
    }
    return events;
  }

  if ('error' in message) {
    events.push({ kind: 'error', message: message.error?.message || 'Unknown Kimi error' });
    events.push({ kind: 'turn_end' });
  }

  return events;
}

/**
 * Encode a message to send to Kimi via stdin.
 * mode: 'idle' → prompt (new turn), 'busy' → steer (interrupt current turn)
 */
export function encodeKimiStdin(text, mode = 'idle') {
  return JSON.stringify({
    jsonrpc: '2.0',
    id: randomUUID(),
    method: mode === 'idle' ? 'prompt' : 'steer',
    params: { user_input: text },
  });
}
