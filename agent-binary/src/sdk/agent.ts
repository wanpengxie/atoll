import { existsSync } from 'node:fs';
import { homedir } from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { CliError } from '../util/simple-error.js';

export interface AgentTurnEvent {
  type?: string;
  subtype?: string;
  result?: string;
  session_id?: string;
  message?: unknown;
  [key: string]: unknown;
}

export interface AgentTurnRequest {
  cwd: string;
  sessionId: string;
  systemPrompt: string;
  userPrompt: string;
  onEvent?: (event: AgentTurnEvent) => void;
  onStderr?: (line: string) => void;
}

export interface AgentTurnResult {
  sessionId: string;
  mode: 'resume' | 'session-id';
  result: string;
  events: AgentTurnEvent[];
}

function claudeProjectSessionFile(cwd: string, sessionId: string): string {
  const projectSlug = cwd.replace(/[/.]/g, '-');
  return path.join(homedir(), '.claude', 'projects', projectSlug, `${sessionId}.jsonl`);
}

function spawnMode(cwd: string, sessionId: string): 'resume' | 'session-id' {
  return existsSync(claudeProjectSessionFile(cwd, sessionId)) ? 'resume' : 'session-id';
}

export async function runAgentTurn(request: AgentTurnRequest): Promise<AgentTurnResult> {
  const mode = spawnMode(request.cwd, request.sessionId);
  const args = [
    '--print',
    '--verbose',
    '--input-format',
    'stream-json',
    '--output-format',
    'stream-json',
    '--dangerously-skip-permissions',
    '--tools',
    'Read,Write,Edit,Grep,Glob,Bash',
    '--system-prompt',
    request.systemPrompt,
    mode === 'resume' ? '--resume' : '--session-id',
    request.sessionId,
  ];

  const proc = spawn('claude', args, {
    cwd: request.cwd,
    env: {
      ...process.env,
      FORCE_COLOR: '0',
      NO_COLOR: '1',
    },
    stdio: ['pipe', 'pipe', 'pipe'],
  });

  let stdoutBuffer = '';
  let stderrBuffer = '';
  let finalResult = '';
  const events: AgentTurnEvent[] = [];

  proc.stdout.setEncoding('utf8');
  proc.stdout.on('data', (chunk) => {
    stdoutBuffer += chunk;
    const lines = stdoutBuffer.split('\n');
    stdoutBuffer = lines.pop() ?? '';

    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;

      try {
        const event = JSON.parse(trimmed) as AgentTurnEvent;
        events.push(event);
        request.onEvent?.(event);
        if (event.type === 'result' && typeof event.result === 'string') {
          finalResult = event.result;
        }
      } catch {}
    }
  });

  proc.stderr.setEncoding('utf8');
  proc.stderr.on('data', (chunk) => {
    stderrBuffer += chunk;
    const lines = stderrBuffer.split('\n');
    stderrBuffer = lines.pop() ?? '';
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      request.onStderr?.(trimmed);
    }
  });

  proc.stdin.end(`${JSON.stringify({
    type: 'user',
    message: {
      role: 'user',
      content: request.userPrompt,
    },
  })}\n`);

  const exitCode = await new Promise<number | null>((resolve, reject) => {
    proc.once('error', reject);
    proc.once('exit', (code) => resolve(code));
  });

  if (exitCode !== 0) {
    throw new CliError(
      'claude_turn_failed',
      `claude exited with code ${exitCode ?? 'unknown'}${stderrBuffer.trim() ? `: ${stderrBuffer.trim()}` : ''}`,
    );
  }

  return {
    sessionId: request.sessionId,
    mode,
    result: finalResult,
    events,
  };
}
