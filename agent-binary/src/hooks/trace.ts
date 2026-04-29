import { appendFileSync, mkdirSync } from 'node:fs';
import path from 'node:path';

function nowIso(): string {
  return new Date().toISOString();
}

export function tracePath(workdir: string, agentName: string, sessionId: string): string {
  return path.join(workdir, 'agents', agentName, 'trace', `${sessionId}.jsonl`);
}

export function appendTrace(
  workdir: string,
  agentName: string,
  sessionId: string,
  record: Record<string, unknown>,
): void {
  const filePath = tracePath(workdir, agentName, sessionId);
  mkdirSync(path.dirname(filePath), { recursive: true });
  appendFileSync(filePath, `${JSON.stringify({ ts: nowIso(), ...record })}\n`, 'utf8');
}
