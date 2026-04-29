import type { StdoutEnvelope } from '../types/ipc.js';

function nowIso(): string {
  return new Date().toISOString();
}

function emit(payload: Omit<StdoutEnvelope, 'ts'>): void {
  process.stdout.write(`${JSON.stringify({ ts: nowIso(), ...payload })}\n`);
}

export function writeStatus(status: string, detail = ''): void {
  emit({ type: 'agent.status', status, detail });
}

export function writeActivity(activity: string, detail = ''): void {
  emit({ type: 'agent.activity', activity, detail });
}

export function writeSession(sessionId: string): void {
  emit({ type: 'agent.session', session_id: sessionId });
}
