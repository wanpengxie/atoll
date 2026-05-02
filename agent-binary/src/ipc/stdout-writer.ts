import type { StdoutEnvelope } from '../types/ipc.js';

type StdoutContext = Partial<Pick<StdoutEnvelope, 'channel_id' | 'agent_pid' | 'session_id'>>;

let stdoutContext: StdoutContext = {};

function nowIso(): string {
  return new Date().toISOString();
}

export function setStdoutContext({
  channelId,
  agentPid,
  sessionId,
}: {
  channelId?: string;
  agentPid?: number;
  sessionId?: string;
}): void {
  stdoutContext = {
    ...(channelId ? { channel_id: channelId } : {}),
    ...(typeof agentPid === 'number' ? { agent_pid: agentPid } : {}),
    ...(sessionId ? { session_id: sessionId } : {}),
  };
}

function emit(payload: Omit<StdoutEnvelope, 'ts'>): void {
  process.stdout.write(`${JSON.stringify({ ts: nowIso(), ...stdoutContext, ...payload })}\n`);
}

export function writeStatus(status: string, detail = ''): void {
  emit({ event: 'agent.status', status, detail });
}

export function writeActivity(activity: string, detail = '', fields: Record<string, unknown> = {}): void {
  emit({ event: 'agent.activity', activity, detail, ...fields });
}

export function writeSession(sessionId: string): void {
  emit({ event: 'agent.session', session_id: sessionId });
}
