import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import type { AgentEnv } from '../types/env.js';

export interface SessionState {
  sessionId: string;
  existed: boolean;
}

export function ensureSessionState(env: AgentEnv): SessionState {
  mkdirSync(path.dirname(env.sessionIdPath), { recursive: true });

  if (existsSync(env.sessionIdPath)) {
    const persisted = readFileSync(env.sessionIdPath, 'utf8').trim();
    if (persisted) {
      return { sessionId: persisted, existed: true };
    }
  }

  writeFileSync(env.sessionIdPath, `${env.sessionId}\n`, 'utf8');
  return { sessionId: env.sessionId, existed: false };
}
