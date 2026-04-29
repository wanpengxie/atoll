export interface AgentEvent {
  type: string;
  source?: string;
  createdAt?: string;
  created_at?: string;
  payload?: Record<string, unknown>;
}

export interface StdinEnvelope {
  type: string;
  event?: AgentEvent;
}

export interface StdoutEnvelope {
  type: string;
  ts: string;
  [key: string]: unknown;
}
