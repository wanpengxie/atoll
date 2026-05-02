export interface AgentEvent {
  type: string;
  id?: string;
  requestId?: string;
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
  event: string;
  ts: string;
  channel_id?: string;
  agent_pid?: number;
  session_id?: string;
  correlation_id?: string;
  [key: string]: unknown;
}
