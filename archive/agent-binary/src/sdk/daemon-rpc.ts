import http from 'node:http';
import { URL } from 'node:url';
import type { AgentEnv } from '../types/env.js';

export interface DaemonRpcCall {
  method: string;
  params: Record<string, unknown>;
}

interface RpcSuccess<T> {
  ok: true;
  result: T;
}

interface RpcFailure {
  ok: false;
  error?: { code?: string; message?: string };
}

type RpcEnvelope<T> = RpcSuccess<T> | RpcFailure;

interface RequestOptions {
  socketPath?: string;
  protocol?: string;
  hostname?: string;
  port?: string | number;
  path: string;
  method: string;
  headers: Record<string, string>;
}

function buildRequestOptions(env: AgentEnv): { transport: 'socket' | 'http'; options: RequestOptions } {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(env.daemonToken ? { Authorization: `Bearer ${env.daemonToken}` } : {}),
    ...(env.agentName ? { 'X-Coagent-Agent-Name': env.agentName } : {}),
    ...(env.channelId ? { 'X-Coagent-Channel-Id': env.channelId } : {}),
    ...(env.sessionId ? { 'X-Coagent-Session-Id': env.sessionId } : {}),
  };

  if (env.daemonSocket) {
    return {
      transport: 'socket',
      options: {
        socketPath: env.daemonSocket,
        path: '/rpc',
        method: 'POST',
        headers,
      },
    };
  }

  if (!env.daemonHttp) {
    throw new Error('Neither COAGENT_DAEMON_SOCKET nor COAGENT_DAEMON_HTTP is configured');
  }

  const url = new URL('/rpc', env.daemonHttp.endsWith('/') ? env.daemonHttp : `${env.daemonHttp}/`);
  return {
    transport: 'http',
    options: {
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: 'POST',
      headers,
    },
  };
}

export async function callDaemonRpc<T>(env: AgentEnv, method: string, params: Record<string, unknown>): Promise<T> {
  const request = buildRequestOptions(env);
  const payload = JSON.stringify({ method, params });

  const body = await new Promise<string>((resolve, reject) => {
    const req = http.request(request.options, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        raw += chunk;
      });
      res.on('end', () => {
        resolve(raw);
      });
    });
    req.on('error', (error) => {
      reject(new Error(`daemon RPC ${method} via ${request.transport} failed: ${error.message}`));
    });
    req.write(payload);
    req.end();
  });

  if (!body.trim()) {
    throw new Error(`daemon RPC ${method} returned empty response`);
  }

  let envelope: RpcEnvelope<T>;
  try {
    envelope = JSON.parse(body) as RpcEnvelope<T>;
  } catch (error) {
    const message = error instanceof Error ? error.message : 'invalid JSON';
    throw new Error(`daemon RPC ${method} returned invalid JSON: ${message}`);
  }

  if (!envelope.ok) {
    const code = envelope.error?.code ?? 'rpc_error';
    const msg = envelope.error?.message ?? `RPC ${method} failed`;
    throw new Error(`${code}: ${msg}`);
  }

  return envelope.result;
}

export async function emitAgentProgress(env: AgentEnv, text: string): Promise<void> {
  const trimmed = text.trim();
  if (!trimmed) return;
  await callDaemonRpc(env, 'message.emit', {
    channel_id: env.channelId,
    text: trimmed,
    envelope: {
      sender: { kind: 'agent', id: env.agentName, name: env.agentName },
    },
    payload: {
      type: 'agent.progress',
      body: { text: trimmed },
    },
    audience: ['channel'],
  });
}
