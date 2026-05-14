import http from 'node:http';
import { resolveDaemonSocketPath, resolveProjectKey } from './coagent-env.js';
import { CliError } from './errors.js';
import type { DaemonRpcConfig } from './coagent-env.js';

interface RpcSuccess<T> {
  ok: true;
  result: T;
}

interface RpcFailure {
  ok: false;
  error?: {
    code?: string;
    message?: string;
  };
}

type RpcEnvelope<T> = RpcSuccess<T> | RpcFailure;

function readJsonResponse<T>(raw: string): T {
  try {
    return JSON.parse(raw) as T;
  } catch (error) {
    throw new CliError(
      'invalid_daemon_response',
      error instanceof Error ? `Daemon returned invalid JSON: ${error.message}` : 'Daemon returned invalid JSON',
      1,
    );
  }
}

function daemonRequestOptions(config: DaemonRpcConfig = {}) {
  const authToken = String(config.token ?? process.env.COAGENT_DAEMON_TOKEN ?? '').trim();
  const agentName = String(process.env.COAGENT_AGENT_NAME ?? '').trim();
  const channelId = String(process.env.COAGENT_CHANNEL_ID ?? process.env.CHANNEL_ID ?? '').trim();
  const sessionId = String(process.env.COAGENT_SESSION_ID ?? process.env.SESSION_ID ?? '').trim();
  const identityHeaders = {
    ...(agentName ? { 'X-Coagent-Agent-Name': agentName } : {}),
    ...(channelId ? { 'X-Coagent-Channel-Id': channelId } : {}),
    ...(sessionId ? { 'X-Coagent-Session-Id': sessionId } : {}),
  };
  const socketPath = String(config.socketPath ?? process.env.COAGENT_DAEMON_SOCKET ?? '').trim();
  if (socketPath) {
    return {
      transport: 'socket' as const,
      options: {
        socketPath,
        path: '/rpc',
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
          ...identityHeaders,
        },
      },
    };
  }

  const daemonHttp = String(config.daemonHttp ?? process.env.COAGENT_DAEMON_HTTP ?? '').trim();
  if (!daemonHttp) {
    throw new CliError(
      'daemon_unavailable',
      'Neither COAGENT_DAEMON_SOCKET nor COAGENT_DAEMON_HTTP is configured',
      1,
    );
  }

  let url: URL;
  try {
    url = new URL('/rpc', daemonHttp.endsWith('/') ? daemonHttp : `${daemonHttp}/`);
  } catch (error) {
    throw new CliError(
      'invalid_daemon_url',
      error instanceof Error ? `Invalid COAGENT_DAEMON_HTTP: ${error.message}` : 'Invalid COAGENT_DAEMON_HTTP',
      1,
    );
  }

  return {
    transport: 'http' as const,
    options: {
      protocol: url.protocol,
      hostname: url.hostname,
      port: url.port,
      path: url.pathname,
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
        ...identityHeaders,
      },
    },
  };
}

function daemonConnectionHint(config: DaemonRpcConfig = {}): string {
  const projectKey = resolveProjectKey();
  const socketPath = String(config.socketPath ?? process.env.COAGENT_DAEMON_SOCKET ?? '').trim()
    || resolveDaemonSocketPath();
  return `Hint: PROJECT_KEY=${projectKey}, socket=${socketPath}. Start the daemon or run make register if machine.key is missing.`;
}

export async function callDaemonRpc<T>(method: string, params: Record<string, unknown>, config: DaemonRpcConfig = {}): Promise<T> {
  const request = daemonRequestOptions(config);
  const payload = JSON.stringify({ method, params });

  const body = await new Promise<string>((resolve, reject) => {
    const req = http.request(request.options, (res) => {
      let raw = '';
      res.setEncoding('utf8');
      res.on('data', (chunk) => {
        raw += chunk;
      });
      res.on('end', () => {
        if (!raw.trim()) {
          reject(new CliError('empty_daemon_response', 'Daemon returned an empty response body', 1));
          return;
        }
        resolve(raw);
      });
    });

    req.on('error', (error) => {
      reject(new CliError(
        'daemon_request_failed',
        `Failed to reach daemon over ${request.transport}: ${error.message}. ${daemonConnectionHint(config)}`,
        1,
      ));
    });

    req.write(payload);
    req.end();
  });

  const envelope = readJsonResponse<RpcEnvelope<T>>(body);
  if (!envelope.ok) {
    throw new CliError(
      envelope.error?.code ?? 'rpc_error',
      envelope.error?.message ?? `RPC ${method} failed`,
      1,
    );
  }

  return envelope.result;
}
