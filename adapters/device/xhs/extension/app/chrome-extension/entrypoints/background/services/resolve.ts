// services/resolve.ts — M1.2-T3
//
// Popup 主入口 1-key 流程：用户填 `coagentServerUrl + apiKey`，扩展走
// `POST {coagentServerUrl}/api/device/resolve` 反查 device 全套连接信息。
//
// 接口（launch 后由 server/devicebus 等 Go 实现承担同合同；早期 Node 实现
// 已归档）：
//   POST {SERVER_URL}/api/device/resolve
//   body: { api_key: "<sk_dev_xxx>" }
//   → 200 { ws_url, http_url, device_id, user_id, channel_id, daemon_id }
//   → 400 { error: 'api_key required' }
//   → 404 { error: 'Device not found' }
//   → 429 { error: 'Too many resolve requests' }
//   → 503 { error: 'Device has no daemon assigned' | 'Daemon endpoint not registered' }
//
// 错误处理：本服务负责把所有 HTTP / 网络 / 解析失败统一映射成 ResolveError，
// 并附友好的中文 message 给 popup 直接展示。401（虽然 server 当前不返）也走
// "api-key 无效" 路径，方便未来 server 端加鉴权时不用再改 popup。

import { TIMEOUTS } from 'coagent-xhs-shared';

const RESOLVE_PATH = '/api/device/resolve';
/** 单次 resolve 整体超时（含 DNS / TCP / TLS / HTTP）。 */
const RESOLVE_TIMEOUT_MS = TIMEOUTS.NETWORK_REQUEST;

/** Resolve API 成功返回体（与 coagent server 合同对齐）。 */
export interface ResolveSuccess {
  ws_url: string;
  http_url: string;
  device_id: string;
  user_id: string;
  channel_id: string;
  daemon_id: string;
}

export type ResolveErrorKind =
  | 'invalid_input'      // serverUrl / apiKey 缺失或格式错
  | 'bad_request'        // server 400
  | 'unauthorized'       // server 401
  | 'not_found'          // server 404
  | 'rate_limited'       // server 429
  | 'unavailable'        // server 5xx / 503
  | 'network'            // fetch 抛错（DNS / 拒连 / 超时 / CORS）
  | 'parse'              // 200 但响应不是合法 JSON / 缺字段
  | 'unknown';

export interface ResolveError {
  kind: ResolveErrorKind;
  /** HTTP status，仅当 server 真返了 response 时存在。 */
  status?: number;
  /** 友好的中文展示字符串（popup 直接显示）。 */
  message: string;
  /** 可选：server 返的原始 error 字段，便于调试。 */
  detail?: string;
}

export type ResolveResult =
  | { ok: true; data: ResolveSuccess }
  | { ok: false; error: ResolveError };

export interface ResolveDeviceConfigOptions {
  serverUrl: string;
  apiKey: string;
  /** 测试 seam — vitest 注入 fetch mock。 */
  fetchImpl?: typeof fetch;
  /** 测试 seam — vitest 缩短超时。 */
  timeoutMs?: number;
}

/** 删尾部斜杠，避免 `https://x.com/` + `/api/...` 拼成两个斜杠。 */
function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

/**
 * 主 API：调 coagent server `/api/device/resolve`，返回 normalize 过的
 * `{ok, data?, error?}` 联合体。永不抛异常 — 调用方只看 `ok` 分支即可。
 */
export async function resolveDeviceConfig(
  options: ResolveDeviceConfigOptions,
): Promise<ResolveResult> {
  const serverUrl = String(options.serverUrl ?? '').trim();
  const apiKey = String(options.apiKey ?? '').trim();

  if (!serverUrl) {
    return {
      ok: false,
      error: { kind: 'invalid_input', message: '请填写 Server URL（coagent server 地址）' },
    };
  }
  if (!apiKey) {
    return {
      ok: false,
      error: { kind: 'invalid_input', message: '请填写 Coagent api-key' },
    };
  }

  // 简单格式校验：必须是 http(s) URL。
  let parsed: URL;
  try {
    parsed = new URL(serverUrl);
  } catch {
    return {
      ok: false,
      error: {
        kind: 'invalid_input',
        message: 'Server URL 格式无效，请填类似 https://coagent.example.com',
      },
    };
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
    return {
      ok: false,
      error: {
        kind: 'invalid_input',
        message: 'Server URL 必须以 http:// 或 https:// 开头',
      },
    };
  }

  const url = `${stripTrailingSlash(serverUrl)}${RESOLVE_PATH}`;
  const fetchImpl = options.fetchImpl ?? fetch;
  const timeoutMs = options.timeoutMs ?? RESOLVE_TIMEOUT_MS;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let resp: Response;
  try {
    resp = await fetchImpl(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_key: apiKey }),
      signal: controller.signal,
    });
  } catch (err: any) {
    clearTimeout(timer);
    const message = err instanceof Error ? err.message : String(err);
    // AbortError = 超时；其他都归入 network。
    const isAbort = err?.name === 'AbortError';
    return {
      ok: false,
      error: {
        kind: 'network',
        message: isAbort
          ? `请求超时（${timeoutMs}ms）：检查 Server URL 与网络连通性`
          : `无法连接 Server：${message}`,
        detail: message,
      },
    };
  }
  clearTimeout(timer);

  // 任何非 2xx 都进 error 分支，按 status 区分 kind / message。
  if (!resp.ok) {
    const detail = await safeReadErrorMessage(resp);
    const status = resp.status;
    if (status === 400) {
      return {
        ok: false,
        error: {
          kind: 'bad_request',
          status,
          message: detail || '请求体非法（api_key 缺失或格式错）',
          detail,
        },
      };
    }
    if (status === 401) {
      return {
        ok: false,
        error: {
          kind: 'unauthorized',
          status,
          message: detail || 'api-key 无效',
          detail,
        },
      };
    }
    if (status === 404) {
      return {
        ok: false,
        error: {
          kind: 'not_found',
          status,
          message: detail || '未找到对应 device — 请检查 api-key 是否正确',
          detail,
        },
      };
    }
    if (status === 429) {
      return {
        ok: false,
        error: {
          kind: 'rate_limited',
          status,
          message: detail || '请求过于频繁，请稍后再试',
          detail,
        },
      };
    }
    if (status >= 500) {
      return {
        ok: false,
        error: {
          kind: 'unavailable',
          status,
          message: detail || 'Server 暂不可用，请稍后重试',
          detail,
        },
      };
    }
    return {
      ok: false,
      error: {
        kind: 'unknown',
        status,
        message: detail || `Server 返回非预期状态码：${status}`,
        detail,
      },
    };
  }

  // 2xx — 解析 body 并校验字段齐全。
  let body: unknown;
  try {
    body = await resp.json();
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      ok: false,
      error: {
        kind: 'parse',
        message: 'Server 响应不是合法 JSON',
        detail: message,
      },
    };
  }

  const data = normalizeResolveSuccess(body);
  if (!data) {
    return {
      ok: false,
      error: {
        kind: 'parse',
        message: 'Server 响应缺少必需字段（ws_url/http_url/device_id 等）',
        detail: typeof body === 'object' ? JSON.stringify(body) : String(body),
      },
    };
  }
  return { ok: true, data };
}

/**
 * 把 server 返回体校验到 `ResolveSuccess`。所有 6 个字段必须存在且为非空字符串。
 * 任何 missing / wrong-type 都返回 null（让上层走 parse error）。
 */
function normalizeResolveSuccess(body: unknown): ResolveSuccess | null {
  if (!body || typeof body !== 'object') return null;
  const b = body as Record<string, unknown>;
  const fields: Array<keyof ResolveSuccess> = [
    'ws_url',
    'http_url',
    'device_id',
    'user_id',
    'channel_id',
    'daemon_id',
  ];
  const out: Record<string, string> = {};
  for (const f of fields) {
    const v = b[f];
    if (typeof v !== 'string' || v.trim() === '') return null;
    out[f] = v;
  }
  return out as unknown as ResolveSuccess;
}

/**
 * 兜底读 server 的 error 字段（server 返 `{error: '...'}`）。任何异常都返回
 * 空字符串，让上层用默认 message。
 */
async function safeReadErrorMessage(resp: Response): Promise<string> {
  try {
    const json: unknown = await resp.clone().json();
    if (json && typeof json === 'object' && typeof (json as any).error === 'string') {
      return String((json as any).error);
    }
  } catch {
    /* fall through to text */
  }
  try {
    const txt = await resp.text();
    return txt.length > 200 ? txt.slice(0, 200) + '…' : txt;
  } catch {
    return '';
  }
}
