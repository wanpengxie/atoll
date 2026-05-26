import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  COAGENT_DEVICE_PROTOCOL,
} from 'coagent-xhs-shared';
import { BaseTool } from './base-tool';
import { getConnectionConfig } from '../connection-state';

/**
 * 需要同步的小红书 Cookie 名称
 * 这些 Cookie 用于创作者平台的身份验证
 */
const REQUIRED_COOKIE_NAMES = [
  'web_session',
  'access-token',
  'customer-sso-sid',
  'a1',
  'webId',
  'gid',
  'webBuild',
  'xsecappid',
];

export function deriveHttpBaseFromWsUrl(wsUrl: string): string {
  const raw = (wsUrl ?? '').trim();
  if (!raw) return '';
  try {
    const u = new URL(raw);
    if (u.protocol === 'ws:') u.protocol = 'http:';
    if (u.protocol === 'wss:') u.protocol = 'https:';
    u.pathname = '';
    u.search = '';
    u.hash = '';
    return u.toString().replace(/\/$/, '');
  } catch {
    return '';
  }
}

/**
 * Cookie 同步工具
 * 从浏览器获取小红书 Cookie 并同步到后端
 */
export class SyncCookiesTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.SYNC_COOKIES;

  async execute(args?: { force?: boolean }): Promise<ToolResult> {
    try {
      // 获取小红书域名下的所有 Cookie
      const cookies = await this.getXHSCookies();

      if (cookies.length === 0) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: false,
                message: '未找到小红书 Cookie，请先登录小红书创作者平台',
              }),
            },
          ],
          isError: false,
        };
      }

      // 检查是否有必需的 Cookie
      const cookieNames = cookies.map(c => c.name);
      const hasWebSession = cookieNames.includes('web_session');
      const hasAccessToken = cookieNames.includes('access-token');

      if (!hasWebSession && !hasAccessToken) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: false,
                message: '缺少关键登录 Cookie (web_session 或 access-token)，请重新登录',
              }),
            },
          ],
          isError: false,
        };
      }

      // 转换为后端需要的格式
      const cookieData = cookies.map(c => ({
        name: c.name,
        value: c.value,
        domain: c.domain,
        path: c.path,
        expires: c.expirationDate ? Math.floor(c.expirationDate) : 0,
        httpOnly: c.httpOnly,
        secure: c.secure,
        sameSite: c.sameSite,
      }));

      // 同步到后端
      const result = await this.syncToBackend(cookieData);

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: result.success,
              message: result.message,
              cookieCount: cookies.length,
              syncedAt: new Date().toISOString(),
            }),
          },
        ],
        isError: !result.success,
      };
    } catch (error) {
      console.error('SyncCookies error:', error);
      return {
        content: [
          {
            type: 'error',
            text: `Cookie 同步失败: ${error instanceof Error ? error.message : '未知错误'}`,
          },
        ],
        isError: true,
      };
    }
  }

  /**
   * 获取小红书域名下的所有 Cookie
   */
  private async getXHSCookies(): Promise<chrome.cookies.Cookie[]> {
    const domains = [
      '.xiaohongshu.com',
      'www.xiaohongshu.com',
      'creator.xiaohongshu.com',
    ];

    const allCookies: chrome.cookies.Cookie[] = [];
    const seenNames = new Set<string>();

    for (const domain of domains) {
      try {
        const cookies = await chrome.cookies.getAll({ domain });
        for (const cookie of cookies) {
          // 避免重复
          if (!seenNames.has(cookie.name)) {
            allCookies.push(cookie);
            seenNames.add(cookie.name);
          }
        }
      } catch (e) {
        console.warn(`Failed to get cookies for domain ${domain}:`, e);
      }
    }

    // 过滤出重要的 Cookie
    return allCookies.filter(c =>
      REQUIRED_COOKIE_NAMES.includes(c.name) ||
      c.name.startsWith('xhs') ||
      c.name.includes('session') ||
      c.name.includes('token')
    );
  }

  /**
   * 同步 Cookie 到 coagent daemon device state 端点（spec §4.3）。
   * URL: POST {daemonHttpBase}/api/device/{deviceId}/session
   * Header: Authorization: Bearer {device_api_key}
   * Body: {cookies:[{name,value,domain,path,expires,httpOnly,secure,sameSite}], login_state}
   */
  private async syncToBackend(cookies: any[]): Promise<{ success: boolean; message: string }> {
    const config = await getConnectionConfig();
    const apiKey = (config.apiKey ?? '').trim();
    const deviceId = (config.deviceId ?? '').trim();
    const httpBase =
      (config.daemonHttpBase ?? '').trim() || deriveHttpBaseFromWsUrl(config.serverUrl ?? '');

    if (!httpBase || !deviceId || !apiKey) {
      return {
        success: false,
        message: 'Device 配置不完整（缺少 daemon HTTP base / device id / device api key）',
      };
    }

    // login_state 用必备 cookie 的存在性近似判断；准确判定请走 check-login.ts。
    const cookieNames = cookies.map((c) => c.name);
    const isLoggedIn =
      cookieNames.includes('web_session') || cookieNames.includes('access-token');

    const apiUrl =
      stripTrailingSlash(httpBase) +
      COAGENT_DEVICE_PROTOCOL.CALLBACK_PATH_PREFIX +
      encodeURIComponent(deviceId) +
      COAGENT_DEVICE_PROTOCOL.CALLBACK_PATH_SUFFIX_SESSION;

    try {
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${apiKey}`,
        },
        body: JSON.stringify({
          user_id: (config.userId ?? '').trim() || undefined,
          cookies,
          login_state: isLoggedIn ? 'logged_in' : 'unknown',
        }),
      });

      if (!response.ok) {
        // 错误路径：daemon 4xx/5xx body 通常是 {ok:false, error:{code,message}} 不含 cookie，
        // 但 response 也不应被无差别拼回 message —— 仅取 status + 解析后的 error.message。
        let errorMessage = '';
        try {
          const parsed = await response.json();
          errorMessage = String(parsed?.error?.message ?? parsed?.message ?? '');
        } catch {
          // 解析失败时回退 status，绝不拼 raw text（防御 5xx 上游回写 cookie）。
          errorMessage = '';
        }
        return {
          success: false,
          message: errorMessage
            ? `device state 上报失败 (${response.status}): ${errorMessage}`
            : `device state 上报失败 (${response.status})`,
        };
      }

      // R3-T4 FX7 / round-2 review codex#t59.2：成功路径以前是
      //   `device state 已上报: ${bodyText}` —— 把 daemon 回的整段 result（含
      //   完整 cookies 数组、access-token / web_session 真值）拼进用户可见 message
      //   + console log。
      // 修复：daemon 改返回脱敏 envelope `{user_id, login_state, cookie_count,
      //   last_updated_at, expires_at}`；这里只读 cookie_count 拼诊断 message，
      //   不再透出 cookie 名 / 值。
      let cookieCount: number | null = null;
      try {
        const parsed = await response.json();
        const raw = parsed?.result?.cookie_count;
        if (typeof raw === 'number' && Number.isFinite(raw)) {
          cookieCount = raw;
        }
      } catch {
        // 兼容空体 / 非 JSON：保持成功语义但不带计数。
      }
      return {
        success: true,
        message:
          cookieCount != null
            ? `device state 已上报 (${cookieCount} cookies)`
            : 'device state 已上报',
      };
    } catch (error) {
      return {
        success: false,
        message: `网络请求失败: ${error instanceof Error ? error.message : '未知错误'}`,
      };
    }
  }
}

function stripTrailingSlash(s: string): string {
  return s.endsWith('/') ? s.slice(0, -1) : s;
}

/**
 * Cookie 同步服务
 * 提供自动同步和监听功能
 *
 * M1.1 Fix-T4 §2 修复：listener bind(this) 泄漏。`bind` 每次产生新函数引用，
 * `add/removeListener` 在不同引用下视为不同 listener，导致 stop() 无法卸载，
 * SW 唤醒重启时 listener 累计 N 份。修复：构造时一次性 bind 存到实例字段，
 * add/remove 都用同一引用。
 */
export class CookieSyncService {
  private syncInterval: ReturnType<typeof setInterval> | null = null;
  private lastSyncTime: number = 0;
  private syncTool: SyncCookiesTool;

  // M1.1 Fix-T4 §2: 必须使用稳定的 bound handler 引用，否则 removeListener 失效。
  private readonly boundHandleTabUpdate: (
    tabId: number,
    changeInfo: chrome.tabs.TabChangeInfo,
    tab: chrome.tabs.Tab
  ) => void;
  private readonly boundHandleCookieChange: (
    changeInfo: chrome.cookies.CookieChangeInfo
  ) => void;
  private started = false;

  constructor() {
    this.syncTool = new SyncCookiesTool();
    this.boundHandleTabUpdate = this.handleTabUpdate.bind(this) as typeof this.boundHandleTabUpdate;
    this.boundHandleCookieChange = this.handleCookieChange.bind(this) as typeof this.boundHandleCookieChange;
  }

  /**
   * 启动自动同步服务
   * 监听创作者平台页面访问 + xhs cookie 变化，自动 POST 到 coagent daemon /api/device/{id}/session。
   * spec §6.2.4：启动时主动 sync 一次。
   *
   * 幂等：重复 start() 不重复添加 listener，避免 SW 唤醒后重复挂载。
   */
  start() {
    if (this.started) {
      console.log('[CookieSyncService] already started, skip duplicate start');
      return;
    }
    this.started = true;

    // 监听页面更新，当用户访问创作者平台时自动同步
    chrome.tabs.onUpdated.addListener(this.boundHandleTabUpdate);

    // 监听 Cookie 变化
    chrome.cookies.onChanged.addListener(this.boundHandleCookieChange);

    console.log('[CookieSyncService] Started');

    // 启动时主动 sync 一次（spec §6.2.4）；不阻塞启动，错误吞掉。
    void this.syncNow().then((result) => {
      console.log('[CookieSyncService] initial sync result:', { isError: result.isError });
    });
  }

  /**
   * 停止自动同步服务
   */
  stop() {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
    chrome.tabs.onUpdated.removeListener(this.boundHandleTabUpdate);
    chrome.cookies.onChanged.removeListener(this.boundHandleCookieChange);
    this.started = false;
    console.log('[CookieSyncService] Stopped');
  }

  /**
   * 处理标签页更新事件
   */
  private async handleTabUpdate(
    tabId: number,
    changeInfo: chrome.tabs.TabChangeInfo,
    tab: chrome.tabs.Tab
  ) {
    // 只在页面加载完成时触发
    if (changeInfo.status !== 'complete') return;

    // 检查是否是创作者平台
    if (tab.url?.includes('creator.xiaohongshu.com')) {
      // 防抖：5分钟内不重复同步
      const now = Date.now();
      if (now - this.lastSyncTime < 5 * 60 * 1000) {
        return;
      }

      console.log('[CookieSyncService] Detected creator platform visit, syncing cookies...');
      this.lastSyncTime = now;

      try {
        const result = await this.syncTool.execute();
        console.log('[CookieSyncService] Sync result:', result);
      } catch (e) {
        console.error('[CookieSyncService] Sync failed:', e);
      }
    }
  }

  /**
   * 处理 Cookie 变化事件
   */
  private async handleCookieChange(changeInfo: chrome.cookies.CookieChangeInfo) {
    // 只关注小红书域名的关键 Cookie
    if (!changeInfo.cookie.domain.includes('xiaohongshu.com')) return;
    if (!REQUIRED_COOKIE_NAMES.includes(changeInfo.cookie.name)) return;

    // 只在 Cookie 被设置时同步（忽略删除）
    if (changeInfo.removed) return;

    // 防抖：30秒内不重复同步
    const now = Date.now();
    if (now - this.lastSyncTime < 30 * 1000) {
      return;
    }

    console.log(`[CookieSyncService] Cookie changed: ${changeInfo.cookie.name}, syncing...`);
    this.lastSyncTime = now;

    try {
      const result = await this.syncTool.execute();
      console.log('[CookieSyncService] Sync result:', result);
    } catch (e) {
      console.error('[CookieSyncService] Sync failed:', e);
    }
  }

  /**
   * 手动触发同步
   */
  async syncNow(): Promise<ToolResult> {
    this.lastSyncTime = Date.now();
    return this.syncTool.execute({ force: true });
  }
}

// 导出单例服务
export const cookieSyncService = new CookieSyncService();
