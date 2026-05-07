import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  XIAOHONGSHU_URLS,
  EXTENSION_CONSTANTS,
  COAGENT_DEVICE_PROTOCOL,
} from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';
import { getConnectionConfig } from '../connection-state';
import { deriveHttpBaseFromWsUrl } from '../services/coagent-device';

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
   * 同步 Cookie 到 coagent daemon device session 端点（spec §4.3）。
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
        const errorText = await response.text().catch(() => '');
        return {
          success: false,
          message: `device session 上报失败 (${response.status}): ${errorText}`,
        };
      }

      // daemon 端 200 即视为成功；body 可能含 {ok:true,...}
      let bodyText = '';
      try {
        bodyText = await response.text();
      } catch {
        // ignore
      }
      return {
        success: true,
        message: bodyText ? `device session 已上报: ${bodyText}` : 'device session 已上报',
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
 */
export class CookieSyncService {
  private syncInterval: ReturnType<typeof setInterval> | null = null;
  private lastSyncTime: number = 0;
  private syncTool: SyncCookiesTool;

  constructor() {
    this.syncTool = new SyncCookiesTool();
  }

  /**
   * 启动自动同步服务
   * 监听创作者平台页面访问 + xhs cookie 变化，自动 POST 到 coagent daemon /api/device/{id}/session。
   * spec §6.2.4：启动时主动 sync 一次。
   */
  start() {
    // 监听页面更新，当用户访问创作者平台时自动同步
    chrome.tabs.onUpdated.addListener(this.handleTabUpdate.bind(this));

    // 监听 Cookie 变化
    chrome.cookies.onChanged.addListener(this.handleCookieChange.bind(this));

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
    chrome.tabs.onUpdated.removeListener(this.handleTabUpdate.bind(this));
    chrome.cookies.onChanged.removeListener(this.handleCookieChange.bind(this));
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
