import { BaseTool } from '../base-tool';
import type { ToolResult } from 'xiaohongshu-mcp-shared';
import { DOUYIN_CREATOR_URLS, DOUYIN_TIMEOUTS } from './selectors';

interface DouyinCheckLoginResult {
  success: boolean;
  data?: {
    logged_in: boolean;
    username?: string;
    user_id?: string;
    avatar_url?: string;
  };
  error?: string;
}

/**
 * douyin_check_login - 检查抖音登录状态
 *
 * 工作流程：
 * 1. 导航到抖音创作者平台
 * 2. 检测页面是否显示登录按钮或用户信息
 * 3. 返回登录状态
 */
export class DouyinCheckLoginTool extends BaseTool {
  name = 'douyin_check_login';

  async execute(): Promise<ToolResult> {
    try {
      // 1. 查找或创建抖音标签页
      const tab = await this.findOrCreateDouyinTab(DOUYIN_CREATOR_URLS.HOME);

      if (!tab.id) {
        throw new Error('无法创建标签页');
      }

      // 2. 等待页面加载完成
      await this.waitForTabLoad(tab.id, DOUYIN_TIMEOUTS.PAGE_LOAD);

      // 3. 额外等待
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // 4. 检查是否被重定向到登录页
      const currentTab = await chrome.tabs.get(tab.id);
      const currentUrl = currentTab.url || '';

      // 如果被重定向到登录页面，说明未登录
      if (
        currentUrl.includes('login') ||
        currentUrl.includes('passport') ||
        currentUrl.includes('sso')
      ) {
        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify(
                {
                  success: true,
                  data: {
                    logged_in: false,
                  },
                },
                null,
                2
              ),
            },
          ],
          isError: false,
        };
      }

      // 5. 在页面中检测登录状态
      const result = await this.executeInTab<DouyinCheckLoginResult>(
        tab.id,
        this.checkLoginStatus
      );

      if (!result.success) {
        return this.createErrorResult(result.error || '检查登录状态失败');
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(result, null, 2),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('[douyin_check_login] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '检查抖音登录状态失败'
      );
    }
  }

  /**
   * 在页面中执行的登录状态检测函数
   */
  private checkLoginStatus = (): DouyinCheckLoginResult => {
    try {
      // 检查是否有登录按钮（未登录状态）
      const loginBtn = document.querySelector(
        '.login-btn, [class*="loginBtn"], [class*="notLogin"], [class*="login-button"]'
      );

      if (loginBtn) {
        return {
          success: true,
          data: {
            logged_in: false,
          },
        };
      }

      // 检查是否有用户头像或用户名（已登录状态）
      const avatarElement = document.querySelector(
        '.avatar img, [class*="avatar"] img, [class*="userAvatar"] img'
      ) as HTMLImageElement | null;

      const usernameElement = document.querySelector(
        '.user-name, .creator-name, [class*="userName"], [class*="nickName"]'
      );

      const username = usernameElement?.textContent?.trim() || '';
      const avatar_url = avatarElement?.src || '';

      // 如果有头像或用户名，认为已登录
      if (avatar_url || username) {
        return {
          success: true,
          data: {
            logged_in: true,
            username: username || undefined,
            avatar_url: avatar_url || undefined,
          },
        };
      }

      // 尝试从 window 对象获取
      const windowData = (window as any).__INITIAL_STATE__ || (window as any).__NUXT__;
      if (windowData?.user?.uid || windowData?.data?.user?.uid) {
        const userData = windowData.user || windowData.data?.user;
        return {
          success: true,
          data: {
            logged_in: true,
            username: userData.nickname || undefined,
            user_id: userData.uid || userData.userId || undefined,
            avatar_url: userData.avatar || undefined,
          },
        };
      }

      // 检查 cookies 中是否有登录凭证
      const cookies = document.cookie;
      const hasLoginCookie =
        cookies.includes('sessionid') ||
        cookies.includes('passport_csrf_token') ||
        cookies.includes('ttwid');

      return {
        success: true,
        data: {
          logged_in: hasLoginCookie,
        },
      };
    } catch (error) {
      console.error('[checkLoginStatus] Error:', error);
      return {
        success: false,
        error: error instanceof Error ? error.message : '检测登录状态失败',
      };
    }
  };
}
