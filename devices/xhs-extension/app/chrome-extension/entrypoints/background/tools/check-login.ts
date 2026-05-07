import { XIAOHONGSHU_TOOL_NAMES, ToolResult, XIAOHONGSHU_URLS } from 'xiaohongshu-mcp-shared';
import { BaseTool, type TabSessionSnapshot } from './base-tool';
import { runInMainWorld } from './inject-script';

export class CheckLoginStatusTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.CHECK_LOGIN_STATUS;

  async execute(): Promise<ToolResult> {
    let tabSnapshot: TabSessionSnapshot | null = null;
    let toolTabId: number | undefined;

    try {
      // 根据 Go 版本的实现，直接访问探索页以触发登录校验
      tabSnapshot = await this.captureTabSessionSnapshot();
      const tab = await this.findOrCreateXHSTab(XIAOHONGSHU_URLS.EXPLORE);

      if (!tab.id) {
        throw new Error('Failed to create tab');
      }
      toolTabId = tab.id;

      const stateResult = await runInMainWorld<{
        isLoggedIn: boolean;
        userName?: string | null;
        userId?: string | null;
      }>(tab.id, () => {
        const state = window.__INITIAL_STATE__ || {};
        const loginUser = state?.user?.userInfo?._rawValue;
        const statusElement = document.querySelector(
          '.main-container .user .link-wrapper .channel'
        );
        console.log('&&&&&&&test debug:\n ', loginUser, 'status: ', statusElement);
        return {
          isLoggedIn: Boolean(
            loginUser?.userId || loginUser?.userID || loginUser?.nickname || statusElement
          ),
          userName: loginUser?.nickname || loginUser?.nickName || null,
          userId: loginUser?.userId || loginUser?.userID || null,
        };
      });

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              isLoggedIn: stateResult.isLoggedIn,
              userName: stateResult.userName,
              userId: stateResult.userId,
              message: stateResult.isLoggedIn ? '已登录小红书' : '未登录小红书，请先登录',
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('CheckLoginStatus error:', error);
      return {
        content: [
          {
            type: 'error',
            text: `检查登录状态失败: ${error instanceof Error ? error.message : '未知错误'}`,
          },
        ],
        isError: true,
      };
    } finally {
      await this.cleanupToolTabSession(toolTabId, tabSnapshot, {
        closeIfCreated: true,
        restoreActiveTab: true,
      });
    }
  }
}
