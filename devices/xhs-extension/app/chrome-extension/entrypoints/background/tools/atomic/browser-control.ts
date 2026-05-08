import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';

interface BrowserControlArgs {
  action: 'close_tab' | 'get_tab' | 'update_tab' | 'query_tabs';
  params: any;
}

/**
 * browser_control - 通用浏览器控制工具
 * 提供标签页管理等浏览器操作能力
 */
export class BrowserControlTool extends BaseTool {
  name = 'browser_control';

  async execute(args: BrowserControlArgs): Promise<ToolResult> {
    const { action, params } = args;

    if (!action || !params) {
      return this.createErrorResult('action 和 params 参数必填');
    }

    try {
      switch (action) {
        case 'close_tab':
          return await this.closeTab(params);

        case 'get_tab':
          return await this.getTab(params);

        case 'update_tab':
          return await this.updateTab(params);

        case 'query_tabs':
          return await this.queryTabs(params);

        default:
          return this.createErrorResult(`未知操作: ${action}`);
      }
    } catch (error) {
      return this.createErrorResult(
        error instanceof Error ? error.message : '操作失败'
      );
    }
  }

  /**
   * 关闭标签页
   */
  private async closeTab(params: { tabId: number }): Promise<ToolResult> {
    if (!params.tabId) {
      return this.createErrorResult('tabId 参数必填');
    }

    await chrome.tabs.remove(params.tabId);

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({ success: true, action: 'close_tab', tabId: params.tabId }),
        },
      ],
      isError: false,
    };
  }

  /**
   * 获取标签页信息
   */
  private async getTab(params: { tabId: number }): Promise<ToolResult> {
    if (!params.tabId) {
      return this.createErrorResult('tabId 参数必填');
    }

    const tab = await chrome.tabs.get(params.tabId);

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            success: true,
            action: 'get_tab',
            tab: {
              id: tab.id,
              url: tab.url,
              title: tab.title,
              status: tab.status,
              active: tab.active,
            },
          }),
        },
      ],
      isError: false,
    };
  }

  /**
   * 更新标签页
   */
  private async updateTab(params: { tabId: number; updateInfo: chrome.tabs.UpdateProperties }): Promise<ToolResult> {
    if (!params.tabId) {
      return this.createErrorResult('tabId 参数必填');
    }

    const updated = await chrome.tabs.update(params.tabId, params.updateInfo || {});

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            success: true,
            action: 'update_tab',
            tab: {
              id: updated.id,
              url: updated.url,
              title: updated.title,
              status: updated.status,
              active: updated.active,
            },
          }),
        },
      ],
      isError: false,
    };
  }

  /**
   * 查询标签页
   */
  private async queryTabs(params: { queryInfo?: chrome.tabs.QueryInfo }): Promise<ToolResult> {
    const tabs = await chrome.tabs.query(params.queryInfo || {});

    return {
      content: [
        {
          type: 'text',
          text: JSON.stringify({
            success: true,
            action: 'query_tabs',
            tabs: tabs.map(tab => ({
              id: tab.id,
              url: tab.url,
              title: tab.title,
              status: tab.status,
              active: tab.active,
            })),
          }),
        },
      ],
      isError: false,
    };
  }
}