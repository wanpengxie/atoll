import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';

interface WaitElementsArgs {
  selector: string; // CSS 选择器
  count: number; // 期望的元素数量
  timeout?: number; // 超时时间(毫秒)，默认 60 秒
  checkInterval?: number; // 检查间隔(毫秒)，默认 500ms
}

/**
 * chrome_wait_elements - 等待指定数量的元素出现
 * 用于等待上传完成等场景
 */
export class ChromeWaitElementsTool extends BaseTool {
  name = 'chrome_wait_elements';

  async execute(args: WaitElementsArgs): Promise<ToolResult> {
    const { selector, count, timeout = 60000, checkInterval = 500 } = args;

    if (!selector) {
      return this.createErrorResult('selector 参数必填');
    }

    if (count === undefined || count < 0) {
      return this.createErrorResult('count 参数必填且必须为非负数');
    }

    try {
      // 获取当前活动标签页
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

      if (!tab?.id) {
        return this.createErrorResult('无法找到活动标签页');
      }

      const tabId = tab.id;
      const startTime = Date.now();

      // 循环检查元素数量
      while (Date.now() - startTime < timeout) {
        try {
          const currentCount = await this.executeInTab<number>(
            tabId,
            (selector: string) => {
              const elements = document.querySelectorAll(selector);
              return elements.length;
            },
            [selector]
          );

          console.log(`[chrome_wait_elements] Current count: ${currentCount}, Expected: ${count}`);

          if (currentCount >= count) {
            return {
              content: [
                {
                  type: 'text',
                  text: JSON.stringify({
                    success: true,
                    selector,
                    expectedCount: count,
                    actualCount: currentCount,
                    elapsed: Date.now() - startTime,
                  }),
                },
              ],
              isError: false,
            };
          }
        } catch (error) {
          console.log(`[chrome_wait_elements] Check failed:`, error);
        }

        // 等待后再次检查
        await new Promise((resolve) => setTimeout(resolve, checkInterval));
      }

      // 超时
      return this.createErrorResult(
        `等待元素超时: 期望 ${count} 个 "${selector}"，超时时间 ${timeout}ms`
      );
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '等待元素失败');
    }
  }
}
