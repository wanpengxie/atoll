import { BaseTool } from '../base-tool';
import { ToolResult } from 'xiaohongshu-mcp-shared';

interface ClickArgs {
  selector: string;       // CSS 选择器
  text?: string;          // 可选：匹配元素文本内容
  timeout?: number;       // 超时时间(毫秒)
  waitClickable?: boolean; // 等待元素可点击
}

/**
 * chrome_click - 点击页面元素
 */
export class ChromeClickTool extends BaseTool {
  name = 'chrome_click';

  async execute(args: ClickArgs): Promise<ToolResult> {
    const { selector, text, timeout = 30000, waitClickable = true } = args;

    if (!selector) {
      return this.createErrorResult('selector 参数必填');
    }

    try {
      // 获取当前活动标签页
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

      if (!tab?.id) {
        return this.createErrorResult('无法找到活动标签页');
      }

      const tabId = tab.id;

      // 等待元素出现
      const elementFound = await this.waitForElement(tabId, selector, timeout);
      if (!elementFound) {
        return this.createErrorResult(`元素未找到: ${selector}`);
      }

      // 执行点击
      await this.executeInTab(
        tabId,
        (selector: string, text?: string, waitClickable?: boolean) => {
          // 查找元素
          const elements = Array.from(document.querySelectorAll(selector));

          if (elements.length === 0) {
            throw new Error(`元素未找到: ${selector}`);
          }

          // 如果指定了 text，则过滤匹配的元素
          let targetElement: Element | null = null;
          if (text) {
            targetElement =
              elements.find((el) => {
                const elementText = el.textContent?.trim() || '';
                return elementText === text || elementText.includes(text);
              }) || null;

            if (!targetElement) {
              throw new Error(`未找到包含文本 "${text}" 的元素`);
            }
          } else {
            targetElement = elements[0];
          }

          // 检查元素是否可点击
          if (waitClickable) {
            const style = window.getComputedStyle(targetElement);
            if (style.display === 'none' || style.visibility === 'hidden') {
              throw new Error('元素不可见');
            }
          }

          // 滚动到元素位置
          targetElement.scrollIntoView({ behavior: 'smooth', block: 'center' });

          // 点击元素
          if (targetElement instanceof HTMLElement) {
            targetElement.click();
          } else {
            // 对于非 HTMLElement，触发 click 事件
            const clickEvent = new MouseEvent('click', {
              bubbles: true,
              cancelable: true,
              view: window,
            });
            targetElement.dispatchEvent(clickEvent);
          }

          return { success: true };
        },
        [selector, text, waitClickable]
      );

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              selector,
              text: text || undefined,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '点击失败');
    }
  }

  /**
   * 等待元素出现
   */
  private async waitForElement(
    tabId: number,
    selector: string,
    timeout: number
  ): Promise<boolean> {
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      try {
        const exists = await this.executeInTab<boolean>(
          tabId,
          (selector: string) => {
            return document.querySelector(selector) !== null;
          },
          [selector]
        );

        if (exists) {
          return true;
        }
      } catch (error) {
        // 继续等待
      }

      await new Promise((resolve) => setTimeout(resolve, 100));
    }

    return false;
  }
}
