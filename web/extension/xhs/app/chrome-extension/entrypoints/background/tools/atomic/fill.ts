import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';

interface FillArgs {
  selector: string;     // CSS 选择器
  value: string;        // 填充值
  method?: string;      // 填充方法: set, type, execCommand
  clearBefore?: boolean; // 填充前清空
  timeout?: number;     // 超时时间(毫秒)
  tabId?: number;       // 指定标签页 ID（可选）
}

/**
 * chrome_fill - 填充输入框或可编辑元素
 * 支持三种填充方法：
 * - set: 直接设置 value (适用于 input/textarea)
 * - type: 模拟逐字输入 (触发输入事件)
 * - execCommand: 使用 document.execCommand (适用于 contenteditable)
 */
export class ChromeFillTool extends BaseTool {
  name = 'chrome_fill';

  async execute(args: FillArgs): Promise<ToolResult> {
    const { selector, value, method = 'set', clearBefore = false, timeout = 30000, tabId: providedTabId } = args;

    if (!selector) {
      return this.createErrorResult('selector 参数必填');
    }

    if (value === undefined || value === null) {
      return this.createErrorResult('value 参数必填');
    }

    try {
      let tabId: number;

      // 如果提供了 tabId，使用提供的；否则获取当前活动标签页
      if (providedTabId !== undefined) {
        tabId = providedTabId;
      } else {
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

        if (!tab?.id) {
          return this.createErrorResult('无法找到活动标签页');
        }

        tabId = tab.id;
      }

      // 等待元素出现
      const elementFound = await this.waitForElement(tabId, selector, timeout);
      if (!elementFound) {
        return this.createErrorResult(`元素未找到: ${selector}`);
      }

      // 执行填充
      await this.executeInTab(
        tabId,
        (
          selector: string,
          value: string,
          method: string,
          clearBefore: boolean
        ) => {
          const element = document.querySelector(selector);

          if (!element) {
            throw new Error(`元素未找到: ${selector}`);
          }

          // 聚焦到元素
          if (element instanceof HTMLElement) {
            element.focus();
          }

          // 清空处理
          if (clearBefore) {
            if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
              element.value = '';
            } else if (element instanceof HTMLElement && element.isContentEditable) {
              element.textContent = '';
            }
          }

          // 根据方法填充
          switch (method) {
            case 'set':
              // 直接设置 value
              if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
                element.value = value;

                // 触发事件
                element.dispatchEvent(new Event('input', { bubbles: true }));
                element.dispatchEvent(new Event('change', { bubbles: true }));
              } else if (element instanceof HTMLElement && element.isContentEditable) {
                element.textContent = value;

                // 触发输入事件
                element.dispatchEvent(new Event('input', { bubbles: true }));
              } else {
                throw new Error('元素不支持 set 方法');
              }
              break;

            case 'type':
              // 模拟逐字输入
              if (element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement) {
                const currentValue = element.value;
                element.value = currentValue + value;

                // 为每个字符触发事件
                for (let i = 0; i < value.length; i++) {
                  element.dispatchEvent(new KeyboardEvent('keydown', { bubbles: true }));
                  element.dispatchEvent(new KeyboardEvent('keypress', { bubbles: true }));
                  element.dispatchEvent(new Event('input', { bubbles: true }));
                  element.dispatchEvent(new KeyboardEvent('keyup', { bubbles: true }));
                }
              } else if (element instanceof HTMLElement && element.isContentEditable) {
                const currentText = element.textContent || '';
                element.textContent = currentText + value;

                element.dispatchEvent(new Event('input', { bubbles: true }));
              } else {
                throw new Error('元素不支持 type 方法');
              }
              break;

            case 'execCommand':
              // 使用 execCommand (适用于复杂的富文本编辑)
              if (element instanceof HTMLElement && element.isContentEditable) {
                element.focus();

                // 选中所有内容（如果需要清空）
                if (clearBefore) {
                  document.execCommand('selectAll', false);
                }

                // 插入文本
                document.execCommand('insertText', false, value);
              } else {
                throw new Error('元素不支持 execCommand 方法（需要 contenteditable）');
              }
              break;

            default:
              throw new Error(`不支持的填充方法: ${method}`);
          }

          return { success: true };
        },
        [selector, value, method, clearBefore]
      );

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              selector,
              method,
              valueLength: value.length,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '填充失败');
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
