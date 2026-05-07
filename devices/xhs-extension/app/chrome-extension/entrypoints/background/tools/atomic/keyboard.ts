import { BaseTool } from '../base-tool';
import { ToolResult } from 'xiaohongshu-mcp-shared';

interface KeyboardArgs {
  keys: string;         // 按键序列，逗号分隔 (如 "#,美,食" 或 "Enter")
  selector?: string;    // 可选：先聚焦到元素
  delay?: number;       // 每个按键间隔(毫秒)
  timeout?: number;     // 超时时间(毫秒)
}

/**
 * chrome_keyboard - 键盘输入
 * 支持：
 * - 普通字符输入（逗号分隔）
 * - 特殊按键（Enter, Space, Backspace, Tab, Escape, ArrowUp, ArrowDown 等）
 * - 可选的聚焦元素
 * - 按键间延迟
 */
export class ChromeKeyboardTool extends BaseTool {
  name = 'chrome_keyboard';

  async execute(args: KeyboardArgs): Promise<ToolResult> {
    const { keys, selector, delay = 0, timeout = 30000 } = args;

    if (!keys) {
      return this.createErrorResult('keys 参数必填');
    }

    try {
      // 获取当前活动标签页
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

      if (!tab?.id) {
        return this.createErrorResult('无法找到活动标签页');
      }

      const tabId = tab.id;

      // 如果指定了 selector，先等待并聚焦元素
      if (selector) {
        const elementFound = await this.waitForElement(tabId, selector, timeout);
        if (!elementFound) {
          return this.createErrorResult(`元素未找到: ${selector}`);
        }

        // 聚焦元素
        await this.executeInTab(
          tabId,
          (selector: string) => {
            const element = document.querySelector(selector);
            if (element instanceof HTMLElement) {
              element.focus();
            }
          },
          [selector]
        );
      }

      // 解析按键序列
      const keySequence = keys.split(',').map((k) => k.trim());

      // 执行按键输入
      await this.executeInTab(
        tabId,
        async (keySequence: string[], delay: number) => {
          // 特殊按键映射
          const specialKeys: Record<string, string> = {
            Enter: 'Enter',
            Space: ' ',
            Backspace: 'Backspace',
            Tab: 'Tab',
            Escape: 'Escape',
            ArrowUp: 'ArrowUp',
            ArrowDown: 'ArrowDown',
            ArrowLeft: 'ArrowLeft',
            ArrowRight: 'ArrowRight',
            Delete: 'Delete',
            Home: 'Home',
            End: 'End',
            PageUp: 'PageUp',
            PageDown: 'PageDown',
          };

          // 延迟函数
          const wait = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

          // 逐个按键输入
          for (const key of keySequence) {
            const actualKey = specialKeys[key] || key;

            // 获取当前焦点元素
            const activeElement = document.activeElement;

            if (!activeElement) {
              throw new Error('没有聚焦的元素');
            }

            // 创建键盘事件
            const keydownEvent = new KeyboardEvent('keydown', {
              key: actualKey,
              code: actualKey,
              bubbles: true,
              cancelable: true,
            });

            const keypressEvent = new KeyboardEvent('keypress', {
              key: actualKey,
              code: actualKey,
              bubbles: true,
              cancelable: true,
            });

            const keyupEvent = new KeyboardEvent('keyup', {
              key: actualKey,
              code: actualKey,
              bubbles: true,
              cancelable: true,
            });

            // 触发事件
            activeElement.dispatchEvent(keydownEvent);
            activeElement.dispatchEvent(keypressEvent);

            // 对于普通字符，更新输入框内容
            if (
              actualKey.length === 1 &&
              (activeElement instanceof HTMLInputElement ||
                activeElement instanceof HTMLTextAreaElement)
            ) {
              const currentValue = activeElement.value;
              const selectionStart = activeElement.selectionStart || currentValue.length;
              const selectionEnd = activeElement.selectionEnd || currentValue.length;

              // 插入字符
              activeElement.value =
                currentValue.substring(0, selectionStart) +
                actualKey +
                currentValue.substring(selectionEnd);

              // 更新光标位置
              activeElement.selectionStart = activeElement.selectionEnd = selectionStart + 1;

              // 触发 input 事件
              activeElement.dispatchEvent(new Event('input', { bubbles: true }));
            } else if (
              actualKey.length === 1 &&
              activeElement instanceof HTMLElement &&
              activeElement.isContentEditable
            ) {
              // contenteditable 元素
              document.execCommand('insertText', false, actualKey);
            } else if (actualKey === 'Enter') {
              // Enter 键特殊处理
              if (
                activeElement instanceof HTMLInputElement ||
                activeElement instanceof HTMLTextAreaElement
              ) {
                // 可能触发表单提交
                const form = activeElement.closest('form');
                if (form) {
                  form.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
                }
              } else if (activeElement instanceof HTMLElement && activeElement.isContentEditable) {
                document.execCommand('insertLineBreak');
              }
            } else if (actualKey === 'Backspace') {
              // Backspace 特殊处理
              if (
                activeElement instanceof HTMLInputElement ||
                activeElement instanceof HTMLTextAreaElement
              ) {
                const currentValue = activeElement.value;
                const selectionStart = activeElement.selectionStart || 0;
                const selectionEnd = activeElement.selectionEnd || 0;

                if (selectionStart === selectionEnd && selectionStart > 0) {
                  // 删除前一个字符
                  activeElement.value =
                    currentValue.substring(0, selectionStart - 1) +
                    currentValue.substring(selectionEnd);
                  activeElement.selectionStart = activeElement.selectionEnd = selectionStart - 1;
                } else if (selectionStart !== selectionEnd) {
                  // 删除选中内容
                  activeElement.value =
                    currentValue.substring(0, selectionStart) + currentValue.substring(selectionEnd);
                  activeElement.selectionStart = activeElement.selectionEnd = selectionStart;
                }

                activeElement.dispatchEvent(new Event('input', { bubbles: true }));
              } else if (activeElement instanceof HTMLElement && activeElement.isContentEditable) {
                document.execCommand('delete');
              }
            }

            activeElement.dispatchEvent(keyupEvent);

            // 延迟
            if (delay > 0) {
              await wait(delay);
            }
          }

          return { success: true };
        },
        [keySequence, delay]
      );

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              keysCount: keySequence.length,
              selector: selector || 'activeElement',
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '键盘输入失败');
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
