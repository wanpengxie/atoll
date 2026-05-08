import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';

interface SetFilesArgs {
  selector: string; // 文件输入框选择器
  files: Array<{
    base64Data: string; // base64 编码的文件数据
    fileName: string; // 文件名
    mimeType: string; // MIME 类型
  }>;
  tabId?: number; // 目标标签页 ID（可选，不指定则使用当前活动标签页）
  timeout?: number; // 超时时间(毫秒)
}

/**
 * chrome_set_files - 设置文件到 input[type=file] 元素
 * 类似 go-rod 的 MustSetFiles
 */
export class ChromeSetFilesTool extends BaseTool {
  name = 'chrome_set_files';

  async execute(args: SetFilesArgs): Promise<ToolResult> {
    const { selector, files, timeout = 30000 } = args;

    if (!selector) {
      return this.createErrorResult('selector 参数必填');
    }

    if (!files || files.length === 0) {
      return this.createErrorResult('files 参数必填且不能为空');
    }

    try {
      // 获取目标标签页 ID
      let tabId = args.tabId;

      if (!tabId) {
        // 如果未指定 tabId，则使用当前活动标签页
        const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

        if (!tab?.id) {
          return this.createErrorResult('无法找到活动标签页');
        }

        tabId = tab.id;
      }

      // 等待文件输入框元素出现
      const elementFound = await this.waitForElement(tabId, selector, timeout);
      if (!elementFound) {
        return this.createErrorResult(`文件输入框未找到: ${selector}`);
      }

      // 执行文件设置
      await this.executeInTab(
        tabId,
        (
          selector: string,
          filesData: Array<{ base64Data: string; fileName: string; mimeType: string }>
        ) => {
          const input = document.querySelector(selector);

          if (!input) {
            throw new Error(`文件输入框未找到: ${selector}`);
          }

          if (!(input instanceof HTMLInputElement) || input.type !== 'file') {
            throw new Error('选择器指向的不是文件输入框');
          }

          // 使用 DataTransfer API 创建文件列表
          const dataTransfer = new DataTransfer();

          for (const fileData of filesData) {
            // 将 base64 转换为 Blob
            const base64String = fileData.base64Data.replace(/^data:[^;]+;base64,/, '');
            const binaryString = atob(base64String);
            const bytes = new Uint8Array(binaryString.length);

            for (let i = 0; i < binaryString.length; i++) {
              bytes[i] = binaryString.charCodeAt(i);
            }

            const blob = new Blob([bytes], { type: fileData.mimeType });

            // 创建 File 对象
            const file = new File([blob], fileData.fileName, { type: fileData.mimeType });
            dataTransfer.items.add(file);
          }

          // 设置到 input 的 files 属性
          input.files = dataTransfer.files;

          // 触发 change 和 input 事件
          const changeEvent = new Event('change', { bubbles: true, cancelable: true });
          const inputEvent = new Event('input', { bubbles: true, cancelable: true });
          input.dispatchEvent(inputEvent);
          input.dispatchEvent(changeEvent);

          return { success: true, count: filesData.length };
        },
        [selector, files]
      );

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              selector,
              fileCount: files.length,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '文件设置失败');
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
