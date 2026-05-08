import { BaseTool } from '../base-tool';
import { ToolResult } from 'coagent-xhs-shared';

interface UploadFileArgs {
  selector: string;     // 文件输入框选择器
  base64Data: string;   // base64 编码的文件数据
  fileName?: string;    // 文件名
  mimeType?: string;    // MIME 类型
  timeout?: number;     // 超时时间(毫秒)
}

/**
 * chrome_upload_file - 上传文件
 * 使用 DataTransfer API 将 base64 数据转换为 File 对象并上传
 */
export class ChromeUploadFileTool extends BaseTool {
  name = 'chrome_upload_file';

  async execute(args: UploadFileArgs): Promise<ToolResult> {
    const {
      selector,
      base64Data,
      fileName = 'file',
      mimeType = 'application/octet-stream',
      timeout = 30000,
    } = args;

    if (!selector) {
      return this.createErrorResult('selector 参数必填');
    }

    if (!base64Data) {
      return this.createErrorResult('base64Data 参数必填');
    }

    try {
      // 获取当前活动标签页
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

      if (!tab?.id) {
        return this.createErrorResult('无法找到活动标签页');
      }

      const tabId = tab.id;

      // 等待文件输入框元素出现
      const elementFound = await this.waitForElement(tabId, selector, timeout);
      if (!elementFound) {
        return this.createErrorResult(`文件输入框未找到: ${selector}`);
      }

      // 执行文件上传
      await this.executeInTab(
        tabId,
        (selector: string, base64Data: string, fileName: string, mimeType: string) => {
          const input = document.querySelector(selector);

          if (!input) {
            throw new Error(`文件输入框未找到: ${selector}`);
          }

          if (!(input instanceof HTMLInputElement) || input.type !== 'file') {
            throw new Error('选择器指向的不是文件输入框');
          }

          // 将 base64 转换为 Blob
          const base64String = base64Data.replace(/^data:[^;]+;base64,/, '');
          const binaryString = atob(base64String);
          const bytes = new Uint8Array(binaryString.length);

          for (let i = 0; i < binaryString.length; i++) {
            bytes[i] = binaryString.charCodeAt(i);
          }

          const blob = new Blob([bytes], { type: mimeType });

          // 创建 File 对象
          const file = new File([blob], fileName, { type: mimeType });

          // 使用 DataTransfer API 创建文件列表
          const dataTransfer = new DataTransfer();
          dataTransfer.items.add(file);

          // 设置到 input 的 files 属性
          input.files = dataTransfer.files;

          // 触发 change 事件
          const changeEvent = new Event('change', { bubbles: true });
          input.dispatchEvent(changeEvent);

          // 触发 input 事件
          const inputEvent = new Event('input', { bubbles: true });
          input.dispatchEvent(inputEvent);

          return { success: true };
        },
        [selector, base64Data, fileName, mimeType]
      );

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              selector,
              fileName,
              mimeType,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(error instanceof Error ? error.message : '文件上传失败');
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
