import { BaseTool } from '../base-tool';
import { ToolResult } from 'xiaohongshu-mcp-shared';

interface ExtractDataArgs {
  selector?: string;      // CSS 选择器
  path?: string;         // window 对象路径，如 __INITIAL_STATE__.user
  attribute?: string;    // 要获取的属性（text, value, href, data-*, 等）
  source?: 'element' | 'window' | 'document'; // 数据源
  all?: boolean;         // 是否获取所有匹配的元素（默认只获取第一个）
  timeout?: number;      // 等待元素出现的超时时间
}

/**
 * chrome_extract_data - 从页面提取数据
 */
export class ChromeExtractDataTool extends BaseTool {
  name = 'chrome_extract_data';

  async execute(args: ExtractDataArgs): Promise<ToolResult> {
    const {
      selector,
      path,
      attribute = 'text',
      source = 'element',
      all = false,
      timeout = 5000
    } = args;

    try {
      // 获取当前活动标签页
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });

      if (!tab?.id) {
        return this.createErrorResult('无法找到活动标签页');
      }

      let result: any;

      switch (source) {
        case 'window':
          // 从 window 对象提取数据
          result = await this.extractWindowData(tab.id, path || '');
          break;

        case 'document':
          // 从 document 提取数据
          result = await this.extractDocumentData(tab.id, path || '');
          break;

        case 'element':
        default:
          // 从 DOM 元素提取数据
          if (!selector) {
            return this.createErrorResult('element 源需要提供 selector 参数');
          }
          result = await this.extractElementData(tab.id, selector, attribute, all, timeout);
          break;
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify({
              success: true,
              source,
              selector: selector || undefined,
              path: path || undefined,
              attribute: source === 'element' ? attribute : undefined,
              data: result,
            }),
          },
        ],
        isError: false,
      };
    } catch (error) {
      return this.createErrorResult(
        error instanceof Error ? error.message : '提取数据失败'
      );
    }
  }

  /**
   * 从 window 对象提取数据
   */
  private async extractWindowData(tabId: number, path: string): Promise<any> {
    return await this.executeInTab(tabId, (path: string) => {
      // 处理空路径，返回整个 window 的可序列化部分
      if (!path) {
        // 只返回一些安全的顶级属性
        return {
          location: {
            href: window.location.href,
            pathname: window.location.pathname,
            search: window.location.search,
            hash: window.location.hash
          },
          title: document.title,
          // 尝试获取常见的状态对象
          __INITIAL_STATE__: (window as any).__INITIAL_STATE__ || null,
        };
      }

      // 按路径提取数据
      const keys = path.split('.');
      let value: any = window;

      for (const key of keys) {
        if (value && typeof value === 'object' && key in value) {
          value = value[key];
        } else {
          return undefined;
        }
      }

      // 尝试序列化结果
      try {
        return JSON.parse(JSON.stringify(value));
      } catch (e) {
        // 如果无法序列化，返回字符串表示
        return String(value);
      }
    }, [path]);
  }

  /**
   * 从 document 对象提取数据
   */
  private async extractDocumentData(tabId: number, path: string): Promise<any> {
    return await this.executeInTab(tabId, (path: string) => {
      if (!path) {
        return {
          title: document.title,
          url: document.URL,
          readyState: document.readyState,
          characterSet: document.characterSet,
        };
      }

      // 按路径提取数据
      const keys = path.split('.');
      let value: any = document;

      for (const key of keys) {
        if (value && typeof value === 'object' && key in value) {
          value = value[key];
        } else {
          return undefined;
        }
      }

      // 尝试序列化结果
      try {
        return JSON.parse(JSON.stringify(value));
      } catch (e) {
        return String(value);
      }
    }, [path]);
  }

  /**
   * 从 DOM 元素提取数据
   */
  private async extractElementData(
    tabId: number,
    selector: string,
    attribute: string,
    all: boolean,
    timeout: number
  ): Promise<any> {
    // 首先等待元素出现
    const elementFound = await this.waitForElement(tabId, selector, timeout);
    if (!elementFound) {
      throw new Error(`元素未找到: ${selector}`);
    }

    return await this.executeInTab(tabId,
      (selector: string, attribute: string, all: boolean) => {
        const elements = all
          ? Array.from(document.querySelectorAll(selector))
          : [document.querySelector(selector)].filter(Boolean);

        if (elements.length === 0) {
          return null;
        }

        const extractValue = (element: Element): any => {
          if (!element) return null;

          switch (attribute) {
            case 'text':
              return (element as HTMLElement).innerText || element.textContent;
            case 'html':
              return (element as HTMLElement).innerHTML;
            case 'outerHTML':
              return (element as HTMLElement).outerHTML;
            case 'value':
              return (element as HTMLInputElement).value;
            case 'checked':
              return (element as HTMLInputElement).checked;
            case 'selected':
              return (element as HTMLOptionElement).selected;
            case 'href':
              return (element as HTMLAnchorElement).href;
            case 'src':
              return (element as HTMLImageElement).src;
            case 'className':
              return element.className;
            case 'id':
              return element.id;
            case 'tagName':
              return element.tagName;
            case 'exists':
              return true;
            default:
              // 处理 data-* 属性或其他自定义属性
              return element.getAttribute(attribute);
          }
        };

        if (all) {
          return elements.map(extractValue);
        } else {
          return extractValue(elements[0]);
        }
      },
      [selector, attribute, all]
    );
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
      const exists = await this.executeInTab<boolean>(tabId, (selector: string) => {
        return document.querySelector(selector) !== null;
      }, [selector]);

      if (exists) {
        return true;
      }

      await new Promise(resolve => setTimeout(resolve, 100));
    }

    return false;
  }
}