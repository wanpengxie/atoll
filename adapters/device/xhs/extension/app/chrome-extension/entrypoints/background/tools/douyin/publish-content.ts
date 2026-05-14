import { BaseTool } from '../base-tool';
import { runInMainWorld } from '../inject-script';
import type { ToolResult } from 'coagent-xhs-shared';
import { DOUYIN_CREATOR_URLS, DOUYIN_TIMEOUTS, DOUYIN_UPLOAD } from './selectors';

interface PublishContentArgs {
  title?: string;
  content?: string;
  images: Array<{ type: 'data' | 'url'; value: string; fileName?: string }>;
  topics?: string[];
}

interface PublishScriptResult {
  success: boolean;
  message: string;
  url?: string;
}

/**
 * douyin_publish_content - 发布抖音图文内容
 *
 * 工作流程：
 * 1. 导航到抖音创作者首页
 * 2. 点击"发布图文"进入发布页面
 * 3. 上传图片、填写标题和内容
 * 4. 返回结果（不自动点击发布，让用户确认）
 */
export class DouyinPublishContentTool extends BaseTool {
  name = 'douyin_publish_content';

  async execute(args: PublishContentArgs): Promise<ToolResult> {
    try {
      // 参数验证
      if (!Array.isArray(args.images) || args.images.length === 0) {
        throw new Error('images 参数必须是非空数组');
      }

      if (args.images.length > DOUYIN_UPLOAD.MAX_IMAGES) {
        throw new Error(`图片数量不能超过${DOUYIN_UPLOAD.MAX_IMAGES}张`);
      }

      const invalidImage = args.images.find((image) => {
        return (
          !image ||
          typeof image !== 'object' ||
          (image.type !== 'url' && image.type !== 'data') ||
          typeof image.value !== 'string' ||
          image.value.trim() === ''
        );
      });

      if (invalidImage) {
        throw new Error('图片资源格式无效，请提供 url 或 data 类型的有效值');
      }

      // 验证标题长度（抖音限制 20 字）
      if (args.title && args.title.length > 20) {
        throw new Error('标题长度不能超过20个字');
      }

      // 验证内容长度（抖音限制 1000 字）
      if (args.content && args.content.length > 1000) {
        throw new Error('文案长度不能超过1000个字');
      }

      // 1. 导航到抖音创作者首页
      const tab = await this.findOrCreateDouyinTab(DOUYIN_CREATOR_URLS.HOME);

      if (!tab.id) {
        throw new Error('无法创建标签页');
      }

      await this.waitForTabLoad(tab.id, DOUYIN_TIMEOUTS.PAGE_LOAD);

      // 检查是否已登录
      const currentTab = await chrome.tabs.get(tab.id);
      if (currentTab.url && !currentTab.url.includes('creator.douyin.com')) {
        throw new Error('需要登录抖音账号才能发布内容');
      }

      // 等待页面稳定
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // 2. 点击"发布图文"进入发布页面
      console.log('[douyin_publish_content] Clicking "发布图文" card...');
      try {
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          func: () => {
            // 查找"发布图文"文本
            const allElements = Array.from(document.querySelectorAll('*'));
            const textElement = allElements.find((el) => {
              return el.textContent?.trim() === '发布图文' && el.children.length === 0;
            });

            if (!textElement) {
              throw new Error('未找到"发布图文"文本');
            }

            // 向上查找可点击的父元素
            let clickTarget = textElement.parentElement;
            for (let i = 0; i < 3 && clickTarget; i++) {
              if (
                clickTarget.onclick ||
                window.getComputedStyle(clickTarget).cursor === 'pointer'
              ) {
                break;
              }
              clickTarget = clickTarget.parentElement;
            }

            if (!clickTarget) {
              throw new Error('未找到"发布图文"的可点击元素');
            }

            (clickTarget as HTMLElement).click();
          },
        });

        // 等待导航完成
        await new Promise((resolve) => setTimeout(resolve, 3000));
      } catch (clickError) {
        console.warn('[douyin_publish_content] Failed to click card:', clickError);
        // 如果点击失败，直接导航到发布页面
        await chrome.tabs.update(tab.id, {
          url: 'https://creator.douyin.com/creator-micro/content/upload?default-tab=3',
        });
        await this.waitForTabLoad(tab.id, DOUYIN_TIMEOUTS.PAGE_LOAD);
      }

      // 等待发布页面稳定
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // 3. 执行发布脚本
      const result = await runInMainWorld<PublishScriptResult>(
        tab.id,
        this.publishContentExecutor,
        args,
        120000
      );

      console.log('[douyin_publish_content] Script result:', result);

      if (!result || typeof result !== 'object') {
        throw new Error('发布脚本返回结果无效');
      }

      if (!result.success) {
        throw new Error(result.message || '发布失败');
      }

      return {
        content: [
          {
            type: 'text',
            text: JSON.stringify(
              {
                success: true,
                message: result.message,
                url: result.url,
                title: args.title,
                content_length: args.content?.length || 0,
                image_count: args.images.length,
                topic_count: args.topics?.length || 0,
              },
              null,
              2
            ),
          },
        ],
        isError: false,
      };
    } catch (error) {
      console.error('[douyin_publish_content] Error:', error);
      return this.createErrorResult(
        error instanceof Error ? error.message : '发布抖音内容失败'
      );
    }
  }

  /**
   * 在页面中执行的发布脚本
   * 注意：此函数将在页面 context 中运行，必须自包含
   */
  private publishContentExecutor = async (args: Record<string, any>): Promise<PublishScriptResult> => {
    const params = args as PublishContentArgs;

    console.log('[Douyin PublishContent] Starting execution with params:', params);

    // 辅助函数：等待元素出现
    const waitForElement = (selector: string, timeout = 60000): Promise<Element> => {
      console.log('[waitForElement] Looking for:', selector);
      return new Promise((resolve, reject) => {
        const existing = document.querySelector(selector);
        if (existing) {
          console.log('[waitForElement] Found immediately:', selector);
          resolve(existing);
          return;
        }

        let timer: ReturnType<typeof setTimeout>;
        const observer = new MutationObserver(() => {
          const target = document.querySelector(selector);
          if (target) {
            observer.disconnect();
            clearTimeout(timer);
            resolve(target);
          }
        });

        timer = setTimeout(() => {
          observer.disconnect();
          reject(new Error('未找到元素 ' + selector));
        }, timeout);

        observer.observe(document.body, { childList: true, subtree: true });
      });
    };

    // 辅助函数：从资源创建文件
    const createFileFromResource = async (resource: any, index: number): Promise<File> => {
      if (!resource || typeof resource !== 'object') {
        throw new Error(`图片资源(${index}) 格式错误`);
      }

      if (resource.type === 'data') {
        const match = resource.value.match(/^data:(.*?);base64,(.*)$/);
        if (!match) {
          throw new Error('无效的 data URL 格式');
        }

        const mime = match[1];
        const binary = atob(match[2]);
        const array = new Uint8Array(binary.length);

        for (let i = 0; i < binary.length; i++) {
          array[i] = binary.charCodeAt(i);
        }

        const blob = new Blob([array], { type: mime || 'image/jpeg' });
        const fileName = resource.fileName || `image_${index}.${mime.split('/')[1] || 'jpg'}`;
        return new File([blob], fileName, { type: mime || 'image/jpeg' });
      }

      if (resource.type === 'url') {
        throw new Error('图片资源类型错误：服务端应该已经预处理 URL 图片');
      }

      throw new Error(`不支持的图片资源类型: ${resource.type}`);
    };

    // 上传图片
    const uploadImages = async (images: any[]): Promise<void> => {
      console.log('[uploadImages] Starting image upload');

      const fileInput = (await waitForElement(
        'input[type="file"][accept*="image"][multiple]'
      )) as HTMLInputElement;
      console.log('[uploadImages] Found file input');

      const dataTransfer = new DataTransfer();

      for (let i = 0; i < images.length; i++) {
        const file = await createFileFromResource(images[i], i);
        console.log(`[uploadImages] Created file ${i}:`, file.name, file.size);
        dataTransfer.items.add(file);
      }

      fileInput.files = dataTransfer.files;
      const changeEvent = new Event('change', { bubbles: true, cancelable: true });
      const inputEvent = new Event('input', { bubbles: true, cancelable: true });
      fileInput.dispatchEvent(inputEvent);
      fileInput.dispatchEvent(changeEvent);

      console.log('[uploadImages] Files set and events dispatched');
      await new Promise((resolve) => setTimeout(resolve, 3000));
    };

    // 填写标题
    const fillTitle = async (title: string): Promise<void> => {
      if (!title) return;

      console.log('[fillTitle] Filling title:', title);
      const titleInput = (await waitForElement(
        'input.semi-input.semi-input-default[placeholder*="标题"]'
      )) as HTMLInputElement;

      titleInput.value = title;
      titleInput.dispatchEvent(new Event('input', { bubbles: true }));
      titleInput.dispatchEvent(new Event('change', { bubbles: true }));

      await new Promise((resolve) => setTimeout(resolve, 500));
    };

    // 填写内容
    const fillContent = async (content: string): Promise<void> => {
      if (!content) return;

      console.log('[fillContent] Filling content:', content);
      const contentEditor = (await waitForElement(
        '.zone-container.editor-kit-container.editor.editor-comp-publish[contenteditable="true"]'
      )) as HTMLElement;

      contentEditor.focus();

      const selection = window.getSelection();
      if (selection) {
        const range = document.createRange();
        range.selectNodeContents(contentEditor);
        selection.removeAllRanges();
        selection.addRange(range);
      }

      document.execCommand('insertText', false, content);

      await new Promise((resolve) => setTimeout(resolve, 500));
    };

    // 添加话题
    const addTopics = async (topics: string[]): Promise<void> => {
      if (!topics || topics.length === 0) return;

      console.log('[addTopics] Adding topics:', topics);

      for (const topic of topics) {
        try {
          const topicButton = (await waitForElement('.toolbar-button-spPS4r')) as HTMLElement;
          topicButton.click();
          await new Promise((resolve) => setTimeout(resolve, 1000));

          const topicInput = document.querySelector(
            'input[placeholder*="搜索话题"]'
          ) as HTMLInputElement;
          if (topicInput) {
            topicInput.value = topic.replace(/^#/, '');
            topicInput.dispatchEvent(new Event('input', { bubbles: true }));
            await new Promise((resolve) => setTimeout(resolve, 1000));

            const firstSuggestion = document.querySelector('.topic-item') as HTMLElement;
            if (firstSuggestion) {
              firstSuggestion.click();
              await new Promise((resolve) => setTimeout(resolve, 500));
            }
          }
        } catch (topicError) {
          console.warn('[addTopics] Failed to add topic:', topic, topicError);
        }
      }
    };

    // 主执行逻辑
    try {
      console.log('[Douyin PublishContent] Current URL:', window.location.href);

      // 步骤 1: 上传图片
      await uploadImages(params.images);
      console.log('[Douyin PublishContent] Images uploaded');

      // 步骤 2: 填写标题
      await fillTitle(params.title || '');
      console.log('[Douyin PublishContent] Title filled');

      // 步骤 3: 填写内容
      await fillContent(params.content || '');
      console.log('[Douyin PublishContent] Content filled');

      // 步骤 4: 添加话题
      await addTopics(params.topics || []);
      console.log('[Douyin PublishContent] Topics added');

      // 步骤 5: 不自动点击发布按钮，让用户手动确认
      try {
        await waitForElement('.button-dhlUZE.primary-cECiOJ, button[class*="publish"]');
        console.log('[Douyin PublishContent] Publish button is ready');
      } catch (error) {
        console.warn('[Douyin PublishContent] Could not find publish button');
      }

      await new Promise((resolve) => setTimeout(resolve, 1000));

      return {
        success: true,
        message: '内容已填写完成，请手动点击发布按钮',
      };
    } catch (error) {
      console.error('[Douyin PublishContent] Execution error:', error);
      return {
        success: false,
        message: error instanceof Error ? error.message : '发布失败',
      };
    }
  };
}
