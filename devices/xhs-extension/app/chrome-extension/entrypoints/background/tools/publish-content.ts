import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  PublishContentArgs,
  XIAOHONGSHU_URLS,
} from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';
import { runInMainWorld } from './inject-script';

// ─── M1.1 Fix-T4 §1: publish 等真发布完成 (P0 critical) ──────────────────────
//
// 旧实现把表单填完即视为成功 → daemon 收到 ok callback 但用户根本没点
// publish。修复方式：runInMainWorld 仍然 fill-only，但在 background SW 这一层
// 挂 `chrome.tabs.onUpdated` + `chrome.tabs.onRemoved` 监听 publish tab：
//   - URL 离开 `/publish/publish` 页面 → resolve {url, note_id?}
//   - tab 被关闭 → reject `publish_canceled`
//   - 10 min 没有任何变化 → reject `publish_timeout`
//
// `waitForPublishCompletion` 抽成顶层 export 是为了 vitest 可注入 chromeApi
// hook，单测里用 fake event 驱动完整生命周期。

/** 完成发布后回吐给 daemon 的最小载荷。 */
export interface PublishCompletionResult {
  /** 跳转到的最终 URL（通常是创作者中心或笔记详情）。 */
  url: string;
  /** 从 URL 解析出的笔记 ID（hex）；无法解析时为 null，不阻塞 ok。 */
  note_id: string | null;
}

/** 等待选项；chromeApi 仅供单测注入。 */
export interface PublishWaitOptions {
  /** 最长等待时间，默认 10 分钟（spec §Fix-T4-1）。 */
  timeoutMs?: number;
  chromeApi?: {
    tabsOnUpdated: Pick<chrome.tabs.TabUpdatedEvent, 'addListener' | 'removeListener'>;
    tabsOnRemoved: Pick<chrome.tabs.TabRemovedEvent, 'addListener' | 'removeListener'>;
  };
}

/** 抛给上层 callback 的取消错误，code 透传到 daemon。 */
export class PublishCanceledError extends Error {
  readonly code = 'publish_canceled';
  constructor(message = 'publish tab closed before completion') {
    super(message);
    this.name = 'PublishCanceledError';
  }
}

/** 抛给上层 callback 的超时错误，code 透传到 daemon。 */
export class PublishTimeoutError extends Error {
  readonly code = 'publish_timeout';
  constructor(message = 'publish wait timed out') {
    super(message);
    this.name = 'PublishTimeoutError';
  }
}

/**
 * publish 表单 URL 模式 —— 用户停留在该页面意味着尚未发布。
 * 离开此模式视为完成（成功或被用户主动跳走，UX 上等价）。
 */
export const PUBLISH_PAGE_PATTERN = /creator\.xiaohongshu\.com\/(?:publish|new)\/(?:publish|note)/i;

/**
 * XHS 笔记详情 URL 模式，用于提取 note_id（hex 24 位）。
 * 兼容 `/explore/<id>` 与 `/discovery/item/<id>` 两种形态。
 */
export const NOTE_ID_URL_PATTERN = /\/(?:explore|discovery\/item)\/([0-9a-f]{20,})/i;

/** 默认 publish 等待超时：10 分钟。 */
export const DEFAULT_PUBLISH_WAIT_TIMEOUT_MS = 10 * 60 * 1000;

/**
 * 等待 publish tab 真正发布完成。
 * 失败时抛 `PublishCanceledError` / `PublishTimeoutError`，code 字段会被
 * cmd-handlers-init.wrapTool 透传到 daemon callback envelope。
 */
export function waitForPublishCompletion(
  tabId: number,
  opts: PublishWaitOptions = {}
): Promise<PublishCompletionResult> {
  const timeoutMs = opts.timeoutMs ?? DEFAULT_PUBLISH_WAIT_TIMEOUT_MS;
  const tabsOnUpdated = opts.chromeApi?.tabsOnUpdated ?? chrome.tabs.onUpdated;
  const tabsOnRemoved = opts.chromeApi?.tabsOnRemoved ?? chrome.tabs.onRemoved;

  return new Promise<PublishCompletionResult>((resolve, reject) => {
    let settled = false;

    const settle = (fn: () => void) => {
      if (settled) return;
      settled = true;
      try {
        tabsOnUpdated.removeListener(onUpdated);
      } catch {
        /* ignore */
      }
      try {
        tabsOnRemoved.removeListener(onRemoved);
      } catch {
        /* ignore */
      }
      clearTimeout(timer);
      fn();
    };

    const onUpdated = (
      id: number,
      changeInfo: chrome.tabs.TabChangeInfo,
      tab?: chrome.tabs.Tab
    ) => {
      if (id !== tabId) return;
      const url = (changeInfo.url ?? tab?.url ?? '').trim();
      if (!url) return;
      // 仍在 publish 表单页 → 还没点 publish。
      if (PUBLISH_PAGE_PATTERN.test(url)) return;
      // 离开 publish 表单 → 视为发布完成（XHS 通常跳到创作者中心或笔记详情）。
      const match = url.match(NOTE_ID_URL_PATTERN);
      const note_id = match ? match[1] : null;
      settle(() => resolve({ url, note_id }));
    };

    const onRemoved = (id: number) => {
      if (id !== tabId) return;
      settle(() => reject(new PublishCanceledError()));
    };

    const timer = setTimeout(() => {
      settle(() => reject(new PublishTimeoutError(`publish wait timed out after ${timeoutMs}ms`)));
    }, timeoutMs);

    tabsOnUpdated.addListener(onUpdated);
    tabsOnRemoved.addListener(onRemoved);
  });
}

type PublishDebugLog = {
  ts: string;
  step: string;
  detail?: string;
};

type PublishScriptResult = {
  success: boolean;
  message: string;
  isScheduled?: boolean;
  publishAt?: string;
  manualPublishPending?: boolean;
  debugLogs?: PublishDebugLog[];
};

// This function will be serialized and executed in the page context
// Following the same logic as the Go version
const publishContentExecutor = async (args: Record<string, any>): Promise<PublishScriptResult> => {
  // All code must be self-contained since this runs in page context
  const params = args;
  const debugLogs: PublishDebugLog[] = [];
  const pushLog = (step: string, detail?: string) => {
    if (debugLogs.length >= 160) {
      debugLogs.shift();
    }
    debugLogs.push({
      ts: new Date().toISOString(),
      step,
      detail,
    });
  };

  console.log('[PublishContent] Starting execution with params:', params);
  pushLog('init', `titleLen=${String(params.title || '').length}, imageCount=${params.images?.length || 0}`);

  const waitForElement = (selector: string, timeout = 60000): Promise<Element> => {
    console.log('[waitForElement] Looking for:', selector);
    pushLog('wait.start', `${selector}, timeout=${timeout}`);
    return new Promise((resolve, reject) => {
      const existing = document.querySelector(selector);
      if (existing) {
        console.log('[waitForElement] Found immediately:', selector);
        pushLog('wait.ok', selector);
        resolve(existing);
        return;
      }

      let timer: NodeJS.Timeout;
      const observer = new MutationObserver(() => {
        const target = document.querySelector(selector);
        if (target) {
          observer.disconnect();
          clearTimeout(timer);
          pushLog('wait.ok', selector);
          resolve(target);
        }
      });

      timer = setTimeout(() => {
        observer.disconnect();
        pushLog('wait.timeout', selector);
        reject(new Error('未找到元素 ' + selector));
      }, timeout);

      observer.observe(document.body, { childList: true, subtree: true });
    });
  };

  // 检查元素是否可见 - 与 Go 版本的 isElementVisible 保持一致
  const isElementVisible = (element: Element): boolean => {
    const style = window.getComputedStyle(element);
    const rect = element.getBoundingClientRect();

    // 检查是否有隐藏样式
    if (style.display === 'none' || style.visibility === 'hidden') {
      return false;
    }

    // 检查位置是否在屏幕外
    if (rect.left < -9000 || rect.top < -9000) {
      return false;
    }

    return rect.width > 0 && rect.height > 0;
  };

  const isAlreadyImagePage = (): boolean => {
    const hasImageUploadBtn = Array.from(document.querySelectorAll('button')).some(
      (btn) => (btn.textContent || '').trim() === '上传图片'
    );
    const hasPublishBtn = Array.from(document.querySelectorAll('button')).some(
      (btn) => (btn.textContent || '').trim() === '发布'
    );
    const hasEditor = Boolean(
      document.querySelector(
        'div[role="textbox"][contenteditable="true"], div.tiptap.ProseMirror, div.ql-editor'
      )
    );
    return hasImageUploadBtn || (hasPublishBtn && hasEditor);
  };

  // 点击上传图文标签页 - 对应 Go 版本的 NewPublishImageAction
  const clickUploadTab = async () => {
    console.log('[clickUploadTab] Starting');
    pushLog('step1.start', '点击上传图文 Tab');

    // 等待 tab 容器可用（比 upload-content 更通用，已在图文态时也适用）
    await waitForElement('div.creator-tab');
    console.log('[clickUploadTab] creator-tab visible');

    // 等待页面稳定
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 查找所有的 creator-tab 元素
    const createElems = document.querySelectorAll('div.creator-tab');
    console.log(`[clickUploadTab] Found ${createElems.length} creator-tab elements`);

    // 过滤可见元素
    const visibleElems: Element[] = [];
    for (const elem of createElems) {
      if (isElementVisible(elem)) {
        visibleElems.push(elem);
      }
    }

    if (visibleElems.length === 0) {
      throw new Error('没有找到上传图文元素');
    }

    // 查找并点击"上传图文"
    let clicked = false;
    for (const elem of visibleElems) {
      const text = (elem.textContent || '').trim();
      console.log(`[clickUploadTab] Tab text: "${text}"`);
      if (text === '上传图文') {
        clickWithEvidence('step1.click', '点击上传图文 Tab', elem as HTMLElement);
        console.log('[clickUploadTab] Clicked 上传图文 tab');
        clicked = true;
        break;
      }
    }

    if (!clicked) {
      if (isAlreadyImagePage()) {
        console.log('[clickUploadTab] already in image page');
        pushLog('step1.skip', '已在图文发布态');
        return;
      }
      throw new Error('未找到可点击的上传图文 Tab');
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
    pushLog('step1.done', '上传图文 Tab 点击完成');
  };

  // 上传图片 - 对应 Go 版本的 uploadImages
  const uploadImages = async (images: any[]) => {
    console.log('[uploadImages] Starting image upload');
    pushLog('step2.start', `上传图片 count=${images.length}`);

    // 等待上传输入框出现
    const uploadInput = (await waitForElement('.upload-input')) as HTMLInputElement;
    console.log('[uploadImages] Found upload input');

    // 创建文件对象
    const dataTransfer = new DataTransfer();

    for (let i = 0; i < images.length; i++) {
      const file = await createFileFromResource(images[i], i);
      console.log(`[uploadImages] Created file ${i}:`, file.name, file.size);
      dataTransfer.items.add(file);
    }

    // 设置文件
    uploadInput.files = dataTransfer.files;

    // 触发事件
    const changeEvent = new Event('change', { bubbles: true, cancelable: true });
    const inputEvent = new Event('input', { bubbles: true, cancelable: true });
    uploadInput.dispatchEvent(inputEvent);
    uploadInput.dispatchEvent(changeEvent);

    console.log('[uploadImages] Files set and events dispatched');

    // 等待上传完成
    await new Promise((resolve) => setTimeout(resolve, 3000));
    pushLog('step2.done', '图片上传触发完成');
  };

  // 查找内容元素 - 对应 Go 版本的 getContentElement
  const getContentElement = async (): Promise<Element> => {
    console.log('[getContentElement] Looking for content element');

    // 方法1: 先查找新版编辑器（tiptap）
    let contentElem = document.querySelector(
      'div[role="textbox"][contenteditable="true"], div.tiptap.ProseMirror'
    );
    if (contentElem) {
      console.log('[getContentElement] Found tiptap editor');
      return contentElem;
    }

    // 方法2: 查找 div.ql-editor
    contentElem = document.querySelector('div.ql-editor');
    if (contentElem) {
      console.log('[getContentElement] Found ql-editor');
      return contentElem;
    }

    // 方法3: 通过 placeholder 查找
    const pElements = document.querySelectorAll('p');
    for (const elem of pElements) {
      const placeholder = elem.getAttribute('data-placeholder');
      if (placeholder && placeholder.includes('输入正文描述')) {
        console.log('[getContentElement] Found placeholder element');

        // 向上查找 textbox 父元素
        let current: Element | null = elem;
        for (let i = 0; i < 5 && current; i++) {
          const parentElement: HTMLElement | null = current.parentElement;
          if (!parentElement) break;

          const role = parentElement.getAttribute('role');
          if (role === 'textbox') {
            console.log('[getContentElement] Found textbox parent');
            return parentElement;
          }
          current = parentElement;
        }
      }
    }

    // 方法4: 最终兜底
    contentElem = document.querySelector('[role="textbox"][contenteditable="true"]');
    if (contentElem) {
      console.log('[getContentElement] Fallback to editable textbox');
      return contentElem;
    }

    throw new Error('没有找到内容输入框');
  };

  // 输入标签 - 对应 Go 版本的 inputTags
  const inputTags = async (contentElem: Element, tags: string[]) => {
    if (!tags || tags.length === 0) return;

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 对于contenteditable元素，使用Selection API移动光标到末尾
    if (contentElem.getAttribute('contenteditable')) {
      const selection = window.getSelection();
      if (selection) {
        const range = document.createRange();
        range.selectNodeContents(contentElem);
        range.collapse(false); // collapse到末尾
        selection.removeAllRanges();
        selection.addRange(range);
      }

      // 使用execCommand插入换行
      document.execCommand('insertParagraph', false);
      document.execCommand('insertParagraph', false);
    } else {
      // 对于input/textarea，移动光标到末尾
      if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
        const inputElem = contentElem as HTMLInputElement;
        inputElem.selectionStart = inputElem.selectionEnd = inputElem.value.length;
        inputElem.value += '\n\n';
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 输入每个标签
    for (const tag of tags) {
      await inputTag(contentElem, tag.replace(/^#/, ''));
    }
  };

  // 输入单个标签 - 对应 Go 版本的 inputTag
  const inputTag = async (contentElem: Element, tag: string) => {
    console.log('[inputTag] Starting to input tag:', tag);

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 对于contenteditable元素，使用execCommand
    if (contentElem.getAttribute('contenteditable')) {
      // 逐字符输入，模拟真实的键盘输入
      // 先输入 # 号
      document.execCommand('insertText', false, '#');
      await new Promise((resolve) => setTimeout(resolve, 200));

      // 逐字符输入标签文本
      for (const char of tag) {
        document.execCommand('insertText', false, char);
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
      // 对于input/textarea元素
      const inputElem = contentElem as HTMLInputElement;
      const start = inputElem.selectionStart || inputElem.value.length;

      // 插入#号
      inputElem.value =
        inputElem.value.substring(0, start) + '#' + inputElem.value.substring(start);
      inputElem.selectionStart = inputElem.selectionEnd = start + 1;
      inputElem.dispatchEvent(
        new InputEvent('input', {
          data: '#',
          inputType: 'insertText',
          bubbles: true,
        })
      );
      await new Promise((resolve) => setTimeout(resolve, 200));

      // 逐字符输入
      for (const char of tag) {
        const cursorPos: number = inputElem.selectionStart || inputElem.value.length;
        inputElem.value =
          inputElem.value.substring(0, cursorPos) + char + inputElem.value.substring(cursorPos);
        inputElem.selectionStart = inputElem.selectionEnd = cursorPos + 1;
        inputElem.dispatchEvent(
          new InputEvent('input', {
            data: char,
            inputType: 'insertText',
            bubbles: true,
          })
        );
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 查找并点击标签联想选项
    const topicContainer = document.querySelector('#creator-editor-topic-container');
    if (topicContainer) {
      console.log('[inputTag] Found topic container, waiting for items...');
      await new Promise((resolve) => setTimeout(resolve, 300)); // 额外等待选项加载

      const firstItem = topicContainer.querySelector('.item') as HTMLElement;
      if (firstItem) {
        console.log('[inputTag] Found and clicking suggestion item');
        firstItem.click();
        await new Promise((resolve) => setTimeout(resolve, 200));
      } else {
        console.log('[inputTag] No suggestion item found, adding space');
        // 没有找到联想选项，输入空格结束标签
        if (contentElem.getAttribute('contenteditable')) {
          document.execCommand('insertText', false, ' ');
        } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
          const inputElem = contentElem as HTMLInputElement;
          const pos = inputElem.selectionStart || inputElem.value.length;
          inputElem.value =
            inputElem.value.substring(0, pos) + ' ' + inputElem.value.substring(pos);
          inputElem.selectionStart = inputElem.selectionEnd = pos + 1;
        }
      }
    } else {
      console.log('[inputTag] No topic container found, adding space');
      // 没有找到下拉框，输入空格结束标签
      if (contentElem.getAttribute('contenteditable')) {
        document.execCommand('insertText', false, ' ');
      } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
        const inputElem = contentElem as HTMLInputElement;
        const pos = inputElem.selectionStart || inputElem.value.length;
        inputElem.value = inputElem.value.substring(0, pos) + ' ' + inputElem.value.substring(pos);
        inputElem.selectionStart = inputElem.selectionEnd = pos + 1;
      }
    }

    console.log('[inputTag] Completed tag input:', tag);
    await new Promise((resolve) => setTimeout(resolve, 500));
  };

  const normalizePublishAt = (
    input?: string
  ): { date: string; time: string; display: string } | null => {
    const value = (input || '').trim();
    if (!value) return null;

    const localPattern = /^(\d{4})[-/](\d{2})[-/](\d{2})[ T](\d{2}):(\d{2})(?::\d{2})?$/;
    const localMatch = value.match(localPattern);
    if (localMatch) {
      const date = `${localMatch[1]}-${localMatch[2]}-${localMatch[3]}`;
      const time = `${localMatch[4]}:${localMatch[5]}`;
      return { date, time, display: `${date} ${time}` };
    }

    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      throw new Error('publish_at 时间格式无效，请使用 YYYY-MM-DD HH:mm 或 RFC3339');
    }

    const pad = (n: number) => String(n).padStart(2, '0');
    const date = `${parsed.getFullYear()}-${pad(parsed.getMonth() + 1)}-${pad(parsed.getDate())}`;
    const time = `${pad(parsed.getHours())}:${pad(parsed.getMinutes())}`;
    return { date, time, display: `${date} ${time}` };
  };

  const normalizeText = (text?: string): string => (text || '').replace(/\s+/g, '').trim();

  const isElementEnabled = (element: Element): boolean => {
    if (element instanceof HTMLButtonElement && element.disabled) return false;
    if (element.hasAttribute('disabled')) return false;
    if (element.getAttribute('aria-disabled') === 'true') return false;
    const dataDisabled = (element.getAttribute('data-disabled') || '').toLowerCase();
    if (dataDisabled === 'true' || dataDisabled === '1') return false;
    const className =
      typeof (element as HTMLElement).className === 'string'
        ? (element as HTMLElement).className.toLowerCase()
        : '';
    if (className.includes('disabled') || className.includes('is-disable')) return false;
    return true;
  };

  const getClickabilityReason = (element: HTMLElement): string | null => {
    if (!isElementVisible(element)) return 'not_visible';
    if (!isElementEnabled(element)) return 'disabled';
    const style = window.getComputedStyle(element);
    if (style.pointerEvents === 'none') return 'pointer_events_none';
    if (style.visibility === 'hidden') return 'visibility_hidden';

    const rect = element.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return 'rect_too_small';
    const x = Math.min(window.innerWidth - 1, Math.max(0, Math.floor(rect.left + rect.width / 2)));
    const y = Math.min(window.innerHeight - 1, Math.max(0, Math.floor(rect.top + rect.height / 2)));
    const topNode = document.elementFromPoint(x, y);
    if (!topNode) return 'element_from_point_null';
    if (topNode !== element && !element.contains(topNode)) {
      return `covered_by:${(topNode as HTMLElement).tagName.toLowerCase()}`;
    }
    return null;
  };

  const describeClickableElement = (element: Element | null): string => {
    if (!(element instanceof HTMLElement)) {
      return 'element=null';
    }
    const rect = element.getBoundingClientRect();
    const className = typeof element.className === 'string' ? element.className : '';
    const text = (element.textContent || '').replace(/\s+/g, ' ').trim();
    return JSON.stringify({
      tag: element.tagName.toLowerCase(),
      id: element.id || '',
      className: className.slice(0, 120),
      type: element.getAttribute('type') || '',
      ariaLabel: element.getAttribute('aria-label') || '',
      text: text.slice(0, 60),
      disabled: element instanceof HTMLButtonElement ? element.disabled : false,
      ariaDisabled: element.getAttribute('aria-disabled') || '',
      rect: {
        left: Math.round(rect.left),
        top: Math.round(rect.top),
        width: Math.round(rect.width),
        height: Math.round(rect.height),
      },
    });
  };

  const clickWithEvidence = (step: string, label: string, element: HTMLElement) => {
    pushLog(`${step}.target`, `${label}: ${describeClickableElement(element)}`);
    try {
      element.scrollIntoView({ block: 'center', inline: 'center' });
    } catch {}
    element.focus();

    const rect = element.getBoundingClientRect();
    const x = Math.min(window.innerWidth - 1, Math.max(0, Math.floor(rect.left + rect.width / 2)));
    const y = Math.min(window.innerHeight - 1, Math.max(0, Math.floor(rect.top + rect.height / 2)));
    const topNode = document.elementFromPoint(x, y);
    if (topNode && topNode !== element && !element.contains(topNode)) {
      pushLog(`${step}.cover`, `center(${x},${y})=${describeClickableElement(topNode)}`);
    }

    element.click();
    pushLog(`${step}.clicked`, label);
  };

  const findVisibleActionButton = (
    texts: string[],
    options?: { root?: ParentNode | null }
  ): HTMLElement | null => {
    const normalizedTargets = texts.map((t) => normalizeText(t));
    const root = options?.root || document;
    const candidates = root.querySelectorAll('button, [role="button"], label, span, div, a');
    let fuzzyMatch: HTMLElement | null = null;

    for (const node of candidates) {
      if (!(node instanceof HTMLElement)) continue;
      if (!isElementVisible(node) || !isElementEnabled(node)) continue;

      const currentText = normalizeText(node.textContent || '');
      if (!currentText) continue;

      for (const target of normalizedTargets) {
        if (currentText === target) return node;
        if (!fuzzyMatch && currentText.includes(target)) {
          fuzzyMatch = node;
        }
      }
    }

    return fuzzyMatch;
  };

  const getScheduleSettingsContainer = (): HTMLElement => {
    const containers = Array.from(document.querySelectorAll('.post-time-wrapper')).filter(
      (node): node is HTMLElement => node instanceof HTMLElement && isElementVisible(node)
    );

    for (const container of containers) {
      if (container.querySelector('.post-time-switch-container')) {
        return container;
      }
      if (
        container.querySelector(
          '.custom-switch-switch .d-switch-simulator, .custom-switch-switch, input[type="checkbox"], .date-picker-container input'
        )
      ) {
        return container;
      }
    }

    throw new Error('未找到定时发布设置区域');
  };

  type PublishButtonMode = 'publish' | 'schedule';

  const matchButtonByText = (
    buttons: HTMLButtonElement[],
    targets: string[]
  ): HTMLButtonElement | null => {
    for (const target of targets) {
      const normalizedTarget = normalizeText(target);
      for (const button of buttons) {
        const buttonText = normalizeText(
          button.textContent || button.getAttribute('aria-label') || ''
        );
        if (!buttonText) continue;
        if (buttonText === normalizedTarget) {
          return button;
        }
      }
    }
    return null;
  };

  const findBottomPublishButton = (mode: PublishButtonMode): HTMLButtonElement | null => {
    const containers = Array.from(document.querySelectorAll('.publish-page-publish-btn')).filter(
      (node): node is HTMLElement => node instanceof HTMLElement && isElementVisible(node)
    );

    const textTargets =
      mode === 'schedule'
        ? ['定时发布', '确认定时发布']
        : ['发布', '立即发布', '确认发布', '发布笔记'];

    for (const container of containers) {
      const buttons = Array.from(container.querySelectorAll('button')).filter(
        (button): button is HTMLButtonElement =>
          button instanceof HTMLButtonElement &&
          isElementVisible(button) &&
          isElementEnabled(button)
      );
      if (buttons.length === 0) {
        continue;
      }

      const exactByText = matchButtonByText(buttons, textTargets);
      if (exactByText) {
        return exactByText;
      }
    }

    return null;
  };

  const waitForBottomPublishButtonClickable = async (
    mode: PublishButtonMode,
    timeoutMs = 25000
  ): Promise<HTMLButtonElement> => {
    const deadline = Date.now() + timeoutMs;
    let lastReason = 'not_found';
    while (Date.now() < deadline) {
      const button = findBottomPublishButton(mode);
      if (button) {
        const reason = getClickabilityReason(button);
        if (!reason) {
          return button;
        }
        lastReason = reason;
      } else {
        lastReason = 'not_found';
      }
      await new Promise((resolve) => setTimeout(resolve, 300));
    }

    const label = mode === 'schedule' ? '定时发布按钮' : '发布按钮';
    throw new Error(`${label}不可点击(${lastReason})`);
  };

  const setInputValue = (
    input: HTMLInputElement,
    value: string,
    options?: { triggerBlur?: boolean }
  ) => {
    input.focus();
    const prototype = Object.getPrototypeOf(input);
    const setter = Object.getOwnPropertyDescriptor(prototype, 'value')?.set;
    if (setter) {
      setter.call(input, value);
    } else {
      input.value = value;
    }
    input.dispatchEvent(new Event('input', { bubbles: true }));
    input.dispatchEvent(new Event('change', { bubbles: true }));
    if (options?.triggerBlur ?? true) {
      input.dispatchEvent(new Event('blur', { bubbles: true }));
    }
  };

  const setupScheduledPublish = async (publishAt: {
    date: string;
    time: string;
    display: string;
  }) => {
    const scheduleContainer = getScheduleSettingsContainer();
    const scheduleDebug = () => {
      const checkbox = scheduleContainer.querySelector(
        'input[type="checkbox"]'
      ) as HTMLInputElement | null;
      const inputs = Array.from(scheduleContainer.querySelectorAll('input')).map((input) => {
        const type = input.getAttribute('type') || '';
        const cls = input.getAttribute('class') || '';
        const ph = input.getAttribute('placeholder') || '';
        const visible = input instanceof HTMLElement ? isElementVisible(input) : false;
        return `${type || 'text'}:${cls}:${ph}:${visible ? 'visible' : 'hidden'}`;
      });
      const hasDatePicker = Boolean(scheduleContainer.querySelector('.date-picker-container'));
      return `checkbox=${checkbox ? checkbox.checked : 'none'}, hasDatePicker=${hasDatePicker}, inputs=[${inputs.join('|')}]`;
    };
    const hasScheduleInput = () => {
      const candidates = Array.from(
        scheduleContainer.querySelectorAll(
          'input[type="datetime-local"], .date-picker-container input.d-text, .post-time-wrapper input.d-text, input[placeholder*="日期"], input[placeholder*="时间"], input[placeholder*="发布"]'
        )
      );

      return candidates.some(
        (input) =>
          input instanceof HTMLInputElement && input.type !== 'checkbox' && isElementVisible(input)
      );
    };

    const enableScheduleInput = async () => {
      const toggleSelectors = [
        '.custom-switch-switch .d-switch',
        '.custom-switch-switch .d-switch-box',
        '.custom-switch-switch .d-switch-top',
        '.custom-switch-switch .d-switch-simulator',
        '.custom-switch-switch [role="switch"]',
        '.custom-switch-switch',
        '.custom-switch-card',
        '.custom-switch-wrapper',
        '.custom-switch-text-content',
        '.custom-switch-text-content .has-tips',
      ];

      const clickToggleOnce = () => {
        for (const selector of toggleSelectors) {
          const target = scheduleContainer.querySelector(selector);
          if (!(target instanceof HTMLElement) || !isElementVisible(target)) {
            continue;
          }
          target.click();
          return true;
        }

        const scheduleCheckbox = scheduleContainer.querySelector(
          'input[type="checkbox"]'
        ) as HTMLInputElement | null;
        if (scheduleCheckbox) {
          scheduleCheckbox.click();
          return true;
        }
        return false;
      };

      const waitDurations = [350, 900, 1800];
      for (const waitMs of waitDurations) {
        if (hasScheduleInput()) {
          return true;
        }

        const clicked = clickToggleOnce();
        if (!clicked) {
          return false;
        }

        await new Promise((resolve) => setTimeout(resolve, waitMs));
      }

      // 某些页面会出现“开关已勾选但输入框未渲染”的状态，二次翻转兜底一次
      if (!hasScheduleInput()) {
        const clicked = clickToggleOnce();
        if (clicked) {
          await new Promise((resolve) => setTimeout(resolve, 700));
        }
      }

      return hasScheduleInput();
    };

    if (!hasScheduleInput()) {
      const enabled = await enableScheduleInput();
      if (!enabled) {
        throw new Error(`未找到定时发布时间输入区域 (${scheduleDebug()})`);
      }
    }

    if (!hasScheduleInput()) {
      throw new Error(`未找到定时发布时间输入区域 (${scheduleDebug()})`);
    }

    const scheduleValue = `${publishAt.date} ${publishAt.time}`;
    const datePickerInput = Array.from(
      scheduleContainer.querySelectorAll(
        '.date-picker-container input.d-text, .date-picker-container input, .post-time-wrapper input.d-text'
      )
    ).find(
      (input): input is HTMLInputElement =>
        input instanceof HTMLInputElement && input.type !== 'checkbox'
    );
    if (datePickerInput instanceof HTMLInputElement) {
      if (datePickerInput.type === 'datetime-local') {
        setInputValue(datePickerInput, `${publishAt.date}T${publishAt.time}`);
        await new Promise((resolve) => setTimeout(resolve, 200));
        return;
      }
      // d-datepicker 在当前页面是受控文本输入，避免 blur 触发组件自动回滚
      setInputValue(datePickerInput, scheduleValue, { triggerBlur: false });
      await new Promise((resolve) => setTimeout(resolve, 200));
      return;
    }

    const datetimeInput = scheduleContainer.querySelector('input[type="datetime-local"]');
    if (datetimeInput instanceof HTMLInputElement) {
      setInputValue(datetimeInput, `${publishAt.date}T${publishAt.time}`);
    } else {
      const inputs = Array.from(scheduleContainer.querySelectorAll('input'));
      const dateInput = inputs.find((input) => {
        const placeholder = input.getAttribute('placeholder') || '';
        return placeholder.includes('日期') || placeholder.includes('日历');
      });
      const timeInput = inputs.find((input) => {
        const placeholder = input.getAttribute('placeholder') || '';
        return placeholder.includes('时间');
      });

      if (dateInput instanceof HTMLInputElement && timeInput instanceof HTMLInputElement) {
        setInputValue(dateInput, publishAt.date);
        await new Promise((resolve) => setTimeout(resolve, 120));
        setInputValue(timeInput, publishAt.time);
      } else {
        const singleInput = inputs.find((input) => {
          const placeholder = input.getAttribute('placeholder') || '';
          return (
            placeholder.includes('发布时间') ||
            placeholder.includes('定时') ||
            placeholder.includes('时间')
          );
        });

        if (!(singleInput instanceof HTMLInputElement)) {
          throw new Error('未找到定时发布时间输入框');
        }
        setInputValue(singleInput, scheduleValue);
      }
    }

    await new Promise((resolve) => setTimeout(resolve, 350));
    const dialogRoot = document.querySelector('.d-modal,[role="dialog"],.modal,.dialog,.d-popover');
    const confirmCandidates = Array.from(
      (dialogRoot || scheduleContainer).querySelectorAll('button')
    ).filter(
      (button): button is HTMLButtonElement =>
        button instanceof HTMLButtonElement && isElementVisible(button) && isElementEnabled(button)
    );
    if (dialogRoot) {
      const confirmButton = matchButtonByText(confirmCandidates, [
        '确认定时发布',
        '定时发布',
        '确定',
        '确认',
      ]);
      if (confirmButton) {
        clickWithEvidence('step4.confirm_schedule', '确认定时发布时间', confirmButton);
        await new Promise((resolve) => setTimeout(resolve, 500));
      } else {
        const candidates = confirmCandidates
          .map((button) => normalizeText(button.textContent || button.getAttribute('aria-label') || ''))
          .filter(Boolean)
          .join('|');
        pushLog('step4.confirm_schedule.skip', `未命中确认按钮 candidates=[${candidates}]`);
      }
    }
  };

  // 注入发布按钮 click 事件监听器（try-catch 包裹，silent fail，不干扰原有功能）。
  //
  // 注意：M1.1 Fix-T4 §1 之前这里发出的 DUMAS_ASYNC_EVENT 期望由
  // `dumas-event-relay.content.ts` 中继到 Background；该 content script 在当前
  // 代码库里**未实现**，相关代码处于 V0.5 待办。Fix-T4 之后真正的"等点击 + 等
  // 发布完成"由 background 端 `waitForPublishCompletion(tabId)` 通过监听
  // chrome.tabs.onUpdated 完成；此处保留 postMessage 仅作为未来 relay 落地后的
  // 观测埋点，**不再是发布完成判定的关键路径**。
  const injectPublishButtonListener = (button: HTMLElement, isScheduled: boolean) => {
    try {
      button.addEventListener(
        'click',
        () => {
          try {
            const publishJobId = (params as any).__publishJobId || '';
            const ts = Date.now();
            window.postMessage(
              {
                type: 'DUMAS_ASYNC_EVENT',
                event: {
                  eventId: `pub_${publishJobId}_${ts}`,
                  taskId: publishJobId,
                  seq: 0,
                  eventType: 'publish_clicked',
                  payload: {
                    stage: 'publish_clicked',
                    publishJobId,
                    toolCallRequestId: (params as any).__toolCallRequestId || '',
                    title: String(params.title || '').slice(0, 100),
                    isScheduled,
                    timestamp: ts,
                  },
                  timestamp: ts,
                  needAck: true,
                },
              },
              '*'
            );
          } catch (_e) {
            // silent fail — 永远不干扰发布按钮原有功能
          }
        },
        { once: true }
      );
      pushLog(
        'listener.injected',
        `isScheduled=${isScheduled}, btn=${(button.textContent || '').trim().slice(0, 20)}`
      );
    } catch (e) {
      pushLog('listener.inject_failed', String(e));
    }
  };

  // 提交发布（填完即止模式：填写内容 + 注入按钮监听，不自动点击发布）
  const submitPublish = async (
    title: string,
    content: string,
    tags: string[],
    publishAtRaw?: string
  ) => {
    console.log('[submitPublish] Starting submit');
    pushLog('step3.start', '填写标题、内容、标签');

    // 输入标题
    const titleInput = document.querySelector('div.d-input input') as HTMLInputElement;
    if (!titleInput) {
      throw new Error('未找到标题输入框');
    }
    titleInput.value = title;
    titleInput.dispatchEvent(new Event('input', { bubbles: true }));

    await new Promise((resolve) => setTimeout(resolve, 1000));

    // 输入内容
    const contentElem = await getContentElement();

    // 确保元素有焦点
    if (contentElem instanceof HTMLElement) {
      contentElem.focus();
    }

    // 根据元素类型输入内容
    if (contentElem.getAttribute('contenteditable')) {
      // 对于contenteditable元素，使用execCommand
      // 先清空内容
      const selection = window.getSelection();
      if (selection) {
        const range = document.createRange();
        range.selectNodeContents(contentElem);
        selection.removeAllRanges();
        selection.addRange(range);
      }
      // 使用execCommand插入内容
      document.execCommand('insertText', false, content);
    } else if (contentElem.tagName === 'INPUT' || contentElem.tagName === 'TEXTAREA') {
      const inputElem = contentElem as HTMLInputElement;
      inputElem.value = content;
      inputElem.dispatchEvent(
        new InputEvent('input', {
          bubbles: true,
        })
      );
    }

    // 输入标签
    await inputTags(contentElem, tags);
    pushLog('step3.done', `tags=${tags.length}`);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const publishAt = normalizePublishAt(publishAtRaw);
    if (publishAt) {
      console.log('[submitPublish] 开始配置定时发布:', publishAt.display);
      pushLog('step4.start', `配置定时发布 ${publishAt.display}`);
      await setupScheduledPublish(publishAt);

      pushLog('step4.wait', '等待定时发布按钮可点击');
      const scheduleBtn = await waitForBottomPublishButtonClickable('schedule', 25000);
      injectPublishButtonListener(scheduleBtn, true);
      pushLog('step4.done', '定时发布按钮监听已注入');

      return {
        message: `定时发布已设置（${publishAt.display}），请确认内容无误后手动点击「定时发布」按钮`,
        isScheduled: true,
        publishAt: publishAt.display,
        manualPublishPending: true,
      };
    }

    // 填完即止：注入发布按钮监听器，等待用户手动确认发布
    console.log('[submitPublish] 查找发布按钮并注入监听');
    pushLog('step4.start', '注入发布按钮监听');
    const publishBtn = await waitForBottomPublishButtonClickable('publish', 25000);
    injectPublishButtonListener(publishBtn, false);
    pushLog('step4.done', '发布按钮监听已注入');

    return {
      message: '图文内容已填写完成，请确认内容无误后手动点击「发布」按钮',
      isScheduled: false,
      manualPublishPending: true,
    };
  };

  // 辅助函数：从资源创建文件
  const createFileFromResource = async (resource: any, index: number) => {
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
      // 服务端应该已经将 URL 转换为 data URL，如果还是 URL 类型说明有问题
      throw new Error('图片资源类型错误：服务端应该已经预处理 URL 图片');
    }

    throw new Error(`不支持的图片资源类型: ${resource.type}`);
  };

  // 主执行逻辑
  let executionResult: PublishScriptResult;

  try {
    console.log('[PublishContent] Current URL:', window.location.href);
    console.log('[PublishContent] Page title:', document.title);

    // 步骤1: 点击上传图文标签页
    await clickUploadTab();

    // 步骤2: 上传图片
    await uploadImages(params.images);
    console.log('[PublishContent] Images uploaded');

    // 步骤3: 填写内容并注入发布按钮监听
    const submitResult = await submitPublish(
      params.title,
      params.content,
      params.tags || [],
      params.publish_at
    );
    console.log('[PublishContent] Content filled, manual publish pending');

    executionResult = {
      success: true,
      message: submitResult.message,
      isScheduled: submitResult.isScheduled,
      publishAt: submitResult.publishAt,
      manualPublishPending: submitResult.manualPublishPending,
      debugLogs,
    };
  } catch (error) {
    console.error('[PublishContent] Execution error:', error);
    const errorMessage = error instanceof Error ? error.message : '发布失败';
    pushLog('failed', errorMessage);
    executionResult = {
      success: false,
      message: errorMessage,
      debugLogs,
    };
  }

  console.log('[PublishContent] Returning result:', executionResult);
  return executionResult;
};

export class PublishContentTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.PUBLISH_CONTENT;

  async execute(args: PublishContentArgs): Promise<ToolResult> {
    const toolLogs: PublishDebugLog[] = [];
    const pushToolLog = (step: string, detail?: string) => {
      if (toolLogs.length >= 80) {
        toolLogs.shift();
      }
      toolLogs.push({
        ts: new Date().toISOString(),
        step,
        detail,
      });
    };
    let scriptLogs: PublishDebugLog[] = [];

    try {
      pushToolLog('tool.start', 'xhs_publish_content');
      // 发布参数由后端统一校验，插件端直接执行页面流程。
      const safeTitle = typeof args.title === 'string' ? args.title : '';
      const safeContent = typeof args.content === 'string' ? args.content : '';
      const safeImages = Array.isArray(args.images) ? args.images : [];
      const safeTags = Array.isArray(args.tags) ? args.tags : [];
      const tabSession = await this.captureTabSessionSnapshot();
      let publishTabId: number | undefined;

      try {
        // 导航到发布页面（与 Go 版本保持一致），发布场景强制使用独立标签页
        const tab = await this.createIsolatedXHSTab(`${XIAOHONGSHU_URLS.PUBLISH}?source=official`);

        if (!tab.id) {
          throw new Error('Failed to create tab');
        }
        publishTabId = tab.id;
        pushToolLog('tool.tab_created', `tabId=${tab.id}`);

        const publishPageProbe = await this.waitForXHSPublishPageReady(tab.id, {
          requiredCreatorTabs: ['上传图文'],
          retryIntervalsMs: [2000, 8000, 16000],
        });
        console.log('[PublishContent] Publish page ready:', publishPageProbe);
        pushToolLog(
          'tool.page_ready',
          `url=${publishPageProbe.currentUrl}, tabs=${publishPageProbe.visibleCreatorTabs.join('|')}`
        );

        // 执行发布脚本
        let result: PublishScriptResult | null = null;

        try {
          // 发布任务需要更长的超时时间：120秒
          result = await runInMainWorld<PublishScriptResult>(
            tab.id,
            publishContentExecutor,
            args,
            120000
          );
          scriptLogs = Array.isArray(result?.debugLogs) ? result.debugLogs : [];
          pushToolLog('tool.script_done', `success=${Boolean(result?.success)}`);
        } catch (executeError) {
          console.error('[PublishContent] publish script error:', executeError);
          const errorMessage =
            executeError instanceof Error ? executeError.message : String(executeError);
          pushToolLog('tool.script_error', errorMessage);
          if (
            errorMessage.includes('Cannot access') ||
            errorMessage.includes('chrome-extension://')
          ) {
            throw new Error('无法在小红书页面执行脚本，请检查是否已登录并在正确的页面');
          }
          throw new Error(`脚本执行失败: ${errorMessage}`);
        }

        // 验证结果
        console.log('[PublishContent] publish script result:', result);

        if (!result || typeof result !== 'object') {
          throw new Error(`Invalid result from publish script: ${JSON.stringify(result)}`);
        }
        if (!result.success) {
          throw new Error(result.message || 'Unknown error in page script');
        }

        // M1.1 Fix-T4 §1: 表单填好之后才进入"等真发布完成"阶段。
        // runInMainWorld 已经返回 → publish 按钮 listener 已注入。
        // 这里在 background SW 长期挂监听等用户真点击 + 后端跳转。
        pushToolLog('tool.wait_publish.start', `tabId=${tab.id}`);
        const completion = await waitForPublishCompletion(tab.id, {
          timeoutMs: DEFAULT_PUBLISH_WAIT_TIMEOUT_MS,
        });
        pushToolLog(
          'tool.wait_publish.done',
          `url=${completion.url}, note_id=${completion.note_id ?? 'null'}`
        );

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                message: result.message,
                title: safeTitle,
                contentLength: safeContent.length,
                imageCount: safeImages.length,
                tagCount: safeTags.length,
                isScheduled: Boolean(result.isScheduled),
                publishAt: result.publishAt || null,
                manualPublishPending: false,
                url: completion.url,
                note_id: completion.note_id,
                toolLogs,
                scriptLogs,
              }),
            },
          ],
          isError: false,
        };
      } finally {
        // 填完即止：Tab 保留不关闭，仅恢复之前的活动标签页
        await this.cleanupToolTabSession(publishTabId, tabSession, {
          closeIfCreated: false,
          restoreActiveTab: true,
        });
      }
    } catch (error) {
      console.error('PublishContent error:', error);
      const errorMessage = error instanceof Error ? error.message : '未知错误';
      // 透传 publish_canceled / publish_timeout 等结构化 code 给 daemon callback。
      const errorCode =
        typeof (error as any)?.code === 'string' ? ((error as any).code as string) : 'publish_failed';
      pushToolLog('tool.failed', `${errorCode}: ${errorMessage}`);
      return {
        content: [
          {
            type: 'error',
            text: JSON.stringify({
              success: false,
              code: errorCode,
              message: errorMessage,
              debug: {
                toolLogs,
                scriptLogs: scriptLogs.slice(-80),
              },
            }),
          },
        ],
        isError: true,
      };
    }
  }
}
