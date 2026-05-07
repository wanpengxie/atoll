import {
  XIAOHONGSHU_TOOL_NAMES,
  ToolResult,
  PublishLongContentArgs,
  XIAOHONGSHU_URLS,
} from 'xiaohongshu-mcp-shared';
import { BaseTool } from './base-tool';
import { runInMainWorld } from './inject-script';

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

// 固定长文模式发布流程：
// 写长文 -> 新的创作 -> 一键排版 -> 下一步 -> 发布页 ->（可选）定时发布
const publishLongContentExecutor = async (
  args: Record<string, any>
): Promise<PublishScriptResult> => {
  const title = typeof args.title === 'string' ? args.title.trim() : '';
  const content = typeof args.content === 'string' ? args.content : '';
  const description = typeof args.description === 'string' ? args.description.trim() : '';
  const publishAtRaw = typeof args.publish_at === 'string' ? args.publish_at.trim() : '';
  const debugLogs: PublishDebugLog[] = [];
  const pushLog = (step: string, detail?: string) => {
    if (debugLogs.length >= 120) {
      debugLogs.shift();
    }
    debugLogs.push({
      ts: new Date().toISOString(),
      step,
      detail,
    });
  };

  const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  const isVisible = (el: Element | null): el is HTMLElement => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    const rect = el.getBoundingClientRect();
    if (style.display === 'none' || style.visibility === 'hidden') return false;
    if (rect.width <= 0 || rect.height <= 0) return false;
    if (rect.left < -9000 || rect.top < -9000) return false;
    return true;
  };

  const waitFor = async <T>(
    label: string,
    finder: () => T | null | false,
    timeout = 20000,
    interval = 120
  ): Promise<T> => {
    pushLog('wait.start', `${label}, timeout=${timeout}`);
    const start = Date.now();
    while (Date.now() - start < timeout) {
      const target = finder();
      if (target) {
        pushLog('wait.ok', label);
        return target;
      }
      await sleep(interval);
    }
    pushLog('wait.timeout', label);
    throw new Error(`等待页面元素超时(${label})`);
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

  const findVisibleButton = (text: string): HTMLButtonElement | null => {
    const buttons = Array.from(document.querySelectorAll('button'));
    for (const btn of buttons) {
      if (!isVisible(btn)) continue;
      if (btn.disabled || btn.getAttribute('aria-disabled') === 'true') continue;
      if ((btn.textContent || '').trim() === text) {
        return btn as HTMLButtonElement;
      }
    }
    return null;
  };
  const findVisibleButtonAnyState = (text: string): HTMLButtonElement | null => {
    const buttons = Array.from(document.querySelectorAll('button'));
    for (const btn of buttons) {
      if (!isVisible(btn)) continue;
      if ((btn.textContent || '').trim() === text) {
        return btn as HTMLButtonElement;
      }
    }
    return null;
  };
  const findVisibleNextButton = (): HTMLButtonElement | null =>
    findVisibleButton('下一步') || findVisibleButton('去发布');
  const findVisibleNextButtonAnyState = (): HTMLButtonElement | null =>
    findVisibleButtonAnyState('下一步') || findVisibleButtonAnyState('去发布');
  const nextButtonStateSummary = () => {
    const btn = findVisibleNextButtonAnyState();
    if (!btn) return 'next_button=not_found';
    return `next_button_text=${(btn.textContent || '').trim()},disabled=${btn.disabled},aria_disabled=${btn.getAttribute('aria-disabled') || 'null'}`;
  };

  const findVisibleCreatorTab = (text: string): HTMLElement | null => {
    const tabs = Array.from(document.querySelectorAll('div.creator-tab'));
    for (const tab of tabs) {
      if (!isVisible(tab)) continue;
      if ((tab.textContent || '').trim() === text) {
        return tab as HTMLElement;
      }
    }
    return null;
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
    if (!isVisible(element)) return 'not_visible';
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
    const normalizedTargets = texts.map((text) => normalizeText(text));
    const root = options?.root || document;
    const candidates = root.querySelectorAll('button, [role="button"], label, span, div, a');
    let fuzzyMatch: HTMLElement | null = null;

    for (const node of candidates) {
      if (!(node instanceof HTMLElement)) continue;
      if (!isVisible(node) || !isElementEnabled(node)) continue;

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
      (node): node is HTMLElement => node instanceof HTMLElement && isVisible(node)
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
      (node): node is HTMLElement => node instanceof HTMLElement && isVisible(node)
    );

    const textTargets =
      mode === 'schedule'
        ? ['定时发布', '确认定时发布']
        : ['发布', '立即发布', '确认发布', '发布笔记'];

    for (const container of containers) {
      const buttons = Array.from(container.querySelectorAll('button')).filter(
        (button): button is HTMLButtonElement =>
          button instanceof HTMLButtonElement && isVisible(button) && isElementEnabled(button)
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
      await sleep(300);
    }

    const label = mode === 'schedule' ? '定时发布按钮' : '发布按钮';
    throw new Error(`${label}不可点击(${lastReason})`);
  };

  const setInputValue = (
    input: HTMLInputElement | HTMLTextAreaElement,
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

  /**
   * 向 ProseMirror/tiptap 编辑器写入文本。
   * 三层策略确保内容正确同步到 ProseMirror 内部状态：
   * 1. DataTransfer paste — ProseMirror 原生 handlePaste 处理
   * 2. execCommand 逐行输入 — ProseMirror 拦截 beforeinput
   * 3. innerHTML 回退 — 兜底方案
   */
  const setEditorText = (editor: HTMLElement, text: string) => {
    const normalized = text.replace(/\r\n/g, '\n');
    const lines = normalized.split('\n');

    editor.focus();

    // 检查内容是否成功写入（允许一定容差）
    const verifyContent = () => {
      const currentText = (editor.textContent || '').replace(/\s+/g, '');
      const expectedText = normalized.replace(/\s+/g, '');
      return currentText.length > 0 && currentText.length >= expectedText.length * 0.5;
    };

    // 清空编辑器的辅助方法
    const clearEditor = () => {
      const sel = window.getSelection();
      if (sel) {
        sel.selectAllChildren(editor);
        if (sel.rangeCount > 0) {
          sel.deleteFromDocument();
        }
      }
    };

    // HTML 转义
    const escapeHtml = (s: string) =>
      s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

    // ── Strategy 1: DataTransfer paste ──
    // ProseMirror 原生处理 paste 事件，内部状态会正确同步
    try {
      clearEditor();

      const htmlContent = lines
        .map((line) => (line.length > 0 ? `<p>${escapeHtml(line)}</p>` : '<p><br></p>'))
        .join('');

      const dt = new DataTransfer();
      dt.setData('text/plain', normalized);
      dt.setData('text/html', htmlContent);

      const pasteEvent = new ClipboardEvent('paste', {
        bubbles: true,
        cancelable: true,
        clipboardData: dt,
      });

      editor.dispatchEvent(pasteEvent);

      // ProseMirror 同步处理 paste，立即验证
      if (verifyContent()) {
        pushLog('setEditorText.paste_ok', `len=${normalized.length}`);
        return;
      }
      pushLog('setEditorText.paste_no_effect', 'trying execCommand fallback');
    } catch (e) {
      pushLog('setEditorText.paste_error', String(e));
    }

    // ── Strategy 2: execCommand 逐行输入 ──
    // ProseMirror 拦截 beforeinput 事件，状态也能正确同步
    try {
      editor.focus();
      document.execCommand('selectAll');
      document.execCommand('delete');

      for (let i = 0; i < lines.length; i++) {
        if (i > 0) {
          document.execCommand('insertParagraph', false);
        }
        if (lines[i].length > 0) {
          document.execCommand('insertText', false, lines[i]);
        }
      }

      if (verifyContent()) {
        pushLog('setEditorText.execCommand_ok', `len=${normalized.length}`);
        return;
      }
      pushLog('setEditorText.execCommand_no_effect', 'trying innerHTML fallback');
    } catch (e) {
      pushLog('setEditorText.execCommand_error', String(e));
    }

    // ── Strategy 3: innerHTML 回退 ──
    // ProseMirror 可能无法感知此变更，但至少内容会显示在页面上
    editor.innerHTML = '';
    if (lines.length === 0) {
      const p = document.createElement('p');
      p.appendChild(document.createElement('br'));
      editor.appendChild(p);
    } else {
      for (const line of lines) {
        const p = document.createElement('p');
        if (line.length === 0) {
          p.appendChild(document.createElement('br'));
        } else {
          p.textContent = line;
        }
        editor.appendChild(p);
      }
    }

    const inputEvent =
      typeof InputEvent === 'function'
        ? new InputEvent('input', {
            bubbles: true,
            inputType: 'insertText',
            data: normalized.length > 0 ? normalized.slice(-1) : null,
          })
        : new Event('input', { bubbles: true });
    editor.dispatchEvent(inputEvent);
    editor.dispatchEvent(new Event('change', { bubbles: true }));
    pushLog('setEditorText.innerHTML_fallback', `len=${normalized.length}`);
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
        const visible = input instanceof HTMLElement ? isVisible(input) : false;
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
          input instanceof HTMLInputElement && input.type !== 'checkbox' && isVisible(input)
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
          if (!(target instanceof HTMLElement) || !isVisible(target)) {
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

        await sleep(waitMs);
      }

      // 某些页面会出现“开关已勾选但输入框未渲染”的状态，二次翻转兜底一次
      if (!hasScheduleInput()) {
        const clicked = clickToggleOnce();
        if (clicked) {
          await sleep(700);
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
        await sleep(200);
        return;
      }
      // d-datepicker 在当前页面是受控文本输入，避免 blur 触发组件自动回滚
      setInputValue(datePickerInput, scheduleValue, { triggerBlur: false });
      await sleep(200);
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
        await sleep(120);
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

    await sleep(350);
    const dialogRoot = document.querySelector('.d-modal,[role="dialog"],.modal,.dialog,.d-popover');
    const confirmCandidates = Array.from(
      (dialogRoot || scheduleContainer).querySelectorAll('button')
    ).filter(
      (button): button is HTMLButtonElement =>
        button instanceof HTMLButtonElement && isVisible(button) && isElementEnabled(button)
    );
    if (dialogRoot) {
      const confirmButton = matchButtonByText(confirmCandidates, [
        '确认定时发布',
        '定时发布',
        '确定',
        '确认',
      ]);
      if (confirmButton) {
        clickWithEvidence('schedule.confirm', '确认定时发布时间', confirmButton);
        await sleep(500);
      } else {
        const candidates = confirmCandidates
          .map((button) => normalizeText(button.textContent || button.getAttribute('aria-label') || ''))
          .filter(Boolean)
          .join('|');
        pushLog('schedule.confirm.skip', `未命中确认按钮 candidates=[${candidates}]`);
      }
    }
  };

  const findDraftTitleInput = (): HTMLTextAreaElement | null => {
    const selectors = ['textarea[placeholder="输入标题"]', 'textarea[placeholder*="标题"]'];
    for (const selector of selectors) {
      const target = document.querySelector(selector);
      if (target instanceof HTMLTextAreaElement && isVisible(target)) {
        return target;
      }
    }
    return null;
  };

  const findDraftEditor = (): HTMLElement | null => {
    const selectors = [
      'div.tiptap.ProseMirror[contenteditable="true"]',
      '.ProseMirror[contenteditable="true"]',
      '[contenteditable="true"].tiptap',
      '[contenteditable="true"][role="textbox"]',
    ];
    for (const selector of selectors) {
      const target = document.querySelector(selector);
      if (target instanceof HTMLElement && isVisible(target)) {
        return target;
      }
    }
    return null;
  };

  const hasBottomPublishButton = () =>
    Boolean(findBottomPublishButton('publish') || findBottomPublishButton('schedule'));
  // 注意：只能用底部发布容器判断“最终发布页”，不能全局文本匹配“发布”，否则会被侧边栏“发布笔记”误判。
  const hasPublishButton = () => hasBottomPublishButton();
  const hasNextButton = () => Boolean(findVisibleNextButton());
  const hasDraftEditor = () => Boolean(findDraftTitleInput() && findDraftEditor());
  const hasNewCreateEntry = () =>
    Boolean(
      findVisibleButton('新的创作') ||
        findVisibleActionButton(['新的创作', '开始创作', '新建长文', '创建长文'])
    );
  // 最终发布页判定只看底部发布容器，避免把发布页的正文/描述编辑器误判为“仍在草稿态”。
  const isFinalPublishStage = () => Boolean(hasBottomPublishButton());
  const flowStateSummary = () =>
    JSON.stringify({
      longTabActive: Boolean(findVisibleCreatorTab('写长文')?.classList.contains('active')),
      hasNewCreateEntry: hasNewCreateEntry(),
      hasDraftEditor: hasDraftEditor(),
      hasNextButton: hasNextButton(),
      hasBottomPublishButton: hasBottomPublishButton(),
      url: window.location.href,
    });

  // 注入发布按钮点击监听器，用户手动确认后通过 postMessage 通知
  // V2: 统一上报 DUMAS_ASYNC_EVENT，由 dumas-event-relay content script 中继到 Background
  const injectPublishButtonListener = (button: HTMLElement, isScheduled: boolean) => {
    try {
      button.addEventListener(
        'click',
        () => {
          try {
            const publishJobId = (args as any).__publishJobId || '';
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
                    toolCallRequestId: (args as any).__toolCallRequestId || '',
                    title: String(title || '').slice(0, 100),
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
            /* silent fail — 永远不干扰发布按钮原有功能 */
          }
        },
        { once: true }
      );
    } catch (e) {
      pushLog('listener.inject_failed', String(e));
    }
  };

  try {
    pushLog('init', `titleLen=${title.length}, contentLen=${content.trim().length}`);
    if (!title) {
      throw new Error('标题不能为空');
    }
    if (!content.trim()) {
      throw new Error('正文不能为空');
    }

    // 步骤 1: 固定切换到“写长文”模式
    pushLog('step1.start', '切换到写长文');
    const longTab = findVisibleCreatorTab('写长文');
    if (!longTab) {
      throw new Error('未找到写长文 Tab');
    }
    if (!longTab.classList.contains('active')) {
      longTab.click();
      await sleep(1000);
    }
    await waitFor(
      '写长文Tab激活',
      () => {
        const tab = findVisibleCreatorTab('写长文');
        return tab && tab.classList.contains('active') ? true : false;
      },
      12000
    );
    pushLog('step1.done', '写长文Tab已激活');

    // 步骤 2: 进入“新的创作”（固定流程，不跳步骤）
    pushLog('step2.start', '进入新的创作');
    const newCreateOrDraftReady = (await waitFor(
      '新的创作入口或草稿编辑器',
      () =>
        findVisibleButton('新的创作') ||
        findVisibleActionButton(['新的创作', '开始创作', '新建长文', '创建长文']) ||
        (hasDraftEditor() ? 'draft_ready' : null),
      15000
    )) as HTMLElement | 'draft_ready';
    if (newCreateOrDraftReady !== 'draft_ready') {
      clickWithEvidence('step2.click', '点击新的创作', newCreateOrDraftReady);
    } else {
      pushLog('step2.skip_click', '已在草稿编辑态，跳过点击新的创作');
    }
    await waitFor('长文草稿编辑器出现', () => (hasDraftEditor() ? true : false), 25000);
    pushLog('step2.done', '长文草稿编辑器可用');

    // 步骤 3: 填写长文标题和正文
    pushLog('step3.start', '填写标题和正文');
    const draftTitleInput = findDraftTitleInput();
    if (!draftTitleInput) {
      throw new Error('未找到长文标题输入框（placeholder 包含“标题”）');
    }
    setInputValue(draftTitleInput, title);

    const draftEditor = findDraftEditor();
    if (!draftEditor) {
      throw new Error('未找到长文正文编辑器（contenteditable）');
    }
    setEditorText(draftEditor, content);
    await sleep(500);
    pushLog('step3.done', '标题和正文填写完成');

    // 步骤 4: 点击“一键排版”（固定流程，不跳步骤）
    pushLog('step4.start', '点击一键排版');
    const layoutBtn = await waitFor(
      '一键排版按钮',
      () => findVisibleButton('一键排版') || findVisibleActionButton(['一键排版', '智能排版']),
      15000
    );
    clickWithEvidence('step4.click', '点击一键排版', layoutBtn);
    await waitFor('一键排版后下一步按钮出现', () => (hasNextButton() ? true : false), 40000);
    pushLog('step4.done', '一键排版完成');

    // 步骤 5: 点击“下一步”进入发布设置页（固定流程，不跳步骤）
    pushLog('step5.start', '点击下一步进入发布设置页');
    await waitFor('下一步按钮出现', () => findVisibleNextButtonAnyState(), 15000);
    let nextBtn: HTMLButtonElement;
    try {
      nextBtn = await waitFor('下一步按钮可点击', () => findVisibleNextButton(), 30000);
    } catch (_err) {
      throw new Error(`下一步按钮未就绪(${nextButtonStateSummary()}; ${flowStateSummary()})`);
    }
    clickWithEvidence('step5.click', '点击下一步', nextBtn);
    await waitFor(
      '进入最终发布阶段(底部发布容器出现)',
      () => (isFinalPublishStage() ? true : false),
      30000
    );
    pushLog('step5.done', '进入发布设置页');

    if (!isFinalPublishStage()) {
      throw new Error(`未进入最终发布阶段 (${flowStateSummary()})`);
    }

    // 步骤 6: 在最终发布页补充标题和描述（可选）
    pushLog('step6.start', '补充发布页信息');
    const finalTitleInput = document.querySelector(
      'input[placeholder*="标题"]'
    ) as HTMLInputElement | null;
    if (finalTitleInput) {
      setInputValue(finalTitleInput, title);
    }

    if (description) {
      const finalDescEditor = (await waitFor(
        '发布页描述输入框(可选)',
        () => {
          const target = document.querySelector(
            '.editor-content .tiptap.ProseMirror[role="textbox"][contenteditable="true"], .editor-content .tiptap.ProseMirror[contenteditable="true"]'
          );
          return target instanceof HTMLElement && isVisible(target) ? target : null;
        },
        5000
      ).catch(() => null)) as HTMLElement | null;
      if (finalDescEditor) {
        setEditorText(finalDescEditor, description);
      } else {
        pushLog('step6.desc.skip', '未找到描述输入框，跳过 description 填写');
      }
    }
    pushLog('step6.done', '发布页信息准备完成');
    pushLog('step6.wait', '等待5秒，确保发布按钮状态稳定');
    await sleep(5000);

    const publishAt = normalizePublishAt(publishAtRaw);
    if (publishAt) {
      pushLog('schedule.start', publishAt.display);
      await setupScheduledPublish(publishAt);
      const scheduleButton = await waitForBottomPublishButtonClickable('schedule', 25000);
      // 不自动点击，注入监听器等待用户手动确认
      injectPublishButtonListener(scheduleButton, true);
      pushLog('schedule.listener_injected', publishAt.display);

      return {
        success: true,
        message: `定时发布已设置（${publishAt.display}），请手动点击"定时发布"按钮确认`,
        isScheduled: true,
        publishAt: publishAt.display,
        manualPublishPending: true,
        debugLogs,
      };
    }

    // 非定时发布：注入监听器，等待用户手动点击
    const publishButton = await waitForBottomPublishButtonClickable('publish', 25000);
    injectPublishButtonListener(publishButton, false);
    pushLog('publish.listener_injected', '发布按钮监听器已注入');

    return {
      success: true,
      message: '长文内容已填写完成，请手动点击发布按钮',
      isScheduled: false,
      manualPublishPending: true,
      debugLogs,
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : '发布失败';
    pushLog('failed', message);
    return {
      success: false,
      message,
      isScheduled: false,
      debugLogs,
    };
  }
};

export class PublishLongContentTool extends BaseTool {
  name = XIAOHONGSHU_TOOL_NAMES.PUBLISH_LONG_CONTENT;

  async execute(args: PublishLongContentArgs): Promise<ToolResult> {
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
      pushToolLog('tool.start', 'xhs_publish_long_content');
      // 发布参数由后端统一校验，插件端直接执行页面流程。
      const safeTitle = typeof args.title === 'string' ? args.title : '';
      const safeContent = typeof args.content === 'string' ? args.content : '';
      const safeDescription = typeof args.description === 'string' ? args.description : '';
      const tabSession = await this.captureTabSessionSnapshot();
      let publishTabId: number | undefined;

      try {
        const tab = await this.createIsolatedXHSTab(`${XIAOHONGSHU_URLS.PUBLISH}?source=official`);
        if (!tab.id) {
          throw new Error('创建发布页面失败');
        }
        publishTabId = tab.id;
        pushToolLog('tool.tab_created', `tabId=${tab.id}`);

        const publishPageProbe = await this.waitForXHSPublishPageReady(tab.id, {
          requiredCreatorTabs: ['写长文'],
          retryIntervalsMs: [2000, 8000, 16000],
        });
        console.log('[PublishLongContent] Publish page ready:', publishPageProbe);
        pushToolLog(
          'tool.page_ready',
          `url=${publishPageProbe.currentUrl}, tabs=${publishPageProbe.visibleCreatorTabs.join('|')}`
        );

        const result = await runInMainWorld<PublishScriptResult>(
          tab.id,
          publishLongContentExecutor,
          {
            title: args.title,
            content: args.content,
            description: args.description || '',
            publish_at: args.publish_at || '',
            __toolCallRequestId: (args as any).__toolCallRequestId || '',
          },
          180000
        );
        scriptLogs = Array.isArray(result?.debugLogs) ? result.debugLogs : [];
        pushToolLog('tool.script_done', `success=${Boolean(result?.success)}`);

        if (!result || typeof result !== 'object') {
          throw new Error(`发布脚本返回结果异常: ${JSON.stringify(result)}`);
        }
        if (!result.success) {
          throw new Error(result.message || '发布失败');
        }

        // V2: 页面脚本已直接发送 DUMAS_ASYNC_EVENT，由常驻 dumas-event-relay.content.ts 中继，
        // 无需再注入 ISOLATED world relay 脚本。

        return {
          content: [
            {
              type: 'text',
              text: JSON.stringify({
                success: true,
                message: result.message,
                title: safeTitle,
                contentLength: safeContent.length,
                hasDescription: Boolean(safeDescription.trim()),
                isScheduled: Boolean(result.isScheduled),
                publishAt: result.publishAt || null,
                manualPublishPending: Boolean(result.manualPublishPending),
                toolLogs,
                scriptLogs,
              }),
            },
          ],
          isError: false,
        };
      } finally {
        await this.cleanupToolTabSession(publishTabId, tabSession, {
          closeIfCreated: false,
          restoreActiveTab: true,
        });
      }
    } catch (error) {
      console.error('[PublishLongContent] Error:', error);
      const errorMessage = error instanceof Error ? error.message : '未知错误';
      pushToolLog('tool.failed', errorMessage);
      return {
        content: [
          {
            type: 'error',
            text: `发布长文失败: ${errorMessage}\n调试日志: ${JSON.stringify({
              toolLogs,
              scriptLogs: scriptLogs.slice(-80),
            })}`,
          },
        ],
        isError: true,
      };
    }
  }
}
