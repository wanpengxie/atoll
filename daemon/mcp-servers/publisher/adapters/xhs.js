/**
 * XHS (小红书) publisher adapter.
 * Uses deterministic CDP operations — no AI vision required.
 */
import { formatTextWithTags } from '../text.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomInt(min, max) { return Math.floor(min + Math.random() * (max - min + 1)); }

const PACE_SCALE = Math.max(0.2, Number(process.env.PUBLISHER_PACE_SCALE ?? '1') || 1);
const DRY_RUN = process.env.XHS_PUBLISH_DRY_RUN !== '0';
async function humanPause(minMs, maxMs, label = '') {
  const ms = Math.round(randomInt(minMs, maxMs) * PACE_SCALE);
  if (label) console.error(`[XhsAdapter] pause ${label}: ${ms}ms`);
  await sleep(ms);
}

const REQUIREMENTS = {
  image_text: {
    max_text_length: 1000,
    max_images: 9,
    image_formats: ['jpg', 'jpeg', 'png', 'webp'],
    required_fields: ['text', 'images'],
    notes: '当前自动化支持上传图文，必须提供至少 1 张本地图片；标题建议 2-20 字；图片建议 3:4 竖版；话题标签写在正文末尾如 #话题名',
  },
  short_video: {
    max_text_length: 1000,
    max_images: 1,
    video_max_duration: 600,
    video_formats: ['mp4', 'mov'],
    required_fields: ['text', 'video'],
    notes: '封面图建议 3:4；视频时长不超过 10 分钟',
  },
};

const PUBLISH_URL = 'https://creator.xiaohongshu.com/publish/publish';
const IMAGE_PUBLISH_URL = 'https://creator.xiaohongshu.com/publish/publish?target=image';
const VIDEO_PUBLISH_URL = 'https://creator.xiaohongshu.com/publish/publish?source=video';

const CREATOR_HOME_URL = 'https://creator.xiaohongshu.com/new/home';
const IMAGE_FILE_INPUT_SELECTOR = 'input.upload-input[type="file"][accept*=".jpg"], input[type="file"][accept*=".png"], input[type="file"][accept*=".webp"]';
const VIDEO_FILE_INPUT_SELECTOR = 'input.upload-input[type="file"][accept*=".mp4"], input[type="file"][accept*=".mov"], input[type="file"][accept*=".mpeg"]';
const TITLE_SELECTOR = 'input[placeholder*="标题"], input.d-text[type="text"]';
const CONTENT_SELECTOR = '.ProseMirror[contenteditable="true"], [contenteditable="true"][role="textbox"], [contenteditable="true"]';

const UPLOAD_SELECTORS = [IMAGE_FILE_INPUT_SELECTOR, VIDEO_FILE_INPUT_SELECTOR, '.upload-input'];

const ERROR_SELECTORS = [
  '[class*="error"]',
  '[class*="fail"]',
  '[class*="tips"]',
  '[class*="toast"]',
  '[class*="Toast"]',
  '[class*="message"]',
  '[role="alert"]',
];

export class XhsAdapter {
  constructor(cdp) {
    this._cdp = cdp;
  }

  getRequirements(contentType) {
    return REQUIREMENTS[contentType] ?? null;
  }

  async checkLoginStatus() {
    const profileDir = process.env.XHS_PROFILE_DIR ?? '(not set)';

    await this._cdp.send('Page.navigate', { url: CREATOR_HOME_URL });
    await sleep(5000);

    const state = await this._inspectPage();
    const url = state.url;
    const loggedIn = state.isCreatorLoggedIn && !state.hasLoginHint;
    console.error(`[XhsAdapter] checkLoginStatus: loggedIn=${loggedIn} url=${url}`);
    return { loggedIn, url, profileDir, userId: null, nickname: null };
  }

  async publishImageText({ title, text, tags = [], images = [] }) {
    await this._openPublishComposer('image');
    await this._assertReadyForPublish();
    await humanPause(2500, 5500, 'composer-ready');

    await this._waitForAny([IMAGE_FILE_INPUT_SELECTOR], 15000);
    await humanPause(900, 2200, 'before-upload');

    // Upload images first (if any)
    if (images.length > 0) {
      await this._uploadFiles(images, 'image');
      await this._waitForUploadSettled(images.length, 90_000);
      await humanPause(2500, 5500, 'after-upload');
    }

    // Fill title
    if (title) {
      await this._fillField(TITLE_SELECTOR, title);
      await humanPause(1200, 2800, 'after-title');
    }

    // Fill content text and only append tags that are not already present.
    const fullText = formatTextWithTags(text, tags);
    await this._fillField(CONTENT_SELECTOR, fullText);

    await humanPause(4500, 9000, 'before-publish-check');
    await this._assertNoBlockingErrors();
    await this._assertPublishButtonReady();

    if (DRY_RUN) {
      console.error('[XhsAdapter] XHS_PUBLISH_DRY_RUN enabled; skipping final publish click.');
      return { success: true, dry_run: true, post_url: null };
    }

    await humanPause(1200, 3000, 'before-publish-click');
    const clicked = await this._clickByText('发布');
    if (!clicked) throw new Error('PUBLISH_FAILED: 找不到发布按钮，页面结构可能已变化');
    const result = await this._waitForPublishResult(90_000);
    return { success: true, post_url: result.postUrl || result.url };
  }

  async publishShortVideo({ title, text, tags = [], video, cover }) {
    await this._openPublishComposer('video');
    await this._assertReadyForPublish();
    await humanPause(2500, 5500, 'composer-ready');

    await this._waitForAny([VIDEO_FILE_INPUT_SELECTOR], 15000);
    await humanPause(900, 2200, 'before-upload');

    // Upload video
    await this._uploadFiles([video], 'video');
    await this._waitForUploadSettled(1, 180_000);
    await humanPause(3000, 6500, 'after-video-upload');

    if (cover) {
      await humanPause(1200, 2800, 'before-cover-upload');
      await this._uploadFiles([cover], 'cover');
      await this._waitForUploadSettled(1, 60_000);
      await humanPause(1800, 4200, 'after-cover-upload');
    }

    if (title) {
      await this._fillField(TITLE_SELECTOR, title);
      await humanPause(1200, 2800, 'after-title');
    }

    const fullText = formatTextWithTags(text, tags);
    await this._fillField(CONTENT_SELECTOR, fullText);

    await humanPause(4500, 9000, 'before-publish-check');
    await this._assertNoBlockingErrors();
    await this._assertPublishButtonReady();

    if (DRY_RUN) {
      console.error('[XhsAdapter] XHS_PUBLISH_DRY_RUN enabled; skipping final publish click.');
      return { success: true, dry_run: true, post_url: null };
    }

    await humanPause(1200, 3000, 'before-publish-click');
    const clicked = await this._clickByText('发布');
    if (!clicked) throw new Error('PUBLISH_FAILED: 找不到发布按钮，页面结构可能已变化');
    const result = await this._waitForPublishResult(120_000);
    return { success: true, post_url: result.postUrl || result.url };
  }

  // ── CDP helpers ─────────────────────────────────────────────────────────────

  async _openPublishComposer(kind) {
    const targetTabText = kind === 'image' ? '上传图文' : '上传视频';
    const inputSelector = kind === 'image' ? IMAGE_FILE_INPUT_SELECTOR : VIDEO_FILE_INPUT_SELECTOR;
    const url = kind === 'video' ? VIDEO_PUBLISH_URL : IMAGE_PUBLISH_URL;

    await this._cdp.send('Page.navigate', { url });
    await this._waitForCreatorShell(20_000);
    await this._assertReadyForPublish();
    await humanPause(1800, 4200, 'after-navigation');

    if (await this._hasSelector(inputSelector)) return;

    await humanPause(1000, 2600, 'before-tab-switch');
    const clicked = await this._clickVisibleByText(targetTabText, '.creator-tab, button, [role="button"], span, div', { throwOnMissing: false });
    if (clicked) {
      await this._waitForAny([inputSelector], 15_000);
      return;
    }

    await this._cdp.send('Page.navigate', { url: kind === 'image' ? `${PUBLISH_URL}?from=tab_switch&target=image` : VIDEO_PUBLISH_URL });
    await this._waitForAny([inputSelector], 15_000);
    await humanPause(1800, 4200, 'after-fallback-navigation');
  }

  async _waitForCreatorShell(timeoutMs = 20_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const state = await this._inspectPage();
      if (state.hasLoginHint) {
        throw new Error(`LOGIN_EXPIRED: 当前页面 ${state.url}，请重新扫码连接小红书`);
      }
      if (state.isCreatorLoggedIn && (state.hasUploader || state.hasPublishEditor || state.text.includes('发布笔记'))) return;
      await sleep(500);
    }
    const state = await this._inspectPage();
    throw new Error(`PUBLISH_FAILED: 小红书创作者后台未加载完成，当前页面 ${state.url}`);
  }

  async _waitForSelector(selector, timeoutMs = 8000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const result = await this._cdp.send('Runtime.evaluate', {
        expression: `!!document.querySelector(${JSON.stringify(selector)})`,
        returnByValue: true,
      });
      if (result.result?.value) return;
      await sleep(500);
    }
    throw new Error(`Timeout waiting for selector: ${selector}`);
  }

  async _waitForAny(selectors, timeoutMs = 15000) {
    const combined = selectors.join(', ');
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const result = await this._cdp.send('Runtime.evaluate', {
        expression: `!!document.querySelector(${JSON.stringify(combined)})`,
        returnByValue: true,
      });
      if (result.result?.value) return;
      await sleep(500);
    }
    throw new Error(`Timeout waiting for any of: ${combined}`);
  }

  async _hasSelector(selector) {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `!!document.querySelector(${JSON.stringify(selector)})`,
      returnByValue: true,
    });
    return result.result?.value === true;
  }

  async _waitForText(text, timeoutMs = 8000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const result = await this._cdp.send('Runtime.evaluate', {
        expression: `document.body?.innerText?.includes(${JSON.stringify(text)})`,
        returnByValue: true,
      });
      if (result.result?.value) return;
      await sleep(500);
    }
    throw new Error(`Timeout waiting for text: ${text}`);
  }

  async _inspectPage() {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const text = document.body?.innerText || '';
          const url = window.location.href;
          const visible = (el) => {
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width >= 0 && r.height >= 0 && r.left > -1000 && s.display !== 'none' && s.visibility !== 'hidden';
          };
          const errorSelectors = ${JSON.stringify(ERROR_SELECTORS)};
          const errors = [...document.querySelectorAll(errorSelectors.join(','))]
            .filter(visible)
            .map(e => e.innerText || e.textContent || '')
            .map(t => t.trim())
            .filter(Boolean)
            .slice(0, 5);
          const buttons = [...document.querySelectorAll('button, [role="button"]')].map(e => ({
            text: (e.innerText || e.textContent || '').trim(),
            disabled: !!e.disabled || e.getAttribute('aria-disabled') === 'true' || e.className?.toString().includes('disabled'),
          }));
          return {
            url,
            text: text.slice(0, 5000),
            errors,
            buttons,
            hasLoginHint: /扫码登录|二维码登录|验证码登录|手机号登录|登录后可/.test(text),
            isCreatorLoggedIn: /创作服务平台/.test(text) && /发布笔记|首页|笔记管理/.test(text),
            hasUploader: !!document.querySelector(${JSON.stringify(UPLOAD_SELECTORS.join(', '))}),
            hasPublishEditor: !!document.querySelector(${JSON.stringify(`${TITLE_SELECTOR}, ${CONTENT_SELECTOR}`)}) && /笔记预览|暂存离开|发布/.test(text),
            postUrl: document.querySelector('a[href*="/explore/"]')?.href || document.querySelector('[class*="success"] a')?.href || '',
          };
        })()
      `,
      returnByValue: true,
    });
    return result.result?.value ?? { url: await this._getUrl(), text: '', errors: [], buttons: [] };
  }

  _isPublishPage(url) {
    return url.includes('creator.xiaohongshu.com/publish/publish');
  }

  async _assertReadyForPublish() {
    const state = await this._inspectPage();
    if ((!this._isPublishPage(state.url) && !state.isCreatorLoggedIn) || state.hasLoginHint) {
      throw new Error(`LOGIN_EXPIRED: 当前页面 ${state.url}，请重新扫码连接小红书`);
    }
    if (!state.hasUploader && !state.hasPublishEditor) {
      await this._assertNoBlockingErrors();
    }
  }

  async _assertNoBlockingErrors() {
    const state = await this._inspectPage();
    const blocking = (state.errors ?? []).find(t =>
      /失败|错误|异常|不可|不能|未通过|风控|审核|违规|实名|认证|权限|请先|过期|登录/.test(t)
    );
    if (blocking) throw new Error(`PUBLISH_BLOCKED: ${blocking}`);
  }

  async _assertPublishButtonReady() {
    const state = await this._inspectPage();
    const button = (state.buttons ?? []).find(b => b.text === '发布' || b.text?.includes('发布'));
    if (!button) throw new Error('PUBLISH_FAILED: 找不到发布按钮，页面结构可能已变化');
    if (button.disabled) {
      const errors = (state.errors ?? []).join('；') || '发布按钮不可点击，可能仍在上传、必填项缺失或账号状态受限';
      throw new Error(`PUBLISH_BLOCKED: ${errors}`);
    }
  }

  async _waitForUploadSettled(expectedCount, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await sleep(1000);
      await this._assertNoBlockingErrors();
      const state = await this._inspectPage();
      const text = state.text ?? '';
      if (/上传失败|上传出错|格式不支持|文件过大/.test(text)) {
        throw new Error(`UPLOAD_FAILED: ${(state.errors ?? []).join('；') || '页面提示上传失败'}`);
      }
      if (/上传中|处理中|转码中|正在上传|解析中/.test(text)) continue;
      if (state.hasPublishEditor || /图片编辑|视频编辑|笔记预览|上传成功|重新上传|更换|已上传|封面/.test(text)) return;
      if (expectedCount === 0) return;
    }
    throw new Error(`UPLOAD_TIMEOUT: 等待小红书上传完成超时`);
  }

  async _waitForPublishResult(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    let lastState = null;
    while (Date.now() < deadline) {
      await sleep(1500);
      const state = await this._inspectPage();
      lastState = state;
      const text = state.text ?? '';
      if (state.postUrl) return { url: state.url, postUrl: state.postUrl };
      if (state.url.includes('success') || state.url.includes('works') || state.url.includes('note-manager')) {
        return { url: state.url, postUrl: state.postUrl || state.url };
      }
      const blocking = (state.errors ?? []).find(t => /失败|错误|异常|不可|不能|未通过|风控|审核|违规|实名|认证|权限/.test(t));
      if (blocking) throw new Error(`PUBLISH_FAILED: ${blocking}`);
      if (/发布成功|提交成功|审核中|已发布/.test(text)) {
        return { url: state.url, postUrl: state.postUrl || state.url };
      }
    }
    const hint = (lastState?.errors ?? []).join('；') || '没有检测到成功跳转或成功提示';
    throw new Error(`PUBLISH_TIMEOUT: ${hint}`);
  }

  async _fillField(selector, value) {
    await humanPause(500, 1400, 'before-focus-field');
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const el = document.querySelector(${JSON.stringify(selector)});
          if (!el) return false;
          el.focus();
          if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set
              || Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
            if (nativeInputValueSetter) nativeInputValueSetter.call(el, '');
            else el.value = '';
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.select?.();
          } else {
            const range = document.createRange();
            range.selectNodeContents(el);
            const selection = window.getSelection();
            selection.removeAllRanges();
            selection.addRange(range);
            document.execCommand?.('delete', false, null);
            el.focus();
            el.dispatchEvent(new InputEvent('input', { bubbles: true }));
          }
          return true;
        })()
      `,
      returnByValue: true,
    });
    if (result.result?.value !== true) throw new Error(`PUBLISH_FAILED: 找不到输入框：${selector}`);
    await this._typeTextHumanLike(value);
    await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const el = document.activeElement;
          if (!el) return false;
          el.dispatchEvent(new Event('change', { bubbles: true }));
          el.dispatchEvent(new Event('blur', { bubbles: true }));
          return true;
        })()
      `,
      returnByValue: true,
    });
    await humanPause(700, 1800, 'after-fill-field');
  }

  async _typeTextHumanLike(value) {
    const text = String(value ?? '');
    let i = 0;
    while (i < text.length) {
      const chunkSize = randomInt(10, 26);
      const chunk = text.slice(i, i + chunkSize);
      await this._cdp.send('Input.insertText', { text: chunk });
      i += chunk.length;
      await humanPause(90, 260);
    }
  }

  async _uploadFiles(filePaths, type = 'image') {
    await humanPause(600, 1700, 'before-set-files');
    const selector = type === 'video' ? VIDEO_FILE_INPUT_SELECTOR : IMAGE_FILE_INPUT_SELECTOR;
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `document.querySelector(${JSON.stringify(selector)})`,
      returnByValue: false,
    });
    if (!result.result?.objectId) throw new Error(`No ${type} file input found on page`);

    await this._cdp.send('DOM.setFileInputFiles', {
      objectId: result.result.objectId,
      files: filePaths,
    });
    await humanPause(1200, 2600, 'after-set-files');
  }

  async _clickVisibleByText(text, selector, { throwOnMissing = true } = {}) {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const visible = (el) => {
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width > 0 && r.height > 0 && r.left > -100 && s.display !== 'none' && s.visibility !== 'hidden';
          };
          const els = [...document.querySelectorAll(${JSON.stringify(selector)})].filter(visible);
          const targetText = ${JSON.stringify(text)};
          const el = els.find(e => {
            const t = (e.innerText || e.textContent || '').trim();
            return t === targetText || t.split(/\\s+/).includes(targetText) || t.includes(targetText);
          });
          const target = el?.closest('.creator-tab, button, [role="button"]') || el;
          if (target) { target.click(); return true; }
          return false;
        })()
      `,
      returnByValue: true,
    });
    const clicked = result.result?.value === true;
    if (!clicked && throwOnMissing) throw new Error(`PUBLISH_FAILED: 找不到小红书发布入口：${text}`);
    if (clicked) await humanPause(1500, 3500, `after-click-${text}`);
    return clicked;
  }

  async _clickByText(text) {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const els = [...document.querySelectorAll('button, [role="button"]')];
          const el = els.find(e => e.innerText?.trim() === ${JSON.stringify(text)} || e.textContent?.trim() === ${JSON.stringify(text)});
          if (!el) return null;
          const r = el.getBoundingClientRect();
          return { x: r.left + r.width / 2, y: r.top + r.height / 2 };
        })()
      `,
      returnByValue: true,
    });
    const point = result.result?.value;
    if (!point) return false;
    await humanPause(500, 1400, `before-click-${text}`);
    await this._cdp.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x: point.x, y: point.y });
    await humanPause(120, 420);
    await this._cdp.send('Input.dispatchMouseEvent', { type: 'mousePressed', x: point.x, y: point.y, button: 'left', clickCount: 1 });
    await humanPause(80, 220);
    await this._cdp.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x: point.x, y: point.y, button: 'left', clickCount: 1 });
    await humanPause(1500, 3500, `after-click-${text}`);
    return true;
  }

  async _getUrl() {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: 'window.location.href',
      returnByValue: true,
    });
    return result.result?.value ?? '';
  }
}
