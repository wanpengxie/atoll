/**
 * Douyin (抖音) publisher adapter.
 * Uses 抖音创作服务平台: https://creator.douyin.com
 */
import { formatTextWithTags } from '../text.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
function randomInt(min, max) { return Math.floor(min + Math.random() * (max - min + 1)); }

const PACE_SCALE = Math.max(0.2, Number(process.env.PUBLISHER_PACE_SCALE ?? '1') || 1);
const DRY_RUN = process.env.DOUYIN_PUBLISH_DRY_RUN !== '0';

async function humanPause(minMs, maxMs, label = '') {
  const ms = Math.round(randomInt(minMs, maxMs) * PACE_SCALE);
  if (label) console.error(`[DouyinAdapter] pause ${label}: ${ms}ms`);
  await sleep(ms);
}

const REQUIREMENTS = {
  image_text: {
    max_text_length: 2200,
    max_images: 35,
    image_formats: ['jpg', 'jpeg', 'png', 'webp'],
    required_fields: ['text', 'images'],
    notes: '图文模式最多 35 张图；正文最多 2200 字；默认停在最终发布前，需设置 DOUYIN_PUBLISH_DRY_RUN=0 才会点击发布',
  },
  short_video: {
    max_text_length: 2200,
    video_max_duration: 900,
    video_formats: ['mp4', 'mov', 'webm'],
    required_fields: ['text', 'video'],
    notes: '视频最长 15 分钟；建议 9:16 竖版；默认停在最终发布前，需设置 DOUYIN_PUBLISH_DRY_RUN=0 才会点击发布',
  },
};

const CREATOR_HOME_URL = 'https://creator.douyin.com/creator-micro/home';
const UPLOAD_URL = 'https://creator.douyin.com/creator-micro/content/upload';
const IMAGE_PUBLISH_URL = `${UPLOAD_URL}?default-tab=3&enter_from=publish`;
const VIDEO_PUBLISH_URL = `${UPLOAD_URL}?default-tab=1&enter_from=publish`;

const IMAGE_FILE_INPUT_SELECTOR = [
  'input[type="file"][accept*="image"]',
  'input[type="file"][accept*=".jpg"]',
  'input[type="file"][accept*=".jpeg"]',
  'input[type="file"][accept*=".png"]',
  'input[type="file"][accept*=".webp"]',
  'input[type="file"][accept="image"]',
  'input[type="file"]',
].join(', ');
const VIDEO_FILE_INPUT_SELECTOR = [
  'input[type="file"][accept*="video"]',
  'input[type="file"][accept*=".mp4"]',
  'input[type="file"][accept*=".mov"]',
  'input[type="file"][accept*=".webm"]',
  'input[type="file"]',
].join(', ');
const FILE_INPUT_SELECTOR = `${IMAGE_FILE_INPUT_SELECTOR}, ${VIDEO_FILE_INPUT_SELECTOR}`;
const TITLE_SELECTOR = [
  'input[placeholder*="标题"]',
  'textarea[placeholder*="标题"]',
  '[contenteditable="true"][data-placeholder*="标题"]',
  '[contenteditable="true"][aria-label*="标题"]',
].join(', ');
const CONTENT_SELECTOR = [
  'textarea[placeholder*="描述"]',
  'textarea[placeholder*="作品描述"]',
  '[placeholder*="添加作品描述"]',
  '[contenteditable="true"][data-placeholder*="描述"]',
  '[contenteditable="true"][aria-label*="描述"]',
  '.ProseMirror[contenteditable="true"]',
  '[contenteditable="true"]',
].join(', ');

const ERROR_SELECTORS = [
  '[class*="error"]',
  '[class*="fail"]',
  '[class*="tips"]',
  '[class*="toast"]',
  '[class*="Toast"]',
  '[class*="message"]',
  '[role="alert"]',
];

export class DouyinAdapter {
  constructor(cdp) {
    this._cdp = cdp;
  }

  getRequirements(contentType) {
    return REQUIREMENTS[contentType] ?? null;
  }

  async checkLoginStatus() {
    const profileDir = process.env.DOUYIN_PROFILE_DIR ?? '(not set)';

    await this._cdp.send('Page.navigate', { url: CREATOR_HOME_URL });
    await sleep(5000);

    const state = await this._inspectPage();
    const hasSessionCookie = await this._hasSessionCookie();
    const loggedIn = hasSessionCookie && !state.hasLoginHint;
    console.error(`[DouyinAdapter] checkLoginStatus: loggedIn=${loggedIn} url=${state.url}`);
    return { loggedIn, url: state.url, profileDir, userId: null, nickname: null };
  }

  async publishImageText({ title, text, tags = [], images = [] }) {
    await this._openPublishComposer('image');
    await this._assertReadyForPublish();
    await humanPause(2500, 5500, 'composer-ready');

    if (images.length > 0) {
      await this._ensureUploaderReady('image');
      await humanPause(900, 2200, 'before-upload');
      await this._uploadFiles(images, 'image');
      await this._waitForUploadSettled(images.length, 120_000);
      await humanPause(2500, 5500, 'after-upload');
    }

    const fullText = formatTextWithTags(text, tags);
    await this._fillField(CONTENT_SELECTOR, fullText, 'content');

    if (title) {
      await this._fillField(TITLE_SELECTOR, title, 'title');
    }

    await humanPause(4500, 9000, 'before-publish-check');
    await this._assertNoBlockingErrors();
    await this._assertPublishButtonReady();

    if (DRY_RUN) {
      console.error('[DouyinAdapter] DOUYIN_PUBLISH_DRY_RUN enabled; skipping final publish click.');
      return { success: true, dry_run: true, post_url: null };
    }

    await humanPause(1200, 3000, 'before-publish-click');
    const clicked = await this._clickByText('发布');
    if (!clicked) throw new Error('PUBLISH_FAILED: 找不到发布按钮，页面结构可能已变化');
    const result = await this._waitForPublishResult(120_000);
    return { success: true, post_url: result.postUrl || result.url };
  }

  async publishShortVideo({ title, text, tags = [], video, cover }) {
    await this._openPublishComposer('video');
    await this._assertReadyForPublish();
    await humanPause(2500, 5500, 'composer-ready');

    await this._ensureUploaderReady('video');
    await humanPause(900, 2200, 'before-upload');
    await this._uploadFiles([video], 'video');
    await this._waitForUploadSettled(1, 180_000);
    await humanPause(3000, 6500, 'after-video-upload');

    if (cover) {
      console.error('[DouyinAdapter] cover was provided, but automatic Douyin cover upload is not implemented yet; continuing with platform default cover.');
    }

    const fullText = formatTextWithTags(text, tags);
    await this._fillField(CONTENT_SELECTOR, fullText, 'content');

    if (title) {
      await this._fillField(TITLE_SELECTOR, title, 'title');
    }

    await humanPause(4500, 9000, 'before-publish-check');
    await this._assertNoBlockingErrors();
    await this._assertPublishButtonReady();

    if (DRY_RUN) {
      console.error('[DouyinAdapter] DOUYIN_PUBLISH_DRY_RUN enabled; skipping final publish click.');
      return { success: true, dry_run: true, post_url: null };
    }

    await humanPause(1200, 3000, 'before-publish-click');
    const clicked = await this._clickByText('发布');
    if (!clicked) throw new Error('PUBLISH_FAILED: 找不到发布按钮，页面结构可能已变化');
    const result = await this._waitForPublishResult(120_000);
    return { success: true, post_url: result.postUrl || result.url };
  }

  async _openPublishComposer(kind) {
    const url = kind === 'video' ? VIDEO_PUBLISH_URL : IMAGE_PUBLISH_URL;
    await this._cdp.send('Page.navigate', { url });
    await this._waitForCreatorShell(20_000);
    await humanPause(1800, 4200, 'after-navigation');
    await this._ensureComposer(kind);
  }

  async _waitForCreatorShell(timeoutMs = 20_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const state = await this._inspectPage();
      if (state.hasLoginHint) {
        throw new Error(`LOGIN_EXPIRED: 当前页面 ${state.url}，请重新扫码连接抖音`);
      }
      if (state.hasUploader || state.hasPublishEditor || state.isCreatorLoggedIn) return;
      await sleep(500);
    }
    const state = await this._inspectPage();
    throw new Error(`PUBLISH_FAILED: 抖音创作平台未加载完成，当前页面 ${state.url}`);
  }

  async _hasSessionCookie() {
    const result = await this._cdp.send('Network.getAllCookies', {});
    const cookies = result.cookies ?? [];
    return cookies.some(c => (c.name === 'sessionid' || c.name === 'sid_guard') && c.value?.length > 0);
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

  async _ensureComposer(kind) {
    if (await this._waitForUploaderOrEditor(8000)) return;

    const labels = kind === 'video'
      ? ['发布视频', '上传视频', '视频', '发布作品', '上传']
      : ['发布图文', '上传图文', '图文', '发布图片', '上传图片', '图片', '发布作品', '上传'];

    for (const label of labels) {
      const clicked = await this._clickByText(label);
      if (!clicked) continue;
      if (await this._waitForUploaderOrEditor(12_000)) return;
    }

    await this._cdp.send('Page.navigate', { url: kind === 'video' ? VIDEO_PUBLISH_URL : IMAGE_PUBLISH_URL });
    if (await this._waitForUploaderOrEditor(15_000)) return;

    const state = await this._inspectPage();
    throw new Error(`PUBLISH_FAILED: 找不到抖音${kind === 'image' ? '图文' : '视频'}上传入口。${this._formatPageHint(state)}`);
  }

  async _ensureUploaderReady(kind) {
    const selector = kind === 'video' ? VIDEO_FILE_INPUT_SELECTOR : IMAGE_FILE_INPUT_SELECTOR;
    if (await this._waitForSelectorQuiet(selector, 8000)) return;
    await this._ensureComposer(kind);
    if (await this._waitForSelectorQuiet(selector, 12_000)) return;

    const labels = kind === 'video'
      ? ['上传视频', '点击上传', '选择视频', '上传']
      : ['上传图片', '添加图片', '点击上传', '选择图片', '上传'];
    for (const label of labels) {
      const clicked = await this._clickByText(label);
      if (!clicked) continue;
      if (await this._waitForSelectorQuiet(selector, 8000)) return;
    }

    const state = await this._inspectPage();
    throw new Error(`PUBLISH_FAILED: 找不到抖音文件上传输入框。${this._formatPageHint(state)}`);
  }

  async _waitForUploaderOrEditor(timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const state = await this._inspectPage();
      if (state.hasLoginHint) {
        throw new Error(`LOGIN_EXPIRED: 当前页面 ${state.url}，请重新扫码连接抖音`);
      }
      if (state.hasUploader || state.hasPublishEditor) return true;
      await sleep(500);
    }
    return false;
  }

  async _waitForSelectorQuiet(selector, timeoutMs = 8000) {
    try {
      await this._waitForSelector(selector, timeoutMs);
      return true;
    } catch {
      return false;
    }
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
          const buttons = [...document.querySelectorAll('button, [role="button"], .semi-button, [class*="button"], [class*="Button"]')].map(e => ({
            text: (e.innerText || e.textContent || '').trim(),
            disabled: !!e.disabled || e.getAttribute('aria-disabled') === 'true' || e.className?.toString().includes('disabled'),
          })).filter(b => b.text).slice(0, 30);
          const links = [...document.querySelectorAll('a')].map(e => ({
            text: (e.innerText || e.textContent || '').trim(),
            href: e.href || '',
          })).filter(a => a.text).slice(0, 30);
          const uploadish = [...document.querySelectorAll('input, button, [role="button"], a, div, span')]
            .map(e => ({
              tag: e.tagName,
              text: (e.innerText || e.textContent || e.getAttribute('aria-label') || e.getAttribute('title') || '').trim(),
              type: e.getAttribute('type') || '',
              accept: e.getAttribute('accept') || '',
              cls: e.className?.toString?.() || '',
            }))
            .filter(e => /上传|图文|图片|视频|作品|file|upload/i.test([e.text, e.type, e.accept, e.cls].join(' ')))
            .slice(0, 40);
          return {
            url,
            text: text.slice(0, 5000),
            errors,
            buttons,
            links,
            uploadish,
            hasLoginHint: /登录|扫码|验证码|手机号/.test(text) && !/发布|作品管理|创作服务平台/.test(text),
            isCreatorLoggedIn: /创作服务平台|创作者中心|发布作品|作品管理|内容管理/.test(text),
            hasUploader: !!document.querySelector(${JSON.stringify(FILE_INPUT_SELECTOR)}),
            hasPublishEditor: !!document.querySelector(${JSON.stringify(`${TITLE_SELECTOR}, ${CONTENT_SELECTOR}`)}) && /发布|作品描述|标题/.test(text),
            postUrl: document.querySelector('a[href*="/video/"]')?.href || document.querySelector('a[href*="/note/"]')?.href || '',
          };
        })()
      `,
      returnByValue: true,
    });
    return result.result?.value ?? { url: await this._getUrl(), text: '', errors: [], buttons: [], links: [], uploadish: [] };
  }

  _formatPageHint(state) {
    const buttons = (state.buttons ?? []).map(b => b.text).filter(Boolean).slice(0, 12).join(' / ');
    const links = (state.links ?? []).map(a => a.text).filter(Boolean).slice(0, 12).join(' / ');
    const uploadish = (state.uploadish ?? [])
      .map(e => [e.tag, e.text, e.type, e.accept].filter(Boolean).join(':'))
      .filter(Boolean)
      .slice(0, 12)
      .join(' / ');
    const text = (state.text ?? '').replace(/\s+/g, ' ').slice(0, 240);
    return `当前 url=${state.url}; buttons=${buttons || 'none'}; links=${links || 'none'}; uploadCandidates=${uploadish || 'none'}; text=${text || 'empty'}`;
  }

  async _assertReadyForPublish() {
    const state = await this._inspectPage();
    const hasSessionCookie = await this._hasSessionCookie();
    if (!hasSessionCookie || state.hasLoginHint) {
      throw new Error(`LOGIN_EXPIRED: 当前页面 ${state.url}，请重新扫码连接抖音`);
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
      if (/上传中|处理中|转码中|正在上传|解析中|合成中/.test(text)) continue;
      if (state.hasPublishEditor || /上传成功|重新上传|更换|已上传|封面|发布作品/.test(text)) return;
      if (expectedCount === 0) return;
    }
    throw new Error('UPLOAD_TIMEOUT: 等待抖音上传完成超时');
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
      if (/发布成功|提交成功|审核中|已发布/.test(text)) {
        return { url: state.url, postUrl: state.postUrl || state.url };
      }
      const blocking = (state.errors ?? []).find(t => /失败|错误|异常|不可|不能|未通过|风控|审核|违规|实名|认证|权限/.test(t));
      if (blocking) throw new Error(`PUBLISH_FAILED: ${blocking}`);
    }
    const hint = (lastState?.errors ?? []).join('；') || '没有检测到成功跳转或成功提示';
    throw new Error(`PUBLISH_TIMEOUT: ${hint}`);
  }

  async _fillField(selector, value, kind = 'content') {
    await humanPause(500, 1400, 'before-focus-field');
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const selector = ${JSON.stringify(selector)};
          const kind = ${JSON.stringify(kind)};
          const visible = (el) => {
            if (!el) return false;
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width > 0 && r.height > 0 && r.left > -1000 && s.display !== 'none' && s.visibility !== 'hidden';
          };
          const metaText = (el) => [
            el.getAttribute('placeholder'),
            el.getAttribute('data-placeholder'),
            el.getAttribute('aria-label'),
            el.getAttribute('title'),
            el.className?.toString?.(),
            el.closest('[class*="editor"], [class*="form"], [class*="field"], [class*="mention"], [class*="caption"]')?.innerText,
          ].filter(Boolean).join(' ');
          const candidates = [
            ...document.querySelectorAll('input, textarea, [contenteditable="true"]')
          ].filter(visible).filter(el => !el.disabled && el.getAttribute('aria-disabled') !== 'true');

          function score(el) {
            const text = metaText(el);
            let s = 0;
            if (kind === 'title') {
              if (/标题|title|caption/i.test(text)) s += 120;
              if (/添加作品标题|图文标题|作品标题/.test(text)) s += 80;
              if (/描述|正文|description/i.test(text)) s -= 120;
              if (el.tagName === 'INPUT') s += 20;
            } else {
              if (/描述|作品描述|添加作品描述|正文|description/i.test(text)) s += 120;
              if (/标题|title|caption/i.test(text)) s -= 100;
              if (el.tagName === 'TEXTAREA') s += 30;
              if (el.getAttribute('contenteditable') === 'true') s += 15;
            }
            const r = el.getBoundingClientRect();
            if (r.width < 40 || r.height < 12) s -= 40;
            return s;
          }

          let el = null;
          const direct = [...document.querySelectorAll(selector)].filter(visible);
          if (direct.length) {
            el = direct.sort((a, b) => score(b) - score(a))[0];
          }
          if (!el || score(el) <= 0) {
            el = candidates.sort((a, b) => score(b) - score(a))[0];
          }
          if (!el) return false;
          el.focus();
          if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            const nativeValueSetter = Object.getOwnPropertyDescriptor(el.tagName === 'INPUT' ? window.HTMLInputElement.prototype : window.HTMLTextAreaElement.prototype, 'value')?.set;
            if (nativeValueSetter) nativeValueSetter.call(el, '');
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
    if (result.result?.value !== true) throw new Error(`PUBLISH_FAILED: 找不到${kind === 'title' ? '标题' : '描述'}输入框：${selector}`);
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

  async _uploadFiles(filePaths, kind = 'image') {
    await humanPause(600, 1700, 'before-set-files');
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const kind = ${JSON.stringify(kind)};
          const inputs = [...document.querySelectorAll('input[type="file"]')];
          if (!inputs.length) return null;
          const score = (el) => {
            const accept = (el.getAttribute('accept') || '').toLowerCase();
            let s = 0;
            if (kind === 'image') {
              if (/image|jpg|jpeg|png|webp/.test(accept)) s += 100;
              if (/video|mp4|mov|webm/.test(accept)) s -= 100;
            } else {
              if (/video|mp4|mov|webm/.test(accept)) s += 100;
              if (/image|jpg|jpeg|png|webp/.test(accept)) s -= 100;
            }
            const containerText = (el.closest('div')?.innerText || '').slice(0, 200);
            if (kind === 'image' && /图文|图片/.test(containerText)) s += 20;
            if (kind === 'video' && /视频/.test(containerText)) s += 20;
            return s;
          };
          return inputs.sort((a, b) => score(b) - score(a))[0] ?? null;
        })()
      `,
      returnByValue: false,
    });
    if (!result.result?.objectId) throw new Error('No file input found on page');
    await this._cdp.send('DOM.setFileInputFiles', {
      objectId: result.result.objectId,
      files: filePaths,
    });
    await humanPause(1200, 2600, 'after-set-files');
  }

  async _clickByText(text) {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const visible = (el) => {
            const r = el.getBoundingClientRect();
            const s = getComputedStyle(el);
            return r.width > 0 && r.height > 0 && r.left > -1000 && s.display !== 'none' && s.visibility !== 'hidden';
          };
          const els = [...document.querySelectorAll('button, [role="button"], a, div, span')]
            .filter(visible)
            .filter(e => {
              const txt = (e.innerText || e.textContent || e.getAttribute('aria-label') || e.getAttribute('title') || '').trim();
              if (!txt) return false;
              return txt === ${JSON.stringify(text)} || txt.includes(${JSON.stringify(text)});
            });
          const el = els.sort((a, b) => {
            const ar = a.getBoundingClientRect();
            const br = b.getBoundingClientRect();
            return (ar.width * ar.height) - (br.width * br.height);
          })[0];
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
