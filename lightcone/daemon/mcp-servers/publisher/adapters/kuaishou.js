/**
 * Kuaishou (快手) publisher adapter.
 * Uses 快手创作者平台: https://cp.kuaishou.com
 */
import { formatTextWithTags } from '../text.js';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

const REQUIREMENTS = {
  image_text: {
    max_text_length: 1000,
    max_images: 12,
    image_formats: ['jpg', 'jpeg', 'png'],
    required_fields: ['text'],
    notes: '图文最多 12 张图；正文最多 1000 字',
  },
  short_video: {
    max_text_length: 1000,
    video_max_duration: 600,
    video_formats: ['mp4', 'mov'],
    required_fields: ['text', 'video'],
    notes: '视频最长 10 分钟；建议 9:16 竖版；封面图可自定义',
  },
};

export class KuaishouAdapter {
  constructor(cdp) {
    this._cdp = cdp;
  }

  getRequirements(contentType) {
    return REQUIREMENTS[contentType] ?? null;
  }

  async checkLoginStatus() {
    const result = await this._cdp.send('Network.getAllCookies', {});
    const cookies = result.cookies ?? [];
    return cookies.some(c => (c.name === 'userId' || c.name === 'passToken') && c.value?.length > 0);
  }

  async publishImageText({ title, text, tags = [], images = [] }) {
    const cdp = this._cdp;

    await cdp.send('Page.navigate', { url: 'https://cp.kuaishou.com/article/publish/image' });
    await this._waitForSelector('input[type="file"], [class*="upload"]', 12000);

    const loggedIn = await this.checkLoginStatus();
    if (!loggedIn) throw new Error('LOGIN_EXPIRED: 快手登录已过期，请重新扫码连接');

    if (images.length > 0) {
      await this._uploadFiles(images);
      await sleep(3000);
    }

    if (title) {
      await this._fillField('[placeholder*="标题"]', title);
    }

    const fullText = formatTextWithTags(text, tags);
    await this._fillField('[placeholder*="描述"], [contenteditable]', fullText);

    await sleep(1000);
    await this._clickByText('发布');
    await sleep(4000);

    const currentUrl = await this._getUrl();
    return { success: true, post_url: currentUrl };
  }

  async publishShortVideo({ title, text, tags = [], video, cover }) {
    const cdp = this._cdp;

    await cdp.send('Page.navigate', { url: 'https://cp.kuaishou.com/article/publish/video' });
    await this._waitForSelector('input[type="file"], [class*="upload"]', 12000);

    const loggedIn = await this.checkLoginStatus();
    if (!loggedIn) throw new Error('LOGIN_EXPIRED: 快手登录已过期，请重新扫码连接');

    await this._uploadFiles([video], 'video');
    await this._waitForText('上传完成', 120000);

    if (title) {
      await this._fillField('[placeholder*="标题"]', title);
    }

    const fullText = formatTextWithTags(text, tags);
    await this._fillField('[placeholder*="描述"], [contenteditable]', fullText);

    await sleep(1000);
    await this._clickByText('发布');
    await sleep(4000);

    const currentUrl = await this._getUrl();
    return { success: true, post_url: currentUrl };
  }

  // ── CDP helpers ──────────────────────────────────────────────────────────────

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

  async _fillField(selector, value) {
    await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const el = document.querySelector(${JSON.stringify(selector)});
          if (!el) return false;
          el.focus();
          if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set
              || Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
            nativeInputValueSetter?.call(el, ${JSON.stringify(value)});
            el.dispatchEvent(new Event('input', { bubbles: true }));
            el.dispatchEvent(new Event('change', { bubbles: true }));
          } else {
            el.innerText = ${JSON.stringify(value)};
            el.dispatchEvent(new InputEvent('input', { bubbles: true }));
          }
          return true;
        })()
      `,
      returnByValue: true,
    });
    await sleep(300);
  }

  async _uploadFiles(filePaths) {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: `document.querySelector('input[type="file"]')`,
      returnByValue: false,
    });
    if (!result.result?.objectId) throw new Error('No file input found on page');
    await this._cdp.send('DOM.setFileInputFiles', {
      objectId: result.result.objectId,
      files: filePaths,
    });
    await sleep(500);
  }

  async _clickByText(text) {
    await this._cdp.send('Runtime.evaluate', {
      expression: `
        (function() {
          const els = [...document.querySelectorAll('button, [role="button"]')];
          const el = els.find(e => e.innerText?.trim() === ${JSON.stringify(text)} || e.textContent?.trim() === ${JSON.stringify(text)});
          if (el) { el.click(); return true; }
          return false;
        })()
      `,
      returnByValue: true,
    });
  }

  async _getUrl() {
    const result = await this._cdp.send('Runtime.evaluate', {
      expression: 'window.location.href',
      returnByValue: true,
    });
    return result.result?.value ?? '';
  }
}
