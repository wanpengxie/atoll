#!/usr/bin/env node
/**
 * Publisher MCP Server
 * Drives a real Chrome browser (via CDP) to publish content to social platforms.
 * Env vars injected at spawn time:
 *   XHS_PROFILE_DIR — Chrome profile directory with XHS login state
 */

import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { z } from 'zod';
import { spawn } from 'child_process';
import { accessSync, constants as fsConstants } from 'fs';
import http from 'http';
import net from 'net';
import { WebSocket } from 'ws';

const XHS_PROFILE_DIR = process.env.XHS_PROFILE_DIR ?? '';

// ── Platform requirements (static) ───────────────────────────────────────────

const REQUIREMENTS = {
  xhs: {
    image_text: {
      title: '2-20 字',
      text: '0-1000 字（建议 100-500）',
      images: '最多 9 张，JPG/PNG/WEBP，建议 3:4 竖版比例',
      tags: '最多 10 个话题标签，传入时不含 # 号',
      notes: '所有发布操作须先通过 request_approval 获得人工审批',
    },
    short_video: {
      title: '2-20 字',
      text: '0-1000 字',
      video: 'MP4/MOV，最长 10 分钟',
      cover: '可选封面图 JPG/PNG',
      tags: '最多 10 个话题标签，传入时不含 # 号',
      notes: '所有发布操作须先通过 request_approval 获得人工审批',
    },
  },
};

// ── Chrome helpers ────────────────────────────────────────────────────────────

function detectChrome() {
  if (process.env.CHROME_BIN) return process.env.CHROME_BIN;
  const candidates = [
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    '/usr/bin/chromium-browser',
    '/usr/bin/chromium',
    '/snap/bin/chromium',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  ];
  for (const p of candidates) {
    try { accessSync(p, fsConstants.X_OK); return p; } catch {}
  }
  return candidates[0];
}

function getFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, () => { const port = srv.address().port; srv.close(() => resolve(port)); });
    srv.on('error', reject);
  });
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => resolve(body));
    }).on('error', reject);
  });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── CDP Session ───────────────────────────────────────────────────────────────

class CdpSession {
  constructor() {
    this._proc = null;
    this._ws = null;
    this._nextId = 1;
    this._pending = new Map();
    this._port = null;
  }

  async launch(profileDir) {
    const chrome = detectChrome();
    this._port = await getFreePort();
    this._proc = spawn(chrome, [
      `--remote-debugging-port=${this._port}`,
      '--no-sandbox',
      '--disable-dev-shm-usage',
      '--headless=new',
      '--disable-blink-features=AutomationControlled',
      '--disable-infobars',
      '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      `--user-data-dir=${profileDir}`,
      '--window-size=1280,900',
      'about:blank',
    ], { stdio: 'ignore' });

    for (let i = 0; i < 40; i++) {
      await sleep(500);
      try { await httpGet(`http://localhost:${this._port}/json/version`); break; } catch {}
      if (i === 39) throw new Error('Chrome did not start in time');
    }

    const pagesJson = await httpGet(`http://localhost:${this._port}/json`);
    const pages = JSON.parse(pagesJson);
    const page = pages.find(p => p.type === 'page') ?? pages[0];
    if (!page?.webSocketDebuggerUrl) throw new Error('No CDP page found');

    await this._connectWs(page.webSocketDebuggerUrl);
    await this.cdp('Page.enable', {});
    await this.cdp('Network.enable', {});
    await this.cdp('DOM.enable', {});
    await this.cdp('Page.addScriptToEvaluateOnNewDocument', {
      source: `Object.defineProperty(navigator, 'webdriver', { get: () => undefined });`,
    });
  }

  _connectWs(wsUrl) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl);
      ws.on('open', () => { this._ws = ws; resolve(); });
      ws.on('message', (data) => {
        let msg; try { msg = JSON.parse(data.toString()); } catch { return; }
        if (msg.id != null) {
          const p = this._pending.get(msg.id);
          if (p) {
            clearTimeout(p.timer);
            this._pending.delete(msg.id);
            msg.error ? p.reject(new Error(msg.error.message ?? 'CDP error')) : p.resolve(msg.result);
          }
        }
      });
      ws.on('error', reject);
      ws.on('close', () => {
        for (const [, p] of this._pending) { clearTimeout(p.timer); p.reject(new Error('WS closed')); }
        this._pending.clear();
        this._ws = null;
      });
    });
  }

  cdp(method, params = {}, timeoutMs = 30_000) {
    return new Promise((resolve, reject) => {
      if (!this._ws) return reject(new Error('Not connected'));
      const id = this._nextId++;
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error(`CDP timeout: ${method}`));
      }, timeoutMs);
      this._pending.set(id, { resolve, reject, timer });
      this._ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async navigate(url, waitMs = 4000) {
    await this.cdp('Page.navigate', { url });
    await sleep(waitMs);
  }

  async eval(expr, awaitPromise = false) {
    const res = await this.cdp('Runtime.evaluate', {
      expression: expr,
      returnByValue: true,
      awaitPromise,
    });
    if (res.exceptionDetails) {
      throw new Error(res.exceptionDetails.exception?.description ?? res.exceptionDetails.text ?? 'JS error');
    }
    return res.result?.value;
  }

  async getCookies() {
    const r = await this.cdp('Network.getAllCookies', {});
    return r.cookies ?? [];
  }

  // Set files on a file input element by CSS selector
  async setFileInput(selector, filePaths) {
    const { root } = await this.cdp('DOM.getDocument', { depth: 0 });
    const { nodeId } = await this.cdp('DOM.querySelector', { nodeId: root.nodeId, selector });
    if (!nodeId) throw new Error(`File input element not found: ${selector}`);
    await this.cdp('DOM.setFileInputFiles', { nodeId, files: filePaths });
  }

  close() {
    if (this._ws) { try { this._ws.close(); } catch {} this._ws = null; }
    if (this._proc) { try { this._proc.kill('SIGKILL'); } catch {} this._proc = null; }
  }
}

// ── MCP Server ────────────────────────────────────────────────────────────────

const server = new McpServer({ name: 'publisher', version: '0.1.0' });

// ── get_platform_requirements ─────────────────────────────────────────────────

server.tool('get_platform_requirements',
  '获取平台内容发布规范（字数限制、图片格式等）',
  {
    platform:     z.string().describe('平台名称，当前支持：xhs'),
    content_type: z.string().describe('内容类型：image_text | short_video'),
  },
  async ({ platform, content_type }) => {
    const reqs = REQUIREMENTS[platform]?.[content_type];
    if (!reqs) {
      return { isError: true, content: [{ type: 'text', text: `不支持的平台或类型: ${platform}/${content_type}` }] };
    }
    const lines = Object.entries(reqs).map(([k, v]) => `**${k}**: ${v}`);
    return { content: [{ type: 'text', text: `## ${platform} ${content_type} 规范\n\n${lines.join('\n')}` }] };
  }
);

// ── check_login_status ────────────────────────────────────────────────────────

server.tool('check_login_status',
  '检查指定平台的登录状态（通过 Chrome Profile 中的 cookies 验证）',
  {
    platform: z.string().describe('平台名称：xhs'),
  },
  async ({ platform }) => {
    const profileDir = platform === 'xhs' ? XHS_PROFILE_DIR : '';
    if (!profileDir) {
      return { isError: true, content: [{ type: 'text', text: `未配置 ${platform} 的 Profile 目录，请先在「设置 → 连接外部账号」中完成扫码授权。` }] };
    }

    const session = new CdpSession();
    try {
      await session.launch(profileDir);
      const cookies = await session.getCookies();

      const loggedIn = platform === 'xhs'
        ? cookies.some(c => c.name === 'web_session' && c.value?.length > 0)
        : false;

      return {
        content: [{ type: 'text', text: loggedIn
          ? `✅ ${platform} 已登录（web_session cookie 有效）`
          : `❌ ${platform} 未登录，请在「设置 → 连接外部账号」重新扫码授权`
        }],
      };
    } finally {
      session.close();
    }
  }
);

// ── publish_content ───────────────────────────────────────────────────────────

server.tool('publish_content',
  '发布图文或短视频到社交平台。调用前必须已通过 request_approval 获得人工审批。',
  {
    platform:     z.string().describe('平台：xhs'),
    content_type: z.string().describe('类型：image_text | short_video'),
    title:        z.string().describe('标题（2-20 字）'),
    text:         z.string().describe('正文内容'),
    tags:         z.array(z.string()).optional().describe('话题标签列表，不含 # 号'),
    images:       z.array(z.string()).optional().describe('图片绝对路径列表（image_text 用，最多 9 张）'),
    video:        z.string().optional().describe('视频绝对路径（short_video 用）'),
    cover:        z.string().optional().describe('封面图绝对路径（short_video 可选）'),
  },
  async ({ platform, content_type, title, text, tags, images, video, cover }) => {
    if (platform !== 'xhs') {
      return { isError: true, content: [{ type: 'text', text: `暂不支持平台: ${platform}` }] };
    }
    if (!XHS_PROFILE_DIR) {
      return { isError: true, content: [{ type: 'text', text: '未配置 XHS_PROFILE_DIR，无法启动浏览器。请先在「连接外部账号」中完成小红书授权。' }] };
    }

    const session = new CdpSession();
    try {
      await session.launch(XHS_PROFILE_DIR);

      // Navigate to XHS creator center
      await session.navigate('https://creator.xiaohongshu.com/publish/publish', 5000);

      // Verify login
      const cookies = await session.getCookies();
      const loggedIn = cookies.some(c => c.name === 'web_session' && c.value?.length > 0);
      if (!loggedIn) {
        return { isError: true, content: [{ type: 'text', text: '小红书未登录，请在「设置 → 连接外部账号」重新扫码授权后重试。' }] };
      }

      if (content_type === 'image_text') {
        return await _publishXhsImageText(session, { title, text, tags: tags ?? [], images: images ?? [] });
      } else if (content_type === 'short_video') {
        return await _publishXhsVideo(session, { title, text, tags: tags ?? [], video: video ?? '', cover });
      } else {
        return { isError: true, content: [{ type: 'text', text: `不支持的内容类型: ${content_type}` }] };
      }
    } finally {
      session.close();
    }
  }
);

// ── XHS 图文发布 ──────────────────────────────────────────────────────────────

async function _publishXhsImageText(session, { title, text, tags, images }) {
  // 1. Upload images
  if (images.length > 0) {
    // Unhide the file input so CDP can target it
    await session.eval(`
      const inp = document.querySelector('input[type="file"][accept*="image"]');
      if (inp) { inp.style.display = 'block'; inp.removeAttribute('hidden'); }
    `);
    await sleep(500);

    try {
      await session.setFileInput('input[type="file"][accept*="image"]', images);
    } catch {
      // Fallback: generic file input
      await session.setFileInput('input[type="file"]', images);
    }

    // Wait for upload to finish (up to 60s)
    for (let i = 0; i < 60; i++) {
      await sleep(1000);
      const uploading = await session.eval(
        `!!document.querySelector('[class*="uploading"], [class*="upload-progress"], [class*="loading"]')`
      ).catch(() => false);
      if (!uploading) break;
    }
    await sleep(2000);
  }

  // 2. Fill title
  await session.eval(`
    const el = document.querySelector(
      'input[placeholder*="标题"], input[placeholder*="title"], ' +
      'textarea[placeholder*="标题"], [contenteditable][placeholder*="标题"]'
    );
    if (el) {
      el.focus();
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set
        ?? Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
      if (nativeSetter) { nativeSetter.call(el, ${JSON.stringify(title)}); }
      else { el.value = ${JSON.stringify(title)}; }
      el.dispatchEvent(new Event('input', { bubbles: true }));
      el.dispatchEvent(new Event('change', { bubbles: true }));
    }
  `);
  await sleep(500);

  // 3. Fill description + append hashtags
  const fullText = text + (tags.length > 0 ? '\n' + tags.map(t => `#${t} `).join('') : '');
  await session.eval(`
    const el = document.querySelector(
      'textarea[placeholder*="正文"], textarea[placeholder*="描述"], textarea[placeholder*="内容"], ' +
      '[contenteditable][placeholder*="正文"], [contenteditable][placeholder*="描述"]'
    );
    if (el) {
      el.focus();
      if (el.tagName === 'TEXTAREA' || el.tagName === 'INPUT') {
        const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
        if (nativeSetter) { nativeSetter.call(el, ${JSON.stringify(fullText)}); }
        else { el.value = ${JSON.stringify(fullText)}; }
        el.dispatchEvent(new Event('input', { bubbles: true }));
      } else {
        // contenteditable
        el.innerText = ${JSON.stringify(fullText)};
        el.dispatchEvent(new InputEvent('input', { bubbles: true, inputType: 'insertText' }));
      }
    }
  `);
  await sleep(1000);

  // 4. Click publish button
  await session.eval(`
    const btn = [...document.querySelectorAll('button, [role="button"]')]
      .find(b => /^(发布|publish|提交)$/i.test(b.textContent?.trim()));
    if (btn) btn.click();
  `);

  // 5. Wait for success or error (up to 20s)
  for (let i = 0; i < 20; i++) {
    await sleep(1000);
    const url = await session.eval(`window.location.href`).catch(() => '');
    if (url && !url.includes('/publish/publish')) {
      // Navigated away — treat as success
      return { content: [{ type: 'text', text: `✅ 小红书图文已发布！标题：${title}` }] };
    }
    const succeeded = await session.eval(
      `!!document.querySelector('[class*="success"], [class*="published"]')`
    ).catch(() => false);
    if (succeeded) {
      return { content: [{ type: 'text', text: `✅ 小红书图文已发布！标题：${title}` }] };
    }
  }

  // Check for visible error message
  const errMsg = await session.eval(
    `document.querySelector('[class*="error"], [class*="fail"], .toast')?.textContent?.trim() ?? ''`
  ).catch(() => '');
  if (errMsg) {
    return { isError: true, content: [{ type: 'text', text: `发布失败：${errMsg}` }] };
  }

  return { content: [{ type: 'text', text: `发布操作已提交，请在小红书创作者中心确认结果。标题：${title}` }] };
}

// ── XHS 短视频发布 ────────────────────────────────────────────────────────────

async function _publishXhsVideo(session, { title, text, tags, video, cover }) {
  if (video) {
    await session.eval(`
      const inp = document.querySelector('input[type="file"][accept*="video"]');
      if (inp) { inp.style.display = 'block'; inp.removeAttribute('hidden'); }
    `);
    await sleep(500);
    await session.setFileInput('input[type="file"][accept*="video"]', [video]);

    // Wait for video upload (up to 4 minutes)
    for (let i = 0; i < 120; i++) {
      await sleep(2000);
      const uploading = await session.eval(
        `!!document.querySelector('[class*="upload-progress"], [class*="uploading"]')`
      ).catch(() => false);
      if (!uploading) break;
    }
    await sleep(3000);
  }

  if (cover) {
    await session.eval(`
      const inp = document.querySelector('input[type="file"][accept*="image"]');
      if (inp) { inp.style.display = 'block'; inp.removeAttribute('hidden'); }
    `);
    await sleep(300);
    try { await session.setFileInput('input[type="file"][accept*="image"]', [cover]); } catch {}
    await sleep(1000);
  }

  // Fill title
  await session.eval(`
    const el = document.querySelector('input[placeholder*="标题"], input[placeholder*="title"]');
    if (el) {
      el.focus();
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, 'value')?.set;
      if (nativeSetter) nativeSetter.call(el, ${JSON.stringify(title)});
      else el.value = ${JSON.stringify(title)};
      el.dispatchEvent(new Event('input', { bubbles: true }));
    }
  `);
  await sleep(300);

  // Fill description + tags
  const fullText = text + (tags.length > 0 ? '\n' + tags.map(t => `#${t} `).join('') : '');
  await session.eval(`
    const el = document.querySelector('textarea[placeholder*="正文"], textarea[placeholder*="描述"]');
    if (el) {
      el.focus();
      const nativeSetter = Object.getOwnPropertyDescriptor(window.HTMLTextAreaElement.prototype, 'value')?.set;
      if (nativeSetter) nativeSetter.call(el, ${JSON.stringify(fullText)});
      else el.value = ${JSON.stringify(fullText)};
      el.dispatchEvent(new Event('input', { bubbles: true }));
    }
  `);
  await sleep(500);

  await session.eval(`
    const btn = [...document.querySelectorAll('button')].find(b => /^(发布|publish)$/i.test(b.textContent?.trim()));
    if (btn) btn.click();
  `);

  await sleep(5000);
  return { content: [{ type: 'text', text: `✅ 小红书短视频发布已提交。标题：${title}` }] };
}

// ── Start ─────────────────────────────────────────────────────────────────────

const transport = new StdioServerTransport();
await server.connect(transport);
console.error('[publisher-mcp] Server started');
