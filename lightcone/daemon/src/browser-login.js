/**
 * Generic browser login via Chrome CDP.
 * Supports multiple platforms (xhs, douyin, etc.)
 * Runs on the daemon machine where Chrome is installed.
 * Screenshots are sent back to server via daemon WebSocket.
 */
import { spawn, execSync } from 'child_process';
import { homedir } from 'os';
import { accessSync, constants as fsConstants, rmSync, existsSync, mkdirSync, writeFileSync } from 'fs';
import path from 'path';
import http from 'http';
import { WebSocket } from 'ws';
import { acquireProfileLock } from './profile-lock.js';

// ── Platform configs ──────────────────────────────────────────────────────────

export const PLATFORM_CONFIGS = {
  xhs: {
    // Use creator subdomain so the QR scan establishes a creator session directly.
    // www.xiaohongshu.com session is separate — it cannot access creator.xiaohongshu.com.
    loginUrl: 'https://creator.xiaohongshu.com',
    // Creator login page defaults to password login; click QR tab first
    qrTabSelector: ['.qrcode-box', '[class*="qrcode"]', '[class*="scan-login"]', '[class*="qr-login"]'],
    qrFallbackScript: `
      const loginBox = document.querySelector('.login-box-container') || document.body;
      const boxRect = loginBox.getBoundingClientRect();
      const visible = (e) => {
        const r = e.getBoundingClientRect();
        const cs = getComputedStyle(e);
        return r.width >= 35 && r.width <= 110
          && r.height >= 35 && r.height <= 110
          && r.left >= boxRect.left
          && r.top >= boxRect.top
          && r.right <= boxRect.right + 4
          && r.bottom <= boxRect.bottom + 4
          && cs.display !== 'none'
          && cs.visibility !== 'hidden';
      };
      const elements = [...loginBox.querySelectorAll('img, canvas, svg, [role="img"], div, span, button')].filter(visible);
      const corner = elements
        .map(e => ({ e, r: e.getBoundingClientRect() }))
        .filter(({ r }) => r.top <= boxRect.top + 120 && r.right >= boxRect.right - 140)
        .sort((a, b) => (b.r.right + boxRect.top - b.r.top) - (a.r.right + boxRect.top - a.r.top))[0]?.e;
      if (corner) return { via: 'corner-qr-icon', rect: corner.getBoundingClientRect().toJSON() };
      return null;
    `,
    getSessionValue: (cookies) =>
      cookies.find(c => c.name === 'access-token-creator.xiaohongshu.com')?.value
      ?? cookies.find(c => c.name === 'galaxy_creator_session_id')?.value
      ?? cookies.find(c => c.name === 'galaxy.creator.beaker.session.id')?.value
      ?? cookies.find(c => c.name === 'customer-sso-sid')?.value
      ?? null,
    isLoggedIn: (cookies, baseline) => {
      const val = PLATFORM_CONFIGS.xhs.getSessionValue(cookies);
      return val !== null && val !== baseline;
    },
  },
  douyin: {
    loginUrl: 'https://creator.douyin.com/creator-micro/content/upload-image-text',
    getSessionValue: (cookies) => cookies.find(c => c.name === 'sessionid')?.value ?? null,
    isLoggedIn: (cookies, baseline) => {
      const val = cookies.find(c => c.name === 'sessionid')?.value ?? null;
      return val !== null && val !== baseline;
    },
  },
  kuaishou: {
    loginUrl: 'https://www.kuaishou.com',
    getSessionValue: (cookies) => cookies.find(c => c.name === 'passToken')?.value ?? null,
    isLoggedIn: (cookies, baseline) => {
      const val = cookies.find(c => c.name === 'passToken')?.value ?? null;
      return val !== null && val !== baseline;
    },
  },
};

export function profileDir(platform, userId) {
  return path.join(homedir(), '.lightcone', 'chrome-profiles', `${platform}-${userId}`);
}

// ── Chrome detection ──────────────────────────────────────────────────────────

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

const CHROME_BIN = detectChrome();
const CDP_PORT = 9225;
const SCREENSHOT_INTERVAL_MS = 2000;
const LOGIN_CHECK_INTERVAL_MS = 3000;

// ── Helpers ───────────────────────────────────────────────────────────────────

function httpGet(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => resolve(body));
    }).on('error', reject);
  });
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ── BrowserLoginSession ───────────────────────────────────────────────────────

export class BrowserLoginSession {
  constructor(platform) {
    this._platform = platform;
    this._config = PLATFORM_CONFIGS[platform];
    if (!this._config) throw new Error(`Unknown platform: ${platform}`);
    this._proc = null;
    this._ws = null;
    this._nextId = 1;
    this._pending = new Map();
    this._screenshotTimer = null;
    this._loginCheckTimer = null;
    this._profileDir = null;
    this._profileLock = null;
    this._closing = null;
  }

  async start(connection, userId) {
    this._profileDir = profileDir(this._platform, userId);
    connection.send({ type: 'browser:login_status', platform: this._platform, status: 'starting', message: '正在启动浏览器...' });
    this._profileLock = await acquireProfileLock(this._platform, this._profileDir, {
      owner: `browser-login:${this._platform}`,
      timeoutMs: 10_000,
      staleMs: 10 * 60 * 1000,
    });

    try {
      // Wipe the profile dir before each login so stale cookies can't fool the login check.
      // This is safe — these dirs are single-platform dedicated profiles; fresh start is correct.
      try { if (existsSync(this._profileDir)) rmSync(this._profileDir, { recursive: true, force: true }); } catch {}
      try { mkdirSync(this._profileDir, { recursive: true }); } catch {}

      // Kill any process holding the CDP port
      try {
        if (process.platform !== 'win32') {
          execSync(`lsof -ti:${CDP_PORT} | xargs kill -9`, { stdio: 'ignore' });
        }
      } catch {}
      await sleep(300);

      // Clean up stale Chrome lock files
      for (const lockFile of ['SingletonLock', 'SingletonCookie', 'SingletonSocket']) {
        const p = path.join(this._profileDir, lockFile);
        try { if (existsSync(p)) rmSync(p, { force: true }); } catch {}
      }

      this._proc = spawn(CHROME_BIN, [
        `--remote-debugging-port=${CDP_PORT}`,
        '--no-sandbox',
        '--disable-dev-shm-usage',
        '--headless=new',
        `--user-data-dir=${this._profileDir}`,
        '--window-size=1280,900',
        '--disable-blink-features=AutomationControlled',
        '--disable-infobars',
        '--disable-component-extensions-with-background-pages',
        '--use-mock-keychain',
        '--password-store=basic',
        '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'about:blank',
      ], { stdio: 'ignore', detached: false });

      this._proc.on('error', (err) => {
        console.error(`[BrowserLogin][${this._platform}] Chrome spawn error: ${err.message}`);
        this._stopTimers();
        if (this._profileLock) { this._profileLock.release(); this._profileLock = null; }
        connection.send({ type: 'browser:login_error', platform: this._platform, error: `Chrome 启动失败: ${err.message}` });
      });

      this._proc.on('exit', (code, signal) => {
        console.log(`[BrowserLogin][${this._platform}] Chrome exited (code=${code} signal=${signal})`);
        this._stopTimers();
        if (this._profileLock) { this._profileLock.release(); this._profileLock = null; }
      });

      // Poll for CDP readiness (up to 15s)
      let ready = false;
      for (let i = 0; i < 30; i++) {
        await sleep(500);
        try {
          await httpGet(`http://localhost:${CDP_PORT}/json/version`);
          ready = true;
          break;
        } catch { /* not ready yet */ }
      }
      if (!ready) throw new Error('Chrome CDP did not become ready in time');
      connection.send({ type: 'browser:login_status', platform: this._platform, status: 'browser_ready', message: '浏览器已启动，正在打开登录页...' });

      const pagesJson = await httpGet(`http://localhost:${CDP_PORT}/json`);
      const pages = JSON.parse(pagesJson);
      const page = pages.find(p => p.type === 'page') ?? pages[0];
      if (!page?.webSocketDebuggerUrl) throw new Error('No page websocket URL from CDP');

      await this._connect(page.webSocketDebuggerUrl);

      await this.send('Page.enable', {});
      await this.send('Network.enable', {});
      await this.send('Page.addScriptToEvaluateOnNewDocument', {
        source: `Object.defineProperty(navigator, 'webdriver', { get: () => undefined });`,
      });

      await this.send('Page.navigate', { url: this._config.loginUrl });
      connection.send({ type: 'browser:login_status', platform: this._platform, status: 'navigating', message: '登录页加载中...' });

      // Wait for page to settle
      await sleep(4000);

      // Try to click the QR scan login tab if present (some platforms default to password login)
      if (this._config.qrTabSelector) {
        try {
          const qrResult = await this._switchToQrLogin();
          console.log(`[BrowserLogin][${this._platform}] QR switch result: ${qrResult?.via ?? 'not-found'}`);
          await sleep(1000);
        } catch (err) {
          console.error(`[BrowserLogin][${this._platform}] QR switch failed: ${err.message}`);
        }
      }

      // Record baseline session cookie value.
      // XHS sets an anonymous web_session on first visit — we only trigger login_complete
      // when the value CHANGES (real login replaces it with an authenticated session).
      const baselineCookies = await this.send('Network.getAllCookies', {});
      const baselineSession = this._config.getSessionValue(baselineCookies.cookies ?? []);

      this._startPolling(connection, baselineSession);
      await this._sendScreenshot(connection);
    } catch (err) {
      if (this._profileLock) { this._profileLock.release(); this._profileLock = null; }
      throw err;
    }
  }

  _connect(wsUrl) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(wsUrl);
      ws.on('open', () => { this._ws = ws; resolve(); });
      ws.on('message', (data) => {
        let msg;
        try { msg = JSON.parse(data.toString()); } catch { return; }
        if (msg.id != null) {
          const entry = this._pending.get(msg.id);
          if (entry) {
            clearTimeout(entry.timer);
            this._pending.delete(msg.id);
            if (msg.error) entry.reject(new Error(msg.error.message ?? 'CDP error'));
            else entry.resolve(msg.result);
          }
        }
      });
      ws.on('error', reject);
      ws.on('close', () => {
        for (const [, entry] of this._pending) { clearTimeout(entry.timer); entry.reject(new Error('WebSocket closed')); }
        this._pending.clear();
        this._ws = null;
      });
    });
  }

  send(method, params = {}, timeoutMs = 10_000) {
    return new Promise((resolve, reject) => {
      if (!this._ws) return reject(new Error('WebSocket not connected'));
      const id = this._nextId++;
      const timer = setTimeout(() => { this._pending.delete(id); reject(new Error(`CDP timeout for ${method}`)); }, timeoutMs);
      this._pending.set(id, { resolve, reject, timer });
      this._ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async screenshot() {
    const result = await this.send('Page.captureScreenshot', { format: 'jpeg', quality: 60 }, 10_000);
    return result.data;
  }

  async _sendScreenshot(connection) {
    try {
      const screenshot = await this.screenshot();
      connection.send({ type: 'browser:screenshot', platform: this._platform, screenshot });
      connection.send({ type: 'browser:login_status', platform: this._platform, status: 'ready', message: '请扫描页面中的二维码完成登录' });
    } catch (err) {
      console.error(`[BrowserLogin][${this._platform}] Screenshot error:`, err.message);
      connection.send({ type: 'browser:login_status', platform: this._platform, status: 'screenshot_error', message: `截图失败：${err.message}` });
    }
  }

  async _switchToQrLogin() {
    const selectors = this._config.qrTabSelector ?? [];
    const result = await this.send('Runtime.evaluate', {
      expression: `(function() {
        const visible = (el) => {
          const r = el.getBoundingClientRect();
          const cs = getComputedStyle(el);
          return r.width > 0 && r.height > 0 && cs.display !== 'none' && cs.visibility !== 'hidden';
        };
        const hit = (el, via) => {
          if (!el) return null;
          const target = el.closest('button, [role="button"], a') || el;
          const rect = target.getBoundingClientRect();
          if (rect.width <= 0 || rect.height <= 0) return null;
          return { via, rect: rect.toJSON() };
        };

        const sels = ${JSON.stringify(selectors)};
        for (const s of sels) {
          const el = document.querySelector(s);
          if (el && visible(el)) return hit(el, s);
        }

        const textElements = [...document.querySelectorAll('a,button,span,div')].filter(visible);
        const textEl = textElements.find(e => {
          const t = (e.innerText || e.textContent || '').trim();
          return t === '扫码登录' || t === '二维码登录' || t === '扫码';
        });
        const textHit = hit(textEl, textEl ? 'text:' + (textEl.innerText || textEl.textContent || '').trim() : '');
        if (textHit) return textHit;

        const fallbackHit = (function() {
          ${this._config.qrFallbackScript ?? 'return null;'}
        })();
        if (fallbackHit) return fallbackHit;

        if (location.hostname.includes('xiaohongshu.com')) {
          return {
            via: 'xhs-calibrated-corner',
            point: {
              x: Math.round(window.innerWidth * 0.897),
              y: Math.round(window.innerHeight * 0.339)
            }
          };
        }
        return null;
      })()`,
      returnByValue: true,
    });

    const hit = result.result?.value;
    const rect = hit?.rect;
    const point = hit?.point;
    if (!rect && !point) return null;

    const x = point?.x ?? Math.round(rect.left + rect.width / 2);
    const y = point?.y ?? Math.round(rect.top + rect.height / 2);
    await this.send('Input.dispatchMouseEvent', { type: 'mouseMoved', x, y }, 5_000);
    await this.send('Input.dispatchMouseEvent', { type: 'mousePressed', x, y, button: 'left', clickCount: 1 }, 5_000);
    await this.send('Input.dispatchMouseEvent', { type: 'mouseReleased', x, y, button: 'left', clickCount: 1 }, 5_000);
    return { via: hit.via, x, y };
  }

  async isLoggedIn(baseline) {
    const result = await this.send('Network.getAllCookies', {});
    return this._config.isLoggedIn(result.cookies ?? [], baseline);
  }

  _startPolling(connection, baselineSession) {
    let _screenshotInProgress = false;
    this._screenshotTimer = setInterval(async () => {
      if (_screenshotInProgress) return;
      _screenshotInProgress = true;
      try {
        await this._sendScreenshot(connection);
      } catch (err) {
        console.error(`[BrowserLogin][${this._platform}] Screenshot error:`, err.message);
      } finally {
        _screenshotInProgress = false;
      }
    }, SCREENSHOT_INTERVAL_MS);

    this._loginCheckTimer = setInterval(async () => {
      try {
        const loggedIn = await this.isLoggedIn(baselineSession);
        if (loggedIn) {
          this._stopTimers();
          // Export decrypted cookies to cookies.json so chrome-pool can inject them
          // without relying on platform-specific keychain encryption (fixes macOS cross-process issue)
          try {
            const cookieResult = await this.send('Network.getAllCookies', {});
            const baseDomain = new URL(this._config.loginUrl).hostname.split('.').slice(-2).join('.');
            const cookies = (cookieResult.cookies ?? []).filter(c =>
              c.domain.includes(baseDomain)
            );
            writeFileSync(path.join(this._profileDir, 'cookies.json'), JSON.stringify(cookies));
            console.log(`[BrowserLogin][${this._platform}] Saved ${cookies.length} cookies to cookies.json`);
          } catch (err) {
            console.error(`[BrowserLogin][${this._platform}] Failed to save cookies: ${err.message}`);
          }
          connection.send({ type: 'browser:login_complete', platform: this._platform, profileDir: this._profileDir });
          await this.close();
        }
      } catch (err) {
        console.error(`[BrowserLogin][${this._platform}] Login check error:`, err.message);
      }
    }, LOGIN_CHECK_INTERVAL_MS);
  }

  async _clearSessionCookies() {
    const cookies = this._config.sessionCookies ?? [];
    for (const { name, domain } of cookies) {
      try {
        await this.send('Network.deleteCookies', { name, domain });
      } catch {}
    }
  }

  _stopTimers() {
    if (this._screenshotTimer) { clearInterval(this._screenshotTimer); this._screenshotTimer = null; }
    if (this._loginCheckTimer) { clearInterval(this._loginCheckTimer); this._loginCheckTimer = null; }
  }

  async close() {
    if (this._closing) return this._closing;
    this._closing = (async () => {
      this._stopTimers();
      // Use CDP Browser.close for graceful shutdown so Chrome flushes cookies to disk
      try { await this.send('Browser.close', {}, 3000); } catch {}
      await sleep(1000);
      if (this._ws) { try { this._ws.close(); } catch {} this._ws = null; }
      if (this._proc) { try { this._proc.kill('SIGKILL'); } catch {} this._proc = null; }
      if (this._profileLock) { this._profileLock.release(); this._profileLock = null; }
    })();
    return this._closing;
  }
}

// ── Singleton map (platform → session) ───────────────────────────────────────

const _sessions = new Map();

export function getSession(platform) { return _sessions.get(platform) ?? null; }

export async function startSession(platform, connection, userId) {
  const existing = _sessions.get(platform);
  if (existing) { await existing.close(); _sessions.delete(platform); }
  const session = new BrowserLoginSession(platform);
  _sessions.set(platform, session);
  try {
    await session.start(connection, userId);
    return session;
  } catch (err) {
    _sessions.delete(platform);
    await session.close();
    throw err;
  }
}

export async function stopSession(platform) {
  const session = _sessions.get(platform);
  if (session) { await session.close(); _sessions.delete(platform); }
}

export async function stopAllSessions() {
  const sessions = [..._sessions.values()];
  _sessions.clear();
  await Promise.allSettled(sessions.map(session => session.close()));
}
