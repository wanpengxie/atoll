/**
 * Chrome session pool for the Publisher MCP.
 * Manages one Chrome process per platform, reusing CDP connections
 * across tool calls rather than restarting Chrome each time.
 */
import { spawn, execSync } from 'child_process';
import { accessSync, constants as fsConstants, rmSync, existsSync, readFileSync } from 'fs';
import path from 'path';
import http from 'http';
import { WebSocket } from 'ws';

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

// platform → { proc, ws, nextId, pending, profileDir, cdpPort }
const _sessions = new Map();
let _nextPort = 9230; // start above browser-login.js port (9225)

function httpGet(url) {
  return new Promise((resolve, reject) => {
    http.get(url, (res) => {
      let body = '';
      res.on('data', d => body += d);
      res.on('end', () => resolve(body));
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── CDP client ────────────────────────────────────────────────────────────────

class CdpClient {
  constructor(ws) {
    this._ws = ws;
    this._nextId = 1;
    this._pending = new Map();
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
    ws.on('close', () => {
      for (const [, entry] of this._pending) {
        clearTimeout(entry.timer);
        entry.reject(new Error('CDP WebSocket closed'));
      }
      this._pending.clear();
    });
  }

  send(method, params = {}, timeoutMs = 15_000) {
    return new Promise((resolve, reject) => {
      if (this._ws.readyState !== WebSocket.OPEN) return reject(new Error('CDP WebSocket not open'));
      const id = this._nextId++;
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error(`CDP timeout: ${method}`));
      }, timeoutMs);
      this._pending.set(id, { resolve, reject, timer });
      this._ws.send(JSON.stringify({ id, method, params }));
    });
  }

  close() {
    try { this._ws.close(); } catch {}
  }
}

// ── Session management ────────────────────────────────────────────────────────

async function _connectCdp(port) {
  const pagesJson = await httpGet(`http://localhost:${port}/json`);
  const pages = JSON.parse(pagesJson);
  const page = pages.find(p => p.type === 'page') ?? pages[0];
  if (!page?.webSocketDebuggerUrl) throw new Error('No page CDP URL');

  return new Promise((resolve, reject) => {
    const ws = new WebSocket(page.webSocketDebuggerUrl);
    ws.on('open', () => resolve(new CdpClient(ws)));
    ws.on('error', reject);
  });
}

async function _spawnChrome(profileDir, port) {
  // Clean up stale lock files
  for (const lockFile of ['SingletonLock', 'SingletonCookie', 'SingletonSocket']) {
    const p = path.join(profileDir, lockFile);
    try { if (existsSync(p)) rmSync(p, { force: true }); } catch {}
  }
  // Kill any process holding the port
  try {
    if (process.platform !== 'win32') {
      execSync(`lsof -ti:${port} | xargs kill -9`, { stdio: 'ignore' });
    }
  } catch {}

  // On macOS with a display, run non-headless for best site compatibility
  // (headless Chrome has detectable fingerprints that some sites block)
  const isHeadless = process.platform !== 'darwin' || !!process.env.FORCE_HEADLESS;
  const chromeArgs = [
    `--remote-debugging-port=${port}`,
    '--no-sandbox',
    '--disable-dev-shm-usage',
    `--user-data-dir=${profileDir}`,
    '--window-size=1280,900',
    '--disable-blink-features=AutomationControlled',
    '--disable-infobars',
    '--disable-component-extensions-with-background-pages',
    '--no-first-run',
    '--no-default-browser-check',
    '--disable-hang-monitor',
    '--use-mock-keychain',
    '--password-store=basic',
    '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'about:blank',
  ];
  if (isHeadless) {
    chromeArgs.splice(3, 0, '--headless=new', '--disable-gpu');
  }
  const proc = spawn(CHROME_BIN, chromeArgs, { stdio: 'ignore', detached: false });

  proc.on('error', (err) => {
    console.error(`[ChromePool] Spawn error: ${err.message}`);
  });

  // Wait up to 15s for CDP readiness
  for (let i = 0; i < 30; i++) {
    await sleep(500);
    try {
      await httpGet(`http://localhost:${port}/json/version`);
      return proc;
    } catch { /* not ready */ }
  }
  proc.kill();
  throw new Error(`Chrome did not become ready on port ${port}`);
}

/**
 * Get or create a CDP session for the given platform.
 * profileDir is injected via env vars by the daemon credential system.
 */
export async function getSession(platform, profileDir) {
  const existing = _sessions.get(platform);
  if (existing) {
    // Health check: try a quick CDP command
    try {
      await existing.cdp.send('Runtime.evaluate', { expression: '1', returnByValue: true }, 3000);
      return existing.cdp;
    } catch {
      console.error(`[ChromePool] Session for ${platform} is stale, recreating`);
      existing.proc?.kill();
      _sessions.delete(platform);
    }
  }

  if (!profileDir) throw new Error(`No profile dir provided for platform=${platform}. Has the user logged in?`);

  const port = _nextPort++;
  console.error(`[ChromePool] Starting Chrome for ${platform} on port ${port}, profile=${profileDir}`);
  const proc = await _spawnChrome(profileDir, port);
  const cdp = await _connectCdp(port);

  // Hide automation fingerprint
  await cdp.send('Page.enable', {});
  await cdp.send('Network.enable', {});
  await cdp.send('Page.addScriptToEvaluateOnNewDocument', {
    source: `Object.defineProperty(navigator, 'webdriver', { get: () => undefined });`,
  });

  // Inject saved cookies from browser-login (bypasses macOS keychain cross-process encryption issue)
  const cookiesFile = path.join(profileDir, 'cookies.json');
  if (existsSync(cookiesFile)) {
    try {
      const rawCookies = JSON.parse(readFileSync(cookiesFile, 'utf8'));
      // Strip read-only fields that Network.setCookies doesn't accept
      const cookies = rawCookies.map(({ name, value, domain, path: p, secure, httpOnly, sameSite, expires, priority }) => {
        const c = { name, value };
        if (domain !== undefined) c.domain = domain;
        if (p !== undefined) c.path = p;
        if (secure !== undefined) c.secure = secure;
        if (httpOnly !== undefined) c.httpOnly = httpOnly;
        if (sameSite !== undefined) c.sameSite = sameSite;
        if (expires !== undefined && expires !== -1) c.expires = expires;
        if (priority !== undefined) c.priority = priority;
        return c;
      });
      await cdp.send('Network.setCookies', { cookies });
      console.error(`[ChromePool] Injected ${cookies.length} cookies from cookies.json for ${platform}`);
    } catch (err) {
      console.error(`[ChromePool] Failed to inject cookies: ${err.message}`);
    }
  } else {
    console.error(`[ChromePool] No cookies.json found for ${platform} at ${cookiesFile}`);
  }

  proc.on('exit', () => {
    console.error(`[ChromePool] Chrome for ${platform} exited`);
    _sessions.delete(platform);
  });

  _sessions.set(platform, { proc, cdp, profileDir, port });
  return cdp;
}

/**
 * Close and remove the session for a platform.
 */
export function closeSession(platform) {
  const s = _sessions.get(platform);
  if (s) {
    s.cdp.close();
    s.proc?.kill('SIGKILL');
    _sessions.delete(platform);
  }
}

/**
 * Check if a session exists and is alive.
 */
export function hasSession(platform) { return _sessions.has(platform); }
