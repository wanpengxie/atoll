import React, { useCallback, useEffect, useState } from 'react';

// VITE_COAGENT_EXTENSION_ID is the stable id derived from the signed
// manifest key (`.dev-secrets/extension-key.pub.b64`). Vite injects it
// via the define plugin at build time (ui/vite.config.js).
const EXTENSION_ID =
  (typeof import.meta !== 'undefined' && import.meta.env?.VITE_COAGENT_EXTENSION_ID) || '';

const DEFAULT_PROXY_ENDPOINT = 'ws://127.0.0.1:10387';
const POLL_INTERVAL_MS = 5_000;

// sendToExtension is a thin promise wrapper over chrome.runtime.sendMessage
// with EXTENSION_ID. Throws when chrome.runtime is unavailable (non-Chrome
// browser or extension not installed). Caller catches and falls back to
// "not installed" state.
function sendToExtension(message, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    if (typeof chrome === 'undefined' || !chrome.runtime || !chrome.runtime.sendMessage) {
      reject(new Error('chrome.runtime unavailable'));
      return;
    }
    if (!EXTENSION_ID) {
      reject(new Error('VITE_COAGENT_EXTENSION_ID not configured'));
      return;
    }
    const timer = window.setTimeout(() => reject(new Error('ext message timeout')), timeoutMs);
    try {
      chrome.runtime.sendMessage(EXTENSION_ID, message, (response) => {
        window.clearTimeout(timer);
        const err = chrome.runtime?.lastError;
        if (err) {
          reject(new Error(err.message || 'ext message failed'));
          return;
        }
        resolve(response);
      });
    } catch (err) {
      window.clearTimeout(timer);
      reject(err);
    }
  });
}

function formatHeartbeat(ms) {
  if (!ms) return '';
  const d = new Date(ms);
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(d);
}

export default function ExtensionPanel() {
  // states:
  //   installed: tri-state — null (probing) / true / false
  //   info: last successful getDeviceInfo response
  //   busy: a connectProxy / unbindDevice call is in flight
  //   error: surface unrecoverable errors from the last action
  const [installed, setInstalled] = useState(null);
  const [info, setInfo] = useState(null);
  const [busy, setBusy] = useState('');
  const [error, setError] = useState('');
  const [endpoint, setEndpoint] = useState(DEFAULT_PROXY_ENDPOINT);

  const refresh = useCallback(async () => {
    try {
      const resp = await sendToExtension({ action: 'getDeviceInfo' });
      if (resp?.status === 'ok') {
        setInstalled(true);
        setInfo(resp);
        if (resp.bound?.proxy_endpoint) setEndpoint(resp.bound.proxy_endpoint);
        setError('');
      } else if (resp?.status === 'failed') {
        // ext installed but responded with failure (e.g. origin not allowed).
        setInstalled(true);
        setError(resp.detail || `ext error: ${resp.reason}`);
      } else {
        setInstalled(false);
      }
    } catch (err) {
      // No response / no chrome.runtime → treat as not installed. We do
      // NOT surface this as an error because "not installed" is a normal
      // state for first-time users.
      setInstalled(false);
      setInfo(null);
    }
  }, []);

  useEffect(() => {
    refresh();
    const t = window.setInterval(refresh, POLL_INTERVAL_MS);
    return () => window.clearInterval(t);
  }, [refresh]);

  const handleConnect = useCallback(async () => {
    setBusy('connect');
    setError('');
    try {
      const resp = await sendToExtension({
        action: 'connectProxy',
        proxy_endpoint: endpoint || DEFAULT_PROXY_ENDPOINT,
      }, 8000);
      if (resp?.status === 'connected') {
        setInfo((prev) => ({ ...(prev || {}), bound: resp.bound }));
      } else if (resp?.status === 'failed') {
        setError(resp.detail || `连接失败 (${resp.reason})`);
      } else {
        setError('未知响应');
      }
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy('');
      refresh();
    }
  }, [endpoint, refresh]);

  const handleDisconnect = useCallback(async () => {
    setBusy('disconnect');
    setError('');
    try {
      const resp = await sendToExtension({ action: 'unbindDevice' });
      if (resp?.status !== 'unbound' && resp?.status !== 'ok') {
        setError(resp?.detail || '断开失败');
      }
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy('');
      refresh();
    }
  }, [refresh]);

  if (installed === null) {
    return (
      <div className="ext-panel ext-panel-probing">
        <span className="ext-panel-title">本机扩展</span>
        <span className="ext-panel-muted">检测中…</span>
      </div>
    );
  }

  if (!installed) {
    return (
      <div className="ext-panel ext-panel-not-installed">
        <span className="ext-panel-title">本机扩展</span>
        <span className="ext-panel-muted">未检测到 coagent xhs 扩展</span>
        <a className="ext-panel-link" href="/downloads/coagent-extension.zip">下载扩展</a>
        <span className="ext-panel-muted">下载后 chrome://extensions → 开发者模式 → 加载已解压</span>
      </div>
    );
  }

  const bound = info?.bound || {};
  const connected = Boolean(bound.connected);
  const cfgEndpoint = bound.proxy_endpoint || DEFAULT_PROXY_ENDPOINT;
  const lastErr = bound.last_error;
  const lastHb = bound.last_updated;

  return (
    <div className={`ext-panel ${connected ? 'ext-panel-connected' : 'ext-panel-disconnected'}`}>
      <div className="ext-panel-head">
        <span className="ext-panel-title">本机扩展</span>
        <span className={`ext-status-pill ${connected ? 'on' : 'off'}`}>
          {connected ? '已连接本机 daemon' : (bound.reconnecting ? '重连中…' : '未连接')}
        </span>
      </div>

      <div className="ext-panel-meta">
        <span>endpoint: <code>{cfgEndpoint}</code></span>
        {info?.version && <span>ext: v{info.version}</span>}
        {lastHb > 0 && <span>last: {formatHeartbeat(lastHb)}</span>}
      </div>

      {lastErr && <div className="ext-panel-error">最近错误: {lastErr}</div>}
      {error && <div className="ext-panel-error">{error}</div>}

      <div className="ext-panel-actions">
        <input
          className="ext-panel-input"
          value={endpoint}
          onChange={(e) => setEndpoint(e.target.value)}
          placeholder={DEFAULT_PROXY_ENDPOINT}
          disabled={Boolean(busy)}
        />
        <button
          type="button"
          className="device-primary-btn"
          onClick={handleConnect}
          disabled={Boolean(busy)}
        >
          {busy === 'connect' ? '连接中…' : (connected ? '重新连接' : '连接本机 daemon')}
        </button>
        {connected && (
          <button
            type="button"
            className="device-secondary-btn"
            onClick={handleDisconnect}
            disabled={Boolean(busy)}
          >
            {busy === 'disconnect' ? '断开中…' : '断开'}
          </button>
        )}
      </div>
    </div>
  );
}
