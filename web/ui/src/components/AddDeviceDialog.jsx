import React, { useMemo, useState } from 'react';
import { api } from '../api.js';

function defaultServerOrigin() {
  const configured = (import.meta.env?.VITE_SERVER_URL || '').trim();
  const fallback = window.location.port === '5173' ? 'http://localhost:8832' : window.location.origin;
  const source = configured || fallback;
  try {
    const u = new URL(source, window.location.origin);
    u.search = '';
    u.hash = '';
    if (u.pathname === '/') {
      u.pathname = '';
    }
    return u.toString().replace(/\/$/, '');
  } catch {
    return 'http://localhost:8832';
  }
}

// installCommand returns the single curl-pipe-sh one-liner the user should
// paste into a Mac/Linux terminal. The server's /install/proxy.sh endpoint
// bakes the server origin into the script, so only the api-key is needed
// as an argument here. Pre-launch UX target — see install-proxy.sh.
function installCommand(apiKey, serverOrigin) {
  return `curl -fsSL ${serverOrigin}/install/proxy.sh | sh -s -- --api-key ${apiKey}`;
}

async function copyText(text) {
  if (navigator.clipboard?.writeText) {
    await navigator.clipboard.writeText(text);
    return;
  }
  const el = document.createElement('textarea');
  el.value = text;
  el.setAttribute('readonly', '');
  el.style.position = 'fixed';
  el.style.left = '-9999px';
  document.body.appendChild(el);
  el.select();
  document.execCommand('copy');
  document.body.removeChild(el);
}

export default function AddDeviceDialog({ channelID = null, open, onClose, onCreated }) {
  const [name, setName] = useState('');
  const [created, setCreated] = useState(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [copied, setCopied] = useState(false);
  const serverOrigin = useMemo(() => defaultServerOrigin(), []);

  if (!open) return null;

  const apiKey = created?.apiKey || created?.api_key || created?.APIKey || '';
  const command = apiKey ? installCommand(apiKey, serverOrigin) : '';

  function close() {
    setName('');
    setCreated(null);
    setBusy(false);
    setError('');
    setCopied(false);
    onClose?.();
  }

  async function submit(e) {
    e.preventDefault();
    const trimmed = name.trim();
    if (!trimmed) return;
    setBusy(true);
    setError('');
    try {
      // channelID set → composite create + attach to this channel.
      // channelID null → owner-scoped create only (called from the
      // standalone 我的设备 page; attach happens later in channel UI).
      const row = channelID
        ? await api.createDaemon(channelID, { name: trimmed })
        : await api.createOwnerDaemon({ name: trimmed });
      setCreated(row);
      setCopied(false);
      onCreated?.(row);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  async function copyCommand() {
    try {
      await copyText(command);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1400);
    } catch (err) {
      setError(err.message || String(err));
    }
  }

  return (
    <div className="device-dialog-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) close(); }}>
      <div className="device-dialog" role="dialog" aria-modal="true" aria-labelledby="add-device-title">
        {!created ? (
          <form onSubmit={submit}>
            <div className="device-dialog-head">
              <h3 id="add-device-title">添加设备</h3>
              <button type="button" className="device-icon-btn" onClick={close} aria-label="关闭">×</button>
            </div>
            <label className="device-field">
              <span>名称</span>
              <input
                value={name}
                onChange={(e) => setName(e.target.value)}
                autoFocus
                required
                placeholder="MacBook Pro"
              />
            </label>
            {error && <div className="device-dialog-error">{error}</div>}
            <div className="device-dialog-actions">
              <button type="button" className="device-secondary-btn" onClick={close}>取消</button>
              <button type="submit" className="device-primary-btn" disabled={busy || !name.trim()}>
                {busy ? '创建中...' : '创建'}
              </button>
            </div>
          </form>
        ) : (
          <>
            <div className="device-dialog-head">
              <h3 id="add-device-title">设备已创建</h3>
              <button type="button" className="device-icon-btn" onClick={close} aria-label="关闭">×</button>
            </div>
            <p className="device-dialog-note">在该设备的终端运行下面命令一键安装 + 启动。API Key 只显示一次，关闭后只能看到 prefix。</p>
            <div className="device-command" title={command}>{command}</div>
            {error && <div className="device-dialog-error">{error}</div>}
            <div className="device-dialog-actions">
              <button type="button" className="device-secondary-btn" onClick={close}>关闭</button>
              <button type="button" className="device-primary-btn" onClick={copyCommand}>
                {copied ? '已复制' : '复制命令'}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
