import React, { useCallback, useEffect, useState } from 'react';

/**
 * KimiBridgeStatus renders a one-line indicator + auto-refresh button
 * for the local kimi-webbridge daemon. The daemon listens on
 * 127.0.0.1:10086 but lacks CORS — UI must hit the server proxy at
 * /api/kimibridge/status.
 *
 * The three observable states:
 *   - available + extension_connected → green "✅ Kimi WebBridge 已就绪"
 *   - available + !extension_connected → yellow "⚠ daemon 运行中，extension 未连接"
 *   - !available → red "❌ daemon 未运行" + install command hint
 *
 * The check is auto-fired on mount and on click of the refresh button.
 * Polling intentionally omitted in v1 — the user clicks when they care.
 */
export default function KimiBridgeStatus() {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false);

  const refresh = useCallback(async () => {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/kimibridge/status', { credentials: 'include' });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const json = await res.json();
      setData(json);
    } catch (err) {
      setError(err.message || String(err));
      setData(null);
    } finally {
      setBusy(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  if (busy && !data) {
    return <span className="kimibridge-status muted">检查 Kimi WebBridge 中…</span>;
  }

  if (error) {
    return (
      <span className="kimibridge-status muted" title={error}>
        Kimi WebBridge 状态查询失败
        <button type="button" className="ghost" onClick={refresh} style={{ marginLeft: 8 }}>
          重试
        </button>
      </span>
    );
  }

  if (!data) {
    return null;
  }

  if (!data.available) {
    return (
      <div className="kimibridge-status">
        <span className="muted">❌ Kimi WebBridge daemon 未运行</span>
        <details style={{ display: 'inline-block', marginLeft: 8 }}>
          <summary className="muted" style={{ fontSize: 12, cursor: 'pointer' }}>
            如何安装
          </summary>
          <pre
            style={{
              background: '#f6f6f6',
              padding: '6px 8px',
              fontSize: 12,
              marginTop: 4,
              borderRadius: 4,
            }}
          >
            {data.daemon_install_command}
          </pre>
        </details>
        <button type="button" className="ghost" onClick={refresh} disabled={busy} style={{ marginLeft: 8 }}>
          {busy ? '…' : '重试'}
        </button>
      </div>
    );
  }

  if (!data.extension_connected) {
    return (
      <div className="kimibridge-status">
        <span className="muted" title={`daemon ${data.version} on :${data.port}`}>
          ⚠ Kimi WebBridge daemon 运行中，但 Chrome 扩展未连接
        </span>
        <a
          href={data.extension_install_url}
          target="_blank"
          rel="noopener noreferrer"
          className="muted"
          style={{ marginLeft: 8, fontSize: 12 }}
        >
          安装扩展
        </a>
        <button type="button" className="ghost" onClick={refresh} disabled={busy} style={{ marginLeft: 8 }}>
          {busy ? '…' : '重新检查'}
        </button>
      </div>
    );
  }

  return (
    <div className="kimibridge-status">
      <span
        className="muted"
        title={`daemon ${data.version} · extension ${data.extension_version || '?'} · uptime ${data.uptime_seconds}s`}
      >
        ✅ Kimi WebBridge 已就绪
      </span>
      <button type="button" className="ghost" onClick={refresh} disabled={busy} style={{ marginLeft: 8 }}>
        {busy ? '…' : '重新检查'}
      </button>
    </div>
  );
}
