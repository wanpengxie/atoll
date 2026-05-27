import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import AddDeviceDialog from './AddDeviceDialog.jsx';
import ExtensionPanel from './ExtensionPanel.jsx';

const POLL_INTERVAL_MS = 10_000;

function normalizeDaemon(row) {
  return {
    ...row,
    id: row.id || row.ID,
    owner_id: row.owner_id || row.ownerID || row.OwnerID,
    name: row.name || row.Name || '',
    api_key_prefix: row.api_key_prefix || row.apiKeyPrefix || row.APIKeyPrefix || '',
    status: row.status || row.Status || 'offline',
    hostname: row.hostname || row.Hostname || '',
    proxy_version: row.proxy_version || row.proxyVersion || row.ProxyVersion || '',
    last_heartbeat: row.last_heartbeat ?? row.lastHeartbeat ?? row.LastHeartbeat ?? 0,
    created_at: row.created_at ?? row.createdAt ?? row.CreatedAt ?? 0,
    attached_channels: row.attached_channels || row.attachedChannels || [],
    hosted_actors: row.hosted_actors || row.hostedActors || [],
  };
}

function actorIcon(actorID) {
  if (actorID.includes('kimi')) return '🧠';
  if (actorID.includes('xhs')) return '📕';
  if (actorID.includes('shell')) return '💻';
  if (actorID.includes('file')) return '📁';
  if (actorID.includes('browser')) return '🌐';
  return '🔧';
}

function formatRefreshTime(ms) {
  if (!ms) return '';
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(ms));
}

function formatHeartbeat(ms) {
  const n = Number(ms || 0);
  if (!n) return '尚无 heartbeat';
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit',
  }).format(new Date(n));
}

// MyDevicesPage is the standalone owner-scoped daemon catalog. Entry
// lives in the sidebar under "全局 · 我的设备". This is where daemons
// are created and revoked; per-channel device pages only attach /
// detach existing entries (no create surface there).
export default function MyDevicesPage({ channelsByID = {} }) {
  const [daemons, setDaemons] = useState([]);
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState('');
  const [addOpen, setAddOpen] = useState(false);
  const [busyDaemonID, setBusyDaemonID] = useState('');
  const [lastLoadedAt, setLastLoadedAt] = useState(0);

  const refresh = useCallback(async (mode = 'refresh') => {
    if (mode === 'initial') setLoading(true);
    else setRefreshing(true);
    setError('');
    try {
      const res = await api.listOwnerDaemons();
      setDaemons((res.daemons || []).map(normalizeDaemon));
      setLastLoadedAt(Date.now());
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    refresh('initial');
    const timer = window.setInterval(() => refresh('poll'), POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [refresh]);

  function channelName(id) {
    const ch = channelsByID[id];
    if (!ch) return id;
    return ch.name || ch.Name || id;
  }

  async function handleRevoke(daemon) {
    if (!daemon?.id) return;
    if (!window.confirm(`彻底删除设备「${daemon.name || daemon.id}」？\n所有 channel 的 attach 关系会一并清除。`)) return;
    setBusyDaemonID(daemon.id);
    setError('');
    try {
      await api.revokeDaemon(daemon.id);
      await refresh('refresh');
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusyDaemonID('');
    }
  }

  function handleCreated() {
    refresh('refresh');
  }

  return (
    <section className="device-page">
      <header className="device-page-header">
        <div>
          <h2>我的设备</h2>
          <span>{daemons.length} 个设备 · owner-scoped 全局视图</span>
        </div>
        <div className="device-toolbar">
          {lastLoadedAt > 0 && <span className="device-refresh-time">{formatRefreshTime(lastLoadedAt)}</span>}
          <button type="button" className="device-secondary-btn" onClick={() => refresh('refresh')} disabled={refreshing || loading}>
            {refreshing ? '刷新中...' : '刷新'}
          </button>
          <button type="button" className="device-primary-btn" onClick={() => setAddOpen(true)}>
            + 新建设备
          </button>
        </div>
      </header>

      {error && <div className="device-error">{error}</div>}

      <ExtensionPanel />

      {loading ? (
        <div className="device-empty">载入中...</div>
      ) : daemons.length === 0 ? (
        <div className="device-empty">
          <strong>还没有设备</strong>
          <p className="device-empty-hint">点击「+ 新建设备」在自己机器上装一个 proxy daemon</p>
          <button type="button" className="device-primary-btn" onClick={() => setAddOpen(true)}>+ 新建设备</button>
        </div>
      ) : (
        <div className="device-grid">
          {daemons.map((daemon) => {
            const online = daemon.status === 'online';
            return (
              <article key={daemon.id} className={`device-card ${online ? 'online' : 'offline'}`}>
                <header className="device-card-head">
                  <div className={`device-status-dot ${online ? 'online' : 'offline'}`} />
                  <div className="device-title-block">
                    <h3>{daemon.name || '未命名设备'}</h3>
                    <span>{daemon.hostname || 'hostname pending'}</span>
                  </div>
                  <span className={`device-status-badge ${online ? 'online' : 'offline'}`}>{online ? 'online' : 'offline'}</span>
                </header>

                <div className="device-meta-grid">
                  <div><span>last heartbeat</span><strong>{formatHeartbeat(daemon.last_heartbeat)}</strong></div>
                  <div><span>api key</span><strong>{daemon.api_key_prefix || '—'}</strong></div>
                  <div><span>proxy</span><strong>{daemon.proxy_version || 'unknown'}</strong></div>
                  <div><span>attached channels</span><strong>{daemon.attached_channels.length}</strong></div>
                </div>

                <div className="device-card-section">
                  <div className="device-section-title">本机插件 (adapters)</div>
                  {daemon.hosted_actors.length === 0 ? (
                    <div className="device-actor-empty">
                      {online ? '尚未上报，等首个 ready frame' : 'daemon 离线，状态未知'}
                    </div>
                  ) : (
                    <ul className="adapter-chip-list">
                      {daemon.hosted_actors.map((h) => (
                        <li key={h.actor_id} className={`adapter-chip ${online ? 'online' : 'offline'}`}>
                          <span className="adapter-icon">{actorIcon(h.actor_id)}</span>
                          <div className="adapter-meta">
                            <strong>{h.actor_id}</strong>
                            <span>{online ? '已就绪' : '未运行'}</span>
                          </div>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>

                <div className="device-card-section">
                  <div className="device-section-title">已 attach 的 channels</div>
                  {daemon.attached_channels.length === 0 ? (
                    <div className="device-actor-empty">未 attach 到任何 channel</div>
                  ) : (
                    <ul className="device-attached-list">
                      {daemon.attached_channels.map((chID) => (
                        <li key={chID}>{channelName(chID)}</li>
                      ))}
                    </ul>
                  )}
                </div>

                <footer className="device-card-actions">
                  <button
                    type="button"
                    className="device-danger-btn"
                    onClick={() => handleRevoke(daemon)}
                    disabled={busyDaemonID === daemon.id}
                  >
                    {busyDaemonID === daemon.id ? '处理中...' : '彻底删除'}
                  </button>
                </footer>
              </article>
            );
          })}
        </div>
      )}

      <AddDeviceDialog
        channelID={null}
        open={addOpen}
        onClose={() => setAddOpen(false)}
        onCreated={handleCreated}
      />
    </section>
  );
}
