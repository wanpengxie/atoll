import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import AddDeviceDialog from './AddDeviceDialog.jsx';
import DeviceCard from './DeviceCard.jsx';

const POLL_INTERVAL_MS = 10_000;

function normalizeDaemon(row) {
  return {
    ...row,
    id: row.id || row.ID,
    channel_id: row.channel_id || row.channelID || row.ChannelID,
    owner_id: row.owner_id || row.ownerID || row.OwnerID,
    name: row.name || row.Name || '',
    api_key_prefix: row.api_key_prefix || row.apiKeyPrefix || row.APIKeyPrefix || '',
    status: row.status || row.Status || 'offline',
    hostname: row.hostname || row.Hostname || '',
    proxy_version: row.proxy_version || row.proxyVersion || row.ProxyVersion || '',
    last_heartbeat: row.last_heartbeat ?? row.lastHeartbeat ?? row.LastHeartbeat ?? 0,
    created_at: row.created_at ?? row.createdAt ?? row.CreatedAt ?? 0,
  };
}

function normalizeActor(row) {
  return {
    ...row,
    actor_id: row.actor_id || row.actorID || row.ActorID,
    display_name: row.display_name || row.displayName || row.DisplayName || '',
    daemon_id: row.daemon_id || row.daemonID || row.DaemonID || '',
    daemon_name: row.daemon_name || row.daemonName || row.DaemonName || '',
    ready: Boolean(row.ready ?? row.Ready),
    ready_reason: row.ready_reason || row.readyReason || row.ReadyReason || 'unknown',
    ready_detail: row.ready_detail || row.readyDetail || row.ReadyDetail,
    types: row.types || row.Types || [],
  };
}

function mergeRevoked(daemons, revoked) {
  const byID = new Map();
  for (const daemon of daemons) {
    const local = revoked[daemon.id];
    byID.set(daemon.id, local ? { ...daemon, ...local, status: 'offline', revoked: true } : daemon);
  }
  for (const daemon of Object.values(revoked)) {
    if (!byID.has(daemon.id)) {
      byID.set(daemon.id, daemon);
    }
  }
  return Array.from(byID.values()).sort((a, b) => Number(b.created_at || 0) - Number(a.created_at || 0));
}

function formatRefreshTime(ms) {
  if (!ms) return '';
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(ms));
}

export default function DeviceList({ channelID, channel }) {
  const [daemons, setDaemons] = useState([]);
  const [actors, setActors] = useState([]);
  const [revokedDaemons, setRevokedDaemons] = useState({});
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [revokingID, setRevokingID] = useState('');
  const [error, setError] = useState('');
  const [addOpen, setAddOpen] = useState(false);
  const [lastLoadedAt, setLastLoadedAt] = useState(0);

  const refresh = useCallback(async (mode = 'refresh') => {
    if (!channelID) return;
    if (mode === 'initial') setLoading(true);
    else setRefreshing(true);
    setError('');
    try {
      const [daemonRes, actorRes] = await Promise.all([
        api.listDaemons(channelID),
        api.listActors(channelID),
      ]);
      setDaemons((daemonRes.daemons || []).map(normalizeDaemon));
      setActors((actorRes.actors || []).map(normalizeActor));
      setLastLoadedAt(Date.now());
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, [channelID]);

  useEffect(() => {
    setDaemons([]);
    setActors([]);
    setRevokedDaemons({});
    setError('');
    setLastLoadedAt(0);
    if (!channelID) return undefined;
    refresh('initial');
    const timer = window.setInterval(() => refresh('poll'), POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [channelID, refresh]);

  const visibleDaemons = useMemo(
    () => mergeRevoked(daemons, revokedDaemons),
    [daemons, revokedDaemons],
  );

  const actorsByDaemon = useMemo(() => {
    const out = new Map();
    for (const actor of actors) {
      if (!actor.daemon_id) continue;
      const list = out.get(actor.daemon_id) || [];
      list.push(actor);
      out.set(actor.daemon_id, list);
    }
    return out;
  }, [actors]);

  function handleCreated(row) {
    const daemon = normalizeDaemon(row);
    setDaemons((prev) => [daemon, ...prev.filter((item) => item.id !== daemon.id)]);
    setRevokedDaemons((prev) => {
      const next = { ...prev };
      delete next[daemon.id];
      return next;
    });
    refresh('refresh');
  }

  async function handleRevoke(daemon) {
    if (!channelID || !daemon?.id) return;
    if (!window.confirm(`撤销设备「${daemon.name || daemon.id}」？`)) return;
    setRevokingID(daemon.id);
    setError('');
    try {
      await api.revokeDaemon(channelID, daemon.id);
      const offline = { ...daemon, status: 'offline', revoked: true };
      setRevokedDaemons((prev) => ({ ...prev, [daemon.id]: offline }));
      setDaemons((prev) => prev.map((item) => (item.id === daemon.id ? offline : item)));
      setActors((prev) => prev.filter((actor) => actor.daemon_id !== daemon.id));
      await refresh('refresh');
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setRevokingID('');
    }
  }

  if (!channelID) {
    return (
      <section className="device-page">
        <div className="device-empty">从左侧选择一个 channel</div>
      </section>
    );
  }

  return (
    <section className="device-page">
      <header className="device-page-header">
        <div>
          <h2>我的设备</h2>
          <span>{channel?.name || channel?.Name || channelID}</span>
        </div>
        <div className="device-toolbar">
          {lastLoadedAt > 0 && <span className="device-refresh-time">{formatRefreshTime(lastLoadedAt)}</span>}
          <button type="button" className="device-secondary-btn" onClick={() => refresh('refresh')} disabled={refreshing || loading}>
            {refreshing ? '刷新中...' : '刷新'}
          </button>
          <button type="button" className="device-primary-btn" onClick={() => setAddOpen(true)}>
            添加设备
          </button>
        </div>
      </header>

      {error && <div className="device-error">{error}</div>}

      {loading ? (
        <div className="device-empty">载入中...</div>
      ) : visibleDaemons.length === 0 ? (
        <div className="device-empty">
          <strong>还没有设备</strong>
          <button type="button" className="device-primary-btn" onClick={() => setAddOpen(true)}>添加设备</button>
        </div>
      ) : (
        <div className="device-grid">
          {visibleDaemons.map((daemon) => (
            <DeviceCard
              key={daemon.id}
              daemon={daemon}
              actors={actorsByDaemon.get(daemon.id) || []}
              onRevoke={handleRevoke}
              revoking={revokingID === daemon.id}
            />
          ))}
        </div>
      )}

      <AddDeviceDialog
        channelID={channelID}
        open={addOpen}
        onClose={() => setAddOpen(false)}
        onCreated={handleCreated}
      />
    </section>
  );
}
