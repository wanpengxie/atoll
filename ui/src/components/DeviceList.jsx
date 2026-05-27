import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';
import AddDeviceDialog from './AddDeviceDialog.jsx';
import DeviceCard from './DeviceCard.jsx';
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

function formatRefreshTime(ms) {
  if (!ms) return '';
  return new Intl.DateTimeFormat('zh-CN', {
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(ms));
}

export default function DeviceList({ channelID, channel }) {
  // T7 attach refactor: the page shows owner's full daemon catalog as
  // checkboxes; checking attaches to current channel, unchecking detaches.
  // The legacy "channel-scoped per-daemon create" UX is moved to a single
  // "+ 新建设备" button at the bottom that does composite create+attach.
  const [daemons, setDaemons] = useState([]); // owner's full daemon list
  const [actors, setActors] = useState([]); // actors active on current channel
  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [busyDaemonID, setBusyDaemonID] = useState('');
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
        api.listOwnerDaemons(),
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
    setError('');
    setLastLoadedAt(0);
    if (!channelID) return undefined;
    refresh('initial');
    const timer = window.setInterval(() => refresh('poll'), POLL_INTERVAL_MS);
    return () => window.clearInterval(timer);
  }, [channelID, refresh]);

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

  function isAttached(daemon) {
    return (daemon.attached_channels || []).includes(channelID);
  }

  async function handleToggleAttach(daemon) {
    if (!channelID || !daemon?.id) return;
    setBusyDaemonID(daemon.id);
    setError('');
    try {
      if (isAttached(daemon)) {
        await api.detachDaemon(channelID, daemon.id);
      } else {
        await api.attachDaemons(channelID, [daemon.id]);
      }
      await refresh('refresh');
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusyDaemonID('');
    }
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

  function handleCreated(row) {
    // Composite create response already attaches to current channel.
    const daemon = normalizeDaemon(row);
    setDaemons((prev) => {
      const without = prev.filter((d) => d.id !== daemon.id);
      const merged = { ...daemon, attached_channels: [channelID, ...(daemon.attached_channels || []).filter((c) => c !== channelID)] };
      return [merged, ...without];
    });
    refresh('refresh');
  }

  if (!channelID) {
    return (
      <section className="device-page">
        <div className="device-empty">从左侧选择一个 channel</div>
      </section>
    );
  }

  const attached = daemons.filter(isAttached);
  const detached = daemons.filter((d) => !isAttached(d));

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
        <>
          <h3 className="device-section-title">已 attach 到本 channel ({attached.length})</h3>
          {attached.length === 0 ? (
            <div className="device-empty-inline">还没有任何设备 attach 到这个 channel，可以从下面已有设备勾选</div>
          ) : (
            <div className="device-grid">
              {attached.map((daemon) => (
                <DeviceCard
                  key={daemon.id}
                  daemon={daemon}
                  actors={actorsByDaemon.get(daemon.id) || []}
                  attached
                  onToggleAttach={() => handleToggleAttach(daemon)}
                  onRevoke={() => handleRevoke(daemon)}
                  revoking={busyDaemonID === daemon.id}
                />
              ))}
            </div>
          )}

          {detached.length > 0 && (
            <>
              <h3 className="device-section-title">我的其他设备（未 attach 本 channel, {detached.length}）</h3>
              <div className="device-grid">
                {detached.map((daemon) => (
                  <DeviceCard
                    key={daemon.id}
                    daemon={daemon}
                    actors={[]}
                    attached={false}
                    onToggleAttach={() => handleToggleAttach(daemon)}
                    onRevoke={() => handleRevoke(daemon)}
                    revoking={busyDaemonID === daemon.id}
                  />
                ))}
              </div>
            </>
          )}
        </>
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
