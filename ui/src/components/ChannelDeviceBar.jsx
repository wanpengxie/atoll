import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';

const REFRESH_MS = 15_000;

function actorIcon(actorID) {
  if (actorID.includes('kimi')) return '🧠';
  if (actorID.includes('xhs')) return '📕';
  if (actorID.includes('shell')) return '💻';
  if (actorID.includes('file')) return '📁';
  if (actorID.includes('browser')) return '🌐';
  return '🔧';
}

function normalizeDaemon(row) {
  return {
    id: row.id || row.ID,
    name: row.name || row.Name || '',
    status: row.status || row.Status || 'offline',
    hosted_actors: row.hosted_actors || row.hostedActors || [],
    attached_channels: row.attached_channels || row.attachedChannels || [],
  };
}

function actorIDOf(row) {
  return row.actor_id || row.actorID || row.ActorID || '';
}

function activeChannelsOf(row) {
  return row.active_channels || row.activeChannels || row.ActiveChannels || [];
}

function routeActiveFor(hostedActor, channelID) {
  const active = activeChannelsOf(hostedActor);
  if (active.length > 0) return active.includes(channelID);
  return hostedActor.route_active === true || hostedActor.routeActive === true || hostedActor.RouteActive === true;
}

function facadeStateOf(row) {
  return row.facade_state || row.facadeState || row.FacadeState || 'unknown';
}

function facadeInstalled(row) {
  return row.facade_installed === true || row.facadeInstalled === true || row.FacadeInstalled === true || facadeStateOf(row) === 'installed';
}

function statusLabel(hostedActor, online, channelID) {
  if (!online) return { state: 'offline', label: 'daemon 离线' };
  const readyState = hostedActor.ready_state || hostedActor.readyState || hostedActor.ReadyState || 'unknown';
  const readyReason = hostedActor.ready_reason || hostedActor.readyReason || hostedActor.ReadyReason || '';
  const checkedAt = Number(
    hostedActor.readiness_checked_at ||
    hostedActor.readinessCheckedAt ||
    hostedActor.ReadinessCheckedAt ||
    0
  );
  const actorReady =
    hostedActor.actor_readiness_ready === true ||
    hostedActor.actorReadinessReady === true ||
    hostedActor.ActorReadinessReady === true ||
    hostedActor.ready === true ||
    hostedActor.Ready === true ||
    readyState === 'ready';
  const routeActive = routeActiveFor(hostedActor, channelID);
  const facadeReady = facadeInstalled(hostedActor);
  const facadeState = facadeStateOf(hostedActor);
  const facadeDetail = hostedActor.facade_detail || hostedActor.facadeDetail || hostedActor.FacadeDetail || '';
  const callable =
    (hostedActor.callable === true || hostedActor.Callable === true || (actorReady && routeActive && facadeReady)) &&
    actorReady &&
    routeActive &&
    facadeReady;
  if (callable) {
    return {
      state: 'ready',
      label: '可调用',
      detail: readyReason && readyReason !== 'ok' ? readyReason : '',
      checkedAt,
    };
  }
  if (!routeActive) {
    return { state: 'unbound', label: '路由同步中', detail: 'daemon 已绑定，adapter route 尚未 active', checkedAt };
  }
  if (!facadeReady) {
    if (facadeState === 'failed') {
      return { state: 'not-ready', label: 'facade 安装失败', detail: facadeDetail || 'update_members rejected', checkedAt };
    }
    return { state: 'unbound', label: 'facade 安装中', detail: facadeState, checkedAt };
  }
  if (readyState === 'unknown') {
    return { state: 'unknown', label: '状态未知', checkedAt };
  }
  return {
    state: 'not-ready',
    label: readyReason ? `不可用 · ${readyReason}` : '不可用',
    detail: readyReason || 'not_ready',
    checkedAt,
  };
}

// ChannelDeviceBar renders adapter readiness from the server-side projection
// of actor.readiness.changed events. The browser does not probe adapters.
export default function ChannelDeviceBar({ channelID }) {
  const [attached, setAttached] = useState([]);            // daemons attached to this channel
  const [ownerDaemons, setOwnerDaemons] = useState([]);    // for attach modal
  const [bindOpen, setBindOpen] = useState(false);
  const [busy, setBusy] = useState('');                    // daemon_id being toggled
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    if (!channelID) return [];
    try {
      const ownerRes = await api.listOwnerDaemons();
      const owner = (ownerRes.daemons || []).map(normalizeDaemon);
      const att = owner.filter((d) => (d.attached_channels || []).includes(channelID));
      setAttached(att);
      setOwnerDaemons(owner);
      return att;
    } catch (err) {
      setError(err.message || String(err));
      return [];
    }
  }, [channelID]);

  useEffect(() => {
    if (!channelID) return undefined;
    refresh();
    const t = window.setInterval(refresh, REFRESH_MS);
    return () => window.clearInterval(t);
  }, [channelID, refresh]);

  const chips = useMemo(() => {
    const out = [];
    for (const d of attached) {
      for (const h of d.hosted_actors || []) {
        const actorID = actorIDOf(h);
        if (!actorID) continue;
        const key = `${d.id}:${actorID}`;
        const online = d.status === 'online';
        const live = statusLabel(h, online, channelID);
        out.push({
          key,
          actor_id: actorID,
          state: live.state,
          label: live.label,
          detail: live.detail,
          checkedAt: live.checkedAt,
          daemon_name: d.name || d.id,
        });
      }
    }
    return out;
  }, [attached]);

  function isAttached(daemonID) {
    return attached.some((d) => d.id === daemonID);
  }

  async function toggle(daemon) {
    if (!channelID || !daemon?.id) return;
    setBusy(daemon.id);
    setError('');
    try {
      if (isAttached(daemon.id)) {
        await api.detachDaemon(channelID, daemon.id);
      } else {
        await api.attachDaemons(channelID, [daemon.id]);
      }
      await refresh();
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy('');
    }
  }

  if (!channelID) return null;

  return (
    <div className="channel-device-bar">
      <div className="channel-device-chips">
        {chips.length === 0 ? (
          <span className="channel-device-empty muted">未绑定设备 / 无 adapter</span>
        ) : (
          chips.map((chip) => (
            <span key={chip.key} className={`channel-device-chip ${chip.state}`}>
              <span className="adapter-icon">{actorIcon(chip.actor_id)}</span>
              <span className="chip-actor">{chip.actor_id}</span>
              <span className="chip-state">· {chip.label}</span>
              <span className="chip-pop">
                <strong>{chip.actor_id}</strong>
                <span>状态: {chip.label}</span>
                <span>来自: {chip.daemon_name}</span>
                {chip.checkedAt > 0 && <span>checked: {new Date(chip.checkedAt).toLocaleTimeString('zh-CN')}</span>}
                {chip.detail && <span className="muted">{chip.detail}</span>}
                <span className="muted">daemon online + channel route + facade ack + actor.readiness.changed 投影</span>
              </span>
            </span>
          ))
        )}
      </div>
      <button type="button" className="device-secondary-btn" onClick={() => setBindOpen(true)}>
        绑定设备
      </button>

      {bindOpen && (
        <div className="device-dialog-overlay" onMouseDown={(e) => { if (e.target === e.currentTarget) setBindOpen(false); }}>
          <div className="device-dialog device-bind-dialog" role="dialog" aria-modal="true">
            <div className="device-dialog-head">
              <h3>绑定设备到本 channel</h3>
              <button type="button" className="device-icon-btn" onClick={() => setBindOpen(false)} aria-label="关闭">×</button>
            </div>
            <p className="device-dialog-note">
              勾选要绑定的设备。新设备到左侧「我的设备」页面创建。
            </p>
            {error && <div className="device-dialog-error">{error}</div>}
            {ownerDaemons.length === 0 ? (
              <div className="device-empty-inline">
                还没有任何设备。<br />
                先到左侧「我的设备」页面新建一台 proxy daemon。
              </div>
            ) : (
              <ul className="bind-daemon-list">
                {ownerDaemons.map((d) => {
                  const attachedNow = isAttached(d.id);
                  const online = d.status === 'online';
                  return (
                    <li key={d.id} className={`bind-daemon-row ${attachedNow ? 'attached' : ''}`}>
                      <label className="bind-checkbox">
                        <input
                          type="checkbox"
                          checked={attachedNow}
                          disabled={busy === d.id}
                          onChange={() => toggle(d)}
                        />
                        <div className="bind-daemon-info">
                          <strong>{d.name || d.id}</strong>
                          <span className={`device-status-badge ${online ? 'online' : 'offline'}`}>
                            {online ? 'online' : 'offline'}
                          </span>
                        </div>
                        <div className="bind-daemon-actors">
                          {(d.hosted_actors || []).map((h) => (
                            <span key={actorIDOf(h)} className="adapter-icon-small" title={actorIDOf(h)}>
                              {actorIcon(actorIDOf(h))}
                            </span>
                          ))}
                        </div>
                      </label>
                    </li>
                  );
                })}
              </ul>
            )}
            <div className="device-dialog-actions">
              <button type="button" className="device-secondary-btn" onClick={() => setBindOpen(false)}>关闭</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
