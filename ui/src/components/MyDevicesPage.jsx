import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { ChannelSocket } from '../ws.js';
import {
  isProvisionalResponse,
  isFinalResponse,
  provisionalDisplay,
} from '../protocol.js';
import AddDeviceDialog from './AddDeviceDialog.jsx';
import ExtensionPanel from './ExtensionPanel.jsx';

const POLL_INTERVAL_MS = 15_000;

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

function actorIDOf(row) {
  return row.actor_id || row.actorID || row.ActorID || '';
}

function activeChannelsOf(row) {
  return row.active_channels || row.activeChannels || row.ActiveChannels || [];
}

function facadeStateOf(row) {
  return row.facade_state || row.facadeState || row.FacadeState || 'unknown';
}

function facadeInstalled(row) {
  return row.facade_installed === true || row.facadeInstalled === true || row.FacadeInstalled === true || facadeStateOf(row) === 'installed';
}

function actorChipState(daemon, hostedActor) {
  const actorID = actorIDOf(hostedActor);
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
  const routeActive =
    hostedActor.route_active === true ||
    hostedActor.routeActive === true ||
    hostedActor.RouteActive === true ||
    activeChannelsOf(hostedActor).length > 0;
  const facadeReady = facadeInstalled(hostedActor);
  const facadeState = facadeStateOf(hostedActor);
  const facadeDetail = hostedActor.facade_detail || hostedActor.facadeDetail || hostedActor.FacadeDetail || '';
  const callable =
    (hostedActor.callable === true ||
      hostedActor.Callable === true ||
      (daemon.status === 'online' && routeActive && facadeReady && actorReady)) &&
    daemon.status === 'online' &&
    routeActive &&
    facadeReady &&
    actorReady;
  if (daemon.status !== 'online') {
    return { actorID, state: 'offline', label: 'daemon 离线' };
  }
  if ((daemon.attached_channels || []).length === 0) {
    return { actorID, state: 'unbound', label: '未绑定 channel' };
  }
  if (!routeActive) {
    return {
      actorID,
      state: 'unbound',
      label: '路由同步中',
      checkedAt,
      detail: '已 attach，等待 daemon ready route',
    };
  }
  if (!facadeReady) {
    if (facadeState === 'failed') {
      return {
        actorID,
        state: 'not-ready',
        label: 'facade 安装失败',
        checkedAt,
        detail: facadeDetail || 'update_members rejected',
      };
    }
    return {
      actorID,
      state: 'unbound',
      label: 'facade 安装中',
      checkedAt,
      detail: facadeState,
    };
  }
  if (callable) {
    return {
      actorID,
      state: 'ready',
      label: '可调用',
      checkedAt,
      detail: readyReason && readyReason !== 'ok' ? readyReason : '',
    };
  }
  if (readyState === 'unknown') {
    return {
      actorID,
      state: 'unknown',
      label: '状态未知',
      checkedAt,
    };
  }
  return {
    actorID,
    state: 'not-ready',
    label: `不可用${readyReason ? ` · ${readyReason}` : ''}`,
    checkedAt,
    detail: readyReason || 'not_ready',
  };
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
      const list = (res.daemons || []).map(normalizeDaemon);
      setDaemons(list);
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

  // In-flight provisional state per (channel × parent_id).
  //
  // Tracks "request <parent_id> is mid-flight" so device cards can show
  // a small spinner badge with the last-known status (queued /
  // processing / <adapter>.<name>). When a final response arrives for
  // the request (parent_id), the entry is dropped — regardless of who
  // sent the final.
  //
  // Indexed by parent_id (not sender) because the system fallback
  // closure (proto-foundation.md §1.6.3 / §1.6.5) emits a final whose
  // sender is the system actor, not the original adapter that emitted
  // the provisional ticks. A sender-keyed map would never find the
  // adapter's entry → spinner stuck forever.
  //
  // The original adapter sender is stored as a field of the entry so
  // the per-actor projection still knows which device chip to badge.
  //
  // Map<channelID, Map<parent_id, { sender, lastStatus, lastSeen }>>
  const [inFlight, setInFlight] = useState(() => new Map());

  // Subscribe to all attached channels for the current owner so device
  // cards can observe provisional ticks even when the user isn't
  // looking at any specific channel chat. One WS connection covers all
  // subscriptions thanks to ChannelSocket multiplexing.
  const socketRef = useRef(null);
  const subscribedChannels = useMemo(() => {
    const s = new Set();
    for (const d of daemons) for (const ch of d.attached_channels || []) s.add(ch);
    return s;
  }, [daemons]);

  useEffect(() => {
    const socket = new ChannelSocket((chID, _seq, envelope) => {
      if (!envelope) return;
      const isProv = isProvisionalResponse(envelope);
      const isFinal = isFinalResponse(envelope);
      if (!isProv && !isFinal) return;
      const pid = envelope.parent_id || envelope.parentID || '';
      if (!pid) return; // without parent_id we can't correlate prov→final
      const senderID = envelope.sender?.id || envelope.sender_id || '';

      setInFlight((prev) => {
        const next = new Map(prev);
        const chMap = new Map(next.get(chID) || new Map());
        if (isFinal) {
          // Drop the in-flight entry for this request regardless of who
          // sent the final. System fallback closure (proto-foundation
          // §1.6.3/§1.6.5) lands here with sender=system; the adapter
          // that originally produced the provisional ticks must still
          // get its spinner cleared.
          chMap.delete(pid);
        } else {
          if (!senderID) return prev; // provisional must identify the adapter
          const existing = chMap.get(pid);
          chMap.set(pid, {
            // Sender is captured from the first provisional tick and
            // kept stable thereafter — the adapter doing the work
            // shouldn't change mid-request.
            sender: existing?.sender || senderID,
            lastStatus: envelope.payload?.status || envelope.payload?.Status || '',
            lastSeen: Date.now(),
          });
        }
        if (chMap.size === 0) next.delete(chID);
        else next.set(chID, chMap);
        return next;
      });
    });
    socketRef.current = socket;
    socket.start();
    return () => {
      socket.stop?.();
      socketRef.current = null;
    };
  }, []);

  useEffect(() => {
    const socket = socketRef.current;
    if (!socket) return;
    for (const chID of subscribedChannels) socket.subscribe(chID);
    return () => {
      for (const chID of subscribedChannels) socket.unsubscribe(chID);
    };
  }, [subscribedChannels]);

  // Aggregate in-flight state per actor_id across all channels. A device
  // card chip is "busy" if any of its attached channels has a live
  // in-flight request whose provisional ticks came from that actor;
  // the headline shows the most recent status (regardless of channel
  // and parent_id).
  const inFlightByActor = useMemo(() => {
    const out = new Map();
    for (const [, chMap] of inFlight) {
      for (const [, entry] of chMap) {
        const senderID = entry.sender;
        if (!senderID) continue;
        const existing = out.get(senderID);
        if (!existing || entry.lastSeen > existing.lastSeen) {
          out.set(senderID, entry);
        }
      }
    }
    return out;
  }, [inFlight]);

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

      <div className="ext-downloads-bar">
        <div className="ext-downloads-title">浏览器扩展</div>
        <div className="ext-downloads-links">
          <a
            className="ext-download-card"
            href="/downloads/coagent-extension.zip"
            download
          >
            <strong>coagent-xhs 扩展</strong>
            <span>下载后在 chrome://extensions 加载；默认连接本机 proxy daemon</span>
          </a>
          <a
            className="ext-download-card"
            href="https://chromewebstore.google.com/detail/kimi-webbridge/fldmhceldgbpfpkbgopacenieobmligc"
            target="_blank"
            rel="noopener noreferrer"
          >
            <strong>Kimi WebBridge 扩展</strong>
            <span>Chrome Web Store；由 coagent-proxy 的 Kimi adapter 接入</span>
          </a>
        </div>
        <p className="ext-downloads-note">
          设备页创建/删除 proxy daemon；channel 顶部「绑定设备」只负责把 daemon attach 到当前 channel。
          xhs 扩展连接本机 endpoint，不在云端页面里配 token。
        </p>
      </div>

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
                      {online ? '尚未上报 adapter manifest' : 'daemon 离线，状态未知'}
                    </div>
                  ) : (
                    <ul className="adapter-chip-list">
                      {daemon.hosted_actors.map((h) => {
                        const chip = actorChipState(daemon, h);
                        const busy = inFlightByActor.get(chip.actorID);
                        const busyDisplay = busy ? provisionalDisplay(busy.lastStatus) : null;
                        return (
                          <li
                            key={chip.actorID}
                            className={`adapter-chip ${chip.state}${busy ? ' in-flight' : ''}`}
                          >
                            <span className="adapter-icon">{actorIcon(chip.actorID)}</span>
                            <div className="adapter-meta">
                              <strong>{chip.actorID}</strong>
                              <span>{chip.label}</span>
                              {busyDisplay && (
                                <span className="adapter-inflight">
                                  <span aria-hidden="true">{busyDisplay.icon}</span>
                                  <span>{busyDisplay.label}</span>
                                </span>
                              )}
                              {chip.checkedAt > 0 && (
                                <span className="adapter-checkedat">
                                  checked · {formatHeartbeat(chip.checkedAt)}
                                </span>
                              )}
                              {chip.detail && <span className="adapter-checkedat">{chip.detail}</span>}
                            </div>
                          </li>
                        );
                      })}
                    </ul>
                  )}
                  {daemon.hosted_actors.length > 0 && online && daemon.attached_channels.length === 0 && (
                    <p className="device-section-note">
                      这台 daemon 尚未绑定到 channel；进入 channel 顶部「绑定设备」后，agent 才能调用这些 adapter。
                    </p>
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
