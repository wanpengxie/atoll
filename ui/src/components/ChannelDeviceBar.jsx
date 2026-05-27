import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { api } from '../api.js';

const POLL_INTERVAL_MS = 10_000;

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

function normalizeActor(row) {
  return {
    actor_id: row.actor_id || row.actorID || row.ActorID,
    ready: Boolean(row.ready ?? row.Ready),
    ready_reason: row.ready_reason || row.readyReason || 'unknown',
    daemon_id: row.daemon_id || row.daemonID || row.DaemonID || '',
  };
}

// ChannelDeviceBar = chat header status strip + bind-device button.
// Shows one chip per adapter that's currently active in this channel
// (driven by /api/channels/:chID/actors), with color reflecting ready
// state. The「绑定设备」button opens a drawer where the user picks
// owner daemons to attach/detach for this channel.
export default function ChannelDeviceBar({ channelID }) {
  const [attached, setAttached] = useState([]); // daemons attached to this channel
  const [actors, setActors] = useState([]);
  const [ownerDaemons, setOwnerDaemons] = useState([]); // all owner daemons (for attach modal)
  const [bindOpen, setBindOpen] = useState(false);
  const [busy, setBusy] = useState(''); // daemon_id being toggled
  const [error, setError] = useState('');

  const refresh = useCallback(async () => {
    if (!channelID) return;
    try {
      const [attachedRes, actorRes, ownerRes] = await Promise.all([
        api.listDaemons(channelID),
        api.listActors(channelID),
        api.listOwnerDaemons(),
      ]);
      setAttached((attachedRes.daemons || []).map(normalizeDaemon));
      setActors((actorRes.actors || []).map(normalizeActor));
      setOwnerDaemons((ownerRes.daemons || []).map(normalizeDaemon));
    } catch (err) {
      setError(err.message || String(err));
    }
  }, [channelID]);

  useEffect(() => {
    if (!channelID) return undefined;
    refresh();
    const t = window.setInterval(refresh, POLL_INTERVAL_MS);
    return () => window.clearInterval(t);
  }, [channelID, refresh]);

  const chips = useMemo(() => {
    // Show one chip per actor present on the channel + daemon online state.
    const daemonByID = new Map(attached.map((d) => [d.id, d]));
    return actors
      .filter((a) => a.actor_id && a.daemon_id)
      .map((a) => {
        const daemon = daemonByID.get(a.daemon_id);
        const online = daemon?.status === 'online';
        return {
          key: `${a.daemon_id}:${a.actor_id}`,
          actor_id: a.actor_id,
          ready: a.ready,
          online,
          reason: a.ready_reason,
          daemon_name: daemon?.name || a.daemon_id,
        };
      });
  }, [actors, attached]);

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
          chips.map((chip) => {
            const cls = chip.ready ? 'ready' : (chip.online ? 'not-ready' : 'offline');
            const tip = chip.ready
              ? `${chip.actor_id} ready · via ${chip.daemon_name}`
              : `${chip.actor_id} not ready (${chip.reason}) · via ${chip.daemon_name}`;
            return (
              <span key={chip.key} className={`channel-device-chip ${cls}`} title={tip}>
                <span className="adapter-icon">{actorIcon(chip.actor_id)}</span>
                <span>{chip.actor_id}</span>
              </span>
            );
          })
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
                  const attached = isAttached(d.id);
                  const online = d.status === 'online';
                  return (
                    <li key={d.id} className={`bind-daemon-row ${attached ? 'attached' : ''}`}>
                      <label className="bind-checkbox">
                        <input
                          type="checkbox"
                          checked={attached}
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
                            <span key={h.actor_id} className="adapter-icon-small" title={h.actor_id}>
                              {actorIcon(h.actor_id)}
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
