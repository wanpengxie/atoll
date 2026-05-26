import React from 'react';

function formatTime(ms) {
  const n = Number(ms || 0);
  if (!n) return '尚无 heartbeat';
  return new Intl.DateTimeFormat('zh-CN', {
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(n));
}

function readiness(actor) {
  if (actor.ready) {
    return { className: 'ready', label: 'ready' };
  }
  const reason = actor.ready_reason || actor.readyReason || 'unknown';
  if (reason === 'unknown') {
    return { className: 'unknown', label: 'unknown' };
  }
  return { className: 'not-ready', label: reason };
}

function actorTypes(actor) {
  const rows = Array.isArray(actor.types) ? actor.types : [];
  return rows.map((t) => t.type || t.Type).filter(Boolean);
}

export default function DeviceCard({ daemon, actors, onRevoke, revoking }) {
  const online = daemon.status === 'online';
  const revoked = Boolean(daemon.revoked);
  const statusLabel = revoked ? '已撤销' : (online ? 'online' : 'offline');

  return (
    <article className={`device-card ${online ? 'online' : 'offline'} ${revoked ? 'revoked' : ''}`}>
      <header className="device-card-head">
        <div className={`device-status-dot ${online ? 'online' : 'offline'}`} />
        <div className="device-title-block">
          <h3>{daemon.name || '未命名设备'}</h3>
          <span>{daemon.hostname || 'hostname pending'}</span>
        </div>
        <span className={`device-status-badge ${online ? 'online' : 'offline'}`}>{statusLabel}</span>
      </header>

      <div className="device-meta-grid">
        <div>
          <span>actors</span>
          <strong>{actors.length}</strong>
        </div>
        <div>
          <span>last heartbeat</span>
          <strong>{formatTime(daemon.last_heartbeat)}</strong>
        </div>
        <div>
          <span>api key</span>
          <strong>{daemon.api_key_prefix || 'prefix pending'}</strong>
        </div>
        <div>
          <span>proxy</span>
          <strong>{daemon.proxy_version || 'unknown'}</strong>
        </div>
      </div>

      <div className="device-card-section">
        <div className="device-section-title">Host actors</div>
        {actors.length === 0 ? (
          <div className="device-actor-empty">无 active actor</div>
        ) : (
          <ul className="device-actor-list">
            {actors.map((actor) => {
              const state = readiness(actor);
              const types = actorTypes(actor);
              return (
                <li key={actor.actor_id} className="device-actor-row">
                  <div className="device-actor-main">
                    <strong>{actor.display_name || actor.actor_id}</strong>
                    <span>{actor.actor_id}</span>
                  </div>
                  <div className="device-actor-side">
                    <span className={`readiness-badge ${state.className}`}>{state.label}</span>
                    {types.length > 0 && <span className="device-type-count">{types.length} types</span>}
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </div>

      <footer className="device-card-actions">
        <button
          type="button"
          className="device-danger-btn"
          onClick={() => onRevoke?.(daemon)}
          disabled={revoking || revoked}
        >
          {revoking ? '撤销中...' : (revoked ? '已撤销' : '撤销')}
        </button>
      </footer>
    </article>
  );
}
