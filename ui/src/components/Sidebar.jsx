import React, { useState } from 'react';

function id(o) {
  return o.id || o.ID;
}

export default function Sidebar({
  me,
  workspaces,
  activeWorkspaceID,
  channels,
  activeChannelID,
  onSelectWorkspace,
  onSelectChannel,
  onCreateWorkspace,
  onCreateChannel,
  onLogout,
}) {
  return (
    <aside id="sidebar">
      <div id="sidebar-header">
        <div className="sidebar-logo-mark">
          <svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" /></svg>
        </div>
        <span className="sidebar-logo-text">coagent</span>
      </div>

      <div className="sidebar-body">
        <SidebarSection
          label="Workspaces"
          createPlaceholder="workspace 名称"
          onCreate={async (name) => onCreateWorkspace(name)}
          items={workspaces}
          activeID={activeWorkspaceID}
          onSelect={onSelectWorkspace}
          renderItem={(w) => <span>{w.name || w.Name}</span>}
        />

        <SidebarSection
          label="Channels"
          createPlaceholder="channel 名称"
          createExtra={{
            kind: 'select',
            default: 'group',
            options: [
              { value: 'group', label: 'Group · 通用' },
              { value: 'xhs-creator', label: 'XHS Creator · 小红书发布' },
            ],
          }}
          onCreate={async (name, extra) => onCreateChannel(name, extra || 'group')}
          items={channels}
          activeID={activeChannelID}
          onSelect={onSelectChannel}
          renderItem={(c) => (
            <span>
              {c.name || c.Name}
              {c.type || c.Type ? (
                <span className="muted" style={{ marginLeft: 6, fontSize: 11 }}>
                  · {c.type || c.Type}
                </span>
              ) : null}
            </span>
          )}
        />
      </div>

      <div id="user-badge">
        <div className="user-avatar" title={me.email}>
          {(me.display_name || me.email || '?').slice(0, 1).toUpperCase()}
        </div>
        <strong id="me-name">{me.display_name || me.email}</strong>
        <button id="btn-logout" title="登出" onClick={onLogout}>⎋</button>
      </div>
    </aside>
  );
}

function SidebarSection({
  label,
  items,
  activeID,
  onSelect,
  renderItem,
  onCreate,
  createPlaceholder,
  createExtra,
}) {
  // Normalize createExtra: string => legacy text input; object => typed field.
  const extraSpec = (() => {
    if (!createExtra) return null;
    if (typeof createExtra === 'string') {
      return { kind: 'text', placeholder: createExtra };
    }
    return createExtra;
  })();
  const extraInitial = extraSpec && extraSpec.kind === 'select' ? (extraSpec.default || '') : '';

  const [adding, setAdding] = useState(false);
  const [name, setName] = useState('');
  const [extra, setExtra] = useState(extraInitial);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  function openForm() {
    setExtra(extraInitial);
    setName('');
    setError('');
    setAdding(true);
  }

  function closeForm() {
    setAdding(false);
  }

  async function submit(e) {
    e.preventDefault();
    if (!name.trim()) return;
    setError('');
    setBusy(true);
    try {
      const extraValue = extraSpec && extraSpec.kind === 'select'
        ? (extra || extraSpec.default || undefined)
        : (extra.trim() || undefined);
      await onCreate(name.trim(), extraValue);
      setName('');
      setExtra(extraInitial);
      setAdding(false);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="sidebar-section">
      <div className="section-header-row">
        <div className="sidebar-label">{label}</div>
        <button
          type="button"
          className="inline-add-btn"
          onClick={() => (adding ? closeForm() : openForm())}
          title={`创建 ${label}`}
        >
          {adding ? '×' : '＋'}
        </button>
      </div>
      <ul className="sidebar-list">
        {items.length === 0 && !adding && (
          <li className="sidebar-empty">还没有 {label}</li>
        )}
        {items.map((item) => {
          const itemID = id(item);
          return (
            <li
              key={itemID}
              className={`sidebar-list-item ${activeID === itemID ? 'active' : ''}`}
              onClick={() => onSelect(itemID)}
            >
              {renderItem(item)}
            </li>
          );
        })}
      </ul>
      {adding && (
        <form className="inline-form" onSubmit={submit}>
          <input
            placeholder={createPlaceholder}
            value={name}
            autoFocus
            onChange={(e) => setName(e.target.value)}
            required
          />
          {extraSpec && extraSpec.kind === 'text' && (
            <input
              placeholder={extraSpec.placeholder}
              value={extra}
              onChange={(e) => setExtra(e.target.value)}
            />
          )}
          {extraSpec && extraSpec.kind === 'select' && (
            <select
              className="inline-select"
              value={extra}
              onChange={(e) => setExtra(e.target.value)}
              aria-label={extraSpec.label || 'type'}
            >
              {extraSpec.options.map((opt) => (
                <option key={opt.value} value={opt.value}>
                  {opt.label}
                </option>
              ))}
            </select>
          )}
          <button type="submit" disabled={busy}>
            {busy ? '…' : '＋'}
          </button>
          {error && <div className="inline-error">{error}</div>}
        </form>
      )}
    </div>
  );
}
