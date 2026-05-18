import React, { useState } from 'react';
import { api } from '../api.js';

export default function Auth({ onAuthed }) {
  const [tab, setTab] = useState('login');
  return (
    <>
      <div className="rainbow-bar"></div>
      <main id="view-auth" className="landing">
        <div className="landing-card">
          <div className="landing-mark">
            <svg viewBox="0 0 24 24"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" /></svg>
          </div>
          <div className="landing-logo">coagent</div>
          <div className="landing-sub">// agent-native slack · actor-message substrate</div>

          <section className="auth-tabs">
            <button
              type="button"
              className={`auth-tab ${tab === 'login' ? 'active' : ''}`}
              onClick={() => setTab('login')}
            >
              登录
            </button>
            <button
              type="button"
              className={`auth-tab ${tab === 'register' ? 'active' : ''}`}
              onClick={() => setTab('register')}
            >
              注册
            </button>
          </section>

          {tab === 'login' ? (
            <LoginForm onAuthed={onAuthed} />
          ) : (
            <RegisterForm onAuthed={onAuthed} />
          )}

          <a
            href="/downloads/coagent-extension.zip"
            download
            className="muted"
            style={{
              display: 'block',
              textAlign: 'center',
              marginTop: 16,
              fontSize: 13,
            }}
          >
            下载 Chrome 插件
          </a>
        </div>
      </main>
    </>
  );
}

function LoginForm({ onAuthed }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(e) {
    e.preventDefault();
    setError('');
    setBusy(true);
    try {
      const res = await api.login(email, password);
      onAuthed(res.user || res);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="auth-form" onSubmit={submit}>
      <div className="auth-field">
        <label className="auth-label">Email</label>
        <input
          className="auth-input"
          type="email"
          autoComplete="email"
          required
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
      </div>
      <div className="auth-field">
        <label className="auth-label">密码</label>
        <input
          className="auth-input"
          type="password"
          autoComplete="current-password"
          required
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>
      {error && <div className="auth-error">{error}</div>}
      <button type="submit" className="auth-submit" disabled={busy}>
        {busy ? '登录中…' : '登录'}
      </button>
    </form>
  );
}

function RegisterForm({ onAuthed }) {
  const [email, setEmail] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(e) {
    e.preventDefault();
    setError('');
    setBusy(true);
    try {
      await api.register({ email, display_name: displayName || undefined, password });
      const res = await api.login(email, password);
      onAuthed(res.user || res);
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <form className="auth-form" onSubmit={submit}>
      <div className="auth-field">
        <label className="auth-label">Email</label>
        <input
          className="auth-input"
          type="email"
          autoComplete="email"
          required
          value={email}
          onChange={(e) => setEmail(e.target.value)}
        />
      </div>
      <div className="auth-field">
        <label className="auth-label">
          昵称 <span style={{ color: 'var(--text-dim)' }}>(可选)</span>
        </label>
        <input
          className="auth-input"
          type="text"
          autoComplete="name"
          value={displayName}
          onChange={(e) => setDisplayName(e.target.value)}
        />
      </div>
      <div className="auth-field">
        <label className="auth-label">
          密码 <span style={{ color: 'var(--text-dim)' }}>(≥ 8 位)</span>
        </label>
        <input
          className="auth-input"
          type="password"
          autoComplete="new-password"
          required
          minLength={8}
          value={password}
          onChange={(e) => setPassword(e.target.value)}
        />
      </div>
      {error && <div className="auth-error">{error}</div>}
      <button type="submit" className="auth-submit" disabled={busy}>
        {busy ? '创建中…' : '创建账号'}
      </button>
    </form>
  );
}
