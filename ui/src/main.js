// main.js — coagent demo SPA entry.
//
// Implements the four-step T7 acceptance flow against the Go server:
//   login → list channels → select channel → see messages →
//   send message → receive responses (live via native WebSocket).
//
// Intentionally small (no framework) — this is the M1.5 demo console,
// not the production UI. L3 polish is a separate milestone.

import { api, APIError } from './api.js';
import { ChannelSocket } from './ws.js';

/** App-wide state. Kept on `window` to ease console debugging. */
const state = {
  me: null,
  workspaces: [],
  activeWorkspaceID: null,
  channels: [],
  activeChannelID: null,
  messages: [],
  cursor: 0,
  members: [],
  socket: null,
};
window.__coagent = state;

// ── DOM lookup helpers ────────────────────────────────────────────────
const $ = (sel) => document.querySelector(sel);

const els = {
  viewAuth: $('#view-auth'),
  viewApp: $('#view-app'),

  tabs: document.querySelectorAll('.tabs button'),
  formLogin: $('#form-login'),
  formRegister: $('#form-register'),
  loginError: $('#login-error'),
  registerError: $('#register-error'),
  registerStatus: $('#register-status'),
  btnIssueCode: $('#btn-issue-code'),

  meName: $('#me-name'),
  btnLogout: $('#btn-logout'),

  wsList: $('#ws-list'),
  formCreateWs: $('#form-create-ws'),
  channelList: $('#channel-list'),
  formCreateChannel: $('#form-create-channel'),

  chatTitle: $('#chat-title'),
  chatMeta: $('#chat-meta'),
  messageList: $('#message-list'),
  formSend: $('#form-send'),
  sendError: $('#send-error'),
};

// ── Auth view ────────────────────────────────────────────────────────
els.tabs.forEach((btn) => {
  btn.addEventListener('click', () => {
    els.tabs.forEach((b) => b.classList.toggle('active', b === btn));
    const tab = btn.dataset.tab;
    els.formLogin.classList.toggle('hidden', tab !== 'login');
    els.formRegister.classList.toggle('hidden', tab !== 'register');
  });
});

els.formLogin.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.loginError.textContent = '';
  const fd = new FormData(els.formLogin);
  try {
    const res = await api.login(fd.get('email'), fd.get('password'));
    state.me = res.user;
    await enterApp();
  } catch (err) {
    els.loginError.textContent = formatError(err);
  }
});

els.btnIssueCode.addEventListener('click', async () => {
  els.registerError.textContent = '';
  els.registerStatus.textContent = '';
  const email = els.formRegister.elements.email.value.trim();
  if (!email) { els.registerError.textContent = 'Enter an email first.'; return; }
  try {
    await api.issueCode(email, 'register');
    els.registerStatus.textContent = 'Code issued — check the server logs (dev mode).';
  } catch (err) {
    els.registerError.textContent = formatError(err);
  }
});

els.formRegister.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.registerError.textContent = '';
  const fd = new FormData(els.formRegister);
  try {
    await api.register({
      email: fd.get('email'),
      password: fd.get('password'),
      code: fd.get('code'),
      display_name: fd.get('display_name') || undefined,
    });
    // Then auto-login so the cookie is set.
    const res = await api.login(fd.get('email'), fd.get('password'));
    state.me = res.user;
    await enterApp();
  } catch (err) {
    els.registerError.textContent = formatError(err);
  }
});

// ── App view ─────────────────────────────────────────────────────────
async function enterApp() {
  els.viewAuth.classList.add('hidden');
  els.viewApp.classList.remove('hidden');
  els.meName.textContent = state.me?.display_name || state.me?.email || '(me)';

  state.socket = new ChannelSocket(handleIncomingMessage);
  state.socket.start();

  await refreshWorkspaces();
}

els.btnLogout.addEventListener('click', async () => {
  try { await api.logout(); } catch { /* ignore — clear UI either way */ }
  if (state.socket) state.socket.stop();
  state.me = null;
  state.workspaces = [];
  state.channels = [];
  state.activeWorkspaceID = null;
  state.activeChannelID = null;
  state.messages = [];
  els.viewApp.classList.add('hidden');
  els.viewAuth.classList.remove('hidden');
});

async function refreshWorkspaces() {
  const res = await api.listWorkspaces();
  state.workspaces = res.workspaces || [];
  renderWorkspaces();
  if (state.workspaces.length > 0) {
    await selectWorkspace(state.workspaces[0].id);
  } else {
    state.channels = [];
    renderChannels();
  }
}

function renderWorkspaces() {
  els.wsList.innerHTML = '';
  for (const w of state.workspaces) {
    const li = document.createElement('li');
    li.textContent = w.name;
    if (w.id === state.activeWorkspaceID) li.classList.add('active');
    li.addEventListener('click', () => selectWorkspace(w.id));
    els.wsList.appendChild(li);
  }
}

async function selectWorkspace(wsID) {
  state.activeWorkspaceID = wsID;
  renderWorkspaces();
  const res = await api.listChannels(wsID);
  state.channels = res.channels || [];
  renderChannels();
  if (state.channels.length > 0) {
    await selectChannel(state.channels[0].id);
  } else {
    state.activeChannelID = null;
    state.messages = [];
    renderMessages();
    els.chatTitle.textContent = 'No channels yet — create one ↘';
    els.chatMeta.textContent = '';
  }
}

function renderChannels() {
  els.channelList.innerHTML = '';
  for (const ch of state.channels) {
    const li = document.createElement('li');
    li.textContent = ch.name;
    if (ch.id === state.activeChannelID) li.classList.add('active');
    li.addEventListener('click', () => selectChannel(ch.id));
    els.channelList.appendChild(li);
  }
}

async function selectChannel(chID) {
  if (state.activeChannelID && state.socket) {
    state.socket.unsubscribe(state.activeChannelID);
  }
  state.activeChannelID = chID;
  renderChannels();
  const ch = state.channels.find((c) => c.id === chID);
  els.chatTitle.textContent = ch?.name || chID;
  els.chatMeta.textContent = chID;

  state.messages = [];
  state.cursor = 0;
  renderMessages();

  // Subscribe over WS first so any messages emitted while we fetch
  // the backlog also land — viewcache dedupes by (channel, seq).
  if (state.socket) state.socket.subscribe(chID);

  try {
    const res = await api.listMessages(chID, 0, 200);
    state.messages = (res.messages || []).map(normalizeStoredMessage);
    state.cursor = state.messages.reduce((m, msg) => Math.max(m, msg.seq), 0);
    renderMessages();
  } catch (err) {
    appendSystemNotice(`load history failed: ${formatError(err)}`);
  }
}

function normalizeStoredMessage(row) {
  // /api/channels/:id/messages returns viewcache.MessageRow which
  // already embeds the envelope; surface a normalized shape so the
  // renderer doesn't care whether the row came from REST or WS push.
  const env = row.envelope || row;
  return {
    seq: Number(row.seq ?? env.seq ?? 0),
    id: env.id || row.id,
    ts: env.ts || row.ts || 0,
    sender: env.sender || { kind: 'system', id: 'unknown' },
    type: env.type || 'text',
    payload: env.payload,
    kind: env.kind || 'event',
  };
}

function renderMessages() {
  els.messageList.innerHTML = '';
  const meActorIDs = new Set();
  if (state.me?.id) meActorIDs.add(state.me.id);
  for (const msg of state.messages) {
    const li = document.createElement('li');
    const isMe = msg.sender?.kind === 'human' && meActorIDs.has(msg.sender.id);
    if (isMe) li.classList.add('me');
    const meta = document.createElement('small');
    meta.textContent = `${msg.sender?.kind || '?'} · ${msg.type} · seq ${msg.seq}`;
    li.appendChild(meta);
    li.appendChild(document.createTextNode(renderPayload(msg)));
    els.messageList.appendChild(li);
  }
  els.messageList.scrollTop = els.messageList.scrollHeight;
}

function renderPayload(msg) {
  const p = msg.payload;
  if (p && typeof p === 'object' && typeof p.text === 'string') return p.text;
  if (typeof p === 'string') return p;
  try { return JSON.stringify(p); } catch { return String(p); }
}

function appendSystemNotice(text) {
  const li = document.createElement('li');
  li.appendChild(document.createTextNode(`⚠ ${text}`));
  els.messageList.appendChild(li);
}

// ── WebSocket push handler ───────────────────────────────────────────
function handleIncomingMessage(channelID, seq, envelope) {
  if (channelID !== state.activeChannelID) return;
  if (seq <= state.cursor) return; // dedupe vs the REST backlog
  state.cursor = seq;
  state.messages.push(normalizeStoredMessage({ seq, envelope }));
  renderMessages();
}

// ── Sidebar forms ────────────────────────────────────────────────────
els.formCreateWs.addEventListener('submit', async (e) => {
  e.preventDefault();
  const fd = new FormData(els.formCreateWs);
  try {
    await api.createWorkspace(fd.get('name'));
    els.formCreateWs.reset();
    await refreshWorkspaces();
  } catch (err) {
    alert(formatError(err));
  }
});

els.formCreateChannel.addEventListener('submit', async (e) => {
  e.preventDefault();
  if (!state.activeWorkspaceID) {
    alert('Pick a workspace first');
    return;
  }
  const fd = new FormData(els.formCreateChannel);
  try {
    await api.createChannel(state.activeWorkspaceID, fd.get('name'));
    els.formCreateChannel.reset();
    await selectWorkspace(state.activeWorkspaceID);
  } catch (err) {
    alert(formatError(err));
  }
});

els.formSend.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.sendError.textContent = '';
  const fd = new FormData(els.formSend);
  const text = String(fd.get('text') || '').trim();
  if (!text || !state.activeChannelID) return;
  try {
    await api.sendMessage(state.activeChannelID, { text }, 'chat.text');
    els.formSend.reset();
  } catch (err) {
    els.sendError.textContent = formatError(err);
  }
});

// ── Boot: restore session if cookie is still valid ───────────────────
async function boot() {
  try {
    const me = await api.me();
    state.me = me;
    await enterApp();
  } catch (err) {
    if (err instanceof APIError && err.status === 401) {
      // Not logged in — show auth view (default).
      return;
    }
    console.error('boot failed', err);
  }
}

function formatError(err) {
  if (err instanceof APIError) {
    if (err.body && err.body.error) return err.body.error;
    return err.message;
  }
  return err?.message || String(err);
}

boot();
