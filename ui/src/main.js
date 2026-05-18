// main.js — coagent SPA entry. Auth + workspace/channel navigation +
// message render pipeline + composer + WS push.
//
// Authoritative spec: .dalek/pm/v4-layer3-spec.md §1-§9. The render
// pipeline delegates to:
//   - protocol.js  closed enums + visibility/kind/audience helpers
//   - threading.js correlation_id + parent_id grouping
//   - renderer.js  envelope → DOM
//   - media.js     doc_refs inline rendering
//   - notify.js    browser Notification API
//   - unread.js    cursor + sidebar badge
//   - errors.js    L1 §10.3 reason → composer / system event mapping
//
// main.js stays the only file that touches the DOM tree wholesale; the
// modules emit nodes and main.js mounts them.

import { api, APIError } from './api.js';
import { ChannelSocket } from './ws.js';
import {
  isExtensionAvailable,
  extensionUnavailableReason,
  getDeviceInfo,
  setDeviceToken,
  unbindDevice,
  defaultServerWsUrl,
  describeReason,
} from './extension.js';
import { VISIBILITY } from './protocol.js';
import { groupTimeline } from './threading.js';
import { buildEntryNode } from './renderer.js';
import { readCursor, writeCursor, unreadCount, mentionedIds } from './unread.js';
import { ensurePermission, classifyNotification, fire as fireNotification } from './notify.js';
import { composerMessage } from './errors.js';

// ── App-wide state ──────────────────────────────────────────────────────
const state = {
  me: null,
  workspaces: [],
  activeWorkspaceID: null,
  channels: [],
  activeChannelID: null,
  /** activeChannelID → normalised messages (envelope-shaped + seq) */
  messagesByChannel: new Map(),
  /** activeChannelID → max known seq */
  cursorByChannel: new Map(),
  /** channelID → Map(actorID → actor record) — display name, kind, deregistered_at */
  actorsByChannel: new Map(),
  /** channelID → Set(envelope.id) for envelopes whose request.sender = self */
  selfRequestIDsByChannel: new Map(),
  /** channelID → Set(envelope.id) the renderer should mark as @-mention */
  mentionedByChannel: new Map(),
  /** channelID → current actor_id_in_channel of viewer */
  viewerActorByChannel: new Map(),
  /** channelID → boolean — show system events drawer expanded */
  showSystemByChannel: new Map(),
  socket: null,
  // T148 (M1.6-T6) — last successful bind result for the currently
  // selected channel. Cleared on channel switch / unbind / logout.
  // Shape: {device_session_id, channel_id, user_id} | null
  deviceBind: null,
  // Pending state so the bind button reflects in-flight work.
  deviceBindBusy: false,
};
window.__coagent = state;

// ── DOM lookup helpers ──────────────────────────────────────────────────
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
  systemDrawer: $('#system-drawer'),
  systemDrawerToggle: $('#system-drawer-toggle'),
  systemDrawerBody: $('#system-drawer-body'),
  formSend: $('#form-send'),
  sendError: $('#send-error'),

  // T148 (M1.6-T6) device-binding controls.
  btnBindDevice: $('#btn-bind-device'),
  btnUnbindDevice: $('#btn-unbind-device'),
  deviceBindStatus: $('#device-bind-status'),
};

// ── Auth view ───────────────────────────────────────────────────────────
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
    const res = await api.login(fd.get('email'), fd.get('password'));
    state.me = res.user;
    await enterApp();
  } catch (err) {
    els.registerError.textContent = formatError(err);
  }
});

// ── App view ────────────────────────────────────────────────────────────
async function enterApp() {
  els.viewAuth.classList.add('hidden');
  els.viewApp.classList.remove('hidden');
  els.meName.textContent = state.me?.display_name || state.me?.email || '(me)';

  state.socket = new ChannelSocket(handleIncomingMessage);
  state.socket.start();

  refreshExtensionAvailabilityHint();
  renderBindButton();
  // Best-effort notification permission prompt — async, non-blocking.
  ensurePermission().catch(() => {});

  await refreshWorkspaces();
}

els.btnLogout.addEventListener('click', async () => {
  try { await api.logout(); } catch { /* clear UI either way */ }
  if (state.socket) state.socket.stop();
  state.me = null;
  state.workspaces = [];
  state.channels = [];
  state.activeWorkspaceID = null;
  state.activeChannelID = null;
  state.messagesByChannel.clear();
  state.cursorByChannel.clear();
  state.actorsByChannel.clear();
  state.selfRequestIDsByChannel.clear();
  state.mentionedByChannel.clear();
  state.viewerActorByChannel.clear();
  state.deviceBind = null;
  renderBindButton();
  setBindStatus('', 'idle');
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
    clearChatPane();
    els.chatTitle.textContent = 'No channels yet — create one ↘';
    els.chatMeta.textContent = '';
  }
}

function renderChannels() {
  els.channelList.innerHTML = '';
  for (const ch of state.channels) {
    const li = document.createElement('li');
    li.classList.add('channel-item');
    const name = document.createElement('span');
    name.className = 'channel-name';
    name.textContent = ch.name;
    li.appendChild(name);
    const badge = document.createElement('span');
    badge.className = 'channel-badge';
    badge.dataset.channelId = ch.id;
    li.appendChild(badge);
    if (ch.id === state.activeChannelID) li.classList.add('active');
    li.addEventListener('click', () => selectChannel(ch.id));
    els.channelList.appendChild(li);
  }
  refreshAllBadges();
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

  clearChatPane();
  // T148 (M1.6-T6): a bind is per-channel; switching channels invalidates
  // the previous bind state in the UI. The extension's WS lives across
  // channel switches until the user explicitly re-binds — we don't auto-
  // unbind because the existing token is still good for its lifetime.
  state.deviceBind = null;
  refreshExtensionAvailabilityHint();
  renderBindButton();

  if (state.socket) state.socket.subscribe(chID);

  try {
    const [msgRes, memberRes] = await Promise.all([
      api.listMessages(chID, 0, 200),
      api.listMembers(chID).catch(() => ({ members: [] })),
    ]);
    indexActors(chID, memberRes.members || []);
    const me = (memberRes.members || []).find((m) => m.user_id === state.me?.id);
    if (me) state.viewerActorByChannel.set(chID, me.actor_id_in_channel);
    const messages = (msgRes.messages || []).map(normalizeStoredMessage);
    indexMessages(chID, messages);
    rerenderChat(chID);
    markChannelSeenAtBottom(chID);
  } catch (err) {
    appendSystemNotice(`load history failed: ${formatError(err)}`);
  }
}

function clearChatPane() {
  els.messageList.innerHTML = '';
  els.systemDrawerBody.innerHTML = '';
  els.sendError.textContent = '';
  if (els.systemDrawer) els.systemDrawer.classList.add('hidden');
}

// ── Message ingestion + indexing ────────────────────────────────────────

function normalizeStoredMessage(row) {
  const env = row.envelope || row;
  return {
    seq: Number(row.seq ?? env.seq ?? 0),
    id: env.id || row.id,
    ts: env.ts || row.ts || 0,
    sender: env.sender || { kind: 'system', id: 'unknown' },
    type: env.type || 'text',
    kind: env.kind || 'event',
    payload: env.payload,
    visibility: env.visibility || VISIBILITY.PUBLIC,
    audience: Array.isArray(env.audience) ? env.audience : [],
    correlation_id: env.correlation_id || '',
    parent_id: env.parent_id || '',
    doc_refs: Array.isArray(env.doc_refs) ? env.doc_refs : null,
    not_before: env.not_before,
    delivery_failed_at: env.delivery_failed_at,
    last_error: env.last_error,
  };
}

function indexActors(channelID, members) {
  const map = new Map();
  for (const m of members) {
    map.set(m.actor_id_in_channel, {
      id: m.actor_id_in_channel,
      display_name: m.display_name || m.user_id,
      kind: m.kind || 'human',
      deregistered_at: m.deregistered_at || null,
    });
  }
  state.actorsByChannel.set(channelID, map);
}

function indexMessages(channelID, messages) {
  state.messagesByChannel.set(channelID, messages);
  state.cursorByChannel.set(channelID, messages.reduce((m, msg) => Math.max(m, msg.seq), 0));
  const viewerActor = state.viewerActorByChannel.get(channelID);
  const selfRequests = new Set();
  for (const m of messages) {
    if (m.kind === 'request' && m.sender && m.sender.id === viewerActor) {
      selfRequests.add(m.id);
    }
  }
  state.selfRequestIDsByChannel.set(channelID, selfRequests);
  recomputeMentions(channelID);
}

function recomputeMentions(channelID) {
  const messages = state.messagesByChannel.get(channelID) || [];
  const viewerActor = state.viewerActorByChannel.get(channelID) || '';
  const ids = mentionedIds(messages, viewerActor, 0);
  state.mentionedByChannel.set(channelID, new Set(ids));
}

function appendMessage(channelID, message) {
  let list = state.messagesByChannel.get(channelID);
  if (!list) {
    list = [];
    state.messagesByChannel.set(channelID, list);
  }
  // Dedup vs REST backlog.
  if (list.some((m) => m.seq === message.seq)) return false;
  list.push(message);
  state.cursorByChannel.set(channelID, Math.max(state.cursorByChannel.get(channelID) || 0, message.seq));

  const viewerActor = state.viewerActorByChannel.get(channelID) || '';
  if (message.kind === 'request' && message.sender?.id === viewerActor) {
    const selfReqs = state.selfRequestIDsByChannel.get(channelID) || new Set();
    selfReqs.add(message.id);
    state.selfRequestIDsByChannel.set(channelID, selfReqs);
  }
  return true;
}

// ── Chat re-render pipeline ─────────────────────────────────────────────

function rerenderChat(channelID) {
  if (channelID !== state.activeChannelID) return;
  const messages = state.messagesByChannel.get(channelID) || [];
  const viewerActor = state.viewerActorByChannel.get(channelID) || '';
  const actors = state.actorsByChannel.get(channelID) || new Map();
  const mentioned = state.mentionedByChannel.get(channelID) || new Set();
  const includeSystem = Boolean(state.showSystemByChannel.get(channelID));

  const entries = groupTimeline(messages, {
    viewerActorID: viewerActor,
    nowMs: Date.now(),
    includeSystem,
  });
  els.messageList.innerHTML = '';
  const ctx = { viewerActorID: viewerActor, channelID, actors, mentionedIds: mentioned };
  let hasSystem = false;
  for (const entry of entries) {
    const node = buildEntryNode(entry, ctx);
    if (entry.kind === 'system-event' && !includeSystem) {
      hasSystem = true;
      // Always render into the system drawer too so the toggle reveals it.
      els.systemDrawerBody.appendChild(buildEntryNode(entry, ctx));
      continue;
    }
    if (entry.systemEvents && entry.systemEvents.length > 0) hasSystem = true;
    els.messageList.appendChild(node);
  }
  if (els.systemDrawer) {
    els.systemDrawer.classList.toggle('hidden', !hasSystem);
  }
  els.messageList.scrollTop = els.messageList.scrollHeight;
}

function appendSystemNotice(text) {
  const li = document.createElement('li');
  li.className = 'entry system-notice';
  li.textContent = `⚠ ${text}`;
  els.messageList.appendChild(li);
}

// ── System events drawer ────────────────────────────────────────────────

if (els.systemDrawerToggle) {
  els.systemDrawerToggle.addEventListener('click', () => {
    const ch = state.activeChannelID;
    if (!ch) return;
    const cur = state.showSystemByChannel.get(ch) || false;
    state.showSystemByChannel.set(ch, !cur);
    els.systemDrawerToggle.textContent = !cur ? '隐藏系统事件' : '显示系统事件';
    els.systemDrawerBody.classList.toggle('hidden', cur);
    rerenderChat(ch);
  });
}

// ── WebSocket push handler ──────────────────────────────────────────────

function handleIncomingMessage(channelID, seq, envelope) {
  const normalised = normalizeStoredMessage({ seq, envelope });
  const fresh = appendMessage(channelID, normalised);
  if (!fresh) return;
  recomputeMentions(channelID);
  if (channelID === state.activeChannelID) {
    rerenderChat(channelID);
    if (document.visibilityState === 'visible') {
      markChannelSeenAtBottom(channelID);
    }
  }
  refreshAllBadges();
  maybeNotify(channelID, normalised);
}

function maybeNotify(channelID, envelope) {
  const viewerActor = state.viewerActorByChannel.get(channelID) || '';
  if (!viewerActor) return;
  const selfReqs = state.selfRequestIDsByChannel.get(channelID) || new Set();
  const descriptor = classifyNotification(envelope, {
    viewerActorID: viewerActor,
    requestSentBySelf: (id) => selfReqs.has(id),
  });
  if (!descriptor) return;
  fireNotification(descriptor, () => {
    if (channelID !== state.activeChannelID) selectChannel(channelID);
  });
}

// ── Unread badges ───────────────────────────────────────────────────────

function refreshAllBadges() {
  for (const ch of state.channels) {
    const badge = els.channelList.querySelector(`.channel-badge[data-channel-id="${ch.id}"]`);
    if (!badge) continue;
    const messages = state.messagesByChannel.get(ch.id) || [];
    const viewer = state.viewerActorByChannel.get(ch.id) || '';
    const cursor = readCursor(ch.id);
    const n = unreadCount(messages, viewer, cursor);
    if (n > 0) {
      badge.textContent = String(n);
      badge.classList.add('has-unread');
    } else {
      badge.textContent = '';
      badge.classList.remove('has-unread');
    }
  }
}

function markChannelSeenAtBottom(channelID) {
  const top = state.cursorByChannel.get(channelID) || 0;
  writeCursor(channelID, top);
  refreshAllBadges();
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible' && state.activeChannelID) {
    markChannelSeenAtBottom(state.activeChannelID);
  }
});

els.messageList.addEventListener('scroll', () => {
  const list = els.messageList;
  if (list.scrollTop + list.clientHeight >= list.scrollHeight - 24) {
    if (state.activeChannelID) markChannelSeenAtBottom(state.activeChannelID);
  }
});

// ── Sidebar forms ───────────────────────────────────────────────────────
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
    const name = fd.get('name');
    const type = (fd.get('type') || 'group').trim() || 'group';
    await api.createChannel(state.activeWorkspaceID, name, type);
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
    await api.sendMessage(state.activeChannelID, { text }, 'human.text');
    els.formSend.reset();
  } catch (err) {
    const apiErr = err instanceof APIError ? err.body?.error : '';
    els.sendError.textContent = composerMessage(apiErr) || formatError(err);
  }
});

// ── Device binding (T148 / M1.6-T6) ──────────────────────────────────
//
// 4-step flow:
//   1. sendMessage(extId, getDeviceInfo)        → extension returns persistent device_id
//   2. GET  /api/placements/:chID               → server returns daemon_id
//   3. POST /api/channels/:chID/devices         → server returns {device_session_id, token, expires_at}
//   4. sendMessage(extId, setDeviceToken)       → extension persists + opens WS

async function bindCurrentChannel() {
  if (!state.activeChannelID) return;
  if (state.deviceBindBusy) return;
  state.deviceBindBusy = true;
  setBindStatus('正在绑定 …', 'pending');
  renderBindButton();
  try {
    // Step 1: device_id from extension.
    const info = await getDeviceInfo();
    if (info.status !== 'ok') {
      throw new Error(describeReason(info.reason, info.detail));
    }
    // Step 2: which daemon owns this channel?
    let placement;
    try {
      placement = await api.getPlacement(state.activeChannelID);
    } catch (err) {
      if (err instanceof APIError && err.status === 404) {
        throw new Error('channel 尚未绑定 daemon — 请先在 server 端 bind channel');
      }
      throw err;
    }
    const daemonID = placement?.daemon_id;
    if (!daemonID) throw new Error('placement 缺少 daemon_id');
    // Step 3: issue a device session row + token.
    const session = await api.issueDeviceSession(state.activeChannelID, {
      device_id: info.device_id,
      daemon_id: daemonID,
      device_type: 'xhs-extension',
    });
    if (!session?.device_session_id || !session?.token) {
      throw new Error('server issue 响应缺字段');
    }
    // Step 4: hand the bundle to the extension. It persists + opens WS.
    const ack = await setDeviceToken({
      server_ws_url: defaultServerWsUrl(),
      device_session_id: session.device_session_id,
      token: session.token,
      channel_id: state.activeChannelID,
      user_id: state.me?.id,
      device_id: info.device_id,
      expires_at: session.expires_at,
    });
    if (ack.status !== 'connected') {
      // If the server already issued the row but the extension failed
      // to connect, revoke it so we don't leak orphan sessions.
      try {
        await api.revokeDeviceSession(session.device_session_id);
      } catch (revokeErr) {
        console.warn('revoke after failed bind', revokeErr);
      }
      throw new Error(describeReason(ack.reason, ack.detail));
    }
    state.deviceBind = {
      device_session_id: ack.device_session_id,
      channel_id: ack.channel_id,
      user_id: ack.user_id,
    };
    setBindStatus(
      `✅ 已绑定 (channel=${ack.channel_id}, user=${ack.user_id || '?'})`,
      'ok',
    );
  } catch (err) {
    state.deviceBind = null;
    setBindStatus(`❌ 绑定失败：${formatError(err)}`, 'error');
  } finally {
    state.deviceBindBusy = false;
    renderBindButton();
  }
}

async function unbindCurrentChannel() {
  if (state.deviceBindBusy) return;
  const sid = state.deviceBind?.device_session_id;
  if (!sid) return;
  state.deviceBindBusy = true;
  setBindStatus('正在解绑 …', 'pending');
  renderBindButton();
  try {
    // Best-effort dual-revoke: server flips row to 'revoked' AND
    // extension drops its local v4 fields. Order: server first so the
    // extension's WS close is initiated server-side, then sendMessage
    // as a safety net in case the close didn't arrive.
    try {
      await api.revokeDeviceSession(sid);
    } catch (err) {
      console.warn('server revoke failed', err);
    }
    const ack = await unbindDevice();
    if (ack.status !== 'unbound' && ack.available !== false) {
      console.warn('extension unbind failed', ack);
    }
    state.deviceBind = null;
    setBindStatus('已解绑', 'idle');
  } catch (err) {
    setBindStatus(`解绑出错：${formatError(err)}`, 'error');
  } finally {
    state.deviceBindBusy = false;
    renderBindButton();
  }
}

function renderBindButton() {
  const btn = els.btnBindDevice;
  const unbindBtn = els.btnUnbindDevice;
  if (!btn || !unbindBtn) return;
  const bound = Boolean(state.deviceBind);
  unbindBtn.hidden = !bound;
  if (state.deviceBindBusy) {
    btn.disabled = true;
    unbindBtn.disabled = true;
    return;
  }
  unbindBtn.disabled = false;
  if (!state.activeChannelID) {
    btn.disabled = true;
    btn.textContent = '绑定 Chrome extension';
    return;
  }
  if (!isExtensionAvailable()) {
    btn.disabled = true;
    btn.textContent = 'Extension 不可用';
    return;
  }
  btn.disabled = false;
  btn.textContent = bound ? '重新绑定 Chrome extension' : '绑定 Chrome extension';
}

function setBindStatus(text, kind) {
  const el = els.deviceBindStatus;
  if (!el) return;
  el.textContent = text;
  el.dataset.kind = kind || '';
}

function refreshExtensionAvailabilityHint() {
  // On boot / channel switch / when no bind has happened yet, surface
  // why the button might be disabled.
  if (state.deviceBind) return;
  if (!isExtensionAvailable()) {
    setBindStatus(describeReason(extensionUnavailableReason()), 'warn');
  } else if (!state.activeChannelID) {
    setBindStatus('先选择一个 channel', 'idle');
  } else {
    setBindStatus('', 'idle');
  }
}

if (els.btnBindDevice) {
  els.btnBindDevice.addEventListener('click', () => {
    void bindCurrentChannel();
  });
}
if (els.btnUnbindDevice) {
  els.btnUnbindDevice.addEventListener('click', () => {
    void unbindCurrentChannel();
  });
}

// ── Boot: restore session if cookie is still valid ───────────────────
async function boot() {
  try {
    const me = await api.me();
    state.me = me;
    await enterApp();
  } catch (err) {
    if (err instanceof APIError && err.status === 401) return;
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
