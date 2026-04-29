import { FeishuBot } from './feishu.js';
import { lightconePost, lightconeSubscribe } from './lightcone.js';

const RELOAD_INTERVAL = 3600_000;

/**
 * Mount feishu bridge routes onto an Express app.
 * @param {import('express').Express} app
 * @param {{ lightconeUrl: string, lightconeToken: string }} config
 */
export function setupFeishuBridge(app, { lightconeUrl, lightconeToken }) {
  // ── Dynamic agent registry ──────────────────────────────────────────────────
  // agentId → { agent config, FeishuBot, activeChats: Set<chatId> }
  const registry = new Map();
  // teamId → Set<agentId>
  const teamWatchers = new Map();
  // chatId → { teamId, agentId } (local cache, source of truth is lightcone DB)
  const chatBindings = new Map();
  const processedMessages = new Set();
  // teamId → { feishuMessageId, reactionId, bot } — tracks pending ⏳ reactions
  const pendingReactions = new Map();

  function lightconeApi(method, path, body) {
    return fetch(`${lightconeUrl}${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${lightconeToken}`,
      },
      body: body != null ? JSON.stringify(body) : undefined,
    }).then(r => r.json());
  }

  // ── Agent config sync ───────────────────────────────────────────────────────

  async function fetchAgentConfigs() {
    try {
      const agents = await lightconeApi('GET', '/api/feishu/agents');
      if (!Array.isArray(agents)) return;

      const seen = new Set();
      for (const a of agents) {
        seen.add(a.id);
        if (!registry.has(a.id)) {
          const bot = new FeishuBot({
            name: a.displayName,
            appId: a.feishuAppId,
            appSecret: a.feishuAppSecret,
            verificationToken: a.feishuVerificationToken,
          });
          registry.set(a.id, { ...a, bot, activeChats: new Set() });
          console.log(`[Bridge] Registered agent: ${a.displayName} (${a.id})`);
          if (a.feishuAppId && a.feishuAppSecret) {
            bot.getBotInfo().then(info => {
              if (info?.app_name && info.app_name !== a.feishuBotName) {
                lightconeApi('PATCH', `/api/agents/${a.id}`, { feishuBotName: info.app_name });
                console.log(`[Bridge] Synced feishuBotName for ${a.displayName}: ${info.app_name}`);
              }
            }).catch(err => console.error(`[Bridge] getBotInfo failed for ${a.displayName}:`, err.message));
          }
        } else {
          const entry = registry.get(a.id);
          Object.assign(entry, a);
          entry.bot.appId = a.feishuAppId;
          entry.bot.appSecret = a.feishuAppSecret;
          entry.bot.verificationToken = a.feishuVerificationToken;
          entry.bot._token = null;
        }
      }
      for (const [id] of registry) {
        if (!seen.has(id)) { registry.delete(id); }
      }
    } catch (err) {
      console.error('[Bridge] fetchAgentConfigs error:', err.message);
    }
  }

  // ── Chat binding helpers ────────────────────────────────────────────────────

  async function getBinding(chatId) {
    if (chatBindings.has(chatId)) {
      const b = chatBindings.get(chatId);
      ensureSubscribed(b.teamId, b.agentId);
      return b;
    }
    try {
      const data = await lightconeApi('GET', `/api/feishu/binding?chatId=${encodeURIComponent(chatId)}`);
      if (data.teamId) {
        chatBindings.set(chatId, { teamId: data.teamId, agentId: data.agentId });
        ensureSubscribed(data.teamId, data.agentId);
        return chatBindings.get(chatId);
      }
    } catch {}
    return null;
  }

  function ensureSubscribed(teamId, agentId) {
    if (!teamWatchers.has(teamId)) {
      teamWatchers.set(teamId, new Set());
      subscribeTeam(teamId);
    }
    teamWatchers.get(teamId).add(agentId);
  }

  async function createBinding(chatId, agentId, chatName, teamId, userId) {
    const data = await lightconeApi('POST', '/api/feishu/bind', { chatId, agentId, teamId, chatName, userId });
    if (data.ok) {
      chatBindings.set(chatId, { teamId: data.teamId, agentId: data.agentId });
      if (!teamWatchers.has(data.teamId)) {
        teamWatchers.set(data.teamId, new Set());
        subscribeTeam(data.teamId);
      }
      teamWatchers.get(data.teamId).add(data.agentId);
    }
    return data;
  }

  // ── lightcone → Feishu ──────────────────────────────────────────────────────

  function subscribeTeam(teamId) {
    lightconeSubscribe(lightconeUrl, teamId, async (msg) => {
      if (msg.senderType === 'user') return;

      for (const [chatId, binding] of chatBindings) {
        if (binding.teamId !== teamId) continue;

        let entry = null;
        for (const [, e] of registry) {
          if (e.id === msg.senderId || e.displayName === msg.senderName) {
            entry = e;
            break;
          }
        }
        // Fall back to the bot bound to this chat if sender has no Feishu app
        if (!entry?.bot) entry = registry.get(binding.agentId);
        if (!entry?.bot) continue;

        const text = msg.content;
        console.log(`[Bridge] lightcone[${teamId}] → Feishu chat ${chatId}: ${text.slice(0, 80)}`);
        try { await entry.bot.sendMessage(chatId, text); }
        catch (err) { console.error(`[Bridge] sendMessage failed:`, err.message); }

        const pending = pendingReactions.get(teamId);
        if (pending) {
          pendingReactions.delete(teamId);
          try {
            await pending.bot.removeReaction(pending.feishuMessageId, pending.reactionId);
            const doneData = await pending.bot.addReaction(pending.feishuMessageId, 'DONE');
            if (doneData.data?.reaction_id) {
              setTimeout(() => {
                pending.bot.removeReaction(pending.feishuMessageId, doneData.data.reaction_id).catch(() => {});
              }, 5000);
            }
          } catch (e) { console.error('[Bridge] reaction cleanup error:', e.message); }
        }
      }
    }, lightconeToken);
  }

  // ── Auto-register user ──────────────────────────────────────────────────────

  async function autoRegisterUser(bot, sender, chatId) {
    const open_id = sender?.sender_id?.open_id;
    const union_id = sender?.sender_id?.union_id;
    if (!open_id || sender?.sender_type !== 'user') return null;
    try {
      let name;
      try {
        const userInfo = await bot.getUserInfo(open_id, chatId);
        name = userInfo?.name;
      } catch {}
      const res = await fetch(`${lightconeUrl}/api/users/feishu-register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${lightconeToken}` },
        body: JSON.stringify({ open_id, union_id, name }),
      });
      const data = await res.json();
      if (data.isNew) {
        const webUrl = process.env.WEB_URL ?? lightconeUrl;
        await bot.sendDm(open_id, `你好！欢迎使用 AI 助手服务。\n配置你的 Agent：${webUrl}`).catch(() => {});
      }
      return data.user ?? null;
    } catch { return null; }
  }

  // ── Webhook routes ──────────────────────────────────────────────────────────

  app.use((req, res, next) => {
    if (req.path.startsWith('/webhook/') && req.path !== '/health') {
      console.log(`[Bridge] ${req.method} ${req.path}`);
    }
    next();
  });

  app.post('/webhook/:agentId', async (req, res) => {
    const body = req.body;
    const isV2 = body.schema === '2.0';
    const eventType = isV2 ? body.header?.event_type : body.type;
    console.log(`[Bridge] eventType=${eventType} isV2=${isV2}`);

    // URL verification — must respond before any other checks
    if (eventType === 'url_verification') {
      const challenge = isV2 ? body.event?.challenge : body.challenge;
      return res.json({ challenge });
    }

    const entry = registry.get(req.params.agentId);
    if (!entry) return res.status(404).json({ error: 'Agent not configured' });

    // Token verification
    const token = isV2 ? body.header?.token : (body.header?.token ?? body.token);
    if (entry.feishuVerificationToken && token !== entry.feishuVerificationToken) {
      return res.status(403).json({ error: 'invalid token' });
    }

    res.json({ code: 0 });

    const event = body.event;
    // ── Regular message ──────────────────────────────────────────────────────
    if (eventType !== 'im.message.receive_v1') return;

    const message = event?.message;
    if (!message) return;

    const messageId = message.message_id;
    if (processedMessages.has(messageId)) return;
    processedMessages.add(messageId);
    if (processedMessages.size > 1000) processedMessages.delete(processedMessages.values().next().value);

    const chatId = message.chat_id;
    const feishuUser = await autoRegisterUser(entry.bot, event?.sender, chatId);
    const senderInfo = feishuUser
      ? { senderId: feishuUser.id, senderName: feishuUser.name || '飞书用户' }
      : {};

    let text = '';
    try {
      const content = JSON.parse(message.content);
      text = content.text ?? content.content ?? JSON.stringify(content);
    } catch { text = message.content ?? ''; }
    const mentions = message.mentions;
    if (Array.isArray(mentions)) {
      for (const m of mentions) {
        if (m.key && m.name) {
          text = text.replace(m.key, `@${m.name}`);
        }
      }
    }
    text = text.replace(/<at[^>]*>([^<]*)<\/at>/g, '@$1').trim();
    if (!text) return;

    // ── Check if message is a binding code ──────────────────────────────────
    const codeMatch = text.match(/^([A-Z0-9]{6})$/);
    if (codeMatch) {
      const data = await lightconeApi('POST', '/api/feishu/bind', {
        chatId, agentId: entry.id, bindingCode: codeMatch[1]
      });
      if (data.ok) {
        chatBindings.set(chatId, { teamId: data.teamId, agentId: data.agentId });
        if (!teamWatchers.has(data.teamId)) {
          teamWatchers.set(data.teamId, new Set());
          subscribeTeam(data.teamId);
        }
        teamWatchers.get(data.teamId).add(data.agentId);
        await entry.bot.sendMessage(chatId, `✅ 绑定成功！此群已关联到对应团队，消息将双向同步。`).catch(() => {});
        console.log(`[Bridge] Bound chat ${chatId} via code to team ${data.teamId}`);
      } else {
        await entry.bot.sendMessage(chatId, `❌ 绑定码无效或已过期，请重新生成。`).catch(() => {});
      }
      return;
    }

    // ── Route to lightcone team ──────────────────────────────────────────
    const binding = await getBinding(chatId);
    if (!binding) {
      let chatName = `${entry.displayName}的群`;
      try {
        const token = await entry.bot.getToken();
        const r = await fetch(`https://open.feishu.cn/open-apis/im/v1/chats/${chatId}`, {
          headers: { Authorization: `Bearer ${token}` }
        });
        const d = await r.json();
        console.log(`[Bridge] getChatInfo ${chatId}: code=${d.code} name=${d.data?.name ?? '(none)'}`);
        if (d.data?.name) chatName = d.data.name;
      } catch (err) {
        console.error(`[Bridge] getChatInfo failed for ${chatId}:`, err.message);
      }
      const data = await createBinding(chatId, entry.id, chatName, undefined, feishuUser?.id);
      if (data.ok) {
        await lightconePost(lightconeUrl, lightconeToken, data.teamId, text, senderInfo);
      }
      return;
    }

    console.log(`[Bridge][${entry.displayName}] Feishu → lightcone #${binding.teamId}: ${text.slice(0, 120)}`);
    try {
      await lightconePost(lightconeUrl, lightconeToken, binding.teamId, text, senderInfo);
      try {
        const rData = await entry.bot.addReaction(messageId, 'OnIt');
        if (rData.data?.reaction_id) {
          pendingReactions.set(binding.teamId, {
            feishuMessageId: messageId, reactionId: rData.data.reaction_id, bot: entry.bot,
          });
        }
      } catch (e) { console.error('[Bridge] addReaction error:', e.message); }
    } catch (err) {
      console.error('[Bridge] lightconePost error:', err.message);
    }
  });

  app.get('/bridge/health', (_, res) =>
    res.json({ ok: true, agents: [...registry.keys()], bindings: chatBindings.size })
  );

  // ── Load existing bindings on startup ────────────────────────────────────
  async function loadExistingBindings() {
    try {
      const bindings = await lightconeApi('GET', '/api/feishu/bindings');
      if (!Array.isArray(bindings)) return;
      for (const b of bindings) {
        if (!chatBindings.has(b.chatId)) {
          chatBindings.set(b.chatId, { teamId: b.teamId, agentId: b.agentId });
        }
        ensureSubscribed(b.teamId, b.agentId);
      }
      console.log(`[Bridge] Loaded ${bindings.length} existing binding(s)`);
    } catch (err) {
      console.error('[Bridge] loadExistingBindings error:', err.message);
    }
  }

  // ── Start background tasks ────────────────────────────────────────────────
  fetchAgentConfigs().then(() => loadExistingBindings());
  setInterval(fetchAgentConfigs, RELOAD_INTERVAL);

  console.log('[Bridge] Feishu bridge routes mounted');
}
