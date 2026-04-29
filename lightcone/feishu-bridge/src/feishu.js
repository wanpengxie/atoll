// Feishu API client — one instance per bot
const FEISHU_API = 'https://open.feishu.cn/open-apis';

export class FeishuBot {
  constructor({ appId, appSecret, verificationToken, name }) {
    this.appId = appId;
    this.appSecret = appSecret;
    this.verificationToken = verificationToken;
    this.name = name;
    this._token = null;
    this._tokenExpiry = 0;
  }

  // Get or refresh tenant_access_token (valid ~2h, cache locally)
  async getToken() {
    if (this._token && Date.now() < this._tokenExpiry - 60_000) {
      return this._token;
    }
    const res = await fetch(`${FEISHU_API}/auth/v3/tenant_access_token/internal`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ app_id: this.appId, app_secret: this.appSecret }),
    });
    const data = await res.json();
    if (data.code !== 0) throw new Error(`[${this.name}] getToken failed: ${data.msg}`);
    this._token = data.tenant_access_token;
    this._tokenExpiry = Date.now() + data.expire * 1000;
    return this._token;
  }

  // Send a text message to a chat
  async sendMessage(chatId, text) {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/im/v1/messages?receive_id_type=chat_id`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({
        receive_id: chatId,
        msg_type: 'text',
        content: JSON.stringify({ text }),
      }),
    });
    const data = await res.json();
    if (data.code !== 0) {
      console.error(`[${this.name}] sendMessage failed:`, data.msg);
    } else {
      console.log(`[${this.name}] sent message to ${chatId}`);
    }
    return data;
  }

  // Send a private DM to a user by open_id
  async sendDm(openId, text) {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/im/v1/messages?receive_id_type=open_id`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({
        receive_id: openId,
        msg_type: 'text',
        content: JSON.stringify({ text }),
      }),
    });
    const data = await res.json();
    if (data.code !== 0) {
      console.error(`[${this.name}] sendDm to ${openId} failed:`, data.msg);
    }
    return data;
  }

  // Get bot info (name etc.) from Feishu API
  async getBotInfo() {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/bot/v3/info/`, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await res.json();
    if (data.code !== 0) throw new Error(`getBotInfo failed: ${data.msg}`);
    return data.bot;  // { app_name, ... }
  }

  // Get user info by open_id (tries contact API first, then chat members as fallback)
  async getUserInfo(openId, chatId) {
    const token = await this.getToken();
    // Try contact API first (requires contact:user.base:readonly scope)
    try {
      const res = await fetch(`${FEISHU_API}/contact/v3/users/${openId}?user_id_type=open_id`, {
        headers: { Authorization: `Bearer ${token}` },
      });
      const data = await res.json();
      if (data.code === 0 && data.data?.user?.name) {
        return data.data.user;
      }
    } catch {}
    // Fallback: get name from chat member list (bots usually have this permission)
    if (chatId) {
      try {
        const res = await fetch(`${FEISHU_API}/im/v1/chats/${chatId}/members?member_id_type=open_id&page_size=50`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const data = await res.json();
        if (data.code === 0 && data.data?.items) {
          const member = data.data.items.find(m => m.member_id === openId);
          if (member?.name) return { name: member.name };
        }
      } catch {}
    }
    return null;
  }

  // Add emoji reaction to a message
  async addReaction(messageId, emojiType) {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/im/v1/messages/${messageId}/reactions`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ reaction_type: { emoji_type: emojiType } }),
    });
    const data = await res.json();
    if (data.code !== 0) {
      console.error(`[${this.name}] addReaction failed:`, data.msg);
    }
    return data;
  }

  // Remove emoji reaction from a message
  async removeReaction(messageId, reactionId) {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/im/v1/messages/${messageId}/reactions/${reactionId}`, {
      method: 'DELETE',
      headers: { Authorization: `Bearer ${token}` },
    });
    const data = await res.json();
    if (data.code !== 0) {
      console.error(`[${this.name}] removeReaction failed:`, data.msg);
    }
    return data;
  }

  // Reply in a thread (reply to a specific message)
  async replyMessage(messageId, text) {
    const token = await this.getToken();
    const res = await fetch(`${FEISHU_API}/im/v1/messages/${messageId}/reply`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
      },
      body: JSON.stringify({
        msg_type: 'text',
        content: JSON.stringify({ text }),
      }),
    });
    const data = await res.json();
    if (data.code !== 0) {
      console.error(`[${this.name}] replyMessage failed:`, data.msg);
    }
    return data;
  }
}
