// ws.js — native WebSocket client talking to GET /ws.
//
// Wire protocol (matches server/pushhub/hub.go):
//
//   client → server:  {"type":"subscribe",   "channel_id":"…"}
//                     {"type":"unsubscribe", "channel_id":"…"}
//
//   server → client:  {"type":"message", "channel_id":"…",
//                      "seq": N, "envelope": { … kernel/message.Envelope … }}
//
// Auth is by cookie — pushhub re-authenticates via the same
// `coagent_session` cookie that the SPA carries.

export class ChannelSocket {
  /**
   * @param {(channelID: string, seq: number, envelope: object) => void} onMessage
   */
  constructor(onMessage) {
    this.onMessage = onMessage;
    this.ws = null;
    this.subscribed = new Set();
    this.pendingSubscribe = new Set();
    // channelID → since_seq for the subscribe frame. A value present here
    // (including 0) makes the server replay every persisted envelope with
    // seq > since_seq before streaming live — closing the narrow window
    // where a message arrives between the HTTP initial load and the WS
    // subscribe registering server-side. Absent (undefined) → live-only.
    this.sinceSeq = new Map();
    this.reconnectAttempts = 0;
    this.shouldRun = false;
  }

  start() {
    this.shouldRun = true;
    this._connect();
  }

  stop() {
    this.shouldRun = false;
    if (this.ws) this.ws.close();
    this.ws = null;
    this.subscribed.clear();
    this.pendingSubscribe.clear();
  }

  /**
   * Subscribe to channelID. When sinceSeq is a number, the server replays
   * persisted envelopes with seq > sinceSeq before streaming live; the
   * caller passes the max seq it already loaded over HTTP so the
   * post-load / pre-subscribe race window is backfilled (dedup is the
   * caller's responsibility — overlap is expected). Omit sinceSeq for a
   * live-only subscribe.
   * @param {string} channelID
   * @param {number} [sinceSeq]
   */
  subscribe(channelID, sinceSeq) {
    this.pendingSubscribe.add(channelID);
    if (typeof sinceSeq === 'number') {
      this.sinceSeq.set(channelID, sinceSeq);
    }
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this._flushPending();
    }
  }

  unsubscribe(channelID) {
    this.pendingSubscribe.delete(channelID);
    this.subscribed.delete(channelID);
    this.sinceSeq.delete(channelID);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: 'unsubscribe', channel_id: channelID }));
    }
  }

  _connect() {
    if (!this.shouldRun) return;
    const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = `${proto}//${location.host}/ws`;
    const ws = new WebSocket(url);
    this.ws = ws;

    ws.addEventListener('open', () => {
      this.reconnectAttempts = 0;
      // Re-subscribe to every channel the SPA is still tracking after
      // a reconnect, then flush any new pending subscriptions.
      for (const chID of this.subscribed) this.pendingSubscribe.add(chID);
      this.subscribed.clear();
      this._flushPending();
    });

    ws.addEventListener('message', (ev) => {
      let frame;
      try { frame = JSON.parse(ev.data); } catch { return; }
      if (frame?.type !== 'message') return;
      this.onMessage(frame.channel_id, Number(frame.seq), frame.envelope);
    });

    const handleClose = () => {
      if (!this.shouldRun) return;
      const delay = Math.min(30_000, 500 * 2 ** Math.min(this.reconnectAttempts, 6));
      this.reconnectAttempts += 1;
      setTimeout(() => this._connect(), delay);
    };
    ws.addEventListener('close', handleClose);
    ws.addEventListener('error', () => ws.close());
  }

  _flushPending() {
    for (const chID of this.pendingSubscribe) {
      const frame = { type: 'subscribe', channel_id: chID };
      const since = this.sinceSeq.get(chID);
      if (typeof since === 'number') frame.since_seq = since;
      this.ws.send(JSON.stringify(frame));
      this.subscribed.add(chID);
    }
    this.pendingSubscribe.clear();
  }
}
