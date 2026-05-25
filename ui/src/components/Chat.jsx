import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { ChannelSocket } from '../ws.js';
import { aggregateEnvelopes } from '../aggregation.js';
import DeviceBind from './DeviceBind.jsx';
import KimiBridgeStatus from './KimiBridgeStatus.jsx';

export default function Chat({ channelID, channel, me }) {
  const [messages, setMessages] = useState([]);
  const [text, setText] = useState('');
  const [error, setError] = useState('');
  const [sending, setSending] = useState(false);
  const socketRef = useRef(null);
  const listRef = useRef(null);

  // (Re)load messages on channel change.
  useEffect(() => {
    setError('');
    setMessages([]);
    if (!channelID) return;
    let alive = true;
    (async () => {
      try {
        const res = await api.listMessages(channelID);
        if (!alive) return;
        setMessages(res.messages || []);
      } catch (err) {
        if (alive) setError(err.message || String(err));
      }
    })();
    return () => {
      alive = false;
    };
  }, [channelID]);

  // WS subscribe per channel.
  useEffect(() => {
    if (!channelID) return;
    const socket = new ChannelSocket((chID, seq, envelope) => {
      if (chID !== channelID) return;
      setMessages((prev) => [...prev, envelope]);
    });
    socketRef.current = socket;
    socket.start();
    socket.subscribe(channelID);
    return () => {
      socket.unsubscribe(channelID);
      socket.stop?.();
      socketRef.current = null;
    };
  }, [channelID]);

  // Auto-scroll to bottom.
  useEffect(() => {
    if (listRef.current) {
      listRef.current.scrollTop = listRef.current.scrollHeight;
    }
  }, [messages]);

  async function send(e) {
    e.preventDefault();
    if (!text.trim() || !channelID) return;
    setError('');
    setSending(true);
    const body = { text };
    try {
      await api.sendMessage(channelID, body);
      setText('');
    } catch (err) {
      setError(err.message || String(err));
    } finally {
      setSending(false);
    }
  }

  if (!channelID) {
    return (
      <section id="main" className="chat">
        <div className="chat-empty">
          <p className="muted">从左侧选择一个 channel，或先创建一个。</p>
        </div>
      </section>
    );
  }

  return (
    <section id="main" className="chat">
      <header className="chat-header">
        <h2>{(channel && (channel.name || channel.Name)) || '…'}</h2>
        <span className="muted">{channel?.type || channel?.Type || ''}</span>
        <DeviceBind channelID={channelID} me={me} />
        <KimiBridgeStatus />
      </header>

      <ol className="messages" ref={listRef}>
        {groupProgressRuns(messages).map((entry, idx) =>
          entry.kind === 'progress-group' ? (
            <ProgressGroup key={entry.key} envelopes={entry.envelopes} />
          ) : (
            <MessageRow key={entry.envelope.id || idx} envelope={entry.envelope} me={me} />
          ),
        )}
        {messages.length === 0 && <li className="messages-empty">还没有消息</li>}
      </ol>

      <form className="composer" onSubmit={send}>
        <input
          name="text"
          placeholder="输入消息，回车发送"
          value={text}
          onChange={(e) => setText(e.target.value)}
          autoComplete="off"
          required
        />
        <button type="submit" disabled={sending}>
          {sending ? '…' : '发送'}
        </button>
      </form>
      {error && <p className="error composer-error">{error}</p>}
    </section>
  );
}

function MessageRow({ envelope, me }) {
  const senderID = envelope.sender?.id || envelope.sender_id || '';
  const senderKind = envelope.sender?.kind || envelope.sender_kind || 'unknown';
  const isSelf = senderID === `user:${me.id}` || senderID === me.id;
  const type = envelope.type || '';
  const visibility = envelope.visibility || 'public';

  // agent.progress is the per-turn "process bubble" — agent is mid-work
  // (tool calls in flight). Visually distinct from agent.text so the
  // user can tell the difference between intermediate steps and the
  // final reply.
  const text = envelope.payload?.text || envelope.payload?.content || '';
  return (
    <li className={`message-row sender-${senderKind} ${isSelf ? 'self' : 'other'} vis-${visibility}`}>
      <div className="message-meta">
        <span className="message-sender">{envelope.sender?.name || senderID}</span>
        <span className="message-type muted">{type}</span>
      </div>
      <div className="message-body">{text || <span className="muted">[empty payload]</span>}</div>
    </li>
  );
}

// Group consecutive agent.progress envelopes into a single render
// entry so the UI shows ONE process bubble per "thinking session"
// regardless of how many turns / steps the agent goes through. A non-
// progress envelope (e.g. agent.text final reply) closes the group.
function groupProgressRuns(messages) {
  const out = [];
  let group = null;
  for (const m of messages) {
    const t = m.type || m.Type || '';
    if (t === 'agent.progress') {
      if (!group) {
        group = { kind: 'progress-group', key: `pg-${m.id || out.length}`, envelopes: [] };
        out.push(group);
      }
      group.envelopes.push(m);
    } else {
      group = null;
      out.push({ kind: 'envelope', envelope: m });
    }
  }
  return out;
}

// ProgressGroup renders a contiguous run of agent.progress envelopes
// as ONE compact "agent working" bubble showing all tool calls in
// chronological order. Replaces the previous per-envelope ProgressRow
// which spammed the chat with one bubble per turn/step.
function ProgressGroup({ envelopes }) {
  const last = envelopes[envelopes.length - 1] || {};
  const lastPayload = last.payload || {};
  const totalTurns = lastPayload.turn_index != null ? lastPayload.turn_index : envelopes.length;

  // Flatten all tool_calls from all envelopes in order.
  const allTools = [];
  for (const e of envelopes) {
    const p = e.payload || {};
    const tcs = Array.isArray(p.tool_calls) ? p.tool_calls : [];
    for (const tc of tcs) allTools.push(tc);
  }
  // Final reasoning (if any envelope carried it).
  const reasoning = envelopes
    .map((e) => (typeof (e.payload || {}).reasoning === 'string' ? e.payload.reasoning : ''))
    .filter(Boolean)
    .pop() || '';

  return (
    <li className="message-row progress">
      <div className="progress-meta">
        <span className="progress-tag">process</span>
        <span className="progress-step">
          {envelopes.length} step{envelopes.length === 1 ? '' : 's'} · {totalTurns} turn
          {totalTurns === 1 ? '' : 's'}
        </span>
      </div>
      <div className="progress-body">
        {allTools.length === 0 && !reasoning && (
          <span className="muted">agent thinking…</span>
        )}
        {allTools.map((tc, i) => (
          <div key={i} className="progress-tool">
            <span className="progress-tool-icon">⚙</span>
            <span className="progress-tool-name">{tc.name || 'tool'}</span>
            {tc.preview && <span className="progress-tool-preview">{tc.preview}</span>}
          </div>
        ))}
        {reasoning && (
          <div className="progress-reasoning">
            <span className="progress-reasoning-icon">💭</span>
            <span>{reasoning}</span>
          </div>
        )}
      </div>
    </li>
  );
}
