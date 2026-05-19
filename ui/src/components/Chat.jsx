import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { ChannelSocket } from '../ws.js';
import { aggregateEnvelopes } from '../aggregation.js';
import DeviceBind from './DeviceBind.jsx';

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
      </header>

      <ol className="messages" ref={listRef}>
        {messages.map((m, idx) => (
          <MessageRow key={m.id || idx} envelope={m} me={me} />
        ))}
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
  if (type === 'agent.progress') {
    return <ProgressRow envelope={envelope} />;
  }

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

// ProgressRow renders an `agent.progress` envelope as a compact,
// muted bubble showing what tools the agent is calling + a short
// reasoning preview (when present). Stands apart from the main
// reply bubble so users see "agent is working" instead of silence.
function ProgressRow({ envelope }) {
  const payload = envelope.payload || {};
  const tools = Array.isArray(payload.tool_calls) ? payload.tool_calls : [];
  const reasoning = typeof payload.reasoning === 'string' ? payload.reasoning : '';
  const turnIndex = payload.turn_index;
  const stepIndex = payload.step_index;
  return (
    <li className="message-row progress">
      <div className="progress-meta">
        <span className="progress-tag">process</span>
        {turnIndex != null && (
          <span className="progress-step">
            turn {turnIndex}
            {stepIndex != null ? ` · step ${stepIndex}` : ''}
          </span>
        )}
      </div>
      <div className="progress-body">
        {tools.length === 0 && !reasoning && (
          <span className="muted">agent thinking…</span>
        )}
        {tools.map((tc, i) => (
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
