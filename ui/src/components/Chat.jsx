import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { ChannelSocket } from '../ws.js';
import { aggregateEnvelopes } from '../aggregation.js';
import ChannelDeviceBar from './ChannelDeviceBar.jsx';
import {
  KIND,
  isProvisionalResponse,
  isFinalResponse,
  provisionalDisplay,
  actorLocalName,
  parseLayer3Status,
} from '../protocol.js';

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
      setMessages((prev) => {
        // Idempotent append keyed by envelope id. WS is live-only (the
        // server does NOT replay history on a since_seq-less subscribe),
        // so the HTTP initial load owns history. The only overlap is the
        // narrow race where a message arrives between the HTTP query and
        // the WS subscribe registering server-side — dedup by id keeps
        // that from double-rendering. (NF1)
        const id = envelope?.id || envelope?.ID;
        if (id) {
          for (const m of prev) {
            if ((m?.id || m?.ID) === id) return prev;
          }
        }
        return [...prev, envelope];
      });
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

  // Derive the set of actor IDs currently emitting provisional responses
  // (i.e. there is at least one provisional response without a matching
  // final response in the log). ChannelDeviceBar uses this to badge
  // device chips with an in-flight spinner.
  const inFlightActors = useMemo(() => {
    const finalByParent = new Map(); // parent_id → true
    for (const m of messages) {
      if (isFinalResponse(m)) {
        const pid = m.parent_id || m.parentID;
        if (pid) finalByParent.set(pid, true);
      }
    }
    const inflight = new Set();
    for (const m of messages) {
      if (!isProvisionalResponse(m)) continue;
      const pid = m.parent_id || m.parentID;
      if (pid && finalByParent.has(pid)) continue;
      const senderID = m.sender?.id || m.sender_id || '';
      if (senderID) inflight.add(senderID);
    }
    return inflight;
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
        <ChannelDeviceBar channelID={channelID} inFlightActors={inFlightActors} />
      </header>

      <ol className="messages" ref={listRef}>
        {groupRenderEntries(messages).map((entry, idx) => {
          if (entry.kind === 'progress-group') {
            return <ProgressGroup key={entry.key} envelopes={entry.envelopes} />;
          }
          if (entry.kind === 'provisional-group') {
            return <ProvisionalGroup key={entry.key} envelopes={entry.envelopes} />;
          }
          return <MessageRow key={entry.envelope.id || idx} envelope={entry.envelope} me={me} />;
        })}
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

// Group rendering rules — produce render entries from the raw envelope
// log:
//
//   1. agent.progress runs collapse into one ProgressGroup (existing
//      per-turn "thinking" bubble pattern).
//   2. Provisional responses (kind=response, status not in final set)
//      collapse per parent_id (the request being serviced) into one
//      ProvisionalGroup card that dynamically updates as new status
//      ticks arrive. When the matching final response lands, the
//      group is suppressed entirely so the final response (rendered
//      via MessageRow) replaces the in-progress card.
//
// Group keys are stable per parent_id so React reconciliation reuses
// the same DOM node across status ticks.
function groupRenderEntries(messages) {
  // First pass: which parent_ids have already received a final response?
  // Those parents' provisional groups are dropped (final replaces them).
  const finalByParent = new Set();
  for (const m of messages) {
    if (!isFinalResponse(m)) continue;
    const pid = m.parent_id || m.parentID;
    if (pid) finalByParent.add(pid);
  }

  const out = [];
  let progressGroup = null;
  const provGroups = new Map(); // parent_id → entry ref in `out`

  for (const m of messages) {
    const t = m.type || m.Type || '';

    if (t === 'agent.progress') {
      if (!progressGroup) {
        progressGroup = { kind: 'progress-group', key: `pg-${m.id || out.length}`, envelopes: [] };
        out.push(progressGroup);
      }
      progressGroup.envelopes.push(m);
      continue;
    }
    progressGroup = null;

    if (isProvisionalResponse(m)) {
      const pid = m.parent_id || m.parentID || '';
      // If a final already exists for this request, suppress provisional
      // entirely — the final response row replaces the in-progress card.
      if (pid && finalByParent.has(pid)) continue;
      // Group consecutive provisional ticks per parent_id. If the same
      // parent_id appears later (after an interleaved unrelated message),
      // we reuse the same group entry so all ticks accrete in one card.
      const existing = pid ? provGroups.get(pid) : null;
      if (existing) {
        existing.envelopes.push(m);
        continue;
      }
      const entry = {
        kind: 'provisional-group',
        key: `prov-${pid || m.id || out.length}`,
        envelopes: [m],
      };
      if (pid) provGroups.set(pid, entry);
      out.push(entry);
      continue;
    }

    // Final response or any other envelope: emit a normal row. Note that
    // final response → MessageRow rendering shows the completed/failed
    // payload; provisional grouping for that parent_id was already
    // suppressed above.
    out.push({ kind: 'envelope', envelope: m });
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

// ProvisionalGroup renders a run of provisional response envelopes
// (kind=response, payload.status not in {completed, failed}) for a
// single in-flight request as ONE dynamically-updating "in-progress"
// card. The most recent status drives the headline; the full tick log
// is shown as a vertical timeline.
//
// When the matching final response (completed/failed) arrives, the
// grouping function suppresses this entry entirely and the final
// response renders in its place — see groupRenderEntries above.
function ProvisionalGroup({ envelopes }) {
  const last = envelopes[envelopes.length - 1] || {};
  const senderID = last.sender?.id || last.sender_id || '';
  const senderName = last.sender?.name || senderID || 'tool';
  const lastPayload = last.payload || {};
  const lastStatus = lastPayload.status || lastPayload.Status || '';
  const head = provisionalDisplay(lastStatus);
  const layer3 = parseLayer3Status(lastStatus);
  const adapter = layer3 ? layer3.namespace : actorLocalName(senderID);

  // Last-known ETA / retry hints (Layer-2 spec §2.5.2 detail fields).
  const etaMs = numericField(lastPayload, ['eta_ms', 'etaMs', 'retry_at_ms', 'retry_after_ms', 'retryAfterMs']);
  const queuePos = numericField(lastPayload, ['queue_position', 'queuePosition']);
  const pct = numericField(lastPayload, ['progress_percent', 'progressPercent']);

  return (
    <li className={`message-row provisional provisional-${head.tone}`}>
      <div className="provisional-meta">
        <span className="provisional-spinner" aria-hidden="true">{head.icon}</span>
        <span className="provisional-headline">{head.label}</span>
        {adapter && <span className="provisional-adapter">· {adapter}</span>}
        <span className="provisional-sender muted">{senderName}</span>
      </div>
      {(etaMs != null || queuePos != null || pct != null) && (
        <div className="provisional-hints">
          {pct != null && (
            <span className="provisional-hint">进度 {Math.round(pct * (pct <= 1 ? 100 : 1))}%</span>
          )}
          {queuePos != null && <span className="provisional-hint">队列位置 {queuePos}</span>}
          {etaMs != null && <span className="provisional-hint">ETA {formatEtaMs(etaMs)}</span>}
        </div>
      )}
      {envelopes.length > 1 && (
        <ol className="provisional-trail">
          {envelopes.map((e, i) => {
            const s = e.payload?.status || e.payload?.Status || '';
            const d = provisionalDisplay(s);
            return (
              <li key={e.id || i} className="provisional-trail-tick">
                <span className="provisional-trail-icon" aria-hidden="true">{d.icon}</span>
                <span className="provisional-trail-label">{d.label}</span>
              </li>
            );
          })}
        </ol>
      )}
    </li>
  );
}

function numericField(payload, keys) {
  for (const k of keys) {
    const v = payload?.[k];
    if (typeof v === 'number' && !Number.isNaN(v)) return v;
  }
  return null;
}

function formatEtaMs(ms) {
  if (typeof ms !== 'number' || ms <= 0) return '';
  if (ms < 1000) return `${ms}ms`;
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `${sec}s`;
  const min = Math.round(sec / 60);
  return `${min}m`;
}
