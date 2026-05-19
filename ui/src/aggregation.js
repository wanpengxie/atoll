// aggregation.js — fold streamed agent.text chunks into a single bubble.
//
// Problem: a single agent reply arrives as N chunk envelopes (each with a
// fragment in payload.text) followed by one final envelope that carries
// the full text plus a terminal signal (next_action="done" or any
// non-empty stop_reason). All envelopes share the same correlation_id
// and parent_id. Rendering each envelope as its own bubble produces N+1
// fragments instead of one growing reply.
//
// Strategy:
//   - Walk envelopes in seq order. Each envelope yields exactly one
//     "bubble" entry, UNLESS it is an agent.text chunk that belongs to
//     a correlation_id already represented by an earlier agent.text
//     bubble — in that case the existing bubble absorbs the new chunk.
//   - When the absorbed envelope is the terminal one (next_action=done
//     or stop_reason set), the bubble's text is replaced wholesale with
//     payload.text (the server has already coalesced the full reply on
//     the final frame; concatenating chunks would duplicate it).
//   - While streaming (no terminal yet) the bubble's text is the
//     concatenation of every chunk's payload.text in seq order.
//   - Non agent.text envelopes (human.text and everything else) always
//     produce a standalone bubble — no aggregation, original ordering
//     preserved by seq.
//
// The function is pure and idempotent: feed it the same envelope list
// (or a longer prefix) and you get the same bubble list. This lets us
// re-derive bubbles on every render without maintaining a separate
// reducer.
//
// Future extension: when other agent.* streaming types appear
// (agent.tool_call, etc.), extend the AGGREGATED_TYPES set and the
// terminal-signal predicate; the rest of the logic is type-agnostic.

const AGGREGATED_TYPES = new Set(['agent.text']);

/**
 * Determine if an envelope should be treated as the terminal frame in a
 * streamed reply. The server emits intermediate chunks with just
 * {text:"..."} payloads; the final frame additionally carries
 * next_action and/or stop_reason. Either signal is sufficient.
 */
function isTerminalFrame(envelope) {
  const p = envelope && envelope.payload;
  if (!p || typeof p !== 'object') return false;
  if (typeof p.next_action === 'string' && p.next_action.length > 0) return true;
  if (typeof p.stop_reason === 'string' && p.stop_reason.length > 0) return true;
  return false;
}

function payloadText(envelope) {
  const p = envelope && envelope.payload;
  if (!p || typeof p !== 'object') return '';
  if (typeof p.text === 'string') return p.text;
  if (typeof p.content === 'string') return p.content;
  return '';
}

/**
 * Fold a flat ordered envelope list into a list of bubbles.
 *
 * Each bubble is `{ key, envelope, text, streaming, envelopes }`:
 *   - key:        stable id for React reconciliation
 *   - envelope:   the headline envelope (first chunk for streamed bubbles,
 *                 or the sole envelope for standalone bubbles). Keeps
 *                 sender / type / visibility info intact for the row UI.
 *   - text:       the text to render (concatenated chunks while streaming;
 *                 final payload.text once terminal arrives).
 *   - streaming:  true while no terminal frame has been seen yet.
 *   - envelopes:  all envelopes folded into this bubble (in seq order),
 *                 for debugging / future trace UI.
 */
export function aggregateEnvelopes(envelopes) {
  const sorted = (envelopes || []).slice().sort(
    (a, b) => Number(a?.seq || 0) - Number(b?.seq || 0),
  );

  const bubbles = [];
  // Map correlation_id → bubble index in `bubbles`, only for aggregated
  // types. Cleared when the bubble's correlation_id is empty (we never
  // aggregate empty-correlation envelopes — each gets its own bubble).
  const openByCorr = new Map();

  for (const env of sorted) {
    if (!env) continue;
    const type = env.type || '';
    const corr = env.correlation_id || '';

    const canAggregate = AGGREGATED_TYPES.has(type) && corr.length > 0;
    if (canAggregate && openByCorr.has(corr)) {
      const idx = openByCorr.get(corr);
      const bubble = bubbles[idx];
      bubble.envelopes.push(env);
      if (isTerminalFrame(env)) {
        // Final frame carries the complete text already — replace, not append.
        bubble.text = payloadText(env);
        bubble.streaming = false;
      } else if (bubble.streaming) {
        // Mid-stream chunk — append.
        bubble.text = bubble.text + payloadText(env);
      } else {
        // Terminal already seen for this correlation; later chunk is a
        // late arrival. Append defensively so nothing is silently lost,
        // but keep streaming=false so the UI stops showing the cursor.
        bubble.text = bubble.text + payloadText(env);
      }
      continue;
    }

    const text = payloadText(env);
    const terminal = canAggregate ? isTerminalFrame(env) : true;
    const bubble = {
      key: env.id || `seq-${env.seq}`,
      envelope: env,
      text,
      streaming: canAggregate && !terminal,
      envelopes: [env],
    };
    bubbles.push(bubble);
    if (canAggregate && !terminal) {
      openByCorr.set(corr, bubbles.length - 1);
    }
  }

  return bubbles;
}
