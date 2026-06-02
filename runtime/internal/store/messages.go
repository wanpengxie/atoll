package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// Messages implements kernel/storespec.MessageLog over the messages table.
//
// Per L2 §1.4.5 engine-append ACL, Messages is a PURE PERSISTENCE SINK:
// every caller MUST run the L1 §10.2 9-step Message-Write Harness chain
// FIRST (runtime/harness.Chain). The chain is the only legitimate
// principal that may call Append; agent / worker / adapter / control
// paths all flow through harness → store. Direct Append calls are a
// debug-only escape hatch that bypasses normalize, sender_kind
// overwrite, type_registry / schema validation, and The One Law
// uniqueness contract.
//
// Append performs two things in one transaction:
//  1. dedupe check by envelope.id (returns Deduped=true if existing row).
//  2. INSERT the messages row (raises *AppendError on terminal-duplicate
//     UNIQUE INDEX violation per L2 §1.4.1).
//     Optional framework observers may enqueue same-transaction side rows
//     after the message insert.
//
// IsTerminal computation is **simplified** for launch T3: response rows
// with non-empty parent_id are treated terminal by default (L2 §1.4.1
// `payload_status` convention) unless the row's type_registry entry
// declares `single-response` — which the harness step 8 has already
// resolved by the time Append is called.
//
// **Protocol contract (FIX-T10):** `env.IsTerminal` is NOT a caller-
// settable knob. It must be resolved by the harness chain (step 8 +
// type_registry semantics) BEFORE Append is reached. Store treats the
// field as a pre-computed harness output and persists it verbatim; it
// neither validates nor recomputes the value. This keeps store layer-
// independent of type_registry semantics and keeps the harness as the
// single source of truth for terminal classification.
type Messages struct {
	db *sql.DB
}

// NewMessages returns a *Messages bound to the channel sqlite.
//
// v2: no fencing — the channel has a SINGLE writer (server harness) by
// construction (proto-v2-physical §4), so the channel-write fence is
// obsolete. No outbox observer — the v1 framework-owned same-tx side-table
// projection is removed (truth lives on server; client push is the gateway
// seam).
func newMessages(db *sql.DB) *Messages { return &Messages{db: db} }

// Append implements storespec.MessageLog. The harness supplies the
// pre-computed is_terminal (step 8) + canonical_hash (step dedupe) since the
// pure envelope no longer carries those store-derived columns.
func (m *Messages) Append(ctx context.Context, env *message.Envelope, isTerminal bool, canonicalHash string) (storespec.AppendResult, error) {
	if env == nil {
		return storespec.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return storespec.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	// FIX-T10 protocol defense: Payload is a REQUIRED field per L0 §2.1
	// (every envelope carries a payload object, even if the body is the
	// empty JSON object `{}`). Silently coercing nil to `{}` masks
	// caller bugs that bypass harness Step 4 normalize
	// and lets non-canonical rows enter the store. Reject loudly so the
	// caller (harness chain) is forced to materialize the payload before
	// reaching the persistence sink.
	if env.Payload == nil {
		return storespec.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := appendTx(ctx, tx, env, isTerminal, canonicalHash)
	if err != nil {
		return storespec.AppendResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append commit: %w", err)
	}
	return res, nil
}

// appendTx is the raw dedupe-check + INSERT of one envelope row within an
// existing tx. It is an UNEXPORTED package func, NOT a method on an exported
// type: there is deliberately no public "append into this tx" primitive. The
// only callers are Append (which wraps it in its own tx) and the membership
// control-plane op in actors.go (which needs the row + its mirror event in one
// atomic tx). No receiver is taken — it touches only tx, so it can never be a
// capability someone obtains by constructing a *Messages.
func appendTx(ctx context.Context, tx *sql.Tx, env *message.Envelope, isTerminal bool, canonicalHash string) (storespec.AppendResult, error) {
	if tx == nil {
		return storespec.AppendResult{}, errors.New("store: append tx nil")
	}
	if env == nil {
		return storespec.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return storespec.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	if env.Payload == nil {
		return storespec.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}

	// 1) dedupe by envelope.id
	const selExist = `SELECT seq, is_terminal FROM messages WHERE id=?`
	var existingSeq int64
	var existingTerm int
	switch err := tx.QueryRowContext(ctx, selExist, env.ID).Scan(&existingSeq, &existingTerm); {
	case err == nil:
		// Dedupe path — row already exists. Return Deduped=true with the
		// existing seq/is_terminal (carried on AppendResult, not on the
		// pure envelope which no longer holds store-derived columns).
		return storespec.AppendResult{Seq: storespec.Seq(existingSeq), IsTerminal: existingTerm == 1, Deduped: true}, nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through
	default:
		return storespec.AppendResult{}, fmt.Errorf("store: append dedupe lookup: %w", err)
	}

	// 2) INSERT row
	docRefsJSON, err := encodeDocRefs(env.DocRefs)
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append docrefs encode: %w", err)
	}
	crossRefsJSON, err := encodeCrossChannelRefs(env.CrossChannelRefs)
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append cross_channel_refs encode: %w", err)
	}
	audJSON, err := json.Marshal(env.Audience)
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append audience encode: %w", err)
	}

	const ins = `INSERT INTO messages (
	   id, ts, ts_received, channel_id,
	   sender_kind, sender_id, sender_name,
	   kind, type, payload,
	   parent_id, correlation_id, doc_refs, cross_channel_refs,
	   visibility, audience, not_before, expires_at,
	   delivered_at, delivery_failed_at, last_error, attempts,
	   is_terminal, canonical_hash
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	terminalInt := 0
	if isTerminal {
		terminalInt = 1
	}
	// A freshly-appended row has never failed delivery: delivery_failed_at
	// NULL, attempts 0. (These store-derived scheduling columns are no
	// longer carried on the pure envelope.)
	res, err := tx.ExecContext(ctx, ins,
		env.ID, env.TS, env.TSReceived, env.ChannelID,
		string(env.Sender.Kind), string(env.Sender.ID), nullableString(env.Sender.Name),
		string(env.Kind), env.Type, string(env.Payload),
		nullableString(string(env.ParentID)), nullableString(string(env.CorrelationID)), nullableString(docRefsJSON), nullableString(crossRefsJSON),
		env.Visibility, string(audJSON),
		nullableInt(env.NotBefore), nullableInt(env.ExpiresAt),
		nullableInt(env.DeliveredAt), nil,
		nullableString(env.LastError), 0,
		terminalInt, canonicalHash,
	)
	if err != nil {
		return storespec.AppendResult{}, classifyAppendErr(err, string(env.ID))
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return storespec.AppendResult{}, fmt.Errorf("store: append last id: %w", err)
	}

	return storespec.AppendResult{Seq: storespec.Seq(seq), IsTerminal: isTerminal, Deduped: false}, nil
}

// PendingDue returns up to `limit` future-message rows that are due for
// dispatch: `not_before IS NOT NULL AND not_before <= ? AND
// delivered_at IS NULL`. Ordered by seq ASC so the scheduler ticks the
// oldest backlog first (matches L1 §5.3 + §6.4 monotonic processing).
//
// `limit <= 0` is clamped to 64 — a bounded per-channel page so the
// scheduler tick has a bounded cost.
func (m *Messages) PendingDue(ctx context.Context, nowMs int64, limit int) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages
	             WHERE not_before IS NOT NULL
	               AND not_before <= ?
	               AND delivered_at IS NULL
	             ORDER BY seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("store: pending due: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: pending due scan: %w", err)
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: pending due rows: %w", err)
	}
	return out, nil
}

// MaxSeq returns the highest seq written for the channel (0 when empty). It is
// the client cursor anchor (last_received_seq): an SDK fetches it before
// subscribing so the WS tail starts from "now".
func (m *Messages) MaxSeq(ctx context.Context, channelID channel.ID) (int64, error) {
	const q = `SELECT COALESCE(MAX(seq), 0) FROM messages WHERE channel_id = ?`
	var seq int64
	if err := m.db.QueryRowContext(ctx, q, channelID).Scan(&seq); err != nil {
		return 0, fmt.Errorf("store: max seq: %w", err)
	}
	return seq, nil
}

// ReadAfterSeq returns up to `limit` envelopes with seq > afterSeq for the
// channel, in seq order. It is the client-push tail: a subscribed WS reads
// forward from its cursor, so no committed envelope is ever missed (the push
// notification only signals "something new" — correctness is seq-based here).
func (m *Messages) ReadAfterSeq(ctx context.Context, channelID channel.ID, afterSeq int64, limit int) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 256
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages
	             WHERE channel_id = ? AND seq > ?
	             ORDER BY seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, channelID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("store: read after seq: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: read after seq scan: %w", err)
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

// LongPendingRequests returns up to `limit` request rows that have
// blown past their expires_at deadline without earning a terminal
// response. Used by the long-pending scheduler (L1 §6.4) to synthesise
// the system-side failed terminal for receivers the trigger gateway
// can no longer honour (deregistered actor, silent agent / system).
//
// Filter clause (uses ix_messages_expires + ix_messages_parent):
//
//   - kind='request'           — only requests can earn a terminal.
//   - expires_at IS NOT NULL   — rows without an SLA never time out.
//   - expires_at > 0           — 0 = "no deadline" sentinel (defensive).
//   - expires_at <= nowMs      — deadline has actually passed.
//   - is_terminal = 0          — defensive; requests should never carry
//     the terminal bit, but the schema allows
//     it and we don't want to re-emit on a
//     row that already settled.
//   - NOT EXISTS (terminal response with parent_id=m.id) — The One Law
//     says exactly one terminal response per
//     request. If one is already on disk we
//     skip this row.
//
// `limit <= 0` clamps to 64 — matches PendingDue's bounded page so a
// single scheduler tick has bounded per-channel cost.
// Ordered by seq ASC so the oldest backlog is drained first (matches L1
// §6.4 monotonic processing semantics).
//
// NOTE: this query intentionally does NOT join actor_registry. The
// caller (daemon scanLongPending) performs the receiver-kind classification
// because the L1 §6.4 dispatch matrix (tool/human skip vs. agent/system
// emit unanswered_timeout vs. deregistered emit receiver_unavailable)
// requires the audience JSON parse and a registry lookup — pushing that
// into SQL would couple the store to the receiver-policy table.
func (m *Messages) LongPendingRequests(ctx context.Context, nowMs int64, limit int) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages m
	             WHERE m.kind = 'request'
	               AND m.expires_at IS NOT NULL
	               AND m.expires_at > 0
	               AND m.expires_at <= ?
	               AND m.is_terminal = 0
	               AND NOT EXISTS (
	                 SELECT 1 FROM messages r
	                  WHERE r.parent_id = m.id
	                    AND r.kind = 'response'
	                    AND r.is_terminal = 1
	               )
	             ORDER BY m.seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, nowMs, limit)
	if err != nil {
		return nil, fmt.Errorf("store: long pending requests: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: long pending requests scan: %w", err)
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: long pending requests rows: %w", err)
	}
	return out, nil
}

// OpenRequestsForActor returns pending request rows addressed to actorID
// (first audience member) with no final response yet — regardless of
// expires_at. Used by the actorrt death-signal supervisor: when a cell dies,
// the substrate closes every in-flight request to the dead actor with
// receiver_unavailable. The substrate never guesses "slow" — it only reports
// death it positively observed (construction-spec §3.3).
func (m *Messages) OpenRequestsForActor(ctx context.Context, actorID actor.ActorID, limit int) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages m
	             WHERE m.kind = 'request'
	               AND m.is_terminal = 0
	               AND json_extract(m.audience, '$[0]') = ?
	               AND NOT EXISTS (
	                 SELECT 1 FROM messages r
	                  WHERE r.parent_id = m.id
	                    AND r.kind = 'response'
	                    AND r.is_terminal = 1
	               )
	             ORDER BY m.seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, string(actorID), limit)
	if err != nil {
		return nil, fmt.Errorf("store: open requests for actor: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: open requests for actor scan: %w", err)
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: open requests for actor rows: %w", err)
	}
	return out, nil
}

// RetryableDeliveries returns request rows whose delivery was previously
// recorded as failed (delivery_failed_at IS NOT NULL, delivered_at still
// NULL) and whose per-attempt backoff window has elapsed
// (delivery_failed_at + backoff(attempts) <= now). Used by the daemon's
// bounded-retry pass (§3 ack 三分 / §6 step1) to re-drive accept of a
// trigger whose previous push failed — instead of relying on the next
// unrelated request to re-spawn the worker.
//
// The query DOES NOT filter on the attempts cap: the daemon applies the
// cap so it can distinguish "retry once more" from "give up and emit a
// terminal closure". Rows that already earned a terminal response are
// excluded (the request is settled regardless of delivery bookkeeping).
//
// `backoffFn(attempts)` returns the minimum elapsed-ms since the last
// failure before a row of that attempt count is eligible again — the
// daemon owns the backoff curve so the store stays policy-free.
func (m *Messages) RetryableDeliveries(
	ctx context.Context,
	nowMs int64,
	limit int,
	backoffFn func(attempts int64) int64,
) ([]storespec.StoredRow, error) {
	if limit <= 0 {
		limit = 64
	}
	if backoffFn == nil {
		backoffFn = func(int64) int64 { return 0 }
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages m
	             WHERE m.kind = 'request'
	               AND m.delivered_at IS NULL
	               AND m.delivery_failed_at IS NOT NULL
	               AND m.is_terminal = 0
	               AND NOT EXISTS (
	                 SELECT 1 FROM messages r
	                  WHERE r.parent_id = m.id
	                    AND r.kind = 'response'
	                    AND r.is_terminal = 1
	               )
	             ORDER BY m.seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("store: retryable deliveries: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []storespec.StoredRow
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: retryable deliveries scan: %w", err)
		}
		// Backoff gate applied in Go so the curve stays daemon-owned.
		if env.DeliveryFailedAt != nil {
			if *env.DeliveryFailedAt+backoffFn(env.Attempts) > nowMs {
				continue
			}
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: retryable deliveries rows: %w", err)
	}
	return out, nil
}

// MarkDelivered stamps messages.delivered_at when it is still NULL and
// clears any previous delivery error. The UPDATE is idempotent
// (rowsAffected=0 when the row was already delivered or missing — caller
// may treat as no-op). Used by the scheduler dispatch path AND the
// harness post-write fan-out, so two concurrent callers cannot
// double-stamp delivery time.
func (m *Messages) MarkDelivered(ctx context.Context, id message.ID, atMs int64) error {
	if id == "" {
		return errors.New("store: mark delivered empty id")
	}
	const q = `UPDATE messages
	             SET delivered_at=?, delivery_failed_at=NULL, last_error=NULL
	           WHERE id=? AND delivered_at IS NULL`
	if _, err := m.db.ExecContext(ctx, q, atMs, id); err != nil {
		return fmt.Errorf("store: mark delivered %q: %w", id, err)
	}
	return nil
}

// MarkDeliveryError records a failed delivery attempt while leaving
// delivered_at NULL so the row remains retryable.
func (m *Messages) MarkDeliveryError(ctx context.Context, id message.ID, atMs int64, errText string) error {
	if id == "" {
		return errors.New("store: mark delivery error empty id")
	}
	if errText == "" {
		errText = "delivery failed"
	}
	const q = `UPDATE messages
	             SET delivery_failed_at=?, last_error=?, attempts=attempts+1
	           WHERE id=? AND delivered_at IS NULL`
	if _, err := m.db.ExecContext(ctx, q, atMs, errText, id); err != nil {
		return fmt.Errorf("store: mark delivery error %q: %w", id, err)
	}
	return nil
}

// HasFinalResponse implements storespec.MessageLog. Returns true when at
// least one kind=response row exists for parent_id=parentID with the
// row's is_terminal column set — store layer has already materialised
// the (kind==response && payload.status ∈ {completed, failed})
// derivation per proto-layer0 §2.5.1 + L2 §1.4.1, so the bit is the
// canonical "final exists" answer.
//
// Used by harness Step 8 (proto-layer1 §2.8) to distinguish
// final-after-final from provisional-after-final. The
// `ux_terminal_response_per_request` UNIQUE INDEX guards final-after-
// final at INSERT time; this query is the pre-check that lets the
// harness reject provisional-after-final with the correct closed-set
// reason instead of silently appending a zombie row.
func (m *Messages) HasFinalResponse(ctx context.Context, channelID channel.ID, parentID message.ID) (bool, error) {
	_ = channelID // per-channel sqlite already scopes the query
	if parentID == "" {
		return false, nil
	}
	const q = `SELECT 1 FROM messages
	            WHERE parent_id = ?
	              AND kind = 'response'
	              AND is_terminal = 1
	            LIMIT 1`
	var one int
	switch err := m.db.QueryRowContext(ctx, q, parentID).Scan(&one); {
	case err == nil:
		return true, nil
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	default:
		return false, fmt.Errorf("store: has final response: %w", err)
	}
}

// FinalResponseSender implements storespec.MessageLog — returns the sender.id of
// the existing Layer 1 final response for parentID (used by harness Step 8
// to detect a caller self-close so a late receiver final can be rewritten
// to observability rather than rejected).
func (m *Messages) FinalResponseSender(ctx context.Context, channelID channel.ID, parentID message.ID) (actor.ActorID, bool, error) {
	_ = channelID
	if parentID == "" {
		return "", false, nil
	}
	const q = `SELECT sender_id FROM messages
	            WHERE parent_id = ?
	              AND kind = 'response'
	              AND is_terminal = 1
	            LIMIT 1`
	var sender string
	switch err := m.db.QueryRowContext(ctx, q, parentID).Scan(&sender); {
	case err == nil:
		return actor.ActorID(sender), true, nil
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	default:
		return "", false, fmt.Errorf("store: final response sender: %w", err)
	}
}

// LookupCanonicalHash implements storespec.MessageLog — returns the row's
// stored canonical_hash for StepDedupe's pre-normalize comparison
// (proto-layer1 §2.3).
func (m *Messages) LookupCanonicalHash(ctx context.Context, channelID channel.ID, id message.ID) (string, bool, error) {
	_ = channelID
	const q = `SELECT canonical_hash FROM messages WHERE id=?`
	var hash string
	err := m.db.QueryRowContext(ctx, q, id).Scan(&hash)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("store: lookup canonical hash: %w", err)
	}
	return hash, true, nil
}

// FindByID implements storespec.MessageLog.
func (m *Messages) FindByID(ctx context.Context, channelID channel.ID, id message.ID) (*storespec.StoredRow, bool, error) {
	_ = channelID // channel_id is enforced by the per-channel db file; query stays scoped.
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs, cross_channel_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages WHERE id=?`
	row := m.db.QueryRowContext(ctx, q, id)
	sr, err := scanEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return &sr, true, nil
}

// rowScanner abstracts *sql.Row / *sql.Rows for the Scan call so
// PendingDue (multi-row) can share the materialization code with FindByID.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanEnvelope materializes a row into a StoredRow.
func scanEnvelope(row *sql.Row) (storespec.StoredRow, error) {
	return scanEnvelopeFrom(row)
}

// scanEnvelopeRows materializes the current *sql.Rows position into a
// StoredRow. Caller is responsible for rows.Next() / rows.Close().
func scanEnvelopeRows(rows *sql.Rows) (storespec.StoredRow, error) {
	return scanEnvelopeFrom(rows)
}

// scanEnvelopeFrom is the shared implementation. It returns a StoredRow:
// the pure Envelope (17 fields + delivery metadata) plus the store-derived
// columns (seq / is_terminal / attempts / delivery_failed_at) that kernel
// keeps off the envelope.
func scanEnvelopeFrom(s rowScanner) (storespec.StoredRow, error) {
	var sr storespec.StoredRow
	env := &sr.Envelope
	var kind, sKind, senderID, vis string
	var audJSON, payloadStr string
	var docRefsStr, crossRefsStr sql.NullString
	var notBefore, expiresAt, deliveredAt, deliveryFailedAt sql.NullInt64
	var termInt int
	if err := s.Scan(
		&env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&sKind, &senderID, &env.Sender.Name,
		&kind, &env.Type, &payloadStr,
		&env.ParentID, &env.CorrelationID, &docRefsStr, &crossRefsStr,
		&vis, &audJSON,
		&notBefore, &expiresAt,
		&deliveredAt, &deliveryFailedAt, &env.LastError, &sr.Attempts,
		&termInt, &sr.Seq,
	); err != nil {
		return storespec.StoredRow{}, err
	}
	env.Sender.Kind = actor.Kind(sKind)
	env.Sender.ID = actor.ActorID(senderID)
	env.Kind = message.Kind(kind)
	env.Visibility = message.Visibility(vis)
	env.Payload = json.RawMessage(payloadStr)
	if err := json.Unmarshal([]byte(audJSON), &env.Audience); err != nil {
		return storespec.StoredRow{}, fmt.Errorf("store: scan audience: %w", err)
	}
	if docRefsStr.Valid {
		var refs []string
		if err := json.Unmarshal([]byte(docRefsStr.String), &refs); err != nil {
			return storespec.StoredRow{}, fmt.Errorf("store: scan doc_refs: %w", err)
		}
		env.DocRefs = &refs
	}
	if crossRefsStr.Valid {
		var refs []message.CrossChannelRef
		if err := json.Unmarshal([]byte(crossRefsStr.String), &refs); err != nil {
			return storespec.StoredRow{}, fmt.Errorf("store: scan cross_channel_refs: %w", err)
		}
		env.CrossChannelRefs = &refs
	}
	if notBefore.Valid {
		v := notBefore.Int64
		env.NotBefore = &v
	}
	if expiresAt.Valid {
		v := expiresAt.Int64
		env.ExpiresAt = &v
	}
	if deliveredAt.Valid {
		v := deliveredAt.Int64
		env.DeliveredAt = &v
	}
	if deliveryFailedAt.Valid {
		v := deliveryFailedAt.Int64
		sr.DeliveryFailedAt = &v
	}
	sr.IsTerminal = termInt == 1
	return sr, nil
}

// encodeDocRefs maps Envelope.DocRefs tri-state to JSON storage:
//   - nil pointer       → "" (column NULL)
//   - non-nil pointer   → JSON of the slice (including "[]" for empty)
func encodeDocRefs(d *[]string) (string, error) {
	if d == nil {
		return "", nil
	}
	b, err := json.Marshal(*d)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// encodeCrossChannelRefs maps Envelope.CrossChannelRefs tri-state to JSON
// storage:
//   - nil pointer       → "" (column NULL)
//   - non-nil pointer   → JSON of the slice (including "[]" for empty)
func encodeCrossChannelRefs(refs *[]message.CrossChannelRef) (string, error) {
	if refs == nil {
		return "", nil
	}
	b, err := json.Marshal(*refs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nullableInt(p *int64) any {
	if p == nil {
		return nil
	}
	return *p
}

// classifyAppendErr maps sqlite UNIQUE constraint failures to typed
// *AppendError so the harness chain can map them to HarnessRejectReason.
func classifyAppendErr(err error, envID string) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "UNIQUE constraint failed: messages.id"):
		return &storespec.AppendError{
			Reason:           "harness_id_duplicate_conflict",
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	case strings.Contains(msg, "ux_terminal_response_per_request") ||
		strings.Contains(msg, "UNIQUE constraint failed: messages.parent_id") ||
		strings.Contains(msg, "parent_id, kind, is_terminal"):
		return &storespec.AppendError{
			Reason:           "harness_terminal_duplicate",
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	default:
		return fmt.Errorf("store: append insert: %w", err)
	}
}
