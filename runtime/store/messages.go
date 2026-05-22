package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/channel"
	klog "github.com/wanpengxie/ActOS/kernel/log"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// Messages implements kernel/log.MessageLog over the messages table.
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
// Append performs three things in one transaction:
//  1. dedupe check by envelope.id (returns Deduped=true if existing row).
//  2. INSERT the messages row (raises *AppendError on terminal-duplicate
//     UNIQUE INDEX violation per L2 §1.4.1).
//  3. INSERT the view_sync_outbox row keyed by seq (drives daemon push).
//
// IsTerminal computation is **simplified** for M1.5 T3: response rows
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
	db   *sql.DB
	lock *ChannelLock // optional — when non-nil, Append enforces fencing.
}

const (
	viewSyncOutboxHighWatermark = 10000
	viewSyncOutboxLowWatermark  = viewSyncOutboxHighWatermark / 2
	viewSyncOutboxThrottleSleep = 50 * time.Millisecond
)

// NewMessages returns a *Messages bound to the channel sqlite WITHOUT
// fencing enforcement. Use this constructor for store-level unit tests
// (transit_test / store_test) and helper paths that seed rows before
// the channel_lock row is even installed.
//
// Production daemon wiring MUST use NewMessagesWithLock so every
// caller-driven mutation is guarded by the FIX-T6 fencing gate.
func NewMessages(db *sql.DB) *Messages { return &Messages{db: db} }

// NewMessagesWithLock returns a *Messages whose Append validates the
// explicit fencing tuple against the channel's channel_lock row INSIDE
// the same transaction as the row INSERT. A stale daemon (or a forgotten
// Append fencing argument) is rejected with
// klog.AppendError{Reason: HarnessWorkerFencingStale} and neither the
// messages row nor the view_sync_outbox row is written.
func NewMessagesWithLock(db *sql.DB, lock *ChannelLock) *Messages {
	return &Messages{db: db, lock: lock}
}

// Append implements log.MessageLog.
func (m *Messages) Append(ctx context.Context, env *message.Envelope, fencing klog.FencingTuple) (klog.AppendResult, error) {
	if env == nil {
		return klog.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return klog.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	// FIX-T10 protocol defense: Payload is a REQUIRED field per L0 §2.1
	// (every envelope carries a payload object, even if the body is the
	// empty JSON object `{}`). Silently coercing nil to `{}` masks
	// caller bugs that bypass harness Step 4 normalize
	// and lets non-canonical rows enter the store. Reject loudly so the
	// caller (harness chain) is forced to materialize the payload before
	// reaching the persistence sink.
	if env.Payload == nil {
		return klog.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}
	if err := m.waitForViewSyncOutboxAdmission(ctx); err != nil {
		return klog.AppendResult{}, err
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	res, err := m.AppendTx(ctx, tx, env, fencing)
	if err != nil {
		return klog.AppendResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append commit: %w", err)
	}
	return res, nil
}

func (m *Messages) waitForViewSyncOutboxAdmission(ctx context.Context) error {
	blocking := false
	for {
		var n int
		if err := m.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM view_sync_outbox WHERE status IN ('pending','pushed')`,
		).Scan(&n); err != nil {
			return fmt.Errorf("store: view-sync outbox admission count: %w", err)
		}
		threshold := viewSyncOutboxHighWatermark
		if blocking {
			threshold = viewSyncOutboxLowWatermark
		}
		if n < threshold || (!blocking && n <= viewSyncOutboxHighWatermark) {
			return nil
		}
		blocking = true
		timer := time.NewTimer(viewSyncOutboxThrottleSleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// AppendTx appends env using an existing transaction. It is used by internal
// runtime lifecycle paths that must mutate a side table and append the mirror
// event atomically.
func (m *Messages) AppendTx(ctx context.Context, tx *sql.Tx, env *message.Envelope, fencing klog.FencingTuple) (klog.AppendResult, error) {
	if tx == nil {
		return klog.AppendResult{}, errors.New("store: append tx nil")
	}
	if env == nil {
		return klog.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return klog.AppendResult{}, errors.New("store: append empty envelope.id")
	}
	if env.Payload == nil {
		return klog.AppendResult{}, errors.New("store: append nil payload (harness step 4 must materialize payload before reaching store)")
	}
	// 0) FIX-T6 fencing gate — when constructed with a *ChannelLock,
	// every Append must present a matching (fencing_token, daemon_epoch)
	// tuple via an explicit Append parameter. The check runs INSIDE
	// the tx so a concurrent RefreshDaemon cannot slip between SELECT
	// and INSERT. Failure path: typed *klog.AppendError so the harness
	// chain maps it to message.HarnessWorkerFencingStale (closed-set
	// reject). No outbox row is written.
	if err := m.checkFencing(ctx, tx, string(env.ID), fencing); err != nil {
		return klog.AppendResult{}, err
	}

	// 1) dedupe by envelope.id
	const selExist = `SELECT seq, is_terminal FROM messages WHERE id=?`
	var existingSeq int64
	var existingTerm int
	switch err := tx.QueryRowContext(ctx, selExist, env.ID).Scan(&existingSeq, &existingTerm); {
	case err == nil:
		// Dedupe path — row already exists. Per L1 §10.2 dedupe semantics,
		// we return Deduped=true with the existing seq. Caller (harness
		// step 0.5) is responsible for verifying canonical-hash match
		// before short-circuiting.
		env.Seq = existingSeq
		env.IsTerminal = existingTerm == 1
		return klog.AppendResult{Seq: klog.Seq(existingSeq), IsTerminal: existingTerm == 1, Deduped: true}, nil
	case errors.Is(err, sql.ErrNoRows):
		// fall through
	default:
		return klog.AppendResult{}, fmt.Errorf("store: append dedupe lookup: %w", err)
	}

	// 2) INSERT row
	docRefsJSON, err := encodeDocRefs(env.DocRefs)
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append docrefs encode: %w", err)
	}
	audJSON, err := json.Marshal(env.Audience)
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append audience encode: %w", err)
	}

	const ins = `INSERT INTO messages (
	   id, ts, ts_received, channel_id,
	   sender_kind, sender_id, sender_name,
	   kind, type, payload,
	   parent_id, correlation_id, doc_refs,
	   visibility, audience, not_before, expires_at,
	   delivered_at, delivery_failed_at, last_error, attempts,
	   is_terminal, canonical_hash
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	terminalInt := 0
	if env.IsTerminal {
		terminalInt = 1
	}
	res, err := tx.ExecContext(ctx, ins,
		env.ID, env.TS, env.TSReceived, env.ChannelID,
		string(env.Sender.Kind), string(env.Sender.ID), nullableString(env.Sender.Name),
		string(env.Kind), env.Type, string(env.Payload),
		nullableString(string(env.ParentID)), nullableString(string(env.CorrelationID)), nullableString(docRefsJSON),
		env.Visibility, string(audJSON),
		nullableInt(env.NotBefore), nullableInt(env.ExpiresAt),
		nullableInt(env.DeliveredAt), nullableInt(env.DeliveryFailedAt),
		nullableString(env.LastError), env.Attempts,
		terminalInt, env.CanonicalHash,
	)
	if err != nil {
		return klog.AppendResult{}, classifyAppendErr(err, string(env.ID))
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append last id: %w", err)
	}
	env.Seq = seq

	// 3) view_sync_outbox row (same transaction — L1 §8.6 at-least-once)
	envJSON, err := json.Marshal(env)
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: outbox encode: %w", err)
	}
	const insOutbox = `INSERT INTO view_sync_outbox
	   (seq, message_id, envelope_json, enqueued_at, status)
	   VALUES (?, ?, ?, ?, 'pending')`
	if _, err := tx.ExecContext(ctx, insOutbox, seq, env.ID, string(envJSON), env.TSReceived); err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: outbox insert: %w", err)
	}

	return klog.AppendResult{Seq: klog.Seq(seq), IsTerminal: env.IsTerminal, Deduped: false}, nil
}

// PendingDue returns up to `limit` future-message rows that are due for
// dispatch: `not_before IS NOT NULL AND not_before <= ? AND
// delivered_at IS NULL`. Ordered by seq ASC so the scheduler ticks the
// oldest backlog first (matches L1 §5.3 + §6.4 monotonic processing).
//
// `limit <= 0` is clamped to 64 — matches the outbox PendingPage
// convention so the scheduler tick has a bounded per-channel cost.
func (m *Messages) PendingDue(ctx context.Context, nowMs int64, limit int) ([]message.Envelope, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs,
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

	var out []message.Envelope
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
// `limit <= 0` clamps to 64 — matches PendingDue + the outbox PendingPage
// convention so a single scheduler tick has bounded per-channel cost.
// Ordered by seq ASC so the oldest backlog is drained first (matches L1
// §6.4 monotonic processing semantics).
//
// NOTE: this query intentionally does NOT join actor_registry. The
// caller (daemon scanLongPending) performs the receiver-kind classification
// because the L1 §6.4 dispatch matrix (tool/human skip vs. agent/system
// emit unanswered_timeout vs. deregistered emit receiver_unavailable)
// requires the audience JSON parse and a registry lookup — pushing that
// into SQL would couple the store to the receiver-policy table.
func (m *Messages) LongPendingRequests(ctx context.Context, nowMs int64, limit int) ([]message.Envelope, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs,
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

	var out []message.Envelope
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

// ReceiverUnavailableRequests returns pending request rows whose first
// audience actor is missing from actor_registry or has been deregistered.
// Unlike LongPendingRequests this scan has no expires_at gate: a receiver
// that no longer exists cannot answer, so the scheduler must close the
// request immediately with receiver_unavailable.
func (m *Messages) ReceiverUnavailableRequests(ctx context.Context, limit int) ([]message.Envelope, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages m
	             LEFT JOIN actor_registry a
	               ON a.actor_id = json_extract(m.audience, '$[0]')
	             WHERE m.kind = 'request'
	               AND m.is_terminal = 0
	               AND NOT EXISTS (
	                 SELECT 1 FROM messages r
	                  WHERE r.parent_id = m.id
	                    AND r.kind = 'response'
	                    AND r.is_terminal = 1
	               )
	               AND (a.actor_id IS NULL OR a.deregistered_at IS NOT NULL)
	             ORDER BY m.seq ASC LIMIT ?`
	rows, err := m.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("store: receiver unavailable requests: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []message.Envelope
	for rows.Next() {
		env, err := scanEnvelopeRows(rows)
		if err != nil {
			return nil, fmt.Errorf("store: receiver unavailable requests scan: %w", err)
		}
		out = append(out, env)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: receiver unavailable requests rows: %w", err)
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

// LookupCanonicalHash implements log.MessageLog — returns the row's
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

// FindByID implements log.MessageLog.
func (m *Messages) FindByID(ctx context.Context, channelID channel.ID, id message.ID) (message.Envelope, bool, error) {
	_ = channelID // channel_id is enforced by the per-channel db file; query stays scoped.
	const q = `SELECT id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal, seq
	             FROM messages WHERE id=?`
	row := m.db.QueryRowContext(ctx, q, id)
	env, err := scanEnvelope(row)
	if errors.Is(err, sql.ErrNoRows) {
		return message.Envelope{}, false, nil
	}
	if err != nil {
		return message.Envelope{}, false, err
	}
	return env, true, nil
}

// rowScanner abstracts *sql.Row / *sql.Rows for the Scan call so
// PendingDue (multi-row) can share the envelope materialization code
// with FindByID (single-row).
type rowScanner interface {
	Scan(dest ...any) error
}

// scanEnvelope materializes a row into an Envelope.
func scanEnvelope(row *sql.Row) (message.Envelope, error) {
	return scanEnvelopeFrom(row)
}

// scanEnvelopeRows materializes the current *sql.Rows position into an
// Envelope. Caller is responsible for rows.Next() / rows.Close().
func scanEnvelopeRows(rows *sql.Rows) (message.Envelope, error) {
	return scanEnvelopeFrom(rows)
}

// scanEnvelopeFrom is the shared implementation used by scanEnvelope
// (FindByID) and scanEnvelopeRows (PendingDue).
func scanEnvelopeFrom(s rowScanner) (message.Envelope, error) {
	var env message.Envelope
	var kind, sKind, senderID, vis string
	var audJSON, payloadStr string
	var docRefsStr sql.NullString
	var notBefore, expiresAt, deliveredAt, deliveryFailedAt sql.NullInt64
	var termInt int
	if err := s.Scan(
		&env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&sKind, &senderID, &env.Sender.Name,
		&kind, &env.Type, &payloadStr,
		&env.ParentID, &env.CorrelationID, &docRefsStr,
		&vis, &audJSON,
		&notBefore, &expiresAt,
		&deliveredAt, &deliveryFailedAt, &env.LastError, &env.Attempts,
		&termInt, &env.Seq,
	); err != nil {
		return message.Envelope{}, err
	}
	env.Sender.Kind = actor.Kind(sKind)
	env.Sender.ID = actor.ActorID(senderID)
	env.Kind = message.Kind(kind)
	env.Visibility = message.Visibility(vis)
	env.Payload = json.RawMessage(payloadStr)
	if err := json.Unmarshal([]byte(audJSON), &env.Audience); err != nil {
		return message.Envelope{}, fmt.Errorf("store: scan audience: %w", err)
	}
	if docRefsStr.Valid {
		var refs []string
		if err := json.Unmarshal([]byte(docRefsStr.String), &refs); err != nil {
			return message.Envelope{}, fmt.Errorf("store: scan doc_refs: %w", err)
		}
		env.DocRefs = &refs
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
		env.DeliveryFailedAt = &v
	}
	env.IsTerminal = termInt == 1
	return env, nil
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

// checkFencing is the FIX-T6 gate. Returns nil when this Messages was
// constructed without a *ChannelLock (test-mode wire). Otherwise validates
// the explicit (token, epoch) tuple inside the supplied tx; on any
// mismatch returns a typed
// *klog.AppendError{Reason: HarnessWorkerFencingStale} so the harness
// chain can surface the canonical reject reason without parsing strings.
func (m *Messages) checkFencing(ctx context.Context, tx *sql.Tx, envID string, fencing klog.FencingTuple) error {
	if m.lock == nil {
		return nil
	}
	if fencing == (klog.FencingTuple{}) {
		return &klog.AppendError{
			Reason:           message.HarnessWorkerFencingStale,
			Detail:           "fencing tuple missing from Append parameter",
			PartialMessageID: message.ID(envID),
		}
	}
	if err := m.lock.ValidateWriteTx(ctx, tx, fencing.Token, fencing.Epoch); err != nil {
		if IsFencingStale(err) {
			return &klog.AppendError{
				Reason:           message.HarnessWorkerFencingStale,
				Detail:           err.Error(),
				PartialMessageID: message.ID(envID),
			}
		}
		return fmt.Errorf("store: append fencing check: %w", err)
	}
	return nil
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
		return &klog.AppendError{
			Reason:           message.HarnessIDDuplicateConflict,
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	case strings.Contains(msg, "ux_terminal_response_per_request") ||
		strings.Contains(msg, "UNIQUE constraint failed: messages.parent_id") ||
		strings.Contains(msg, "parent_id, kind, is_terminal"):
		return &klog.AppendError{
			Reason:           message.HarnessTerminalDuplicate,
			Detail:           msg,
			PartialMessageID: message.ID(envID),
		}
	default:
		return fmt.Errorf("store: append insert: %w", err)
	}
}
