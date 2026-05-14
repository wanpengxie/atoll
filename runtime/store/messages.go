package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

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
// resolved by the time Append is called. For T3 we trust the caller
// (harness chain) to set env.IsTerminal correctly before Append. This
// keeps store layer-independent of type_registry semantics.
type Messages struct {
	db *sql.DB
}

// NewMessages returns a *Messages bound to the channel sqlite.
func NewMessages(db *sql.DB) *Messages { return &Messages{db: db} }

// Append implements log.MessageLog.
func (m *Messages) Append(ctx context.Context, env *message.Envelope) (klog.AppendResult, error) {
	if env == nil {
		return klog.AppendResult{}, errors.New("store: append nil envelope")
	}
	if env.ID == "" {
		return klog.AppendResult{}, errors.New("store: append empty envelope.id")
	}

	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

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
		if err := tx.Commit(); err != nil {
			return klog.AppendResult{}, fmt.Errorf("store: append dedupe commit: %w", err)
		}
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
	if env.Payload == nil {
		env.Payload = json.RawMessage("{}")
	}

	const ins = `INSERT INTO messages (
	   id, ts, ts_received, channel_id,
	   sender_kind, sender_id, sender_name,
	   kind, type, payload,
	   parent_id, correlation_id, doc_refs,
	   visibility, audience, not_before, expires_at,
	   delivered_at, delivery_failed_at, last_error, attempts,
	   is_terminal
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	terminalInt := 0
	if env.IsTerminal {
		terminalInt = 1
	}
	res, err := tx.ExecContext(ctx, ins,
		env.ID, env.TS, env.TSReceived, env.ChannelID,
		string(env.Sender.Kind), env.Sender.ID, nullableString(env.Sender.Name),
		string(env.Kind), env.Type, string(env.Payload),
		nullableString(env.ParentID), nullableString(env.CorrelationID), nullableString(docRefsJSON),
		string(env.Visibility), string(audJSON),
		nullableInt(env.NotBefore), nullableInt(env.ExpiresAt),
		nullableInt(env.DeliveredAt), nullableInt(env.DeliveryFailedAt),
		nullableString(env.LastError), env.Attempts,
		terminalInt,
	)
	if err != nil {
		return klog.AppendResult{}, classifyAppendErr(err, env.ID)
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

	if err := tx.Commit(); err != nil {
		return klog.AppendResult{}, fmt.Errorf("store: append commit: %w", err)
	}
	return klog.AppendResult{Seq: klog.Seq(seq), IsTerminal: env.IsTerminal, Deduped: false}, nil
}

// FindByID implements log.MessageLog.
func (m *Messages) FindByID(ctx context.Context, channelID channel.ID, id string) (message.Envelope, bool, error) {
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

// scanEnvelope materializes a row into an Envelope.
func scanEnvelope(row *sql.Row) (message.Envelope, error) {
	var env message.Envelope
	var kind, sKind, vis string
	var audJSON, payloadStr string
	var docRefsStr sql.NullString
	var notBefore, expiresAt, deliveredAt, deliveryFailedAt sql.NullInt64
	var termInt int
	if err := row.Scan(
		&env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&sKind, &env.Sender.ID, &env.Sender.Name,
		&kind, &env.Type, &payloadStr,
		&env.ParentID, &env.CorrelationID, &docRefsStr,
		&vis, &audJSON,
		&notBefore, &expiresAt,
		&deliveredAt, &deliveryFailedAt, &env.LastError, &env.Attempts,
		&termInt, &env.Seq,
	); err != nil {
		return message.Envelope{}, err
	}
	env.Sender.Kind = message.SenderKind(sKind)
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
			Reason:           message.HarnessMessageIDConflict,
			Detail:           msg,
			PartialMessageID: envID,
		}
	case strings.Contains(msg, "ux_terminal_response_per_request"):
		return &klog.AppendError{
			Reason:           message.HarnessTerminalDuplicate,
			Detail:           msg,
			PartialMessageID: envID,
		}
	default:
		return fmt.Errorf("store: append insert: %w", err)
	}
}
