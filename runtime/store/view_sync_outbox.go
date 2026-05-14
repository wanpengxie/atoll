package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coagent-ai/coagent/kernel/channel"
	klog "github.com/coagent-ai/coagent/kernel/log"
	"github.com/coagent-ai/coagent/kernel/message"
	"github.com/coagent-ai/coagent/kernel/viewsync"
)

// ViewSyncOutbox provides CRUD over the view_sync_outbox table that
// drives runtime/transit push.
//
// Rows are inserted by Messages.Append (same transaction) and deleted
// by AckUpTo when the server acknowledges a contiguous waterline.
type ViewSyncOutbox struct {
	db        *sql.DB
	channelID channel.ID
}

// NewViewSyncOutbox returns an outbox view over the channel sqlite.
func NewViewSyncOutbox(db *sql.DB, channelID channel.ID) *ViewSyncOutbox {
	return &ViewSyncOutbox{db: db, channelID: channelID}
}

// ChannelID returns the channel this outbox belongs to.
func (o *ViewSyncOutbox) ChannelID() channel.ID { return o.channelID }

// PendingPage returns up to `limit` outbox rows with status='pending',
// ordered by seq ASC (the push order required by L1 §8.6).
func (o *ViewSyncOutbox) PendingPage(ctx context.Context, limit int) ([]viewsync.PushFrame, error) {
	if limit <= 0 {
		limit = 64
	}
	const q = `SELECT seq, message_id, envelope_json
	            FROM view_sync_outbox
	            WHERE status='pending'
	            ORDER BY seq ASC LIMIT ?`
	rows, err := o.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("store: outbox pending: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []viewsync.PushFrame
	for rows.Next() {
		var seq int64
		var msgID, envJSON string
		if err := rows.Scan(&seq, &msgID, &envJSON); err != nil {
			return nil, fmt.Errorf("store: outbox pending scan: %w", err)
		}
		var env message.Envelope
		if err := json.Unmarshal([]byte(envJSON), &env); err != nil {
			return nil, fmt.Errorf("store: outbox decode envelope %s: %w", msgID, err)
		}
		out = append(out, viewsync.PushFrame{
			ChannelID: o.channelID,
			Seq:       viewsync.Seq(seq),
			MessageID: msgID,
			Envelope:  env,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: outbox pending rows: %w", err)
	}
	return out, nil
}

// MarkPushed flips status pending → pushed and records pushed_at.
// Idempotent — re-pushing a 'pushed' row remains 'pushed'.
func (o *ViewSyncOutbox) MarkPushed(ctx context.Context, seq viewsync.Seq, pushedAt int64) error {
	const q = `UPDATE view_sync_outbox SET status='pushed', pushed_at=?
	            WHERE seq=? AND status='pending'`
	if _, err := o.db.ExecContext(ctx, q, pushedAt, int64(seq)); err != nil {
		return fmt.Errorf("store: outbox mark pushed: %w", err)
	}
	return nil
}

// ResetPushed forces a row back to pending — used after a push fails
// transiently so the next pump retries.
func (o *ViewSyncOutbox) ResetPushed(ctx context.Context, seq viewsync.Seq) error {
	const q = `UPDATE view_sync_outbox SET status='pending', pushed_at=NULL
	            WHERE seq=?`
	if _, err := o.db.ExecContext(ctx, q, int64(seq)); err != nil {
		return fmt.Errorf("store: outbox reset: %w", err)
	}
	return nil
}

// AckUpTo deletes every outbox row with seq <= lastAckedSeq (L1 §8.6 GC
// rule — server ack drives daemon-side outbox eviction).
func (o *ViewSyncOutbox) AckUpTo(ctx context.Context, lastAckedSeq viewsync.Seq) error {
	const q = `DELETE FROM view_sync_outbox WHERE seq <= ?`
	if _, err := o.db.ExecContext(ctx, q, int64(lastAckedSeq)); err != nil {
		return fmt.Errorf("store: outbox ack: %w", err)
	}
	return nil
}

// HighestSeq returns the largest seq currently in the outbox (pending
// or pushed). Returns 0, false when the table is empty.
func (o *ViewSyncOutbox) HighestSeq(ctx context.Context) (viewsync.Seq, bool, error) {
	const q = `SELECT MAX(seq) FROM view_sync_outbox`
	var ns sql.NullInt64
	if err := o.db.QueryRowContext(ctx, q).Scan(&ns); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("store: outbox highest seq: %w", err)
	}
	if !ns.Valid {
		return 0, false, nil
	}
	return viewsync.Seq(ns.Int64), true, nil
}

// MessagesByRange returns messages in [since, until] (closed interval)
// ordered by seq ASC. Used by transit.resync_server.ServeResync.
//
// Reads from the messages table directly (rather than outbox) so we can
// answer Resync for already-GC'd outbox rows.
func (o *ViewSyncOutbox) MessagesByRange(
	ctx context.Context,
	since, until viewsync.Seq,
) ([]viewsync.ResyncMessage, error) {
	if since > until {
		return nil, fmt.Errorf("store: resync range invalid: since=%d > until=%d", since, until)
	}
	const q = `SELECT seq, id, ts, ts_received, channel_id,
	                  sender_kind, sender_id, COALESCE(sender_name,''),
	                  kind, type, payload,
	                  COALESCE(parent_id,''), COALESCE(correlation_id,''), doc_refs,
	                  visibility, audience,
	                  not_before, expires_at,
	                  delivered_at, delivery_failed_at, COALESCE(last_error,''), attempts,
	                  is_terminal
	            FROM messages
	            WHERE seq BETWEEN ? AND ?
	            ORDER BY seq ASC`
	rows, err := o.db.QueryContext(ctx, q, int64(since), int64(until))
	if err != nil {
		return nil, fmt.Errorf("store: resync range query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []viewsync.ResyncMessage
	for rows.Next() {
		var env message.Envelope
		var kind, sKind, vis string
		var audJSON, payloadStr string
		var docRefsStr sql.NullString
		var notBefore, expiresAt, deliveredAt, deliveryFailedAt sql.NullInt64
		var termInt int
		if err := rows.Scan(
			&env.Seq, &env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
			&sKind, &env.Sender.ID, &env.Sender.Name,
			&kind, &env.Type, &payloadStr,
			&env.ParentID, &env.CorrelationID, &docRefsStr,
			&vis, &audJSON,
			&notBefore, &expiresAt,
			&deliveredAt, &deliveryFailedAt, &env.LastError, &env.Attempts,
			&termInt,
		); err != nil {
			return nil, fmt.Errorf("store: resync range scan: %w", err)
		}
		env.Sender.Kind = message.SenderKind(sKind)
		env.Kind = message.Kind(kind)
		env.Visibility = message.Visibility(vis)
		env.Payload = json.RawMessage(payloadStr)
		if err := json.Unmarshal([]byte(audJSON), &env.Audience); err != nil {
			return nil, fmt.Errorf("store: resync audience: %w", err)
		}
		if docRefsStr.Valid {
			var refs []string
			if err := json.Unmarshal([]byte(docRefsStr.String), &refs); err != nil {
				return nil, fmt.Errorf("store: resync doc_refs: %w", err)
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
		out = append(out, viewsync.ResyncMessage{
			Seq:       viewsync.Seq(env.Seq),
			MessageID: env.ID,
			Envelope:  env,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: resync range rows: %w", err)
	}
	return out, nil
}

// Ensure interface satisfaction at compile time (compile-only assertion).
var _ klog.Seq = klog.Seq(0)
