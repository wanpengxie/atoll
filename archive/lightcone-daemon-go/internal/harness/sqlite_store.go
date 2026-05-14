// Package harness ties the shared pkg/harness 9-step Write body to the
// daemon-side bindings:
//
//   - sqlite_store.go: adapts internal/store (channel-local sqlite) into
//     a pkg/harness.Store implementation.
//   - sqlite_actors.go: adapts internal/registry actor lookups into the
//     pkg/harness.ActorLookup interface.
//   - sqlite_types.go: in-memory TypeLookup populated from type_registry
//     rows + compiled schemas.
//   - sqlite_worker_locks.go: adapts internal/supervisor worker_locks
//     into pkg/harness.WorkerLockLookup.
//   - binding_daemon_rpc.go: POST /api/rpc/message.send HTTP endpoint +
//     L2 §3.6.1 reason→HTTP status mapping.
//
// Tests live in *_test.go files alongside.
package harness

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	pkgharness "github.com/coagent-ai/daemon-go/pkg/harness"
	"github.com/coagent-ai/daemon-go/pkg/v4types"
)

// SQLiteStore adapts a channel-local *sql.DB into a pkg/harness.Store.
// Construct one per channel; the harness Deps holds a reference for
// the lifetime of the binding.
type SQLiteStore struct {
	db *sql.DB
	// conn, when non-nil, scopes the SELECT / INSERT calls to a single
	// dedicated connection already inside a BEGIN IMMEDIATE block
	// (built by WithTerminalTx). Production callers obtain a SQLiteStore
	// via NewSQLiteStore which leaves conn nil — the per-call helpers
	// then run against the pool.
	conn *sql.Conn
}

// NewSQLiteStore wraps db. The caller owns db's lifecycle (open via
// internal/store.OpenChannel).
func NewSQLiteStore(db *sql.DB) *SQLiteStore {
	return &SQLiteStore{db: db}
}

// sqlExec is the smallest database/sql subset SQLiteStore uses for
// query / exec calls. *sql.DB and *sql.Conn both satisfy it.
type sqlExec interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func (s *SQLiteStore) exec() sqlExec {
	if s.conn != nil {
		return s.conn
	}
	return s.db
}

// FindByID looks up one message row by envelope id.
func (s *SQLiteStore) FindByID(ctx context.Context, id string) (*v4types.Envelope, error) {
	return s.findOne(ctx, `WHERE id = ?`, id)
}

// FindParent is identical to FindByID at the SQL level — separating
// the interface methods keeps the harness pseudocode readable.
func (s *SQLiteStore) FindParent(ctx context.Context, id string) (*v4types.Envelope, error) {
	return s.findOne(ctx, `WHERE id = ?`, id)
}

// FindTerminalResponse returns the existing terminal kind=response row
// pointing at parentID, or (nil, nil) when none. The query is backed by
// the partial UNIQUE INDEX ux_terminal_response_per_request — at most
// one row matches by construction.
func (s *SQLiteStore) FindTerminalResponse(ctx context.Context, parentID string) (*v4types.Envelope, error) {
	return s.findOne(ctx, `WHERE parent_id = ? AND kind = 'response' AND is_terminal = 1`, parentID)
}

// InsertMessage writes the envelope row. UNIQUE constraint violations
// are surfaced as pkg/harness.ErrUniqueViolation so callers can fall
// back to the dedupe / terminal_duplicate paths.
func (s *SQLiteStore) InsertMessage(ctx context.Context, env *v4types.Envelope, tsReceived int64) error {
	audience, err := json.Marshal(env.Audience)
	if err != nil {
		return fmt.Errorf("sqlite_store: marshal audience: %w", err)
	}
	var docRefs any
	if env.DocRefs != nil {
		b, mErr := json.Marshal(*env.DocRefs)
		if mErr != nil {
			return fmt.Errorf("sqlite_store: marshal doc_refs: %w", mErr)
		}
		docRefs = string(b)
	}
	var parentID any
	if env.ParentID != "" {
		parentID = env.ParentID
	}
	var correlationID any
	if env.CorrelationID != "" {
		correlationID = env.CorrelationID
	}
	var senderName any
	if env.Sender.Name != "" {
		senderName = env.Sender.Name
	}
	var notBefore, expiresAt any
	if env.NotBefore != nil {
		notBefore = *env.NotBefore
	}
	if env.ExpiresAt != nil {
		expiresAt = *env.ExpiresAt
	}
	isTerminal := 0
	if env.IsTerminal {
		isTerminal = 1
	}
	_, err = s.exec().ExecContext(ctx,
		`INSERT INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id, sender_name,
		    kind, type, payload, parent_id, correlation_id, doc_refs,
		    visibility, audience, not_before, expires_at,
		    delivered_at, delivery_failed_at, last_error, attempts, is_terminal)
		 VALUES
		   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, 0, ?)`,
		env.ID, env.TS, tsReceived, env.ChannelID,
		string(env.Sender.Kind), env.Sender.ID, senderName,
		string(env.Kind), env.Type, string(env.Payload),
		parentID, correlationID, docRefs,
		string(env.Visibility), string(audience),
		notBefore, expiresAt, isTerminal,
	)
	if err != nil && isUniqueViolation(err) {
		return fmt.Errorf("%w: %s", pkgharness.ErrUniqueViolation, err.Error())
	}
	return err
}

// WithTerminalTx runs body inside a BEGIN IMMEDIATE block bound to a
// dedicated *sql.Conn so FindTerminalResponse + InsertMessage execute
// atomically against a single connection (matching the L1 §10.2.1 Step 8
// "事务边界" requirement). On success COMMIT; on plain error ROLLBACK; on
// a pkg/harness.RejectError COMMIT (the reject is a "logical no-op" —
// no row was inserted, so committing is safe and avoids losing the
// reject's diagnostic on the unwind path).
//
// This mirrors internal/store.WithImmediate but presents the body with a
// pkg/harness.Store view rather than the raw *sql.Conn — keeping the
// harness pseudocode free of database/sql plumbing.
func (s *SQLiteStore) WithTerminalTx(ctx context.Context, body func(tx pkgharness.Store) error) (err error) {
	if s.conn != nil {
		// Already inside a tx (defensive — supports nested calls).
		return body(s)
	}
	conn, cerr := s.db.Conn(ctx)
	if cerr != nil {
		return fmt.Errorf("sqlite_store: acquire conn: %w", cerr)
	}
	defer func() { _ = conn.Close() }()

	if _, berr := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); berr != nil {
		return fmt.Errorf("sqlite_store: begin immediate: %w", berr)
	}

	defer func() {
		if err != nil {
			var rerr *pkgharness.RejectError
			if errors.As(err, &rerr) {
				// Commit the (empty) tx so we don't surface a misleading
				// "ROLLBACK" log; the reject is the actual outcome.
				if _, cmerr := conn.ExecContext(context.Background(), "COMMIT"); cmerr != nil {
					err = fmt.Errorf("%w; commit after reject failed: %v", err, cmerr)
				}
				return
			}
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return
		}
		if _, cmerr := conn.ExecContext(ctx, "COMMIT"); cmerr != nil {
			err = fmt.Errorf("sqlite_store: commit: %w", cmerr)
		}
	}()

	txStore := &SQLiteStore{db: s.db, conn: conn}
	return body(txStore)
}

// findOne is the shared SELECT helper used by FindByID / FindParent /
// FindTerminalResponse.
func (s *SQLiteStore) findOne(ctx context.Context, where string, args ...any) (*v4types.Envelope, error) {
	row := s.exec().QueryRowContext(ctx,
		`SELECT id, ts, ts_received, channel_id, sender_kind, sender_id, sender_name,
		        kind, type, payload, parent_id, correlation_id, doc_refs,
		        visibility, audience, not_before, expires_at,
		        delivered_at, delivery_failed_at, last_error, attempts, is_terminal
		   FROM messages `+where,
		args...,
	)
	return scanEnvelope(row)
}

func scanEnvelope(row *sql.Row) (*v4types.Envelope, error) {
	var (
		env            v4types.Envelope
		senderName     sql.NullString
		parentID       sql.NullString
		correlationID  sql.NullString
		docRefs        sql.NullString
		notBefore      sql.NullInt64
		expiresAt      sql.NullInt64
		deliveredAt    sql.NullInt64
		deliveryFailed sql.NullInt64
		lastErr        sql.NullString
		audienceRaw    string
		isTerminalInt  int64
		payloadRaw     string
		senderKindStr  string
		kindStr        string
		visibilityStr  string
	)
	if err := row.Scan(
		&env.ID, &env.TS, &env.TSReceived, &env.ChannelID,
		&senderKindStr, &env.Sender.ID, &senderName,
		&kindStr, &env.Type, &payloadRaw,
		&parentID, &correlationID, &docRefs,
		&visibilityStr, &audienceRaw, &notBefore, &expiresAt,
		&deliveredAt, &deliveryFailed, &lastErr, &env.Attempts, &isTerminalInt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("sqlite_store: scan: %w", err)
	}
	env.Sender.Kind = v4types.SenderKind(senderKindStr)
	if senderName.Valid {
		env.Sender.Name = senderName.String
	}
	env.Kind = v4types.Kind(kindStr)
	env.Visibility = v4types.Visibility(visibilityStr)
	env.Payload = []byte(payloadRaw)
	if parentID.Valid {
		env.ParentID = parentID.String
	}
	if correlationID.Valid {
		env.CorrelationID = correlationID.String
	}
	if docRefs.Valid {
		var refs []string
		if err := json.Unmarshal([]byte(docRefs.String), &refs); err != nil {
			return nil, fmt.Errorf("sqlite_store: parse doc_refs: %w", err)
		}
		env.DocRefs = &refs
	}
	if err := json.Unmarshal([]byte(audienceRaw), &env.Audience); err != nil {
		return nil, fmt.Errorf("sqlite_store: parse audience: %w", err)
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
	if deliveryFailed.Valid {
		v := deliveryFailed.Int64
		env.DeliveryFailedAt = &v
	}
	if lastErr.Valid {
		env.LastError = lastErr.String
	}
	env.IsTerminal = isTerminalInt == 1
	return &env, nil
}

// isUniqueViolation mirrors registry.isUniqueViolation — string-match
// modernc.org/sqlite's stable UNIQUE constraint message rather than
// coupling to the driver's typed error (extended-code surface drifts
// across point releases).
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "constraint failed: UNIQUE")
}
