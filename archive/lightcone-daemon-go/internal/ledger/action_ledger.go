// Package ledger implements the action_ledger Reserve / Commit
// primitives that back turn-replay idempotency per L2 §1.4.10.1
// (M1.3 ticket T6).
//
// The two-phase contract sits *in front of* the message-write harness:
//
//	Phase 1 (Reserve): caller derives ledger_key = SHA-256(canonical_json(
//	  {turn_id, semantic_action_key})). If a row already exists for that
//	  key we surface the previously-reserved envelope_id so the caller
//	  re-emits the same id — harness step 0.5 dedupes on the second
//	  attempt and no duplicate side effect escapes.
//
//	Phase 2 (harness write): caller writes the message with the envelope
//	  Reserve handed back. Failure: nothing to do, the Reserve row stays
//	  in `reserved` state and replays will find it.
//
//	Phase 3 (Commit): caller flips the row to `committed`. Commit is
//	  idempotent; a Reserve→Commit→Commit sequence is a no-op on the
//	  second Commit.
//
// The envelope id used to satisfy `messages.id UNIQUE` is generated
// here (UUID v4). Tests inject a deterministic generator to keep the
// replay assertions readable.
package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Status is the closed enum for action_ledger.status (L2 §1.4.10.1).
type Status string

// Status closed set — matches the CHECK constraint in
// store.ChannelLocalDDL.
const (
	StatusReserved  Status = "reserved"
	StatusCommitted Status = "committed"
)

// Entry is the in-memory representation of one action_ledger row.
type Entry struct {
	LedgerKey   string
	TurnID      string
	ActorID     string
	EnvelopeID  string
	Status      Status
	ReservedAt  int64
	CommittedAt *int64 // nil while status='reserved'
}

// ReserveResult bundles the envelope id the caller MUST emit alongside
// the bookkeeping the supervisor / observability layer want to log.
//
// Replayed == true means the row pre-existed and EnvelopeID was reused
// — the caller's harness write should expect a step-0.5 dedupe (or a
// brand-new row if the original write never made it).
type ReserveResult struct {
	EnvelopeID string
	Status     Status
	Replayed   bool
}

// Sentinel errors. Production callers surface ErrInvalidInput as a 400
// to the worker ABI; ErrLedgerMissing only appears on Commit when the
// caller invented a key Reserve never wrote.
var (
	// ErrInvalidInput is returned for empty ledger_key / turn_id /
	// actor_id, non-positive timestamps, or nil executor.
	ErrInvalidInput = errors.New("ledger_invalid_input")

	// ErrLedgerMissing is returned by Commit when no row exists for
	// the given key — meaning the caller skipped Reserve, which is a
	// programming error (the harness wrapper should always Reserve
	// first).
	ErrLedgerMissing = errors.New("ledger_missing")
)

// Executor is the smallest database/sql interface that supports the
// ledger read + write paths. *sql.DB, *sql.Conn, and *sql.Tx all
// satisfy it.
//
// The two-phase protocol works on ANY of these — the caller decides
// whether Reserve and Commit run in the same outer transaction or
// not. Both helpers happen to be single-statement so the spec's
// "same tx" suggestion (`action_ledger 同事务 reserve—commit`,
// L2 §1.4.10.1) is a convenience, not a correctness requirement.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// NewEnvelopeIDFunc is the signature of the envelope id generator
// Reserve hands fresh rows. Tests inject a deterministic counter; the
// production default (defaultEnvelopeIDFunc) returns UUID v4.
type NewEnvelopeIDFunc func() string

// Options tunes Reserve behaviour. Zero value is fine for production;
// callers that want deterministic ids in tests build it via
// WithNewEnvelopeID.
type Options struct {
	NewEnvelopeID NewEnvelopeIDFunc
}

// Reserve executes Phase 1 of the L2 §1.4.10.1 two-phase protocol.
//
//	row absent → INSERT status='reserved', return generated envelope_id
//	row present → return existing envelope_id with Replayed=true
//
// `now` is Unix seconds (matches the rest of the daemon-go time
// convention; the spec column is `INTEGER NOT NULL`).
//
// The optional opts.NewEnvelopeID is used only on the insert path —
// when a row already exists we always return the persisted id.
func Reserve(
	ctx context.Context,
	exec Executor,
	ledgerKey, turnID, actorID string,
	now int64,
	opts Options,
) (ReserveResult, error) {
	if err := validateReserveInput(ledgerKey, turnID, actorID, now); err != nil {
		return ReserveResult{}, err
	}
	if exec == nil {
		return ReserveResult{}, fmt.Errorf("%w: exec is nil", ErrInvalidInput)
	}

	// Look first — the common replay path is "row exists, return its
	// envelope_id". SELECT-then-INSERT under sqlite's single writer is
	// safe because the busy_timeout + the IMMEDIATE tx callers wrap
	// around us serialise contending Reserves.
	existing, err := Get(ctx, exec, ledgerKey)
	switch {
	case err == nil:
		return ReserveResult{
			EnvelopeID: existing.EnvelopeID,
			Status:     existing.Status,
			Replayed:   true,
		}, nil
	case errors.Is(err, ErrLedgerMissing):
		// fallthrough to insert
	default:
		return ReserveResult{}, err
	}

	gen := opts.NewEnvelopeID
	if gen == nil {
		gen = defaultEnvelopeIDFunc
	}
	envelopeID := gen()
	if strings.TrimSpace(envelopeID) == "" {
		return ReserveResult{}, fmt.Errorf("%w: NewEnvelopeID returned empty", ErrInvalidInput)
	}

	if _, err := exec.ExecContext(ctx,
		`INSERT INTO action_ledger
		   (ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at)
		 VALUES (?, ?, ?, ?, ?, ?, NULL)`,
		ledgerKey, turnID, actorID, envelopeID, string(StatusReserved), now,
	); err != nil {
		// Concurrent reservers may both miss the SELECT and race the
		// INSERT. The losing tx surfaces a UNIQUE-constraint error;
		// recover by re-reading the persisted row and reporting it as
		// a replay. The end result for the caller is identical to the
		// single-reserver case.
		if isUniqueViolation(err) {
			existing, gerr := Get(ctx, exec, ledgerKey)
			if gerr != nil {
				return ReserveResult{}, fmt.Errorf("ledger: reserve race resolve: %w", gerr)
			}
			return ReserveResult{
				EnvelopeID: existing.EnvelopeID,
				Status:     existing.Status,
				Replayed:   true,
			}, nil
		}
		return ReserveResult{}, fmt.Errorf("ledger: insert %s: %w", ledgerKey, err)
	}
	return ReserveResult{
		EnvelopeID: envelopeID,
		Status:     StatusReserved,
		Replayed:   false,
	}, nil
}

// Commit executes Phase 3 of the L2 §1.4.10.1 protocol. The UPDATE
// is idempotent — already-committed rows return nil (no-op). Missing
// rows return ErrLedgerMissing.
//
// `now` is Unix seconds and is written into committed_at; the spec
// keeps reserved_at intact so audit can reconstruct timings.
func Commit(ctx context.Context, exec Executor, ledgerKey string, now int64) error {
	if strings.TrimSpace(ledgerKey) == "" {
		return fmt.Errorf("%w: ledger_key required", ErrInvalidInput)
	}
	if now <= 0 {
		return fmt.Errorf("%w: now must be positive, got %d", ErrInvalidInput, now)
	}
	if exec == nil {
		return fmt.Errorf("%w: exec is nil", ErrInvalidInput)
	}

	// CAS predicate `status='reserved'` keeps Commit→Commit a no-op
	// without spuriously rewriting committed_at on the second call.
	res, err := exec.ExecContext(ctx,
		`UPDATE action_ledger
		    SET status = ?, committed_at = ?
		  WHERE ledger_key = ? AND status = ?`,
		string(StatusCommitted), now, ledgerKey, string(StatusReserved),
	)
	if err != nil {
		return fmt.Errorf("ledger: commit %s: %w", ledgerKey, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("ledger: commit %s rowsAffected: %w", ledgerKey, err)
	}
	if affected == 1 {
		return nil
	}

	// 0 rows — distinguish "missing entirely" from "already committed".
	got, gerr := Get(ctx, exec, ledgerKey)
	if errors.Is(gerr, ErrLedgerMissing) {
		return ErrLedgerMissing
	}
	if gerr != nil {
		return gerr
	}
	if got.Status == StatusCommitted {
		// Idempotent re-commit — spec invariant: status='reserved' /
		// 'committed' is audit-only, never affects correctness.
		return nil
	}
	// Unreachable in practice: status enum is closed and SELECT just
	// returned a row that wasn't 'reserved' but also isn't 'committed'.
	return fmt.Errorf("ledger: commit %s unexpected status %q", ledgerKey, got.Status)
}

// Get returns one action_ledger row by ledger_key. ErrLedgerMissing on
// no-row. Used by Reserve internally and exposed for supervisor /
// observability callers that want to inspect status without driving
// the state machine.
func Get(ctx context.Context, q Executor, ledgerKey string) (*Entry, error) {
	if strings.TrimSpace(ledgerKey) == "" {
		return nil, fmt.Errorf("%w: ledger_key required", ErrInvalidInput)
	}
	if q == nil {
		return nil, fmt.Errorf("%w: executor is nil", ErrInvalidInput)
	}
	row := q.QueryRowContext(ctx,
		`SELECT ledger_key, turn_id, actor_id, envelope_id, status, reserved_at, committed_at
		   FROM action_ledger
		  WHERE ledger_key = ?`,
		ledgerKey,
	)
	var e Entry
	var status string
	var committedAt sql.NullInt64
	if err := row.Scan(
		&e.LedgerKey, &e.TurnID, &e.ActorID, &e.EnvelopeID,
		&status, &e.ReservedAt, &committedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrLedgerMissing
		}
		return nil, fmt.Errorf("ledger: get %s: %w", ledgerKey, err)
	}
	e.Status = Status(status)
	if committedAt.Valid {
		v := committedAt.Int64
		e.CommittedAt = &v
	}
	return &e, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func validateReserveInput(ledgerKey, turnID, actorID string, now int64) error {
	if strings.TrimSpace(ledgerKey) == "" {
		return fmt.Errorf("%w: ledger_key required", ErrInvalidInput)
	}
	if strings.TrimSpace(turnID) == "" {
		return fmt.Errorf("%w: turn_id required", ErrInvalidInput)
	}
	if strings.TrimSpace(actorID) == "" {
		return fmt.Errorf("%w: actor_id required", ErrInvalidInput)
	}
	if now <= 0 {
		return fmt.Errorf("%w: now must be positive, got %d", ErrInvalidInput, now)
	}
	return nil
}

// defaultEnvelopeIDFunc is the production envelope id generator —
// UUID v4. Kept private so tests inject deterministic counters.
func defaultEnvelopeIDFunc() string { return uuid.NewString() }

// isUniqueViolation mirrors registry.isUniqueViolation — we string-match
// the canonical sqlite UNIQUE error since modernc returns a typed error
// whose extended code surface is unstable across point releases.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "constraint failed: UNIQUE")
}
