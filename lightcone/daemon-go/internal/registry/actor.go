package registry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ActorKind matches the actor_registry CHECK enum (L2 §1.4.6 / L1 §12.2).
type ActorKind string

const (
	KindHuman  ActorKind = "human"
	KindAgent  ActorKind = "agent"
	KindSystem ActorKind = "system"
	KindTool   ActorKind = "tool"
)

// IsValid reports whether k is one of the four protocol-baseline kinds.
func (k ActorKind) IsValid() bool {
	switch k {
	case KindHuman, KindAgent, KindSystem, KindTool:
		return true
	default:
		return false
	}
}

// ActorBinding matches the actor_registry CHECK enum (L2 §1.4.6).
// Empty string represents NULL — only valid for human/system actors.
type ActorBinding string

const (
	BindingNone        ActorBinding = "" // NULL — human/system actors
	BindingDaemonRPC   ActorBinding = "daemon_rpc"
	BindingInWorkerBus ActorBinding = "in_worker_bus"
)

// IsValid reports whether b is "" or one of the two binding constants.
func (b ActorBinding) IsValid() bool {
	switch b {
	case BindingNone, BindingDaemonRPC, BindingInWorkerBus:
		return true
	default:
		return false
	}
}

// ActorMeta is the in-memory representation of one actor_registry row.
//
// Binding is "" when actor_kind is human/system (the L1 §12.2 contract:
// "actor_binding = NULL 仅允许 actor_kind IN ('human','system')").
//
// DeregisteredAt is nil for active actors. Get returns the row regardless
// of deregistration; ListActive / GetKind filter to active rows only.
type ActorMeta struct {
	ActorID        string
	Kind           ActorKind
	Binding        ActorBinding
	CreatedAt      int64
	DeregisteredAt *int64
}

// Sentinel errors callers inspect to map to L2 §3.6.1 daemon_rpc reasons
// or L1 §10.3 install/harness reasons.
var (
	// ErrActorNotFound is returned when a Get / GetKind / Deregister call
	// targets a row that does not exist (or, for the active-only queries,
	// is already soft-deregistered).
	ErrActorNotFound = errors.New("actor_not_found")

	// ErrActorExists is returned when Register is called with an actor_id
	// that already has a row (active or deregistered) — the actor_registry
	// PK conflict (and L1 §12.4 "不重用 deregistered id" rule).
	ErrActorExists = errors.New("actor_exists")

	// ErrInvalidKind is returned when ActorMeta.Kind is not one of the
	// four protocol-baseline kinds.
	ErrInvalidKind = errors.New("actor_invalid_kind")

	// ErrInvalidBinding is returned when ActorMeta.Binding either:
	//   - is non-empty for human/system kind, or
	//   - is empty for agent/tool kind, or
	//   - is a non-enum string.
	ErrInvalidBinding = errors.New("actor_invalid_binding")

	// ErrInvalidActorID is returned when ActorMeta.ActorID is empty or
	// whitespace-only.
	ErrInvalidActorID = errors.New("actor_invalid_id")

	// ErrInvalidChannelID is returned when channelID is empty or
	// whitespace-only.
	ErrInvalidChannelID = errors.New("actor_invalid_channel_id")

	// ErrInvalidNow is returned when ActorMeta.CreatedAt (Register) or
	// the now arg (Deregister) is non-positive — registry must record
	// real wall-clock time so reconcile / audit ordering works.
	ErrInvalidNow = errors.New("actor_invalid_now")
)

// Executor is the smallest database/sql interface that supports the
// registry's read + write paths. *sql.DB, *sql.Conn, and *sql.Tx all
// satisfy it.
//
// The bootstrap saga calls Register with the *sql.Conn it already holds
// inside store.WithImmediate, so the three INSERTs join the saga's
// IMMEDIATE tx. Standalone callers should wrap their own WithImmediate
// to keep the L2 §1.4.6 "actor_registry + actor_cursors 同事务"
// invariant.
type Executor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// ---------------------------------------------------------------------------
// Register / Deregister (write path)
// ---------------------------------------------------------------------------

// Register writes the three rows demanded by L2 §1.4.6 / L1 §12.3 in a
// single tx (the executor's tx, if any):
//
//  1. actor_registry — the canonical metadata row.
//  2. actor_cursors  — INSERT OR IGNORE seed (last_consumed_seq=0); keeps
//     the supervisor backlog scan JOIN non-empty for new actors.
//  3. messages       — `system.event payload.kind=actor_registered`
//     audit row (visibility=system, audience=['*']). The envelope id is
//     deterministic (`actor_registered:{channel_id}:{actor_id}`) so
//     re-runs of an enclosing saga dedupe via the messages.id UNIQUE
//     constraint.
//
// The caller is responsible for transactional atomicity: pass either a
// *sql.Conn already holding a BEGIN IMMEDIATE (the bootstrap saga
// pattern) or wrap with store.WithImmediate.
//
// Errors:
//   - ErrInvalidActorID / ErrInvalidChannelID / ErrInvalidKind /
//     ErrInvalidBinding / ErrInvalidNow on input validation.
//   - ErrActorExists on actor_registry PK conflict.
//   - other sql errors are wrapped and returned verbatim.
func Register(ctx context.Context, exec Executor, channelID string, meta ActorMeta) error {
	if err := validateMeta(channelID, meta); err != nil {
		return err
	}

	// Step 1: actor_registry INSERT.
	var bindingArg any
	if meta.Binding == BindingNone {
		bindingArg = nil
	} else {
		bindingArg = string(meta.Binding)
	}
	if _, err := exec.ExecContext(ctx,
		`INSERT INTO actor_registry
		   (actor_id, actor_kind, actor_binding, created_at, deregistered_at)
		 VALUES (?, ?, ?, ?, NULL)`,
		meta.ActorID, string(meta.Kind), bindingArg, meta.CreatedAt,
	); err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: %s", ErrActorExists, meta.ActorID)
		}
		return fmt.Errorf("registry: insert actor_registry: %w", err)
	}

	// Step 2: actor_cursors seed (idempotent — re-running Register-ish
	// flows during reconcile must not error if the cursor already exists).
	if _, err := exec.ExecContext(ctx,
		`INSERT OR IGNORE INTO actor_cursors
		   (actor_id, last_consumed_seq, last_consumed_id, updated_at)
		 VALUES (?, 0, NULL, ?)`,
		meta.ActorID, meta.CreatedAt,
	); err != nil {
		return fmt.Errorf("registry: seed actor_cursors: %w", err)
	}

	// Step 3: emit system.event payload.kind=actor_registered.
	if err := emitActorRegistered(ctx, exec, channelID, meta); err != nil {
		return fmt.Errorf("registry: emit actor_registered: %w", err)
	}

	return nil
}

// Deregister soft-deletes an actor (L1 §12.4). It only flips the
// deregistered_at timestamp; the row is preserved so historical
// messages.sender_id stays interpretable.
//
// The CAS condition `WHERE actor_id=? AND deregistered_at IS NULL`
// guarantees idempotence and protects against re-deregistering an
// already-removed actor (which would silently overwrite the original
// timestamp).
//
// Returns ErrActorNotFound when there is no active row to deregister
// (either the actor was never registered or is already deregistered).
func Deregister(ctx context.Context, exec Executor, actorID string, now int64) error {
	if strings.TrimSpace(actorID) == "" {
		return ErrInvalidActorID
	}
	if now <= 0 {
		return ErrInvalidNow
	}
	res, err := exec.ExecContext(ctx,
		`UPDATE actor_registry
		    SET deregistered_at = ?
		  WHERE actor_id = ? AND deregistered_at IS NULL`,
		now, actorID,
	)
	if err != nil {
		return fmt.Errorf("registry: deregister %s: %w", actorID, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("registry: deregister %s rowsAffected: %w", actorID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: %s", ErrActorNotFound, actorID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Get / GetKind / ListActive (read path)
// ---------------------------------------------------------------------------

// Get returns the actor_registry row for actorID regardless of
// deregistration state — the acceptance criterion "Deregister 后 Get 仍
// 返回 row 但 deregistered_at 非 NULL" depends on this.
//
// Returns ErrActorNotFound on missing row; sql errors are wrapped.
func Get(ctx context.Context, q Executor, actorID string) (*ActorMeta, error) {
	if strings.TrimSpace(actorID) == "" {
		return nil, ErrInvalidActorID
	}
	row := q.QueryRowContext(ctx,
		`SELECT actor_id, actor_kind, actor_binding, created_at, deregistered_at
		   FROM actor_registry
		  WHERE actor_id = ?`,
		actorID,
	)
	return scanActor(row)
}

// GetKind returns the actor_kind for an *active* actor (L2 §1.4.4 sample
// SQL: `SELECT actor_kind, actor_binding FROM actor_registry WHERE
// actor_id = ? AND deregistered_at IS NULL`). Deregistered or missing
// actors yield ErrActorNotFound — the harness step 3 / scheduler step 3
// branches both treat "deregistered" the same as "missing".
func GetKind(ctx context.Context, q Executor, actorID string) (ActorKind, error) {
	if strings.TrimSpace(actorID) == "" {
		return "", ErrInvalidActorID
	}
	row := q.QueryRowContext(ctx,
		`SELECT actor_kind
		   FROM actor_registry
		  WHERE actor_id = ? AND deregistered_at IS NULL`,
		actorID,
	)
	var kind string
	if err := row.Scan(&kind); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: %s", ErrActorNotFound, actorID)
		}
		return "", fmt.Errorf("registry: GetKind %s: %w", actorID, err)
	}
	return ActorKind(kind), nil
}

// ListActive returns every actor with deregistered_at IS NULL, ordered
// by actor_kind then actor_id (deterministic so callers can rely on it
// for `audience=['*']` expansion — L1 §12.1 row 4). The query is backed
// by ix_actor_registry_active.
func ListActive(ctx context.Context, q Executor) ([]*ActorMeta, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT actor_id, actor_kind, actor_binding, created_at, deregistered_at
		   FROM actor_registry
		  WHERE deregistered_at IS NULL
		  ORDER BY actor_kind ASC, actor_id ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("registry: ListActive: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []*ActorMeta
	for rows.Next() {
		var m ActorMeta
		var binding sql.NullString
		var dereg sql.NullInt64
		if err := rows.Scan(&m.ActorID, (*string)(&m.Kind), &binding, &m.CreatedAt, &dereg); err != nil {
			return nil, fmt.Errorf("registry: ListActive scan: %w", err)
		}
		if binding.Valid {
			m.Binding = ActorBinding(binding.String)
		}
		if dereg.Valid {
			v := dereg.Int64
			m.DeregisteredAt = &v
		}
		out = append(out, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("registry: ListActive rows: %w", err)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func validateMeta(channelID string, meta ActorMeta) error {
	if strings.TrimSpace(channelID) == "" {
		return ErrInvalidChannelID
	}
	if strings.TrimSpace(meta.ActorID) == "" {
		return ErrInvalidActorID
	}
	if !meta.Kind.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidKind, meta.Kind)
	}
	if !meta.Binding.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidBinding, meta.Binding)
	}
	// L1 §12.2 contract: human/system MUST have NULL binding;
	// agent/tool MUST have non-NULL binding.
	switch meta.Kind {
	case KindHuman, KindSystem:
		if meta.Binding != BindingNone {
			return fmt.Errorf("%w: kind=%s requires empty binding, got %q",
				ErrInvalidBinding, meta.Kind, meta.Binding)
		}
	case KindAgent, KindTool:
		if meta.Binding == BindingNone {
			return fmt.Errorf("%w: kind=%s requires non-empty binding",
				ErrInvalidBinding, meta.Kind)
		}
	}
	if meta.CreatedAt <= 0 {
		return ErrInvalidNow
	}
	return nil
}

// scanActor reads one actor_registry row from a sql.Row.
func scanActor(row *sql.Row) (*ActorMeta, error) {
	var m ActorMeta
	var binding sql.NullString
	var dereg sql.NullInt64
	if err := row.Scan(&m.ActorID, (*string)(&m.Kind), &binding, &m.CreatedAt, &dereg); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrActorNotFound
		}
		return nil, fmt.Errorf("registry: scan actor: %w", err)
	}
	if binding.Valid {
		m.Binding = ActorBinding(binding.String)
	}
	if dereg.Valid {
		v := dereg.Int64
		m.DeregisteredAt = &v
	}
	return &m, nil
}

// actorRegisteredEventID is the deterministic envelope id used by step 3
// of Register. Same channel_id + actor_id always yields the same id, so
// the bootstrap saga / a future reconcile can re-INSERT without
// duplicating the event (messages.id UNIQUE handles dedupe).
func actorRegisteredEventID(channelID, actorID string) string {
	return "actor_registered:" + channelID + ":" + actorID
}

// emitActorRegistered writes the system.event row that L1 §12.3 mandates
// as the audit trail for actor lifecycle transitions. The event is
// addressed to the channel-wide audience (`["*"]`) at visibility=system
// so any listener can react (informational; not used for control flow).
func emitActorRegistered(ctx context.Context, exec Executor, channelID string, meta ActorMeta) error {
	payload := map[string]any{
		"kind":       "actor_registered",
		"actor_id":   meta.ActorID,
		"actor_kind": string(meta.Kind),
	}
	if meta.Binding != BindingNone {
		payload["actor_binding"] = string(meta.Binding)
	} else {
		payload["actor_binding"] = nil
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	audience, err := json.Marshal([]string{"*"})
	if err != nil {
		return fmt.Errorf("marshal audience: %w", err)
	}
	_, err = exec.ExecContext(ctx,
		`INSERT OR IGNORE INTO messages
		   (id, ts, ts_received, channel_id, sender_kind, sender_id,
		    sender_name, kind, type, payload, parent_id, correlation_id,
		    doc_refs, visibility, audience, not_before, expires_at,
		    delivered_at, delivery_failed_at, last_error, attempts, is_terminal)
		 VALUES
		   (?, ?, ?, ?, 'system', 'system',
		    NULL, 'event', 'system.event', ?, NULL, NULL,
		    NULL, 'system', ?, NULL, NULL,
		    NULL, NULL, NULL, 0, 0)`,
		actorRegisteredEventID(channelID, meta.ActorID), meta.CreatedAt, meta.CreatedAt, channelID,
		string(payloadBytes), string(audience),
	)
	return err
}

// isUniqueViolation reports whether err looks like a sqlite UNIQUE
// constraint violation. modernc.org/sqlite returns `sqlite3.Error` with
// extended code 1555 / 2067 (PK / UNIQUE); rather than coupling to the
// driver's type we string-match the canonical "UNIQUE constraint failed"
// message it embeds, which is stable across the driver version we pin
// (modernc.org/sqlite v1.50.x — see go.mod).
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") ||
		strings.Contains(msg, "constraint failed: UNIQUE")
}
