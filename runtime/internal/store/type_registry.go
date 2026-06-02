package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/storespec"
)

// typeRegistryStore is the sqlite-backed implementation of
// storespec.TypeStore (the install state machine + lookup + list) over the
// channel-local type_registry table (L2 §1.4.2). It also exposes a
// runtime/storespec.TypeViewLookup projection via HarnessView so the harness
// Chain reads the same row the installer wrote.
//
// §4.5: the installed row is reachable ONLY through BeginInstall →
// MarkInstalled (which pairs it with its system.type.installed mirror event).
// There is no public Upsert bypass — the canonical write goes through
// upsertWithStatusTx inside the state machine, never as a standalone op.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload is opaque
// to the protocol layer; this registry stores NO payload schema fields.
//
// One *typeRegistryStore per channel sqlite. Safe for concurrent use.
type typeRegistryStore struct {
	db    *sql.DB
	nowFn func() int64
}

const (
	TypeInstallStatusInstalling = "installing"
	TypeInstallStatusInstalled  = "installed"
	TypeInstallStatusFailed     = "failed"
)

// NewTypeRegistry builds the registry over the given channel sqlite.
// nowFn stamps the created_at column on install writes; passing nil falls back
// to a no-op zero (callers that need real timestamps MUST inject NowFn).
func newTypeRegistry(db *sql.DB, nowFn func() int64) *typeRegistryStore {
	if nowFn == nil {
		nowFn = func() int64 { return 0 }
	}
	return &typeRegistryStore{db: db, nowFn: nowFn}
}

// BeginInstall stages an installing row outside the canonical installed row.
// Lookup/List/HarnessView continue to see the prior installed row until the
// matching install attempt is marked installed after mirror emit.
func (r *typeRegistryStore) BeginInstall(ctx context.Context, row storespec.TypeRow) (storespec.TypeInstallAttempt, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return storespec.TypeInstallAttempt{}, err
	}
	defer func() { _ = tx.Rollback() }()

	if err := r.failIfInstallingPendingTx(ctx, tx, row.Type); err != nil {
		return storespec.TypeInstallAttempt{}, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM type_registry_pending WHERE type=? AND install_status='failed'`,
		row.Type,
	); err != nil {
		return storespec.TypeInstallAttempt{}, fmt.Errorf("store: type_registry clear failed pending %q: %w", row.Type, err)
	}

	_, existed, err := r.lookupInstalledTx(ctx, tx, row.Type)
	if err != nil {
		return storespec.TypeInstallAttempt{}, err
	}
	attemptID := uuid.NewString()
	persisted, err := r.insertPendingInstallTx(ctx, tx, attemptID, row)
	if err != nil {
		return storespec.TypeInstallAttempt{}, err
	}
	if err := tx.Commit(); err != nil {
		return storespec.TypeInstallAttempt{}, fmt.Errorf("store: type_registry begin install commit %q: %w", row.Type, err)
	}
	return storespec.TypeInstallAttempt{Row: persisted, AttemptID: attemptID, Existed: existed}, nil
}

func (r *typeRegistryStore) MarkInstalled(ctx context.Context, typeName, attemptID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: type_registry mark installed begin %q: %w", typeName, err)
	}
	defer func() { _ = tx.Rollback() }()

	row, ok, err := r.lookupPendingAttemptTx(ctx, tx, typeName, attemptID, TypeInstallStatusInstalling)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("store: type_registry mark installed %q: install attempt %q not installing", typeName, attemptID)
	}
	if _, err := r.upsertWithStatusTx(ctx, tx, row, TypeInstallStatusInstalled, ""); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx,
		`DELETE FROM type_registry_pending
		  WHERE type=? AND install_attempt_id=? AND install_status='installing'`,
		typeName, attemptID,
	)
	if err != nil {
		return fmt.Errorf("store: type_registry clear pending %q: %w", typeName, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: type_registry clear pending rows %q: %w", typeName, err)
	}
	if n != 1 {
		return fmt.Errorf("store: type_registry clear pending %q: rows=%d want 1", typeName, n)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: type_registry mark installed commit %q: %w", typeName, err)
	}
	return nil
}

func (r *typeRegistryStore) MarkInstallFailed(ctx context.Context, typeName, attemptID, reason string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE type_registry_pending
		    SET install_status='failed', install_error=?
		  WHERE type=? AND install_attempt_id=? AND install_status='installing'`,
		reason, typeName, attemptID,
	)
	if err != nil {
		return fmt.Errorf("store: type_registry mark failed %q: %w", typeName, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: type_registry mark failed rows %q: %w", typeName, err)
	}
	if n != 1 {
		return fmt.Errorf("store: type_registry mark failed %q: install attempt %q not installing", typeName, attemptID)
	}
	return nil
}

func (r *typeRegistryStore) RecoverInstalling(ctx context.Context, reason string) (int, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT type, install_attempt_id FROM type_registry_pending WHERE install_status='installing'`)
	if err != nil {
		return 0, fmt.Errorf("store: type_registry recover installing: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type pendingInstall struct {
		typ       string
		attemptID string
	}
	var pending []pendingInstall
	for rows.Next() {
		var p pendingInstall
		if err := rows.Scan(&p.typ, &p.attemptID); err != nil {
			return 0, fmt.Errorf("store: type_registry recover scan: %w", err)
		}
		pending = append(pending, p)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: type_registry recover rows: %w", err)
	}
	var recovered int
	for _, p := range pending {
		hasMirror, err := r.typeInstalledMirrorExists(ctx, p.typ, p.attemptID)
		if err != nil {
			return recovered, err
		}
		if hasMirror {
			if err := r.MarkInstalled(ctx, p.typ, p.attemptID); err != nil {
				return recovered, fmt.Errorf("store: type_registry recover installed %q: %w", p.typ, err)
			}
		} else {
			if err := r.MarkInstallFailed(ctx, p.typ, p.attemptID, reason); err != nil {
				return recovered, fmt.Errorf("store: type_registry recover failed %q: %w", p.typ, err)
			}
		}
		recovered++
	}
	return recovered, nil
}

func (r *typeRegistryStore) InstallStatus(ctx context.Context, typeName string) (status, reason string, ok bool, err error) {
	err = r.db.QueryRowContext(ctx,
		`SELECT install_status, install_error
		   FROM type_registry_pending
		  WHERE type=?
		  ORDER BY created_at DESC
		  LIMIT 1`,
		typeName,
	).Scan(&status, &reason)
	if err == nil {
		return status, reason, true, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", "", false, fmt.Errorf("store: type_registry install status pending %q: %w", typeName, err)
	}
	err = r.db.QueryRowContext(ctx,
		`SELECT install_status, install_error FROM type_registry WHERE type=?`,
		typeName,
	).Scan(&status, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, fmt.Errorf("store: type_registry install status %q: %w", typeName, err)
	}
	return status, reason, true, nil
}

func (r *typeRegistryStore) upsertWithStatusTx(ctx context.Context, tx *sql.Tx, row storespec.TypeRow, installStatus, installError string) (storespec.TypeRow, error) {
	if err := row.Validate(); err != nil {
		return storespec.TypeRow{}, err
	}
	if strings.HasPrefix(row.Type, "system.") || strings.HasPrefix(row.Type, "actor.") {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry reserved namespace %q: %s",
			row.Type, "type_registry_reserved_namespace")
	}

	allowedKinds, err := marshalAllowedKinds(row.AllowedKinds)
	if err != nil {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry upsert marshal allowed_kinds: %w", err)
	}

	var maxPending any
	if row.MaxPendingMs <= 0 {
		maxPending = nil
	} else {
		maxPending = row.MaxPendingMs
	}

	var handler any
	if row.HandlerActorID == "" {
		handler = nil
	} else {
		handler = string(row.HandlerActorID)
	}

	const q = `INSERT INTO type_registry
		(type, allowed_kinds, handler_binding,
		 max_pending_ms, handler_actor_id, install_status, install_error, created_at)
		VALUES (?,?,?,?,?,?,?,?)
		ON CONFLICT(type) DO UPDATE SET
			allowed_kinds            = excluded.allowed_kinds,
			handler_binding          = excluded.handler_binding,
			max_pending_ms           = excluded.max_pending_ms,
			handler_actor_id         = excluded.handler_actor_id,
			install_status          = excluded.install_status,
			install_error           = excluded.install_error
	`
	if _, err := tx.ExecContext(ctx, q,
		row.Type,
		allowedKinds,
		string(row.HandlerBinding),
		maxPending,
		handler,
		installStatus,
		installError,
		r.nowFn(),
	); err != nil {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry upsert %q: %w", row.Type, err)
	}

	persisted, ok, err := r.lookupAnyTx(ctx, tx, row.Type)
	if err != nil {
		return storespec.TypeRow{}, err
	}
	if !ok {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry upsert %q vanished post-insert", row.Type)
	}
	return persisted, nil
}

func (r *typeRegistryStore) insertPendingInstallTx(ctx context.Context, tx *sql.Tx, attemptID string, row storespec.TypeRow) (storespec.TypeRow, error) {
	if err := row.Validate(); err != nil {
		return storespec.TypeRow{}, err
	}
	if strings.HasPrefix(row.Type, "system.") || strings.HasPrefix(row.Type, "actor.") {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry reserved namespace %q: %s",
			row.Type, "type_registry_reserved_namespace")
	}

	allowedKinds, err := marshalAllowedKinds(row.AllowedKinds)
	if err != nil {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry pending marshal allowed_kinds: %w", err)
	}
	var maxPending any
	if row.MaxPendingMs <= 0 {
		maxPending = nil
	} else {
		maxPending = row.MaxPendingMs
	}
	var handler any
	if row.HandlerActorID == "" {
		handler = nil
	} else {
		handler = string(row.HandlerActorID)
	}

	const q = `INSERT INTO type_registry_pending
		(install_attempt_id, type, allowed_kinds, handler_binding,
		 max_pending_ms, handler_actor_id,
		 install_status, install_error, created_at)
		VALUES (?,?,?,?,?,?,?,?,?)`
	if _, err := tx.ExecContext(ctx, q,
		attemptID,
		row.Type,
		allowedKinds,
		string(row.HandlerBinding),
		maxPending,
		handler,
		TypeInstallStatusInstalling,
		"",
		r.nowFn(),
	); err != nil {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry pending insert %q: %w", row.Type, err)
	}
	persisted, ok, err := r.lookupPendingAttemptTx(ctx, tx, row.Type, attemptID, TypeInstallStatusInstalling)
	if err != nil {
		return storespec.TypeRow{}, err
	}
	if !ok {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry pending %q vanished post-insert", row.Type)
	}
	return persisted, nil
}

// Lookup satisfies storespec.TypeStore — returns the installed-row
// view of a registered type; ok=false when the row is missing.
func (r *typeRegistryStore) Lookup(ctx context.Context, typeName string) (storespec.TypeRow, bool, error) {
	return r.lookup(ctx, typeName)
}

// List satisfies storespec.TypeStore — returns every installed row sorted
// by type for deterministic output.
func (r *typeRegistryStore) List(ctx context.Context) ([]storespec.TypeRow, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  max_pending_ms, handler_actor_id
			             FROM type_registry
			            WHERE install_status='installed'`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: type_registry list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]storespec.TypeRow, 0)
	for rows.Next() {
		row, err := scanTypeRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: type_registry list rows: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out, nil
}

// LookupView returns the runtime/storespec.TypeViewLookup view of one
// registered type. Implemented as a thin reshape over the framework row
// so a single sqlite row backs both the adapter install path and the
// harness write path.
func (r *typeRegistryStore) LookupView(ctx context.Context, typeName string) (storespec.TypeView, bool, error) {
	row, ok, err := r.lookup(ctx, typeName)
	if err != nil || !ok {
		return storespec.TypeView{}, ok, err
	}
	return storespec.TypeView{
		Type:           row.Type,
		AllowedKinds:   append([]message.Kind(nil), row.AllowedKinds...),
		MaxPendingMs:   row.MaxPendingMs,
		HandlerActorID: row.HandlerActorID,
	}, true, nil
}

// HarnessView returns a runtime/storespec.TypeViewLookup view that delegates
// every Lookup back to LookupView. Used by daemon composition root so
// the harness Chain shares storage with framework.Manager.Install.
func (r *typeRegistryStore) HarnessView() storespec.TypeViewLookup {
	return typeRegistryHarnessAdapter{r}
}

type typeRegistryHarnessAdapter struct{ inner *typeRegistryStore }

func (a typeRegistryHarnessAdapter) Lookup(ctx context.Context, typeName string) (storespec.TypeView, bool, error) {
	return a.inner.LookupView(ctx, typeName)
}

// ------------------------------------------------------------------
// internal helpers
// ------------------------------------------------------------------

func (r *typeRegistryStore) lookup(ctx context.Context, typeName string) (storespec.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
		                  max_pending_ms, handler_actor_id
		             FROM type_registry
		            WHERE type=? AND install_status='installed'`
	return r.lookupQuery(ctx, q, typeName)
}

func (r *typeRegistryStore) lookupAnyTx(ctx context.Context, tx *sql.Tx, typeName string) (storespec.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  max_pending_ms, handler_actor_id
			             FROM type_registry
			            WHERE type=?`
	row, err := scanTypeRowFrom(tx.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.TypeRow{}, false, nil
	}
	if err != nil {
		return storespec.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *typeRegistryStore) lookupInstalledTx(ctx context.Context, tx *sql.Tx, typeName string) (storespec.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  max_pending_ms, handler_actor_id
			             FROM type_registry
			            WHERE type=? AND install_status='installed'`
	row, err := scanTypeRowFrom(tx.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.TypeRow{}, false, nil
	}
	if err != nil {
		return storespec.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *typeRegistryStore) lookupPendingAttemptTx(ctx context.Context, tx *sql.Tx, typeName, attemptID, status string) (storespec.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  max_pending_ms, handler_actor_id
			             FROM type_registry_pending
			            WHERE type=? AND install_attempt_id=? AND install_status=?`
	row, err := scanTypeRowFrom(tx.QueryRowContext(ctx, q, typeName, attemptID, status))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.TypeRow{}, false, nil
	}
	if err != nil {
		return storespec.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *typeRegistryStore) failIfInstallingPendingTx(ctx context.Context, tx *sql.Tx, typeName string) error {
	var attemptID string
	err := tx.QueryRowContext(ctx,
		`SELECT install_attempt_id
		   FROM type_registry_pending
		  WHERE type=? AND install_status='installing'
		  LIMIT 1`,
		typeName,
	).Scan(&attemptID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: type_registry pending lookup %q: %w", typeName, err)
	}
	return fmt.Errorf("store: type_registry install already in progress %q", typeName)
}

func (r *typeRegistryStore) lookupQuery(ctx context.Context, q string, typeName string) (storespec.TypeRow, bool, error) {
	row, err := scanTypeRowSingle(r.db.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return storespec.TypeRow{}, false, nil
	}
	if err != nil {
		return storespec.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *typeRegistryStore) typeInstalledMirrorExists(ctx context.Context, typeName, attemptID string) (bool, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT payload FROM messages WHERE type='system.type.installed'`)
	if err != nil {
		return false, fmt.Errorf("store: type_registry recover scan mirrors: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return false, fmt.Errorf("store: type_registry recover mirror scan: %w", err)
		}
		var payload struct {
			Type      string `json:"type"`
			AttemptID string `json:"install_attempt_id"`
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		if payload.Type == typeName && payload.AttemptID == attemptID {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("store: type_registry recover mirror rows: %w", err)
	}
	return false, nil
}

type typeRowScanner interface {
	Scan(dest ...any) error
}

func scanTypeRow(rows *sql.Rows) (storespec.TypeRow, error)     { return scanTypeRowFrom(rows) }
func scanTypeRowSingle(row *sql.Row) (storespec.TypeRow, error) { return scanTypeRowFrom(row) }

func scanTypeRowFrom(s typeRowScanner) (storespec.TypeRow, error) {
	var (
		typ, allowedRaw, binding string
		maxPending               sql.NullInt64
		handler                  sql.NullString
	)
	if err := s.Scan(&typ, &allowedRaw, &binding,
		&maxPending, &handler); err != nil {
		return storespec.TypeRow{}, err
	}
	allowed, err := unmarshalAllowedKinds(allowedRaw)
	if err != nil {
		return storespec.TypeRow{}, fmt.Errorf("store: type_registry scan allowed_kinds %q: %w", typ, err)
	}
	row := storespec.TypeRow{
		Type:           typ,
		HandlerBinding: actor.Binding(binding),
		AllowedKinds:   allowed,
	}
	if maxPending.Valid {
		row.MaxPendingMs = maxPending.Int64
	}
	if handler.Valid {
		row.HandlerActorID = actor.ActorID(handler.String)
	}
	return row, nil
}

func marshalAllowedKinds(in []message.Kind) (string, error) {
	if len(in) == 0 {
		return "[]", nil
	}
	tmp := make([]string, len(in))
	for i, k := range in {
		tmp[i] = string(k)
	}
	raw, err := json.Marshal(tmp)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func unmarshalAllowedKinds(raw string) ([]message.Kind, error) {
	if raw == "" {
		return nil, nil
	}
	var tmp []string
	if err := json.Unmarshal([]byte(raw), &tmp); err != nil {
		return nil, err
	}
	out := make([]message.Kind, len(tmp))
	for i, k := range tmp {
		out[i] = message.Kind(k)
	}
	return out, nil
}

// Compile-time interface checks — both contracts stay in sync with code.
// (*typeRegistryStore satisfies the FULL TypeStore — install state machine +
// reads; TypeRegistry is the read-only subset of those reads, also satisfied.)
var (
	_ storespec.TypeStore      = (*typeRegistryStore)(nil)
	_ storespec.TypeRegistry   = (*typeRegistryStore)(nil)
	_ storespec.TypeViewLookup = typeRegistryHarnessAdapter{}
)
