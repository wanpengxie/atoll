package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
	"github.com/wanpengxie/ActOS/runtime/harness"
)

// TypeRegistry is the sqlite-backed implementation of
// kernel/adapter.TypeRegistry (upsert + lookup + list at adapter install
// time) over the channel-local type_registry table (L2 §1.4.2). It also
// exposes a runtime/harness.TypeRegistry projection via HarnessView so
// the harness Chain reads the same row Manager.Install wrote.
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload is opaque
// to the protocol layer; this registry stores NO payload schema fields.
//
// One *TypeRegistry per channel sqlite. Safe for concurrent use.
type TypeRegistry struct {
	db    *sql.DB
	nowFn func() int64
}

const (
	TypeInstallStatusInstalling = "installing"
	TypeInstallStatusInstalled  = "installed"
	TypeInstallStatusFailed     = "failed"
)

// NewTypeRegistry builds the registry over the given channel sqlite.
// nowFn stamps the created_at column on Upsert; passing nil falls back
// to a no-op zero (callers that need real timestamps MUST inject NowFn).
func NewTypeRegistry(db *sql.DB, nowFn func() int64) *TypeRegistry {
	if nowFn == nil {
		nowFn = func() int64 { return 0 }
	}
	return &TypeRegistry{db: db, nowFn: nowFn}
}

// Upsert satisfies kernel/adapter.TypeRegistry. It validates row, then
// INSERTs (or replaces on PK conflict) into the type_registry table.
// Returns the persisted row (round-tripped through JSON marshal so
// callers observe canonicalised bytes).
func (r *TypeRegistry) Upsert(ctx context.Context, row adapter.TypeRow) (adapter.TypeRow, error) {
	return r.upsertWithStatus(ctx, row, TypeInstallStatusInstalled, "")
}

// BeginInstall writes an installing row and returns whether an installed row
// was visible before the install attempt began. Installing rows are intentionally
// hidden from Lookup/List/HarnessView until MarkInstalled succeeds.
func (r *TypeRegistry) BeginInstall(ctx context.Context, row adapter.TypeRow) (adapter.TypeRow, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	defer func() { _ = tx.Rollback() }()
	_, existedAny, status, err := r.lookupAnyStatusTx(ctx, tx, row.Type)
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	if existedAny && status == TypeInstallStatusInstalling {
		return adapter.TypeRow{}, false, fmt.Errorf("store: type_registry install already in progress %q", row.Type)
	}
	persisted, err := r.upsertWithStatusTx(ctx, tx, row, TypeInstallStatusInstalling, "")
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return adapter.TypeRow{}, false, fmt.Errorf("store: type_registry begin install commit %q: %w", row.Type, err)
	}
	existed := existedAny && status == TypeInstallStatusInstalled
	return persisted, existed, err
}

func (r *TypeRegistry) MarkInstalled(ctx context.Context, typeName string) error {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE type_registry
		    SET install_status='installed', install_error=''
		  WHERE type=? AND install_status IN ('installing','installed')`,
		typeName,
	); err != nil {
		return fmt.Errorf("store: type_registry mark installed %q: %w", typeName, err)
	}
	return nil
}

func (r *TypeRegistry) MarkInstallFailed(ctx context.Context, typeName, reason string) error {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE type_registry SET install_status='failed', install_error=? WHERE type=?`,
		reason, typeName,
	); err != nil {
		return fmt.Errorf("store: type_registry mark failed %q: %w", typeName, err)
	}
	return nil
}

func (r *TypeRegistry) RecoverInstalling(ctx context.Context, reason string) (int, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT type FROM type_registry WHERE install_status='installing'`)
	if err != nil {
		return 0, fmt.Errorf("store: type_registry recover installing: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var types []string
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			return 0, fmt.Errorf("store: type_registry recover scan: %w", err)
		}
		types = append(types, typ)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("store: type_registry recover rows: %w", err)
	}
	var recovered int
	for _, typ := range types {
		hasMirror, err := r.typeInstalledMirrorExists(ctx, typ)
		if err != nil {
			return recovered, err
		}
		if hasMirror {
			if _, err := r.db.ExecContext(ctx,
				`UPDATE type_registry SET install_status='installed', install_error='' WHERE type=? AND install_status='installing'`,
				typ,
			); err != nil {
				return recovered, fmt.Errorf("store: type_registry recover installed %q: %w", typ, err)
			}
		} else {
			if _, err := r.db.ExecContext(ctx,
				`UPDATE type_registry SET install_status='failed', install_error=? WHERE type=? AND install_status='installing'`,
				reason, typ,
			); err != nil {
				return recovered, fmt.Errorf("store: type_registry recover failed %q: %w", typ, err)
			}
		}
		recovered++
	}
	return recovered, nil
}

func (r *TypeRegistry) InstallStatus(ctx context.Context, typeName string) (status, reason string, ok bool, err error) {
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

func (r *TypeRegistry) upsertWithStatus(ctx context.Context, row adapter.TypeRow, installStatus, installError string) (adapter.TypeRow, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert begin %q: %w", row.Type, err)
	}
	defer func() { _ = tx.Rollback() }()
	persisted, err := r.upsertWithStatusTx(ctx, tx, row, installStatus, installError)
	if err != nil {
		return adapter.TypeRow{}, err
	}
	if err := tx.Commit(); err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert commit %q: %w", row.Type, err)
	}
	return persisted, nil
}

func (r *TypeRegistry) upsertWithStatusTx(ctx context.Context, tx *sql.Tx, row adapter.TypeRow, installStatus, installError string) (adapter.TypeRow, error) {
	if err := row.Validate(); err != nil {
		return adapter.TypeRow{}, err
	}
	if strings.HasPrefix(row.Type, "system.") {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry reserved namespace %q: %s",
			row.Type, message.InstallTypeRegistryReservedNamespace)
	}

	allowedKinds, err := marshalAllowedKinds(row.AllowedKinds)
	if err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert marshal allowed_kinds: %w", err)
	}

	terminal := row.TerminalConvention
	if terminal == "" {
		terminal = adapter.TerminalPayloadStatus
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
		(type, allowed_kinds, handler_binding, terminal_convention,
		 max_pending_ms, handler_actor_id, install_status, install_error, created_at)
		VALUES (?,?,?,?,?,?,?,?,?)
		ON CONFLICT(type) DO UPDATE SET
			allowed_kinds            = excluded.allowed_kinds,
			handler_binding          = excluded.handler_binding,
			terminal_convention      = excluded.terminal_convention,
			max_pending_ms           = excluded.max_pending_ms,
			handler_actor_id         = excluded.handler_actor_id,
			install_status          = excluded.install_status,
			install_error           = excluded.install_error
	`
	if _, err := tx.ExecContext(ctx, q,
		row.Type,
		allowedKinds,
		string(row.HandlerBinding),
		string(terminal),
		maxPending,
		handler,
		installStatus,
		installError,
		r.nowFn(),
	); err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert %q: %w", row.Type, err)
	}

	persisted, ok, err := r.lookupAnyTx(ctx, tx, row.Type)
	if err != nil {
		return adapter.TypeRow{}, err
	}
	if !ok {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert %q vanished post-insert", row.Type)
	}
	return persisted, nil
}

// Lookup satisfies kernel/adapter.TypeRegistry — returns the framework
// view of a registered type; ok=false when the row is missing.
func (r *TypeRegistry) Lookup(ctx context.Context, typeName string) (adapter.TypeRow, bool, error) {
	return r.lookup(ctx, typeName)
}

// List satisfies kernel/adapter.TypeRegistry — returns every row sorted
// by type for deterministic test output.
func (r *TypeRegistry) List(ctx context.Context) ([]adapter.TypeRow, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  terminal_convention, max_pending_ms, handler_actor_id
			             FROM type_registry
			            WHERE install_status='installed'`
	rows, err := r.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("store: type_registry list: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]adapter.TypeRow, 0)
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

// LookupView returns the runtime/harness.TypeRegistry view of one
// registered type. Implemented as a thin reshape over the framework row
// so a single sqlite row backs both the adapter install path and the
// harness write path.
func (r *TypeRegistry) LookupView(ctx context.Context, typeName string) (harness.TypeView, bool, error) {
	row, ok, err := r.lookup(ctx, typeName)
	if err != nil || !ok {
		return harness.TypeView{}, ok, err
	}
	return harness.TypeView{
		Type:               row.Type,
		AllowedKinds:       append([]message.Kind(nil), row.AllowedKinds...),
		MaxPendingMs:       row.MaxPendingMs,
		HandlerActorID:     row.HandlerActorID,
		TerminalConvention: string(row.TerminalConvention),
	}, true, nil
}

// HarnessView returns a runtime/harness.TypeRegistry view that delegates
// every Lookup back to LookupView. Used by daemon composition root so
// the harness Chain shares storage with framework.Manager.Install.
func (r *TypeRegistry) HarnessView() harness.TypeRegistry { return typeRegistryHarnessAdapter{r} }

type typeRegistryHarnessAdapter struct{ inner *TypeRegistry }

func (a typeRegistryHarnessAdapter) Lookup(ctx context.Context, typeName string) (harness.TypeView, bool, error) {
	return a.inner.LookupView(ctx, typeName)
}

// ------------------------------------------------------------------
// internal helpers
// ------------------------------------------------------------------

func (r *TypeRegistry) lookup(ctx context.Context, typeName string) (adapter.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
		                  terminal_convention, max_pending_ms, handler_actor_id
		             FROM type_registry
		            WHERE type=? AND install_status='installed'`
	return r.lookupQuery(ctx, q, typeName)
}

func (r *TypeRegistry) lookupAny(ctx context.Context, typeName string) (adapter.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
		                  terminal_convention, max_pending_ms, handler_actor_id
		             FROM type_registry
		            WHERE type=?`
	return r.lookupQuery(ctx, q, typeName)
}

func (r *TypeRegistry) lookupAnyTx(ctx context.Context, tx *sql.Tx, typeName string) (adapter.TypeRow, bool, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  terminal_convention, max_pending_ms, handler_actor_id
			             FROM type_registry
			            WHERE type=?`
	row, err := scanTypeRowFrom(tx.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.TypeRow{}, false, nil
	}
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *TypeRegistry) lookupAnyStatusTx(ctx context.Context, tx *sql.Tx, typeName string) (adapter.TypeRow, bool, string, error) {
	const q = `SELECT type, allowed_kinds, handler_binding,
			                  terminal_convention, max_pending_ms, handler_actor_id, install_status
			             FROM type_registry
			            WHERE type=?`
	var (
		typ, allowedRaw, binding, terminal, status string
		maxPending                                 sql.NullInt64
		handler                                    sql.NullString
	)
	err := tx.QueryRowContext(ctx, q, typeName).Scan(&typ, &allowedRaw, &binding, &terminal, &maxPending, &handler, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.TypeRow{}, false, "", nil
	}
	if err != nil {
		return adapter.TypeRow{}, false, "", err
	}
	allowed, err := unmarshalAllowedKinds(allowedRaw)
	if err != nil {
		return adapter.TypeRow{}, false, "", fmt.Errorf("store: type_registry scan allowed_kinds %q: %w", typ, err)
	}
	row := adapter.TypeRow{
		Type:               typ,
		HandlerBinding:     actor.Binding(binding),
		TerminalConvention: adapter.TerminalConvention(terminal),
		AllowedKinds:       allowed,
	}
	if maxPending.Valid {
		row.MaxPendingMs = maxPending.Int64
	}
	if handler.Valid {
		row.HandlerActorID = actor.ActorID(handler.String)
	}
	return row, true, status, nil
}

func (r *TypeRegistry) lookupQuery(ctx context.Context, q string, typeName string) (adapter.TypeRow, bool, error) {
	row, err := scanTypeRowSingle(r.db.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.TypeRow{}, false, nil
	}
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	return row, true, nil
}

func (r *TypeRegistry) typeInstalledMirrorExists(ctx context.Context, typeName string) (bool, error) {
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
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			continue
		}
		if payload.Type == typeName {
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

func scanTypeRow(rows *sql.Rows) (adapter.TypeRow, error)     { return scanTypeRowFrom(rows) }
func scanTypeRowSingle(row *sql.Row) (adapter.TypeRow, error) { return scanTypeRowFrom(row) }

func scanTypeRowFrom(s typeRowScanner) (adapter.TypeRow, error) {
	var (
		typ, allowedRaw, binding, terminal string
		maxPending                         sql.NullInt64
		handler                            sql.NullString
	)
	if err := s.Scan(&typ, &allowedRaw, &binding, &terminal,
		&maxPending, &handler); err != nil {
		return adapter.TypeRow{}, err
	}
	allowed, err := unmarshalAllowedKinds(allowedRaw)
	if err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry scan allowed_kinds %q: %w", typ, err)
	}
	row := adapter.TypeRow{
		Type:               typ,
		HandlerBinding:     actor.Binding(binding),
		TerminalConvention: adapter.TerminalConvention(terminal),
		AllowedKinds:       allowed,
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
var (
	_ adapter.TypeRegistry = (*TypeRegistry)(nil)
	_ harness.TypeRegistry = typeRegistryHarnessAdapter{}
)
