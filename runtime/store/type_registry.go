package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

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
	if err := row.Validate(); err != nil {
		return adapter.TypeRow{}, err
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
		 max_pending_ms, handler_actor_id, created_at)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(type) DO UPDATE SET
			allowed_kinds            = excluded.allowed_kinds,
			handler_binding          = excluded.handler_binding,
			terminal_convention      = excluded.terminal_convention,
			max_pending_ms           = excluded.max_pending_ms,
			handler_actor_id         = excluded.handler_actor_id
	`
	if _, err := r.db.ExecContext(ctx, q,
		row.Type,
		allowedKinds,
		string(row.HandlerBinding),
		string(terminal),
		maxPending,
		handler,
		r.nowFn(),
	); err != nil {
		return adapter.TypeRow{}, fmt.Errorf("store: type_registry upsert %q: %w", row.Type, err)
	}

	persisted, ok, err := r.lookup(ctx, row.Type)
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
	             FROM type_registry`
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
	             FROM type_registry WHERE type=?`
	row, err := scanTypeRowSingle(r.db.QueryRowContext(ctx, q, typeName))
	if errors.Is(err, sql.ErrNoRows) {
		return adapter.TypeRow{}, false, nil
	}
	if err != nil {
		return adapter.TypeRow{}, false, err
	}
	return row, true, nil
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
