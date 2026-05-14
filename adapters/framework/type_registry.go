package framework

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/coagent-ai/coagent/kernel/actor"
	"github.com/coagent-ai/coagent/kernel/adapter"
)

// TypeRow is the framework-level view of one type_registry row (L2
// §1.4.2). The runtime/store backend (T3) fills in additional columns
// like fallback_response_schema; the framework only needs the four
// fields that drive Install / Dispatch.
type TypeRow struct {
	// Type is the envelope.type value (e.g. "feishu.chat.send").
	Type string

	// HandlerActorID is the actor that handles the type — must point at
	// a tool actor in actor_registry per L2 §1.4.2.
	HandlerActorID actor.ActorID

	// HandlerBinding mirrors the actor's binding kind (in_process /
	// outbound_http / via_server_transit).
	HandlerBinding adapter.BindingKind

	// MaxPendingMs is the per-type request timeout in milliseconds. MUST
	// be > 0 for tool receivers — install rejects with
	// adapter_timeout_missing otherwise.
	MaxPendingMs int64
}

// Validate returns a friendly error when a field is missing or invalid.
func (t TypeRow) Validate() error {
	if t.Type == "" {
		return errors.New("framework: TypeRow.Type required")
	}
	if t.HandlerActorID == "" {
		return fmt.Errorf("framework: TypeRow[%s].HandlerActorID required", t.Type)
	}
	if _, ok := adapter.NormalizeBinding(string(t.HandlerBinding)); !ok {
		return fmt.Errorf("framework: TypeRow[%s].HandlerBinding %q invalid", t.Type, t.HandlerBinding)
	}
	if t.MaxPendingMs <= 0 {
		return fmt.Errorf("framework: TypeRow[%s].MaxPendingMs must be > 0", t.Type)
	}
	return nil
}

// TypeRegistry is the framework-private seam over the L2 §1.4.2
// type_registry table. Manager.Install Upserts one row per
// Declaration.Type. T3 will swap the InMemory impl for a sqlite-backed
// implementation in runtime/store.
//
// Implementations MUST be safe for concurrent use.
type TypeRegistry interface {
	// Upsert inserts row or overwrites an existing row with the same
	// Type. Returns the persisted row (caller may compare to detect
	// conflicts).
	Upsert(ctx context.Context, row TypeRow) (TypeRow, error)

	// Lookup returns the row for type (ok=false when absent).
	Lookup(ctx context.Context, typeName string) (TypeRow, bool, error)

	// List returns every row in the registry. Order is not stable
	// across implementations; callers sort if they need deterministic
	// output.
	List(ctx context.Context) ([]TypeRow, error)
}

// InMemoryTypeRegistry is the framework-shipped TypeRegistry used by
// tests + the default Manager when no production registry is wired.
type InMemoryTypeRegistry struct {
	mu   sync.RWMutex
	rows map[string]TypeRow
}

// NewInMemoryTypeRegistry returns an empty in-memory TypeRegistry.
func NewInMemoryTypeRegistry() *InMemoryTypeRegistry {
	return &InMemoryTypeRegistry{rows: map[string]TypeRow{}}
}

// Upsert validates row then writes / overwrites.
func (r *InMemoryTypeRegistry) Upsert(_ context.Context, row TypeRow) (TypeRow, error) {
	if err := row.Validate(); err != nil {
		return TypeRow{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rows[row.Type] = row
	return row, nil
}

// Lookup returns the row for typeName.
func (r *InMemoryTypeRegistry) Lookup(_ context.Context, typeName string) (TypeRow, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	row, ok := r.rows[typeName]
	return row, ok, nil
}

// List returns every row sorted by Type (deterministic for tests).
func (r *InMemoryTypeRegistry) List(_ context.Context) ([]TypeRow, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]TypeRow, 0, len(r.rows))
	for _, row := range r.rows {
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out, nil
}
