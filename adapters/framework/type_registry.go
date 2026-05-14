package framework

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/adapter"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TerminalConvention describes how harness step 8 decides whether a
// response is terminal for a given business type (L1 §10.2 / L2 §1.4.2).
type TerminalConvention string

// TerminalConvention closed set per L2 §1.4.2.
const (
	// TerminalPayloadStatus is the default — response is terminal when
	// payload.status ∈ {"completed","failed"}.
	TerminalPayloadStatus TerminalConvention = "payload_status"

	// TerminalSingleResponse means every response is terminal (no
	// payload.status required). Used by simple request/response types.
	TerminalSingleResponse TerminalConvention = "single-response"
)

// TypeRow is the framework-level view of one type_registry row (L2
// §1.4.2). Each row carries the fields the Message-Write Harness 9-step
// chain (L1 §10.2) needs for steps 4 (type whitelist), 5 (kind × type
// + audience handler), 6 (payload schema), and 8 (terminal convention).
//
// All schema fields are JSON byte slices: the framework keeps schemas
// opaque so the validator can swap implementations (M1.5 baseline uses
// a JSON-Schema subset; M1.x can drop in a full JSON Schema Draft
// 2020-12 library per L2 §1.4.2).
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

	// AllowedKinds lists every envelope.kind the harness will accept for
	// this type. Non-empty; subset of {event, request, response}.
	// Step 5 reject reason: kind_not_allowed.
	AllowedKinds []message.Kind

	// SchemasByKind maps kind → JSON Schema (opaque bytes). Keys MUST be
	// a subset of AllowedKinds. Step 6 looks up the schema for the
	// envelope's kind; missing key → payload_schema_violation.
	SchemasByKind map[message.Kind]json.RawMessage

	// FallbackResponseSchema is the response schema's "failure branch"
	// projection. install validates it accepts the 3 spec-mandated
	// system fallback payloads (L2 §1.4.2 install rules). Optional when
	// AllowedKinds does NOT include request (no system fallback needed).
	FallbackResponseSchema json.RawMessage

	// TerminalConvention controls harness step 8 terminal computation.
	// Default = payload_status. Optional when AllowedKinds excludes
	// `response`.
	TerminalConvention TerminalConvention
}

// Validate returns a friendly error when a field is missing or invalid.
// Returns nil on success.
//
// Schema-shape validation (allowed_kinds + schemas_by_kind +
// fallback_response_schema) is L1 §10.3.2 install_reason territory and
// lives in validateTypeRowForInstall — Validate covers only the
// surface-level non-empty + binding-format checks used by tests.
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
