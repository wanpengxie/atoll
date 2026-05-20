package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TerminalConvention describes how harness step 8 decides whether a
// response is terminal for a given business type (L1 §10.2 / L2 §1.4.2).
//
// Lives in kernel/adapter because two packages persist it:
//
//   - adapters/framework — InMemoryTypeRegistry used by tests / outbound
//     adapter setups that don't want a sqlite dependency.
//   - runtime/store — sqlite-backed TypeRegistry mirrored into the
//     channel-local type_registry table (L2 §1.4.2).
//
// Keeping the contract in kernel ensures both implementations stay in
// lockstep without runtime/** taking a dependency on adapters/**.
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

// TypeRow is the per-row projection of one type_registry entry (L2
// §1.4.2). Each row carries the fields the Message-Write Harness 9-step
// chain (L1 §10.2) needs for steps 4 (type whitelist), 5 (kind × type +
// audience handler), 6 (payload schema), and 8 (terminal convention).
//
// All schema fields are JSON byte slices: implementations keep the
// schema opaque so the validator can swap (M1.5 baseline uses a JSON
// Schema subset; M1.x may drop in a full Draft 2020-12 library).
type TypeRow struct {
	// Type is the envelope.type value (e.g. "feishu.chat.send").
	Type string

	// HandlerActorID is the actor that handles the type — must point at
	// a tool actor in actor_registry per L2 §1.4.2.
	HandlerActorID actor.ActorID

	// HandlerBinding mirrors the actor's binding (embedded /
	// runtime_outbound / runtime_inbound_via_relay).
	HandlerBinding actor.Binding

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
// lives in framework.ValidateTypeSchema — Validate covers only the
// surface-level non-empty + binding-format checks used by tests +
// sqlite store.
func (t TypeRow) Validate() error {
	if t.Type == "" {
		return errors.New("adapter: TypeRow.Type required")
	}
	if t.HandlerActorID == "" {
		return fmt.Errorf("adapter: TypeRow[%s].HandlerActorID required", t.Type)
	}
	if _, ok := actor.ParseBinding(string(t.HandlerBinding)); !ok {
		return fmt.Errorf("adapter: TypeRow[%s].HandlerBinding %q invalid",
			t.Type, t.HandlerBinding)
	}
	if t.MaxPendingMs <= 0 {
		return fmt.Errorf("adapter: TypeRow[%s].MaxPendingMs must be > 0", t.Type)
	}
	return nil
}

// TypeRegistry is the framework-private seam over the L2 §1.4.2
// type_registry table. Manager.Install Upserts one row per
// Declaration.Type.
//
// adapters/framework.InMemoryTypeRegistry implements this for tests;
// runtime/store.TypeRegistry implements it over a channel sqlite.
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
