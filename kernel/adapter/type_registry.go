package adapter

import (
	"context"
	"errors"
	"fmt"

	"github.com/wanpengxie/ActOS/kernel/actor"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// TypeRow is the per-row projection of one type_registry entry (L2
// §1.4.2). Each row carries the fields the Message-Write Harness chain
// (proto-layer1 §2) needs for steps 5 (type ∈ registry + kind ∈
// allowed_kinds) and 7 (handler_actor_id audience match). is_terminal is
// computed purely from payload.status (proto-layer1 §2.8).
//
// Level A (proto-layer0 §1.4.1 / proto-layer1 §1.3): payload is opaque
// to the protocol layer; type_registry stores NO payload schema fields.
// Payload consistency is a product-layer concern.
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
}

// Validate returns a friendly error when a field is missing or invalid.
// Returns nil on success.
//
// Closed-set install validation (allowed_kinds non-empty / known kinds)
// lives in framework.ValidateTypeDeclaration; Validate covers only the
// surface-level non-empty + binding-format checks used by tests + sqlite
// store.
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
