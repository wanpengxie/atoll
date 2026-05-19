package actor

import "context"

// Binding is the actor-level transport descriptor. It mirrors the
// `actor_registry.actor_binding` column (L2 §1.4.6) and is identical to
// the M1.5 closed enum defined in L1 §11.7. The string values match the
// SQL CHECK constraint — keep in sync with kernel/adapter/binding.go.
type Binding string

// Binding closed set — kept duplicated as raw strings here (rather than
// importing kernel/adapter) so kernel/actor stays a leaf package. Both
// packages reference L1 §11.7 as the single source of truth.
const (
	BindingInProcess        Binding = "in_process"
	BindingOutboundHTTP     Binding = "outbound_http"
	BindingViaServerTransit Binding = "via_server_transit"
)

// Record is the channel-local actor row exposed via the registry query
// API (L1 §12.2 minimum field set).
//
// `Binding` is empty string for human / system actors per L1 §12.2. The
// SQL CHECK in L2 §1.4.6 keeps the column NULL for those rows; this
// kernel-level interface uses zero-value (empty string) to mean the
// same.
type Record struct {
	ID             ActorID
	Kind           Kind
	Binding        Binding // empty for human / system
	DisplayName    string  // optional; informative only (L1 §12.2 fields ⬜)
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active; non-zero = soft-deregister timestamp
}

// IsActive reports whether the actor is still active per L1 §12.2.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

// Registry is the channel-local actor registry query contract (L1 §12.1
// — 4 query patterns the harness / scheduler / type_registry install
// depend on).
//
// The kernel package only declares the interface; concrete sqlite /
// postgres / federation backends live in runtime/store.
type Registry interface {
	// Lookup returns the actor record by id. Returns ok=false if the
	// id is not present in the registry. Soft-deregistered rows are
	// still returned (with DeregisteredAt set) so historical message
	// references can still be explained — caller may filter via
	// Record.IsActive(). L1 §12.4 lifecycle.
	Lookup(ctx context.Context, id ActorID) (Record, bool, error)

	// Exists is a fast-path "registered at all?" check (returns true
	// even for soft-deregistered rows). Type-registry install uses it
	// to validate `handler_actor_id` foreign-key reference per L2
	// §1.4.2 install rules.
	Exists(ctx context.Context, id ActorID) (bool, error)

	// ListActive returns every active actor in the channel (filtered by
	// `deregistered_at IS NULL`). Used by trigger gateway to expand
	// `audience=['*']` per L1 §5.1.
	ListActive(ctx context.Context) ([]Record, error)

	// Insert writes a new actor row. The row is active immediately
	// (DeregisteredAt = 0). L1 §12.3 write timings (channel bootstrap /
	// member CRUD / agent spawn / adapter install) all funnel through
	// this entry point. The implementation MUST also seed the
	// actor_cursors row in the same transaction (L2 §1.4.6 invariant).
	Insert(ctx context.Context, rec Record) error

	// Deregister applies a soft-delete (sets DeregisteredAt). The row
	// is preserved so historical message sender.id references remain
	// resolvable. Re-using the id is forbidden by L1 §12.4 protocol
	// baseline — implementations should reject Insert of a previously
	// deregistered id.
	Deregister(ctx context.Context, id ActorID, at int64) error
}
