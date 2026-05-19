package actorreg

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Registry is the channel-local actor registry query contract (L1 §12.1
// — 4 query patterns the harness / scheduler / type_registry install
// depend on).
//
// This package only declares the interface; concrete sqlite /
// postgres / federation backends live in runtime/store.
type Registry interface {
	// Lookup returns the actor record by id. Returns ok=false if the
	// id is not present in the registry. Soft-deregistered rows are
	// still returned (with DeregisteredAt set) so historical message
	// references can still be explained — caller may filter via
	// Record.IsActive(). L1 §12.4 lifecycle.
	Lookup(ctx context.Context, id actor.ActorID) (Record, bool, error)

	// Exists is a fast-path "registered at all?" check (returns true
	// even for soft-deregistered rows). Type-registry install uses it
	// to validate `handler_actor_id` foreign-key reference per L2
	// §1.4.2 install rules.
	Exists(ctx context.Context, id actor.ActorID) (bool, error)

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
	Deregister(ctx context.Context, id actor.ActorID, at int64) error
}
