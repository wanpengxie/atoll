package actor

import "context"

// Record is the channel-local actor row exposed via the registry query API
// (L1 §12.2 minimum field set). It is the DATA CONTRACT of a registry query
// result; the projection STORAGE (actor_registry table + sqlite/in-memory
// backend) lives in runtime/store. Kept in kernel/actor because the query
// contract is consumed by the harness/scheduler (which depend on kernel
// abstractions, not the store implementation — avoids a runtime/store ↔
// runtime/harness import cycle).
//
// Binding is empty string for human / system actors per L1 §12.2.
type Record struct {
	ID             ActorID
	Kind           Kind
	Binding        Binding // empty for human / system
	DisplayName    string  // optional; informative only
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active; non-zero = soft-deregister timestamp
	Readiness      Readiness
}

// IsActive reports whether the actor is still active per L1 §12.2.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

// Registry is the channel-local actor registry query contract (L1 §12.1 — the
// query patterns harness / scheduler / type_registry install depend on).
// Concrete sqlite / in-memory backends live in runtime/store (ActorRegistry).
type Registry interface {
	// Lookup returns the actor record by id (ok=false if absent).
	// Soft-deregistered rows are still returned (DeregisteredAt set).
	Lookup(ctx context.Context, id ActorID) (Record, bool, error)

	// Exists is a fast-path "registered at all?" check (true even for
	// soft-deregistered rows).
	Exists(ctx context.Context, id ActorID) (bool, error)

	// ListActive returns every active actor in the channel.
	ListActive(ctx context.Context) ([]Record, error)

	// Insert writes a new actor row (active immediately). The implementation
	// MUST also seed the actor_cursors row in the same transaction.
	Insert(ctx context.Context, rec Record) error

	// Deregister applies a soft-delete (sets DeregisteredAt); the row is
	// preserved so historical sender.id references remain resolvable.
	Deregister(ctx context.Context, id ActorID, at int64) error
}

// ReadinessUpdater is an optional extension implemented by registries that can
// persist actor readiness.
type ReadinessUpdater interface {
	UpdateReadiness(ctx context.Context, id ActorID, update ReadinessUpdate) (ReadinessTransition, error)
}

// ActiveAudienceExcept returns active actor ids in the channel, excluding the
// supplied ids. Helper for emit points needing channel-wide delivery after
// wildcard removal.
func ActiveAudienceExcept(ctx context.Context, reg Registry, exclude ...ActorID) ([]ActorID, error) {
	rows, err := reg.ListActive(ctx)
	if err != nil {
		return nil, err
	}
	excludeSet := make(map[ActorID]struct{}, len(exclude))
	for _, id := range exclude {
		excludeSet[id] = struct{}{}
	}
	out := make([]ActorID, 0, len(rows))
	for _, rec := range rows {
		if _, drop := excludeSet[rec.ID]; drop {
			continue
		}
		out = append(out, rec.ID)
	}
	return out, nil
}
