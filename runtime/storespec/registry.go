package storespec

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/actor"
)

// Record is the channel-local actor membership row exposed via the registry
// query API. The projection STORAGE (actor_registry table) lives
// in runtime/store.
//
// substrate scope: Record carries ONLY membership — who is registered, and
// when they deregistered (durable truth). Liveness (whether a compute node is
// currently online) and readiness (whether an actor can currently serve) are
// not membership projections and are not modelled here: liveness is volatile
// compute state, readiness is application business state. Neither is substrate
// membership.
type Record struct {
	ID             actor.ActorID
	Kind           actor.Kind
	Principal      string
	Binding        actor.Binding // empty for human / system
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active
}

// IsActive reports whether the actor is still active.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

// Registry is the channel-local actor membership READ contract —
// deliberately SEGREGATED from the membership-write surface so a read-only
// consumer never receives membership mutation methods.
// Durable identity mutation is available only through DeclAdmissionStore and
// CascadeStore; this read face cannot be widened back into a write surface.
type Registry interface {
	Lookup(ctx context.Context, id actor.ActorID) (Record, bool, error)
	Exists(ctx context.Context, id actor.ActorID) (bool, error)
	ListActive(ctx context.Context) ([]Record, error)
}

type PrincipalRegistry interface {
	LookupActivePrincipal(ctx context.Context, kind actor.Kind, principal string) (Record, bool, error)
}
