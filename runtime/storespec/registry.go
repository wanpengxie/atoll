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
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active
}

// IsActive reports whether the actor is still active.
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

type PrincipalRegistry interface {
	LookupActivePrincipal(ctx context.Context, kind actor.Kind, principal string) (Record, bool, error)
}
