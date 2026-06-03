package storespec

import (
	"context"

	"github.com/wanpengxie/ActOS/kernel/actor"
)

// Record is the channel-local actor membership row exposed via the registry
// query API (L1 §12.2). The projection STORAGE (actor_registry table) lives
// in runtime/store.
//
// substrate scope: Record carries ONLY membership — who is registered, and
// when they deregistered (durable truth). Presence (whether a compute node is
// currently online) and readiness (whether an actor can currently serve) are
// not membership projections and are not modelled here: presence is volatile
// compute state, readiness is application business state. Neither is substrate
// membership.
type Record struct {
	ID             actor.ActorID
	Kind           actor.Kind
	Binding        actor.Binding // empty for human / system
	CreatedAt      int64
	DeregisteredAt int64 // 0 = active
}

// IsActive reports whether the actor is still active (L1 §12.2).
func (r Record) IsActive() bool { return r.DeregisteredAt == 0 }

// Registry is the channel-local actor membership READ contract (L1 §12.1) —
// deliberately SEGREGATED from the membership-write surface so a read-only
// consumer never receives Insert/Deregister.
// Membership mutation lives on MembershipWriter / MembershipControlPlane (a
// control-plane write that is NOT a query). Forward-derived from the reader's
// role, not from any one consumer. Concrete sqlite backend lives in
// runtime/store (actorRegistry, which satisfies all three interfaces).
type Registry interface {
	Lookup(ctx context.Context, id actor.ActorID) (Record, bool, error)
	Exists(ctx context.Context, id actor.ActorID) (bool, error)
	ListActive(ctx context.Context) ([]Record, error)
}

// MembershipWriter is the single-actor membership-write surface (Insert /
// Deregister). It is SEGREGATED from the read-only Registry: Insert seeds a
// new membership row (L2 §1.4.6) and Deregister soft-removes one. These are
// control-plane writes, not queries, so a read-only caller cannot reach them.
// The log-emitting batch transition lives on MembershipControlPlane; this is
// the imperative single-actor seed/teardown path. Concrete impl in
// runtime/store (actorRegistry).
type MembershipWriter interface {
	Insert(ctx context.Context, rec Record) error
	Deregister(ctx context.Context, id actor.ActorID, at int64) error
}
