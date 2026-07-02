package resourcespec

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
)

// StateStore is the byte realizer for the ACTOR-SCOPED storage locus — the
// second, structurally separate home of objects (an actor_state-shaped table
// keyed (owner, id)), dual to the channel-scoped resources table. Scope is
// expressed by WHICH structure an object lives in, never by a column (§12.9
// 拍点 8.1). It is a driver in RESPONSIBILITY (giftless byte realizer,
// substrate-owned, closed) but NOT a DriverTable entry: DriverTable routes
// channel-scoped kind→driver; the actor-scoped locus has no kind routing
// (day-1 one mechanical shape: inline small bytes, plaintext).
//
// The collapsed authorization (owner-only) is NOT re-checked here: the door
// welds owner at handle mint and the reachable set is structurally ≡ {owner}.
// Registry(R)/membership are not consulted — that absence IS the scope law.
// No Resolve (each op's own row-hit IS the existence check — the channel-
// scoped door needs Resolve only to route meta.Kind before the R query; the
// collapsed branch has neither), no List (enumeration is not access; "不进
// 可枚举 registry" is scope-law content, 零预留).
//
// Load-bearing (§3.1 注):
//  1. owner is a COORDINATE, not a check — StateStore trusts the door (mirrors
//     storespec's store-not-validate), and owner is always the door-welded
//     caller.
//  2. No kind, no grant, no Resolve, no List — each absence is the scope law's
//     positive value, not a deferred debt.
//  3. ErrAlreadyExists is the SAME resourcespec sentinel the channel-scoped
//     tree uses (one collision vocabulary, two loci).
type StateStore interface {
	// Create is the atomic birth: INSERT (owner, id, bytes). A colliding
	// (owner, id) → ErrAlreadyExists (shared sentinel; shared verdict
	// mapping). No grant row — R does not apply to this locus.
	Create(ctx context.Context, owner actor.ActorID, id resource.ResourceID, initial []byte) error

	// Read returns the current bytes; present=false = no row (the door maps
	// it to resource_not_found, uniform with the channel-scoped tree even at
	// the degenerate point). A present row with NULL bytes is resolved-but-
	// empty: the door maps it to Found=false (opus-B2, same bit meaning as
	// the channel-scoped driver), value=nil; empty non-nil bytes are a value.
	Read(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (value []byte, present bool, err error)

	// Write overwrites an EXISTING row (PUT semantics, idempotent);
	// present=false when no row was hit (door → resource_not_found — birth
	// is Create, not Write).
	Write(ctx context.Context, owner actor.ActorID, id resource.ResourceID, value []byte) (present bool, err error)

	// Delete removes the row; present=false when no row was hit (door →
	// resource_not_found; repeated delete is honestly not-found). The OTHER
	// death is scope-expiry (owner deregister → clearActorScopedTx,
	// store-internal, not an op).
	Delete(ctx context.Context, owner actor.ActorID, id resource.ResourceID) (present bool, err error)
}
