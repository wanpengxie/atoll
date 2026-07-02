package resourcespec

import (
	"context"
	"errors"

	"github.com/wanpengxie/ActOS/protocol/access"
	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/resource"
)

// ErrAlreadyExists is the atomic-create collision sentinel; the door maps it to
// access.AlreadyExists. create is a test-and-set on existence, so the collision
// is decided INSIDE Registry.Create's transaction (within the race window) —
// the door never resolves-then-creates in two steps.
var ErrAlreadyExists = errors.New("resourcespec: resource already exists")

// ResourceMeta is what the registry knows about an existing resource besides
// its bytes. There is NO Controller field: control is a full-rights entry in R,
// not a separate owner column. There is NO Scope field — and never will be:
// actor-scoped objects live in a SEPARATE storage locus (an actor_state-shaped
// table, keyed by owner), so scope is expressed by the STRUCTURE an object
// lives in, not by a column (the Unix calibration: an anonymous mapping is not
// a file tagged "anonymous", it simply is not in the fs namespace). This
// Registry and its table hold only channel-scoped objects.
type ResourceMeta struct {
	Kind      ResourceKind
	CreatedAt int64
}

// Registry is the R (authorization relation) + resource-existence contract —
// the object-lifecycle truth the door consults and mutates. One per channel
// (access is channel-封). Implemented by runtime/internal/store.
type Registry interface {
	// Resolve reports whether id exists and its meta. This is the door's
	// authoritative RESOLVE stage; it never asks the driver.
	Resolve(ctx context.Context, id resource.ResourceID) (ResourceMeta, bool, error)

	// Create is op=create's ATOMIC birth event: the existence row + the
	// creator's full-rights grant (an actor entry, ops = read/write/set/delete)
	// + the initial bytes, all in ONE transaction. The atomicity is a
	// door-visible contract, not an implementation coincidence: create is the
	// single event that spans existence, R, and bytes, so splitting it would
	// open a half-built window (a visible row with no grant / no bytes). A
	// colliding id returns ErrAlreadyExists. The byte realizer participates as
	// store-internal collaboration (day-1: same DB, same transaction, free); a
	// future external-byte driver orders its own internals as "bytes first,
	// existence row last", leaving at worst an invisible orphan byte, never a
	// visible half-built resource (Resolve only sees the existence row).
	Create(ctx context.Context, id resource.ResourceID, kind ResourceKind, creator actor.ActorID, initial []byte) error

	// ActorAllows is the actor-entry half of R.allows for OBJECT ops: whether
	// caller's direct actor entry grants op. members late-binding is NOT here —
	// that is the door's job: the door unions this with MembersAllow gated by a
	// membership check, resolved at check time (grant.go: "resolved by the door
	// AT CHECK TIME").
	ActorAllows(ctx context.Context, caller actor.ActorID, id resource.ResourceID, op access.Operation) (bool, error)

	// MembersAllow reports whether a members-kind entry on id grants op. It does
	// NOT look at caller: whether caller is a current member is decided by the
	// door's membership check, and the two halves are unioned at the door
	// (allow-only, no precedence).
	MembersAllow(ctx context.Context, id resource.ResourceID, op access.Operation) (bool, error)

	// SetGrant implements op=set: REPLACE the grantee's entry with g (chmod/
	// setfacl SET semantics; g.Ops == ∅ REVOKES = deletes the row). g has
	// already passed the door's ingress ValidateGrant, so the Registry trusts
	// the caller and only stores (mirrors storespec's store-not-validate
	// discipline). The entry key is (id, g.GranteeKind, g.Grantee) — the sum
	// form persisted in full.
	SetGrant(ctx context.Context, id resource.ResourceID, g access.Grant) error

	// Delete removes the resource row + ALL its grants in one transaction.
	// Non-lossy is guaranteed by the door only reaching here after Allows
	// passes. The byte half belongs to Driver.Delete, which the door orders
	// before this ("bytes first, existence row last"): a mid-flight failure
	// leaves a "resolved but empty" state (legal) — delete is idempotent and
	// retryable, needing no cross-call atomicity (only create does, for its
	// controller-grab window).
	Delete(ctx context.Context, id resource.ResourceID) error
}
