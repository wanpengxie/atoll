package accessdoor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// objectOps is the closed set of R-governed object verbs — the ops any
// Grant.Ops structurally ranges over (create is container-locus, never an R
// grant; ingress.ValidateGrant enforces the identical set on the wire side).
// effectiveOps ranges its per-op union computation over exactly this set.
var objectOps = []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete}

// opAllowed is THE formula in its single-op form: the non-owner half of
// IsOwner(caller) ∪ ActorAllows(caller) ∪ (MembersAllow ∧ IsMember(caller)),
// asked for ONE op on ONE resource. Every locus that judges rights by ASKING
// THE REGISTRY goes through here — there is no second registry-side copy:
//   - the invoke gate (door.go): the base authorization of every
//     read/write/set/delete, asked for exactly the op that call carries;
//   - effectiveOps below: the same predicate ranged over objectOps.
//
// The owner root deliberately stays at each caller rather than being folded in
// here: it short-circuits BEFORE any per-op work (owner ⇒ full ops, no registry
// round trip at all), and both callers already hold the facts it reads.
//
// isMember is a parameter, not a lookup: each caller resolves the caller's
// registry row EXACTLY ONCE per call, and membership does not change between
// the per-op checks within one of them.
//
// Computing this union differently in even one locus would let a departed
// member's residual members-row rights leak through that ONE path while the
// others correctly deny it — the spec's explicit regression ("actor 移出频道后
// members 权利立即不再计入，三处同断言") is really a demand that there be
// exactly one formula, not a copy per caller.
func (d *door) opAllowed(
	ctx context.Context,
	caller actor.ActorID,
	id resource.ResourceID,
	op access.Operation,
	isMember bool,
) (bool, error) {
	allowed, err := d.deps.Registry.ActorAllows(ctx, caller, id, op)
	if err != nil || allowed {
		return allowed, err
	}
	mAllow, err := d.deps.Registry.MembersAllow(ctx, id, op)
	if err != nil {
		return false, err
	}
	return mAllow && isMember, nil
}

// effectiveOps computes caller's UNION of grantable rights on an EXISTING
// resource, by ranging opAllowed over objectOps. Door-internal only: it never
// crosses the wire and never appears in a public signature. Two loci consume
// the whole set (期11 spec §2 item 2):
//   - the set arm's escalation check (door.go): set(X, ops) requires
//     ops ⊆ effectiveOps(caller);
//   - Stat's echoed ops (query.go stat — calls effectiveOps directly).
//
// List does NOT come through here. It computes the SAME union over the grant
// rows it has ALREADY fetched (effectiveOpsFromGrants, query.go) — a different
// DATA SOURCE, not a second formula: re-asking the registry per op per row
// would cost a page of N resources N×(ActorAllows+MembersAllow) round trips.
//
// The caller's registry row is resolved EXACTLY ONCE per call, eagerly at
// entry: the owner-root check needs it before any per-op work (owner ⇒ full
// ops, short-circuit), and the same lookup then serves the IsMember leg —
// membership doesn't change between the per-op checks within one call.
func (d *door) effectiveOps(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (map[access.Operation]bool, error) {
	eff := make(map[access.Operation]bool, len(objectOps))
	facts, err := d.deps.Authority.ResourceActorFacts(ctx, caller)
	if err != nil {
		return nil, err
	}
	if facts.Active && facts.Owner {
		for _, op := range objectOps {
			eff[op] = true
		}
		return eff, nil
	}

	for _, op := range objectOps {
		allowed, err := d.opAllowed(ctx, caller, id, op, facts.Active)
		if err != nil {
			return nil, err
		}
		eff[op] = allowed
	}
	return eff, nil
}
