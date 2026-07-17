package accessdoor

import (
	"context"

	"github.com/wanpengxie/atoll/protocol/access"
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
	"github.com/wanpengxie/atoll/runtime/storespec"
)

// objectOps is the closed set of R-governed object verbs — the ops any
// Grant.Ops structurally ranges over (create is container-locus, never an R
// grant; ingress.ValidateGrant enforces the identical set on the wire side).
// effectiveOps ranges its per-op union computation over exactly this set.
var objectOps = []access.Operation{access.OpRead, access.OpWrite, access.OpSet, access.OpDelete}

// effectiveOps computes caller's UNION of grantable rights on an EXISTING
// resource — the owner-root extension of the A8 formula: for each object op,
// IsOwner(caller) ∪ ActorAllows(caller) ∪ (MembersAllow ∧ IsMember(caller)). Door-internal only:
// it never crosses the wire and never appears in a public signature. THREE
// loci share this ONE formula (期11 spec §2 item 2 — all three wired):
//   - the set arm's escalation check (door.go): set(X, ops) requires
//     ops ⊆ effectiveOps(caller);
//   - Stat's echoed ops (query.go stat — calls effectiveOps directly);
//   - List's per-row projection (query.go list — effectiveOpsFromGrants, the
//     same formula computed over the grant rows List already fetched).
//
// Computing the union differently in even one of the three would let a
// departed member's residual members-row rights leak through that ONE path
// while the other two correctly deny it — the spec's explicit regression
// ("actor 移出频道后 members 权利立即不再计入，三处同断言") is really a
// demand that there be exactly one formula, not three copies of it.
//
// The caller's registry row is resolved EXACTLY ONCE per call, eagerly at
// entry: the owner-role check needs it before any per-op work (owner ⇒ full
// ops, short-circuit), and the same lookup then serves the IsMember leg of
// the members-arm — membership doesn't change between the per-op checks
// within one effectiveOps call, so a single resolve covers both judgments.
func (d *door) effectiveOps(ctx context.Context, caller actor.ActorID, id resource.ResourceID) (map[access.Operation]bool, error) {
	eff := make(map[access.Operation]bool, len(objectOps))
	row, isMember, err := d.deps.Authority.LookupActive(ctx, caller)
	if err != nil {
		return nil, err
	}
	if isMember && row.Role == storespec.RoleOwner {
		for _, op := range objectOps {
			eff[op] = true
		}
		return eff, nil
	}

	for _, op := range objectOps {
		allowed, err := d.deps.Registry.ActorAllows(ctx, caller, id, op)
		if err != nil {
			return nil, err
		}
		if !allowed {
			allowed, err = d.deps.Overlay.ActorAllows(ctx, caller, id, op)
			if err != nil {
				return nil, err
			}
		}
		if !allowed {
			mAllow, err := d.deps.Registry.MembersAllow(ctx, id, op)
			if err != nil {
				return nil, err
			}
			if mAllow {
				allowed = isMember
			}
		}
		eff[op] = allowed
	}
	return eff, nil
}
