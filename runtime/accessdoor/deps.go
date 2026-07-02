package accessdoor

import (
	"context"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/runtime/resourcespec"
)

// MembershipCheck is the door's narrow channel-membership seam. Two loci consult
// it: op=create's container check (member ⟹ create-own) AND the members-grant
// late-binding resolution for object ops (GranteeMembers = "resolved by the door
// AT CHECK TIME"). One narrow method covers both — no snapshot, no stale set.
// The implementor adapts storespec.Registry.Lookup + Record.IsActive (NOT Exists,
// which does not distinguish a deregistered actor).
type MembershipCheck interface {
	IsMember(ctx context.Context, id actor.ActorID) (bool, error)
}

// DriverTable resolves a ResourceKind to its Driver. Closed-but-additive: a plain
// map, one entry per substrate driver, populated at assembly. A kind reaching the
// tree with no entry is an assembly defect (a Go error), never a verdict.
type DriverTable map[resourcespec.ResourceKind]resourcespec.Driver

// Deps bundles the collaborators the door needs. The channel-scoped tree uses the
// Registry (R + existence), the Drivers (bytes per kind), and the Membership seam
// (create locus + members late-binding). The actor-scoped (collapsed) branch uses
// only State — the owner-keyed byte realizer for the second, structurally separate
// storage locus (no R, no membership, no kind routing: that absence is the scope
// law). All four are required — New fail-fasts on any missing.
type Deps struct {
	Registry   resourcespec.Registry
	Drivers    DriverTable
	Membership MembershipCheck
	State      resourcespec.StateStore
}
