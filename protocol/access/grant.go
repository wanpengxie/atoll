package access

import "github.com/wanpengxie/ActOS/protocol/actor"

// GranteeKind is the CLOSED SET of grantee principal classes a Grant can name
// (§2.4). Day-1 two kinds; growing it is a protocol revision (like adding an
// Operation verb). `group` (arbitrary named sets, a group registry) is the one
// KNOWN additive extension — deferred, zero pre-reservation (§9): it lands as a
// third kind value + a second set source at the door, with this wire shape and
// the by-identity path unchanged.
type GranteeKind string

const (
	// GranteeActor — a single actor IDENTITY (Grantee field required). Grants
	// bind at identity, not incarnation: they survive the grantee's restarts
	// (like a Unix uid). A deregistered grantee's entry goes INERT, not scrubbed
	// (the door welds Caller from connection auth, and a dead identity cannot
	// authenticate — dangling grants are unexercisable dead weight, tolerated
	// like POSIX orphan uids / Windows dangling SIDs). Whether a later
	// re-INSERT of the same ActorID is "the same subject" is admission NAMING
	// governance (§11 A6 cluster), not R's concern: same name = same subject
	// by definition; guard the name assignment, not the grants.
	GranteeActor GranteeKind = "actor"

	// GranteeMembers — the container channel's CURRENT members, resolved by the
	// door AT CHECK TIME (Grantee field empty; the set needs no id — it IS the
	// container's membership, which the substrate authoritatively maintains and
	// the door already consults for op=create's container locus). This is the
	// indirection + late-binding shape every calibrated system converges on
	// (POSIX group/other, K8s RBAC groups, Zanzibar usersets): membership
	// changes propagate automatically — joiners gain, leavers lose, no stale
	// snapshot enumeration, and deregister needs no R cleanup for these
	// entries. It is the plane-2 dual of plane-1's audience broadcast. NOT
	// ambient authority: a members entry exists on an object only because a
	// set-right holder explicitly op=set it (member⟹create-own stays benign,
	// no member⟹access-others).
	GranteeMembers GranteeKind = "members"
)

var allGranteeKinds = []GranteeKind{GranteeActor, GranteeMembers}

func (k GranteeKind) String() string { return string(k) }

// ParseGranteeKind gates a wire/dispatch string against the closed set (no bare cast).
func ParseGranteeKind(raw string) (GranteeKind, bool) {
	for _, k := range allGranteeKinds {
		if string(k) == raw {
			return k, true
		}
	}
	return "", false
}

// Grant is the operand of OpSet — the grantee's NEW grant the substrate AUTHZ MANAGER
// writes into the object's authorization relation R (§2.4), chmod/setfacl SET semantics:
// it REPLACES the grantee's grant, and Ops=∅ revokes. It is PROTO (a typed Invocation.Grant
// field, not opaque Args) because the substrate authoritatively manages it AND it crosses
// the wire as a contract both ends must agree on (§0.1 认证判准 + envelope/payload rule:
// substrate-managed → typed, driver-interpreted → opaque payload).
//
// Shape rule (enforced at the door's ingress step, runtime — not a proto method,
// same discipline as Invocation's op×field rules §3.4):
// GranteeKind ∈ closed set; GranteeKind=actor ⟺ Grantee non-empty;
// GranteeKind=members ⟺ Grantee empty.
type Grant struct {
	// GranteeKind — which principal class this grant names (closed set above).
	GranteeKind GranteeKind `json:"grantee_kind"`

	// Grantee — the subject whose grant is being set when GranteeKind=actor: an
	// actor IDENTITY, not incarnation. Grants survive the grantee's restarts
	// (like a Unix uid) — this is where the object's single level (§1.2) meets
	// the subject's two levels: authorization binds at IDENTITY. Empty (and
	// omitted on the wire) when GranteeKind=members.
	Grantee actor.ActorID `json:"grantee,omitempty"`

	// Ops — the grantee's new granted operations (∅ = revoke). STRUCTURALLY ⊆ OBJECT-OPS
	// {read,write,set,delete} — NEVER create (create is container-locus, never an R grant, §2.4).
	// DAY-1 further narrowed to {read,write} by runtime ValidateGrant (granting set/delete =
	// delegating control = §11; that §11 widening goes toward {read,write,set,delete}, still NOT
	// create). This is also why transfer is a §11 Ops-policy widening, not a new op:
	// set(Y,full) + set(self,∅). Rights across multiple matching entries UNION
	// (R is allow-only, no deny entries — so actor-entry vs members-entry needs
	// no precedence rule).
	Ops []Operation `json:"ops"`
}
