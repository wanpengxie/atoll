package access

import "github.com/wanpengxie/ActOS/protocol/actor"

// Grant is the operand of OpSet — the grantee's NEW grant the substrate AUTHZ MANAGER
// writes into the object's authorization relation R (§2.4), chmod/setfacl SET semantics:
// it REPLACES the grantee's grant, and Ops=∅ revokes. It is PROTO (a typed Invocation.Grant
// field, not opaque Args) because the substrate authoritatively manages it AND it crosses
// the wire as a contract both ends must agree on (§0.1 认证判准 + envelope/payload rule:
// substrate-managed → typed, driver-interpreted → opaque payload).
type Grant struct {
	// Grantee — the subject whose grant is being set: an actor IDENTITY, not incarnation.
	// Grants survive the grantee's restarts (like a Unix uid) — this is where the object's
	// single level (§1.2) meets the subject's two levels: authorization binds at IDENTITY.
	Grantee actor.ActorID `json:"grantee"`

	// Ops — the grantee's new granted operations (∅ = revoke). STRUCTURALLY ⊆ OBJECT-OPS
	// {read,write,set,delete} — NEVER create (create is container-locus, never an R grant, §2.4).
	// DAY-1 further narrowed to {read,write} by runtime ValidateGrant (granting set/delete =
	// delegating control = §11; that §11 widening goes toward {read,write,set,delete}, still NOT
	// create). This is also why transfer is a §11 Ops-policy widening, not a new op:
	// set(Y,full) + set(self,∅).
	Ops []Operation `json:"ops"`
}
