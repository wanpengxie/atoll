package access

import (
	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/resource"
)

// Invocation is one access — a subject invokes one lifecycle/access operation on one
// object with operands (§2.2). Off-log and transient (§2.5): NO id/ts/seq (not truth),
// NO channel_id (connection/door-scoped), NO audience/visibility (one object, one
// subject), NO parent_id/correlation (single-shot, no closure). Authorization is two-locus
// (§2.4): object ops (read/write/set/delete) via R.allows(caller, resource, op), create via
// channel membership (no object/R exists yet) — by-identity day-1; NO presented-cap field
// (§11-deferred).
type Invocation struct {
	// Caller — the subject, door-welded and NEVER self-reported (isolation/ocap, same
	// principle as pen's Sender / emit's p.id). Transport-neutral welding: in a CELL the
	// handle is welded to the actor-id at construction; over a PORT the home side welds
	// the connection's authenticated id. A NON-EMPTY caller on the wire is REJECTED (fail-fast,
	// exactly like pen rejecting a pre-filled Sender — NOT silently overwritten); the wire MUST
	// leave caller empty, the home stamps it. Authority comes from the binding, never the wire
	// payload. The caller-side handle is a PROXY only: it forwards the Invocation and receives
	// the verdict; it holds no authorization state, makes no local R decision, caches nothing
	// authoritative — authz is decided at the resource's home (§2.4.1 enforcement invariant).
	Caller actor.ActorID `json:"caller"`

	// Resource — the object (the access-control unit). Cardinality 1. Granularity is the
	// driver's model (§3.1); the kernel never interprets it.
	Resource resource.ResourceID `json:"resource"`

	// Operation — the lifecycle/access verb (§3.2).
	Operation Operation `json:"operation"`

	// Args — the DRIVER-OPAQUE operand for create/read/write (create's initial bytes, write's
	// value, read's selector); delete and set have NO Args operand (delete is by-id, set's
	// operand is the typed Grant field below). RAW []byte (字节 opaque), NOT json.RawMessage
	// (which validates as JSON on marshal and cannot carry a binary secret/file). NO omitempty:
	// an empty value (empty-file write, empty sign input) is legal and must survive — nil = no
	// operand (null on wire); []byte{} = present-but-empty ("" on wire). WIRE FORM pinned:
	// base64 JSON string, or null for nil (Go encoding/json's []byte encoding) — fixed by
	// invocation_test, not left to implicit behavior.
	Args []byte `json:"args"`

	// Grant — the SUBSTRATE-TYPED operand, populated ONLY for op=set (nil otherwise): the
	// grantee's new grant the substrate authz manager writes to R (§2.4; SET semantics, ∅
	// ops = revoke). Typed (not opaque Args) because the substrate authoritatively manages it
	// and both wire ends must agree on its shape (envelope/payload rule + 认证判准). This is
	// the one place Invocation's operand is proto-typed rather than driver-opaque — the
	// envelope/payload split applied per operand kind. omitempty: pointer presence (nil vs
	// present) is the intended absent/present signal, unlike Args where omitempty would
	// conflate empty-value with no-operand.
	Grant *Grant `json:"grant,omitempty"`
}

// contentFields backs the wire-contract drift guard (the bytes both ends agree on must
// not silently drift) — same DISCIPLINE as message.contentFields, not the same field list.
//
// Unexported: an exported mutable slice would let an importer rewrite the protocol field
// set. The invocation_test.go drift guard keeps the Go struct 1:1 with it; drift on either
// side trips the test. Unlike envelope's flattened sender.{kind,id}, grant stays a single
// top-level key here — Grant is an omitempty optional operand with its own grant_test.go
// guarding its internal shape; contentFields treats grant as one opaque top-level key.
var contentFields = []string{"caller", "resource", "operation", "args", "grant"}
