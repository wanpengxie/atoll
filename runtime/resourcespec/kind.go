package resourcespec

// ResourceKind is runtime-owned, NOT proto (protocol/resource/id.go keeps
// kind/controller/grants OUT of the pure ResourceID: they are lifecycle
// runtime state, not a cross-boundary name). Unlike actor.Kind — a permanent
// closed set of four — ResourceKind is a SEMI-CLOSED set that grows by pain,
// one driver at a time; a new value = a new substrate driver (kernel change +
// review), not a protocol revision, and NOT an open extension point for domain
// code.
//
// The naming axis is the DOOR-BACK IMPLEMENTATION VARIANT (mechanical: byte
// size / at-rest encryption / storage locus), NOT the use case. kind is a
// durable persisted value (the resources.kind column), so the implementation
// name never lies about the bytes behind it; use case lives in an id naming
// convention instead (the Unix /etc pattern). The kind axis belongs ONLY to
// the channel-scoped locus (this table): actor-scoped state has NO kind — it
// lives in a structurally separate locus (StateStore / the actor_state table,
// keyed by owner), where scope is expressed by structure and day-1 has a
// single mechanical shape, so no kind column exists there to route.
//
// 期11 (resource 轴完备化) closes the day-1 set at exactly {KindKV, KindFile}
// — file lands additively on this axis (a value + a, so far daemon-side-only,
// driver) precisely because its mechanical difference (daemon-local bytes vs
// inline bytes) is now real; secret remains future additive when ITS
// mechanical difference (at-rest encryption) becomes real.
type ResourceKind string

const (
	// KindKV is day-1's inline driver: channel-scoped, small bytes, plaintext,
	// stored directly in the resources row.
	//
	// Value size (S6 account item ⑥ — 申报, no new enforcement code): kv sets
	// NO cap of its own. A daemon-hosted caller's value nonetheless cannot
	// exceed runtime/ipc.MaxFrameBytes (16 MiB) — it rides the wire inside an
	// `access` ipc frame (protocol/access.Invocation.Args), and frame encode/
	// decode already fail-fast past that bound (runtime/ipc/frame.go). A
	// home-hosted (in-proc) caller has no such wire hop and so is bounded
	// only by process memory. The "cap" is therefore DERIVED from the ipc
	// transport's own limit, not an independent kv-layer constant — this
	// comment is that derivation's one authoritative statement; a future
	// explicit kv value cap (if one is ever needed for a reason other than
	// the wire) is additive, not a correction of this one.
	KindKV ResourceKind = "kv"

	// KindFile is the daemon-local file driver (期11 §1): bytes live on one
	// daemon's physical disk (ResourceMeta.PlacementKind=PlacementDaemonLocal),
	// addressed by an opaque placement_coord the owning daemon's Streamer
	// interprets — never inline in the resources row (its bytes column stays
	// NULL for this kind, same "resolved but not stored here" shape as an
	// empty kv row, different reason). The byte-realizing Driver (Allocator/
	// Streamer, §4, a LATER daemon-runtime addition) is not required for this
	// kind's closed-set membership or ingress validation to exist — a kind can
	// be a legal name before anything creates one, the same way a protocol
	// Operation can be parsed before every executor exists.
	KindFile ResourceKind = "file"
)

// allResourceKinds backs ValidKind. UNEXPORTED for the same reason
// access.allOperations is: the closed-set contract is the predicate below,
// not a mutable slice an importer could rewrite.
var allResourceKinds = []ResourceKind{KindKV, KindFile}

// ValidKind reports whether k is a member of the day-1 closed set
// {KindKV, KindFile}. This is the set's OWN membership test — the ingress
// gate that REJECTS an out-of-set CreateSpec.Kind before it reaches the
// door's decision tree lives at the door layer (§3), the same split
// access.ParseOperation draws between "what the set is" (a protocol/runtime
// leaf) and "where it is enforced" (the door's ingress step).
func ValidKind(k ResourceKind) bool {
	for _, want := range allResourceKinds {
		if k == want {
			return true
		}
	}
	return false
}
