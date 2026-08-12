package resourcespec

// ResourceKind is runtime-owned, NOT proto (protocol/resource/id.go keeps
// kind/controller/grants OUT of the pure ResourceID: they are lifecycle
// runtime state, not a cross-boundary name). Unlike actor.Kind — a permanent
// closed set of four — ResourceKind is a SEMI-CLOSED set that grows by pain,
// one driver at a time; a new value = a new substrate driver (kernel change +
// review), not a protocol revision, and NOT an open extension point for domain
// code.
//
// Kind selects the create call's byte locus. Only KindKV is persisted in the
// resources table; KindFile is routed directly from its daemon URI.
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

	// KindFile routes an address straight to the named daemon's channel
	// directory. It never creates a resources row.
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
