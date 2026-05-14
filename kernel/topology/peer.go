package topology

// Peer is the M2+ federation peer-server placeholder per
// .dalek/pm/m1.5-tickets.md §T10. M1.5 has no peers — this type exists
// so federation-aware control-plane code can be written today against
// a stable shape without rewriting call sites when the real federation
// transport lands.
//
// Pure value type — safe to copy, compare with ==, use as map key.
//
// Semantics (informative — non-normative for M1.5):
//
//   - Node MUST have Kind == NodePeerServer in any production
//     federation deployment; M1.5 callers leave the entire struct at
//     its zero value.
//   - Origin is the future federation peer origin (URL / region tag /
//     mTLS identity). The exact format is defined by the federation
//     control-plane spec — left as a plain string here to avoid
//     committing to a wire format pre-spec.
type Peer struct {
	Node   Node
	Origin string // M2+ federation peer origin; "" in M1.5
}

// IsZero reports whether p is the zero-value placeholder. Convenience
// helper for callers that conditionally enter the federation path.
func (p Peer) IsZero() bool {
	return p == Peer{}
}
