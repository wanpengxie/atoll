package actor

// Kind is the actor physical-position classifier:
// the 4-value closed set human / agent / system / tool.
type Kind string

const (
	KindHuman  Kind = "human"
	KindAgent  Kind = "agent"
	KindSystem Kind = "system"
	KindTool   Kind = "tool"
)

// allKinds backs ParseKind. UNEXPORTED: a closed set's invariant is that it
// cannot be extended, so the substrate must not hand callers a mutable slice of
// it — the public contract is the ParseKind predicate, not the enumeration.
var allKinds = []Kind{KindHuman, KindAgent, KindSystem, KindTool}

// String returns the wire form.
func (k Kind) String() string { return string(k) }

// ParseKind resolves a canonical wire-form actor-kind string against the closed
// set. The closed set is ENFORCED here, not just documented: any code
// deserializing a kind from the wire or DB MUST go through ParseKind rather than
// a bare actor.Kind(string) cast, so an out-of-set value can never enter the ADT.
func ParseKind(raw string) (Kind, bool) {
	for _, k := range allKinds {
		if string(k) == raw {
			return k, true
		}
	}
	return "", false
}
