package actor

// Kind is the actor physical-position classifier (L0 §2.3 normative):
// the 4-value closed set human / agent / system / tool.
type Kind string

const (
	KindHuman  Kind = "human"
	KindAgent  Kind = "agent"
	KindSystem Kind = "system"
	KindTool   Kind = "tool"
)

// AllKinds enumerates every valid actor kind value, in spec order.
var AllKinds = []Kind{KindHuman, KindAgent, KindSystem, KindTool}
