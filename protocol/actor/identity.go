package actor

// ActorID is the channel-local sender identifier. It is identical to
// envelope `sender.id` — same namespace.
//
// Naming convention reference (informative — non-normative):
//
//	user:<short-id>      - human members
//	system               - the channel-local system actor (fixed id)
//	agent:<role-name>    - channel agents / sub-agents
//	tool:<tool-name>     - tool actors
//
// Cross-channel uniqueness is NOT guaranteed; mapping a real-world user
// to multiple channels is out of scope for the channel-local registry.
type ActorID string

// Well-known fixed actor ids.
const (
	// SystemActorID is the channel-local system actor — every channel
	// has exactly one, seeded at channel genesis.
	SystemActorID ActorID = "system"
)

// String returns the wire form of the actor id.
func (a ActorID) String() string { return string(a) }
