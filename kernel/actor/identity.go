// Package actor defines the L0 channel-actor identity model: ActorID,
// actor Kind, Binding, and the reserved-type closed sets. Pure proto — no
// context, no storage, no interfaces.
package actor

// ActorID is the channel-local sender identifier. It is identical to
// envelope `sender.id` (L0 §2.1 — same namespace, see L1 §3.2).
//
// Naming convention reference (informative — non-normative; L1 §3.2):
//
//	user:<short-id>      - human members
//	system               - the channel-local system actor (fixed id)
//	agent:<role-name>    - channel agents / sub-agents
//	tool:<adapter-name>  - tool/adapter actors (embedded /
//	                       runtime_outbound / runtime_inbound_via_relay per
//	                       L1 §11.7)
//
// Cross-channel uniqueness is NOT guaranteed; mapping a real-world user
// to multiple channels is out of scope for the channel-local registry
// (L1 §12.5).
type ActorID string

// Well-known fixed actor ids per L1 §3.2.
const (
	// SystemActorID is the channel-local system actor — every channel
	// has exactly one, seeded at channel genesis.
	SystemActorID ActorID = "system"
)

// String returns the wire form of the actor id.
func (a ActorID) String() string { return string(a) }
