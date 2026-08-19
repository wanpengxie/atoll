package actor

// ActorID is the channel-local sender identifier. It is identical to
// envelope `sender.id` — same namespace.
//
// Inserted members use three colon-free segments: <kind>:<seed>:<timestamp>.
// The channel-local system actor is the sole exception: it is the kernel
// constant "system" and is never inserted into the member registry.
//
// Cross-channel uniqueness is NOT guaranteed; mapping a real-world user
// to multiple channels is out of scope for the channel-local registry.
type ActorID string

// Well-known fixed actor ids.
const (
	// SystemActorID is the channel-local system actor. Every channel has
	// exactly one as a kernel constant; it is not a member-registry row.
	SystemActorID ActorID = "system"
)

// String returns the wire form of the actor id.
func (a ActorID) String() string { return string(a) }
