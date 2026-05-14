package actor

// ActorId is the actor naming scalar.
//
// Naming convention (per v4 spec L0 §2.3 + L1 §12 actor_registry):
//
//   - human:<user_id>
//   - agent:<agent_id>
//   - system:<role>
//   - tool:<adapter_name>[/<sub-name>]
//
// The leading SenderKind prefix is required and immutable once
// registered. ActorRegistry validates the prefix matches the registered
// kind.
type ActorId string

// String returns the wire form of a.
func (a ActorId) String() string { return string(a) }
