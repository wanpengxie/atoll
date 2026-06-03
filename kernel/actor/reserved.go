package actor

// Reserved-type vocabulary closed sets.
//
// These are protocol-reserved `envelope.type` names that the substrate
// treats specially. They were previously scattered as string literals
// across runtime/lib; consolidated here as frozen closed sets so callers
// reference constants, not bare strings (target-state §3.7 / §4.2,
// topology §8.7). Changing a value or adding/removing a member is a
// protocol-level revision (proto-*), not an impl-layer edit.

// ReservedActorTypeSet is the set of reserved `kind=request` types that
// any actor self-answers about itself (the reserved-type self-answer
// surface — observation of an actor's identity / capabilities / state
// without a bespoke endpoint). INVARIANT-0 read side.
const (
	// ReservedActorStatus — query one actor's own live/readiness status
	// ("ask the actor itself"; advisory).
	ReservedActorStatus = "actor.status"
	// ReservedActorDescribe — query one actor's static declaration /
	// capability surface.
	ReservedActorDescribe = "actor.describe"
	// ReservedActorList — channel-wide actor catalog (composed view,
	// answered by the channel system actor / sysactor).
	ReservedActorList = "actor.list"
)

// (No exported enumeration slice: the reserved-type closed sets are the
// constants above. A mutable []string of them is a redundant second
// representation that invites range-and-mutate; any consumer needing the set
// builds it from the constants, or a Parse predicate is added when a real
// substrate validation use-case demands one.)

// ReservedSystemEventTypeSet is the set of reserved `kind=event` types
// the channel system actor emits to mirror control-plane mutations into
// the message log (INVARIANT-0 write side / observation-bounded truth).
// These are channel envelopes (they enter the log); the wire-only control
// frame `system.heartbeat` is intentionally NOT here (it is a transport
// control frame, never a channel envelope — see core_types.go).
const (
	ReservedSystemChannelCreated    = "system.channel.created"
	ReservedSystemActorRegistered   = "system.actor.registered"
	ReservedSystemActorDeregistered = "system.actor.deregistered"
	ReservedSystemConfigUpdated     = "system.config.updated"
)

// NOTE: there are NO system.type.installed/deprecated events. A "type" is not a
// substrate/truth first-class entity — it is an actor's method, discovered live
// via the actor's own actor.describe self-answer (capability is the actor's
// volatile state, not truth). Membership (actor.registered/deregistered) is the
// channel-level truth; capability-change history, if ever needed, is an
// observability concern, not the truth log.
