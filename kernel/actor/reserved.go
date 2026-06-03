package actor

// Reserved-type vocabulary closed sets.
//
// These are protocol-reserved `envelope.type` names the substrate
// FUNCTIONALLY enforces (the system.* events below: per-name authority — only
// the channel system actor may emit them, harness-gated to prevent forgery).
// Changing a value or adding/removing a member is a protocol-level revision.
//
// NOTE: the actor.* introspection queries (actor.describe/list) are NOT
// here. The substrate does not enforce them — the generic harness
// sender-consistency step already prevents an actor from forging an answer
// about another actor, and actor.* is otherwise a plain (type-agnostic) type.
// They are a stdlib self-answer CONVENTION, owned by lib/introspect (the names,
// the response shapes, and the answering behaviour), not kernel.

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
