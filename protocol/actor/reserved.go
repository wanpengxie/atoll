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
// They are a self-answer CONVENTION owned by a higher layer (the names, the
// response shapes, and the answering behaviour), not kernel.

// The reserved `kind=event` type names below are the frozen group the channel
// system actor emits to mirror channel membership/config mutations into the
// message log (INVARIANT-0 write side / observation-bounded truth). These are
// WORK events (kind=event, they enter the log as truth) — NOT the actor control
// channel (reload/quota/stop signals, which are non-truth). The wire-only
// `system.heartbeat` keepalive is intentionally NOT here (it is a transport
// keepalive frame, never a channel envelope).
//
// Deliberately NO membership predicate (no IsReservedSystemEventType) and no
// backing slice — UNLIKE the Kind/Binding/Visibility ADT closed sets. Those
// gate a wire string AS IT deserializes into a narrowed Go type, so the Parse*
// predicate is part of ADT integrity. `envelope.type` is the opposite: an OPEN
// string the substrate stays type-AGNOSTIC about, never narrowed into an ADT —
// there is no deserialization gate to own. kernel only OWNS these canonical
// names (only the substrate can authoritatively define its own mirror-event
// vocabulary); deciding whether a given type is reserved AND whether its sender
// is authorized to emit it is the write engine's job (in the harness), never a
// kernel concern. These are plain string consts by design, not a set type.
const (
	ReservedSystemChannelCreated    = "system.channel.created"
	ReservedSystemActorRegistered   = "system.actor.registered"
	ReservedSystemActorDeregistered = "system.actor.deregistered"
)

// NOTE: there is NO system.config.updated event. The substrate has no
// channel-level config as a first-class concept: a config surface is only
// admissible as one complete vertical slice — the state, its guardian (who
// may mutate it, harness-enforced), and the mirror event together. Vocabulary
// without the guardian would be an unguarded mutation surface on the channel.
// If rule-managed channel config ever becomes substrate-essential, the whole
// slice comes back additively (protocol revision).

// NOTE: there are NO system.type.installed/deprecated events. A "type" is not a
// substrate/truth first-class entity — it is an actor's method, discovered live
// via the actor's own actor.describe self-answer (capability is the actor's
// volatile state, not truth). Membership (actor.registered/deregistered) is the
// channel-level truth; capability-change history, if ever needed, is an
// observability concern, not the truth log.
