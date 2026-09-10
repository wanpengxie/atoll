package message

// Cause lives with the envelope, not with the builders, because it IS a
// property of the message: parent_id and correlation_id are envelope fields,
// and this is the one value they are both derived from. Every layer that hands
// work onward — the schedulers and controllers above, the builders below —
// needs to name a cause, and the message is the only place all of them can see.

// Cause answers the one question every envelope must answer before it can be
// built: why does this message exist. The ledger has exactly two answers and
// this type has exactly two constructors, so there is no third.
//
//	Root()      an errand that starts here — a person spoke, a frame crossed
//	            the membrane, a clock went off. Parent is empty and correlation
//	            is the message's own id.
//	From(env)   written because of env — a receiver answering it, or an actor
//	            calling out while serving it. Parent is env, correlation is
//	            ENV'S correlation, not env's id.
//
// Answering and calling-out derive identically; the difference between them is
// kind, not cause.
//
// WHY ONE VALUE INSTEAD OF A PARENT FIELD BESIDE A CORRELATION FIELD: the two
// are not independent — correlation is derived from parent — so two separate
// inputs can disagree. Kept apart they spell four combinations of which only
// two mean anything: "no parent yet belonging to someone's tree" and "has a
// parent yet roots a tree of its own" are both writable and both nonsense. One
// value makes them unwritable.
//
// WHY From TAKES THE ENVELOPE AND NOT ITS ID: the new message's correlation is
// the old message's correlation, and only the envelope carries that. An id
// would force the caller to supply the correlation separately — which is the
// two-field shape this type exists to remove.
//
// THE ZERO VALUE IS ILLEGAL and every builder refuses it. That is the whole
// point. A caller with nothing to say must not be able to spell it as "there is
// no cause": an omission would then read as a claim, and the claim would be
// wrong exactly when it mattered — every message written to serve another one.
type Cause struct {
	parent      ID
	correlation ID
	stated      bool
}

// CorrelationID derives which errand a message belongs to: the tree its cause
// is already in, falling back to the cause's own id when that message headed
// its own tree. It lives beside Cause because Cause is its only real caller —
// one derivation of "which tree", not one per layer that needs to know.
func CorrelationID(chain, rootID ID) ID {
	if chain != "" {
		return chain
	}
	return rootID
}

// Root is the cause of a message that begins an errand rather than continuing
// one. Correlation is the message's own id, which does not exist until the
// builder mints it, so Root carries none and the builder fills it in.
func Root() Cause { return Cause{stated: true} }

// From is the cause of a message written because of env.
func From(env Envelope) Cause {
	return Cause{
		parent:      env.ID,
		correlation: CorrelationID(env.CorrelationID, env.ID),
		stated:      true,
	}
}

// Anchored rebuilds the cause of work that outlives the message that started
// it. An agent turn interrupted by a restart holds its trigger's id and
// correlation on disk, not the envelope — the envelope is gone, the errand is
// not, and the work resuming still belongs to it.
//
// This is the ONLY way to make a Cause without an envelope in hand, and it is
// for restore paths alone. Both halves must come from one message that really
// was on this ledger; inventing a pair here re-opens exactly the two-field
// disagreement this type exists to close.
func Anchored(parent, correlation ID) Cause {
	if parent == "" {
		return Cause{}
	}
	return Cause{parent: parent, correlation: CorrelationID(correlation, parent), stated: true}
}

// Stated reports whether a cause was given. The zero Cause is not a root — it
// is silence, and builders reject it.
func (c Cause) Stated() bool { return c.stated }

// IsRoot reports whether this cause begins a new tree.
func (c Cause) IsRoot() bool { return c.stated && c.parent == "" }

// Resolve answers (parent, correlation) for an envelope whose id has just been
// minted. It is the ONE place the two fields are derived, so the four-way
// disagreement they used to permit cannot be expressed anywhere.
//
// Exporting it does not re-open that door: a Cause can only come from the
// constructors above, so whatever comes out of here is consistent by
// construction. What was removed was two INDEPENDENT inputs, not the ability to
// read what the single input derives to.
func (c Cause) Resolve(id ID) (ID, ID) {
	if c.parent == "" {
		return "", id
	}
	return c.parent, c.correlation
}
