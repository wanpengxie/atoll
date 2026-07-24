// Package actorrt provides the atomic runtime for one local actor
// incarnation.
//
// A Unit owns one actor implementation, one bounded mailbox, and one
// serializing goroutine. Prepare allocates the exact Unit/Self shell before
// calling the builder; Start admits work; Stop requests teardown; Done closes
// only after the actor's cleanup and every Unit-owned organ have returned.
//
// The package deliberately has no ActorID registry, current selection,
// replacement policy, placement, wire binding, retry loop, aggregate Close, or
// leak registry. runtime/actorhost owns execution-domain desired-to-actual
// convergence and runtime/actorctl owns channel-scoped logical lifecycle.
//
// Unit events always carry the exact Unit and Incarnation. They are wake hints
// to an external owner; they never authorize this package to locate or mutate a
// same-ID successor.
package actorrt
