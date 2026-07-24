// Package presence composes actor existence and advisory testimony without
// teaching observation vocabulary to the runtime broker.
//
// The view is a total four-cell state space over active membership and live
// incarnation. Testimony modifies an existing actor's answer but never creates
// existence, never trains substrate liveness, and remains advisory: authoritative
// reachability is still send to terminal outcome.
package presence
