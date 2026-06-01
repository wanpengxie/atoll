// Package actorrt is the actor-runtime substrate: it gives each actor a
// long-lived object identity (a cell with a private struct, a single
// goroutine, and a bounded mailbox) and the four substrate guarantees of
// actor-runtime-redesign.md §1.1:
//
//  1. identity + addressability — sending to an ActorID reaches it;
//  2. private sequential delivery — one actor's messages are processed
//     one-at-a-time by its own goroutine, so the actor can hold mutable
//     state WITHOUT locks or atomics (the core "gift");
//  3. lifecycle boundary — Start acquires resources, Stop releases them;
//  4. isolation — nobody reaches into an actor's state; they only send
//     messages.
//
// This package REPLACES runtime/scheduler.Deliverer's lock-free, stateless
// "concurrent handler" model. An actor is no longer a HandlerFn registered
// in a map; it is an object instance owned by exactly one cell goroutine.
//
// closure is NOT in this package: per actor-runtime-redesign.md §0.5 the
// closure timer/pending-set lives in the sender actor (caller-scoped), and
// the only substrate obligation on closure is the death signal (a cell
// supervisor observing its child panic — see Supervisor — or a relay
// disconnect observed by the adapter actor), which materialises a
// receiver_unavailable terminal. There is no global closure scanner here.
package actorrt
