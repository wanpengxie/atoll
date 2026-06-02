// Package actorrt is the actor-runtime substrate: it gives each actor a
// long-lived object identity (a cell with a private struct, a single goroutine,
// and a bounded envelope mailbox) and four substrate guarantees:
//
//  1. identity + addressability — sending to an ActorID reaches its current
//     cell. Identity is single-level: a stable ActorID names at most one live
//     cell at a time (Spawn replaces; death is terminal — no transparent
//     same-id respawn). Death and replacement are checked by pointer identity,
//     so a dying predecessor can never evict its successor.
//  2. private sequential delivery — one actor's messages are processed
//     one-at-a-time by its own goroutine, so the actor can hold mutable state
//     WITHOUT locks or atomics (the core "gift").
//  3. lifecycle boundary — Start acquires resources, Stop releases them; on
//     death a cell self-evicts from the addressing map (it never stops/joins
//     itself) and signals the supervisor.
//  4. isolation — nobody reaches into an actor's state. The mailbox carries
//     ONLY envelopes; there is no path to run caller-supplied code on the cell
//     goroutine. An out-of-band signal is a message an actor sends itself
//     (ActorContext.Deliver); a synchronous request-reply is a message pair,
//     not a closure.
//
// Deliver reports a structured, per-audience Outcome (Delivered/NotHosted/
// MailboxFull/Stopped): the substrate knows whether it hosts an addressed actor
// and reports that truthfully so the seam can fast-fail rather than wait for a
// timeout.
//
// closure is NOT in this package: the closure timer/pending-set lives in the
// sender actor (caller-scoped), and the only substrate obligation on closure is
// the death signal (a Supervisor observing its child die), which materialises a
// receiver_unavailable terminal. There is no global closure scanner here.
package actorrt
