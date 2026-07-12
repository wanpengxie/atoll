// Package channelkit is the channel template sitting on top of runtime/actorrt.
// It ASSEMBLES a channel's intrinsic cells (the system actor) and SUBSCRIBES to the
// substrate's obs down edge: on a unit's death (the down edge)
// it materialises the receiver_unavailable closure. It is a MECHANISM watcher,
// not a Supervisor and not an actor — there is no supervision tree; death is
// obs, and the closure reaction is the only domain duty here. Domain
// coordination is the system actor's job.
//
// Reconcile is the level-triggered backstop for the same closure: the death
// edge is a lossy fast path (a missed or predating edge leaves an orphan open
// request), so Reconcile scans for receivers that are CLOSED FOREVER
// (deregistered / never a member — the injected monotone predicate) and closes
// them directly — idempotent against the terminal-uniqueness index, driven by
// the composition root on a ticker, not by any edge. A receiver merely absent
// from liveness is still a member: it is left to the deadline reaper, never
// closed on a reversible liveness dip.
package channelkit
