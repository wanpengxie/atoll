// Package schedule is the time axis's engine + caps access surface: the
// substrate mechanism that lets an actor say "wake ME at FireAt with this
// message" — the missing half of the two things substrate time can do
// today (it can only kill via ExpiresAt; it could never wake).
//
// timer/pending is NOT a plane-2 (accessdoor) object: it is not a passive
// byte payload some caller reads/writes, it is substrate's own future
// intent to act. So this package is not a "door" (accessdoor's mint/decision-
// tree shape) — it is an ENGINE that runs a continuous poll/wake loop
// (tap.Pump structural twin) alongside its caps mint face. Two Scheduler
// storage homes share the one engine and one fire path:
//
//   - home=durable: intent keyed by author identity, stored in
//     timerspec.TimerStore (runtime/internal/store's timers table) — survives
//     Scheduler restarts, cleared when its ActorID ends.
//   - home=memory: intent kept only in this Channel/Scheduler instance's
//     memory (never a store row, never serialised) and vanishes with that
//     instance.
//
// Both homes are ActorID-owned and cross actor AttemptKey/Incarnation
// replacement. The home is a storage choice, not an actor lifecycle axis.
//
// Firing does exactly one thing: append one envelope AS the row's author,
// through the caller-injected FireSink (the harness pen, mirrored) — the
// engine never touches delivery (append→pump→fanout already carries any
// committed row to its mailbox). FireSink is therefore the package's one
// non-ambient seam: it is the only place that can mint a message under an
// author the caller did not supply, so ScheduleHandle/Minter weld the
// SCHEDULING author (self-targeted — ScheduleReq has no target field,
// structurally), and the fired message's author is always the row's own
// author_id, never chosen at fire time.
//
// The package exports the caps surface (ScheduleHandle, Minter) and the
// engine (Engine) that drives it; New returns both, never a bare handle to
// the internal due-set. Assembly (wiring a real TimerStore/FireSink/
// Authority/Clock, and the engine's Start/Close lifecycle) is the
// Platform composition root's job — not this package's.
package schedule
