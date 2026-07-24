// Package schedule is the time axis's engine + caps access surface: the
// substrate mechanism that lets an actor say "wake ME at FireAt with this
// message" — the missing half of the two things substrate time can do
// today (it can only kill via ExpiresAt; it could never wake).
//
// timer/pending is NOT a plane-2 (accessdoor) object: it is not a passive
// byte payload some caller reads/writes, it is substrate's own future
// intent to act. So this package is not a "door" (accessdoor's mint/decision-
// tree shape) — it is an ENGINE that runs a continuous poll/wake loop
// (tap.Pump structural twin) alongside its caps mint face. Two lifecycle
// levels share the one engine and one fire path:
//
//   - bind=identity: durable intent, keyed by author identity, stored in
//     timerspec.TimerStore (runtime/internal/store's timers table) — survives
//     restarts, cleared on deregister.
//   - bind=incarnation: intent welded to the CURRENT incarnation, kept
//     ONLY in this engine's memory (never a store row, never serialised) —
//     dies with that incarnation, vanishes with the process (matching the
//     precedent of BEAM in-VM timers / Orleans in-activation Timers / POSIX
//     timers on task_struct: ephemeral intent lives in ephemeral memory).
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
// runtime root's job (runtime.OpenScheduler) — not this package's.
package schedule
