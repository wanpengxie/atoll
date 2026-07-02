// Package timerspec is the kernel-only leaf that declares the durable
// pending-timer CONTRACT — the interface runtime/internal/store implements
// over sqlite for the IDENTITY-level half of the time axis (forward §7),
// dual to resourcespec on the object plane and storespec on the message
// plane.
//
// Why a dedicated leaf (mirrors resourcespec/storespec): this contract sits
// between the schedule engine (runtime/schedule, the consumer) and the store
// (the implementation). A kernel-only leaf keeps the contract independent of
// both — the engine imports the seam, never the implementation. The set is
// closed-but-additive and substrate-owned: the leaf is importable ONLY within
// the runtime tree, so the implementation set cannot be extended by
// downstream code, and — more load-bearing here than for resourcespec — a
// raw TimerStore reachable downstream would let anyone insert a row with a
// forged author_id: a delayed forged-sender write path around the pen (build
// spec 红线❻). That confinement is enforced at the compile layer (archtest),
// not by convention.
//
// timerspec imports ONLY kernel (pure types) + context/errors. It holds ONLY
// the durable half of the time axis: identity-bind pending intent, keyed by
// a durable name (author identity). Incarnation-bind timers are NOT here and
// never will be — they are the schedule engine's in-memory due-set, welded
// to the live embodiment, vanishing with the process (v1.1 历史校准: BEAM
// in-VM timers / Orleans in-activation Timers / POSIX timers on
// task_struct — ephemeral intent lives in ephemeral memory, never gets a
// durable account). So this leaf has no Bind field and no Bind type at all —
// Bind is a routing choice that lives in runtime/schedule, not a persisted
// tag (§3.1 钉3).
package timerspec
