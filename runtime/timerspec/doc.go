// Package timerspec is the kernel-only leaf that declares the durable
// pending-timer CONTRACT — the interface runtime/internal/store implements
// over sqlite for the Durable Scheduler home, dual to
// resourcespec on the object plane and storespec on the message plane.
//
// Why a dedicated leaf (mirrors resourcespec/storespec): this contract sits
// between the schedule engine (runtime/schedule, the consumer) and the store
// (the implementation). A kernel-only leaf keeps the contract independent of
// both — the engine imports the seam, never the implementation. The set is
// closed-but-additive and substrate-owned: the leaf is importable ONLY within
// the runtime tree, so the implementation set cannot be extended by
// downstream code, and — more load-bearing here than for resourcespec — a
// raw TimerStore reachable downstream would let anyone insert a row with a
// forged author_id: a delayed forged-sender write path around the pen. That
// confinement is enforced at the compile layer (archtest), not by
// convention.
//
// timerspec imports ONLY kernel (pure types) + context/errors. It holds ONLY
// the Durable Scheduler home, keyed by author ActorID. Memory-home timers live
// in the current runtime/schedule Engine instance and vanish with that
// Channel/Scheduler instance. Both homes cross actor replacement; neither is
// an actor AttemptKey/Incarnation binding. The storage home is not persisted
// as a per-row tag: being in this table already means Durable.
package timerspec
