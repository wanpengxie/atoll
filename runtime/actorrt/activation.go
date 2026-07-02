package actorrt

import "github.com/wanpengxie/ActOS/protocol/actor"

// StateRestore is the third activation注入点契约 (§3.4 契约③): after
// SpawnIfAbsent mints a fresh incarnation for an existing durable member, this
// hook reads back that member's private persistent state (§6) into it. This
// period (期2) declares the type only — the reconcile loop wiring that calls
// it lives in the NEXT phase (channelkit), and the only implementation that
// phase wires in is a no-op stub (func(actor.ActorID, Incarnation) error {
// return nil }): activation this period serves fresh (stateless) revival only;
// "died with data, came back with the data" is period 3's completion of this
// same seam, not a redefinition of it.
type StateRestore func(id actor.ActorID, inc Incarnation) error
