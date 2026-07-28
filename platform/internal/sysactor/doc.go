// Package sysactor is the channel's control-plane door: the single AUTHORIZED
// entry by which the actor/logical world observes (and, in time, operates) the
// channel's own physical substrate. It is an INWARD projection / syscall door
// (NOT a foreign-world adapter): it reflects the physical world (platform:
// compute nodes / link / liveness / resources) into the message universe so the
// logical universe closes over its own physical body. Other actors do NOT import
// it — they reach it by sending a message to the well-known actor.SystemActorID,
// exactly as userspace traps into the kernel by syscall number.
//
// WHY platform/internal (ring 0): the door needs platform-internal capability
// that ordinary actors must NOT hold. Living in lib would give it the same
// import surface as every actor — zero structural privilege, and a privileged
// capability could be placed in any actor. Under platform/internal an ordinary
// lib/domain actor cannot import this package or its platform-internal handles;
// privilege exclusivity is thus STRUCTURAL (the internal/ boundary = Go's
// compile-time ring boundary), while addressing stays open (internal blocks
// import, not message routing). BY DESIGN the door is where authorization
// belongs (gated by policy): holding the public SystemActorID lets you knock,
// NOT pass — a syscall number is not a capability. That policy gate is a FUTURE
// surface; TODAY this actor is advisory with no authz gate (see "What it does
// today" below).
//
// Discipline: sysactor does ONLY door + authz/policy (thin). The heavy physical
// logic (assembly, wiring, actuation) lives in the platform assembly root
// (Home.Open); sysactor CALLS platform capabilities, it does not IMPLEMENT them.
// It keeps NO copy of physical state: it READS liveness via an injected seam and
// composes on-read, exactly as it reads membership from the Registry.
//
// Shape (actorbase-spec-v1 §3's out-generation matrix): sysactor is a ring0
// SPECIAL Proc — a lib/actorbase Proc whose Caps are hand-built raw by the
// platform assembly root (no live/incarnation membrane on any arm — the
// anchor posture the system pen already wears, authority itself sets no gate
// on itself) rather than welded through buildCaps. Caps has FIVE arms, and the
// system bundle fills three of them: systemcaps.Minter.Mint (runtime/systemcaps/
// minter.go) is one synchronous call returning Pen, Access and Schedule, each
// minted against the same root authority, all three assembled before the mint —
// nothing here is late-bound or captured through a closure. State is a refusing
// stub (unsupportedState) and Lifecycle is nil: the system cell forks nothing
// and ends nothing, and lib/actorbase reads that nil as what it is, an honest
// capability absence. It still enters through the SAME actorbase.New seam every
// other actor does. Its privilege is entirely in WHERE its Caps come from
// (platform itself is the authority, not a minted membrane) and in wearing no
// incarnation gate.
//
// What it does today: the channel's LIVENESS projection. It answers the
// channel-wide directory query (actor.list) as a composed, on-read view
// (membership ∧ liveness, the formula owned by introspect.QueryList). It is
// ADVISORY — never a dispatch gate: reachability authority is
// send→terminal, and the dispatch path never reads this actor's view. It runs as
// a channel-intrinsic cell, spawned once per channel at creation. Operating the
// physical body (scale/budget) is the same door's future additive surface, not
// yet built.
//
// Three-axis model (runtime/storespec.Record): membership is durable registry
// truth; LIVENESS is volatile and AUTHORITY-OWNED (the component holding the
// compute leases/connections) — never a message, never truth, a volatile state
// read. DEVICE PRESENCE is a third, separate axis: an ADVISORY three-state
// signal (device_presence events → DevicePresence fold) about an external
// device's own reachability, orthogonal to whether the actor itself has an
// incarnation — it never gates liveness and liveness never answers it. readiness
// is NOT a fourth axis — whether an actor can service a request is the OUTCOME
// of send→terminal, not a stored state the system actor projects or composes.
package sysactor
