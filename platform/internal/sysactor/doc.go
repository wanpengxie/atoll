// Package sysactor is the channel's control-plane door: the single AUTHORIZED
// entry by which the actor/logical world observes (and, in time, operates) the
// channel's own physical substrate. It is an INWARD projection / syscall door
// (NOT a foreign-world adapter): it reflects the physical world (platform:
// compute nodes / link / presence / resources) into the message universe so the
// logical universe closes over its own physical body. Other actors do NOT import
// it — they reach it by sending a message to the well-known actor.SystemActorID,
// exactly as userspace traps into the kernel by syscall number. Full design:
// .dalek/pm/sysactor-design.md.
//
// WHY platform/internal (ring 0): the door needs platform-internal capability
// that ordinary actors must NOT hold. Living in lib would give it the same
// import surface as every actor — zero structural privilege, and a privileged
// capability could be placed in any actor. Under platform/internal an ordinary
// lib/domain actor cannot import this package or its platform-internal handles;
// privilege exclusivity is thus STRUCTURAL (the internal/ boundary = Go's
// compile-time ring boundary), while addressing stays open (internal blocks
// import, not message routing). Authorization is enforced AT the door by policy:
// holding the public SystemActorID lets you knock, NOT pass — having the syscall
// number is not having the capability.
//
// Discipline: sysactor does ONLY door + authz/policy (thin). The heavy physical
// logic (assembly, wiring, actuation) lives in the platform assembly root
// (Home.Open); sysactor CALLS platform capabilities, it does not IMPLEMENT them.
// It keeps NO copy of physical state: it READS presence via an injected seam and
// composes on-read, exactly as it reads membership from the Registry.
//
// What it does today: the channel's PRESENCE projection. It answers the
// channel-wide directory query (actor.list) as a composed, on-read view
// (membership ∧ presence, the formula owned by introspect.QueryList). It is
// ADVISORY — never a dispatch gate (P15/P16): reachability authority is
// send→terminal, and the dispatch path never reads this actor's view. It runs as
// a channel-intrinsic cell, spawned once per channel at creation. Operating the
// physical body (scale/budget) is the same door's future additive surface, not
// yet built.
//
// Two-axis model (runtime/storespec.Record): membership is durable registry
// truth; PRESENCE is volatile and AUTHORITY-OWNED (the component holding the
// compute leases/connections). Presence is never a message and never truth — a
// volatile state read. readiness is NOT a third axis — whether an actor can
// service a request is the OUTCOME of send→terminal, not a stored state the
// system actor projects or composes.
package sysactor
