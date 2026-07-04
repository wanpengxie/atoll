// Package actorbase is the L1 syscall face of the "actor is a process"
// abstraction (.dalek/pm/actorbase-spec-v1.md §1-§2): the vocabulary a Proc
// body speaks to the substrate underneath it, and nothing else.
//
// This slice (S1) delivers only the WORDS — Sys's verb table, Msg's
// projection, Proc/Def's registration shape, and the Serve routing sugar. The
// engine that actually pumps mailboxes, keeps the two in-flight ledgers, and
// mints a live Sys sits in a later slice (S2); nothing here reaches for it
// early. Consequently this package imports substrate leaf vocabulary
// (protocol/*, runtime/accessdoor, runtime/schedule, runtime/actorrt) but
// never runtime/harness.Minter-adjacent assembly, never lib/actorcaps (the
// caps→Sys weld is S2's assembly seam, actorbase.New), and never
// platform/agent (a Proc body must be nameable without importing the platform
// assembly root, mirroring lib/actorcaps's own placement rationale).
package actorbase
