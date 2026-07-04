// Package actorbase is the L1 syscall face of the "actor is a process"
// abstraction (.dalek/pm/actorbase-spec-v1.md §1-§2): the vocabulary a Proc
// body speaks to the substrate underneath it, and nothing else.
//
// S1 delivered only the WORDS — Sys's verb table, Msg's projection, Proc/
// Def's registration shape, and the Serve routing sugar. S2 (this package's
// engine.go/ledger_*.go/hooks.go) adds the engine that actually pumps
// mailboxes, keeps the two in-flight ledgers (serve/call), and mints a live
// Sys. This package imports substrate leaf vocabulary (protocol/*,
// runtime/accessdoor, runtime/schedule, runtime/actorrt, runtime/harness.Pen)
// and lib/behavior (the envelope-building primitives the engine's Reply/Fail/
// Call reuse rather than re-implement), but never runtime/harness.Minter-
// adjacent assembly and never platform/agent (a Proc body must be nameable
// without importing the platform assembly root). lib/actorcaps is imported
// in exactly one place, engine.go's New — the caps→Sys weld IS S2's assembly
// seam (spec §3), never reached for anywhere else in this package.
package actorbase
