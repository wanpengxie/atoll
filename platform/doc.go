// Package platform is the cross-host membrane: the shared word table both
// physical hosts — the channel home (platform/home) and the attached compute
// (platform/compute) — read and write against, so a def/decl built for one
// admission path is the SAME shape the other consumes. It is not an assembly
// package itself any longer (platform-topology 批 T5b moved both concrete
// hosts out into their own importable packages); it is the thin membrane
// each host's own construction seam welds a raw capability against.
//
// # What lives here
//
//   - ActorDecl (decl.go) — the one decl-family word BOTH admission paths
//     read (registry.Constructor's return shape: id + kind + factory triple).
//     Everything else in the decl family (Builder / LocalFileOpener /
//     StorageHost / the storage mirror types) is spoken by compute alone and
//     lives in platform/compute/decl.go — a decl-family word stays on this
//     root ONLY when both hosts actually read it (B′ judgement, decl.go's own
//     header comment).
//   - ActorFactory (actorfactory.go) — a type alias onto
//     platform/internal/hostcommon.ActorFactory, the one def shape every
//     out-generation entry point (activation, fork, daemon build) speaks on
//     BOTH hosts. The concrete representation + the shared Build/OutcomeString
//     helpers live in platform/internal/hostcommon (T5a); the root only
//     re-exports the name downstream code imports.
//   - PlanActor (plan.go) — the authenticated link-plan DTO shared by the home
//     provider and compute sink, including the declaration version and canonical
//     declaration metadata both hosts compare.
//
// # Root-file topology
//
// Every non-test .go file directly under platform/ (sub-packages excluded —
// platform/home, platform/compute, platform/subjectgate, and
// platform/internal/* are packages of their own, not root files) falls into
// this closed set: doc.go, decl.go, actorfactory.go, plan.go. archtest's root-
// classification anchor enforces this as a closed set — a new root file
// turns that tripwire red; the root does not grow by accretion, only by a
// spec decision that a new word is genuinely cross-host truth.
//
// # The four jurisdictions
//
// platform/home is the channel-home assembly root (server side — one
// channel's truth and execution wiring: Open/View/Admit/Restart/
// ServeAttach/Subscribe/Close + the subjectgate slot seam). platform/compute
// is the attached-compute assembly root (daemon side — one attached
// process's own reconcile ring: Run/Config, dialing in, reattaching,
// building/reopening streams). platform/subjectgate is the per-identity
// binding-slot protocol both the gateway and a subject's own frame-driven
// actions speak through. platform/internal/* is everything neither host's
// public capability set names — hostcommon (the shared Build/OutcomeString +
// ActorFactory representation), link (the wire membrane), humancell (the
// human actor's frame interpreter), and the other assembly-private
// wiring no downstream package may reach around the two hosts' own faces.
package platform
