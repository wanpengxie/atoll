// Package platform is the channel-home and attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical world.
// Home is the channel-home assembly root — it owns truth and embodiment wiring
// for one channel and delivers a narrow capability set (not an organ bag):
//
//	Open(cfg) → *Home
//	View()  View              — read-only observation set (ReadAfterSeq/MaxSeq/ListActors/Stat/Snapshot/IsAttached)
//	Admit(ctx,kind,principal) — mint/idempotently resolve active membership
//	EnsureSubjectSlot(id) / SubjectSlotFor(id) / RemoveSubjectSlot(id) — the per-identity subjectgate binding slot seam the gateway drives (a subject's own Submit/Resolve/Cancel/After actions arrive as wire frames onto their cell through the slot; welded pen stays in the wall)
//	Restart(ctx,id)           — accepted-unconfirmed embodiment replacement request
//	ServeAttach(w,r,daemonID) — attach acceptance surface (app hands an upgraded WS here)
//	Subscribe() (<-chan struct{}, func()) — subscription registration surface (client push)
//	Close() error
//
// Everything else (runtime, deliverer, membership, registry, the harness Minter)
// is internal wiring — Home holds the Minter and Mints a welded Pen at each
// admission point (activation / attach / system closure); a bare writer and the Minter
// itself never escape Home. Post-commit effects are tap subscribers, not inline
// writer steps: cell delivery
// is a Pump over the commit Signal (backed by the Deliverer, DeliverResult observed here),
// client push is the Signal directly. Centralised multi-tenant is a factory over Open, not a
// second Home shape.
//
// # Root-file topology
//
// Every non-test .go file directly under platform/ (sub-packages excluded —
// platform/subjectgate and platform/internal/* are packages of their own, not
// root files) falls into exactly one of three classes. archtest's root-
// classification anchor enforces this as a closed set: a new root file that
// lands in none of the three below turns that tripwire red.
//
//   - Channel host (the *Home assembly, server side — one channel's truth and
//     embodiment wiring): home.go (types + checkpoint), open.go (Open),
//     reconcile.go (activation reconcile ring + sweep), census.go
//     (Admit/PrincipalOf/ResolvePrincipal), control.go
//     (CancelRequest/KickDaemon/ServeAttach/Subscribe), close.go (Close),
//     view.go (View), remove.go (Remove), expiry.go (deadline-closure reaper),
//     scheduler.go (revive backoff + fire sink + reviver), storagehost.go
//     (home-side routing half of the daemon storage host), humancell_wiring.go
//     (home-side wiring shell for human embodiment — factory + Proc seam over
//     platform/internal/humancell), testing.go (black-box fixture seam over
//     Admit).
//
//   - Compute host (the RunCompute assembly, daemon side — one attached-
//     compute process's own reconcile ring): compute.go (RunCompute +
//     ComputeConfig), ring.go (computeRing — dial/reattach/spawn/dispatch/
//     cancel + redial backoff), compute_forwarders.go (cellDownWatcher +
//     obs/cancel/storage-host forwarders), decl.go (ActorDecl/ComputeBuilder/
//     StorageHost declaration surface both hosts share).
//
//   - Membrane (the caps weld between the position-blind logical world and the
//     positioned physical world — where a raw minted handle is wrapped in its
//     live membrane and handed out, never before, never again downstream):
//     caps.go (buildCaps — the single home-side five-capability assembler),
//     sysanchorcaps.go (the system anchor's late-bound Schedule/Spawn arms),
//     actorfactory.go (ActorFactory — the one def shape every out-generation
//     entry point speaks), spawnhandle.go (spawnHandle — Fork/Despawn woven
//     over the caps assembler).
package platform
