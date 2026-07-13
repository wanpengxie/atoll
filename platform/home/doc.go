// Package home is the channel-home assembly: it puts the position-blind
// logical world (substrate) into the positioned physical world for ONE
// channel. Home is the assembly root — it owns truth and embodiment wiring
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
// # File map
//
// home.go (types + checkpoint), open.go (Open), reconcile.go (activation
// reconcile ring + sweep), census.go (Admit/PrincipalOf/ResolvePrincipal),
// control.go (CancelRequest/KickDaemon/ServeAttach/Subscribe), close.go
// (Close), view.go (View), remove.go (Remove), expiry.go (deadline-closure
// reaper), scheduler.go (revive backoff + fire sink + reviver),
// storagehost.go (home-side routing half of the daemon storage host),
// humancell_wiring.go (home-side wiring shell for human embodiment — factory
// + Proc seam over platform/internal/humancell), testing.go (black-box
// fixture seam over Admit), caps.go (buildCaps — the single home-side
// five-capability assembler), sysanchorcaps.go (the system anchor's
// late-bound Schedule/Spawn arms), spawnhandle.go (spawnHandle —
// Fork/Despawn woven over the caps assembler). ActorFactory (the def shape
// buildCaps assembles against) is platform.ActorFactory (platform-topology
// 批 T5b: home consumes the cross-host membrane's word, never defines its
// own).
package home
