// Package platform is the channel-home and attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical world.
// home.go is the channel-home assembly root — it owns truth and embodiment wiring
// for one channel and delivers a narrow capability set (not an organ bag):
//
//	Open(cfg) → *Home
//	View()  View              — read-only observation set (ReadAfterSeq/MaxSeq/ListActors/Stat/DevicePresence/IsAttached)
//	Admit(ctx,id,kind)        — pure-membership admission (neutral row; embodiment left to the ring/attach)
//	Human(ctx,id) → HumanHandle — subjectgate door面 (a subject's Submit/Resolve/Cancel/After verbs; welded pen stays in the wall)
//	Spawn(ctx,id,kind,factory) — in-process cell placement (membership + Mint welded Pen + spawn)
//	ServeAttach(w,r,daemonID) — attach acceptance surface (app hands an upgraded WS here)
//	Subscribe() (<-chan struct{}, func()) — subscription registration surface (client push)
//	Close() error
//
// Everything else (runtime, deliverer, membership, registry, the harness Minter)
// is internal wiring — Home holds the Minter and Mints a welded Pen at each
// admission point (Spawn / attach / system closure); a bare writer and the Minter
// itself never escape Home. Post-commit effects are tap subscribers, not inline
// writer steps: cell delivery
// is a Pump over the commit Signal (backed by the Deliverer, DeliverResult observed here),
// client push is the Signal directly. Centralised multi-tenant is a factory over Open, not a
// second Home shape.
package platform
