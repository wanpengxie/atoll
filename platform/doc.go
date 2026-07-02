// Package platform is the channel-home and attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical world.
// home.go is the channel-home assembly root — it owns truth and embodiment wiring
// for one channel and delivers a narrow capability set (not an organ bag):
//
//	Open(cfg) → *Home
//	View()  View              — read-only observation set (ReadAfterSeq/MaxSeq/ListActors)
//	Spawn(ctx,id,kind,factory) — in-process cell placement (membership + Mint welded Pen + spawn); factory==nil = cell-less member
//	ServeAttach(w,r,daemonID) — attach受理面 (app hands an upgraded WS here)
//	Subscribe() (<-chan struct{}, func()) — subscription注册面 (client push)
//	Close() error
//
// Everything else (runtime, deliverer, membership, registry, the harness Minter)
// is internal wiring — Home holds the Minter and Mints a welded Pen at each
// admission point (Spawn / attach / system closure); a bare writer and the Minter
// itself never escape Home. Post-commit effects are tap subscribers, not inline
// writer steps: cell delivery
// is a Pump over the commit Signal (持 Deliverer, DeliverResult observed here),
// client push is the Signal directly. Centralised multi-tenant = Open的工厂化, not a
// second Home shape.
package platform
