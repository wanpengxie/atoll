// Package platform is the channel-home and attached-compute assembly: it puts the
// position-blind logical world (substrate) into the positioned physical world.
// home.go is the channel-home assembly root — it owns truth and presence wiring
// for one channel and delivers a narrow capability set (not an organ bag):
//
//	Open(cfg) → *Home
//	Gate()  harness.Writer    — the commit write门 (the harness chain; commit signal fires at the store append chokepoint)
//	View()  View              — read-only observation set (ReadAfterSeq/MaxSeq/ListActors)
//	Spawn(ctx,id,kind,impl)   — in-process cell安置 (membership + spawn)
//	Links() *link.Acceptor    — attach受理面 (app hands an upgraded WS here)
//	Taps()  *tap.Signal       — subscription注册面 (client push)
//	Close() error
//
// Everything else (runtime, deliverer, membership, registry) is internal wiring.
// Post-commit effects are tap subscribers, not inline writer steps: cell delivery
// is a Pump over the commit Signal (持 Deliverer, DeliverResult observed here),
// client push is the Signal directly. Centralised multi-tenant = Open的工厂化, not a
// second Home shape.
package platform
