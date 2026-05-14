// Package addressing declares the v5 forward-compatible global address
// types for actors and channels. It is the M1.5 §T10 placeholder layer
// that lets future M1.4 (channel-as-actor) and M2+ (federation /
// SaaS / multi-tenant) work treat any participant as a logical address
// (`ChannelRef` / `ActorRef`) and any transport hop as a `Route` —
// without rewriting kernel/message or kernel/placement.
//
// Authoritative spec: .dalek/pm/m1.5-tickets.md §T10 ("kernel/ 加扩展位
// — 仅接口/类型，无逻辑").
//
// Invariants:
//
//   - Pure types only. No goroutines, no IO, no state.
//   - Demo / M1.5 deployments leave OrgID empty (single-org) and use
//     ActorRef.ChannelRef.OrgID == "" → addressing is functionally
//     equivalent to (channel.ID, actor.ActorID) today.
//   - Federation / channel-as-actor (M1.4) and multi-org / SaaS (M2+)
//     populate the optional fields without changing the L0 envelope:
//     the envelope still carries channel_id + sender.id, while addressing
//     types are used by control-plane / routing code to express logical
//     references.
//   - kernel/addressing depends only on kernel/channel + kernel/actor.
//     No vendor imports beyond stdlib.
package addressing
