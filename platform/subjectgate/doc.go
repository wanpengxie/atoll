// Package subjectgate holds the platform-internal machinery of the human
// 接入轴 (gateway 期 S2): the per-identity slot registry (在场与递交接头盒), and the
// wire frame contract the connector/gateway speak to a subject's home-side driver.
//
// It is platform内政 (the concrete lives here; platform re-exports the wire
// types the drivers/gateway伞包 needs — the Frame family — as its own exported
// surface, so a driver never imports platform/internal). Two structures live
// here:
//
//   - Frame family (frame.go): the逐帧字段表 wire正本 (build spec §S2 / 连接模型
//     勘误期 v2). Pure DTOs over protocol scalars + string + json.RawMessage
//     (裁决3): no schedule/accessdoor type ever crosses the wire. attach is
//     channel-blind (a游标表 handoff), business frames carry a required
//     channel_id, and there is no client-visible binding_gen.
//   - Slot registry + per-identity Slot (slot.go): the presence-and-delivery
//     接头盒 — the layer-3 presence register (the gateway's device-aggregate
//     在场证词) + the帧递交端 (synchronous frame delivery into the cell's
//     interpreter). The边沿交付模型: observers registered by incarnation token,
//     updates delivered as {epoch, edgeSeq, level} with a slot-minted monotonic
//     edgeSeq, new-epoch revoke-then-snapshot, the current value delivered as the
//     observer's first callback (出生握手), PublishCurrent idempotent补发, and
//     ForgetEpoch conditional (CAS) 清账. (The client-visible binding-generation
//     axis was整删 with the假轴 it guarded — 连接模型勘误期.)
package subjectgate
