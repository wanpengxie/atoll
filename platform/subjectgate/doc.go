// Package subjectgate holds the platform-internal machinery of the human
//接入轴 (gateway 期 S2): the per-identity binding registry + slot, and the wire
// frame contract the connector/gateway speak to a subject's home-side driver.
//
// It is platform内政 (the concrete lives here; platform re-exports the wire
// types the drivers/gateway伞包 needs — the Frame family — as its own exported
// surface, so a driver never imports platform/internal). Two structures live
// here:
//
//   - Frame family (frame.go): the逐帧字段表 wire正本 (build spec §S2). Pure
//     DTOs over protocol scalars + string + json.RawMessage (裁决3): no
//     schedule/accessdoor type ever crosses the wire.
//   - Binding registry + per-identity Slot (slot.go): the four-tuple
//     {绑定世代, gateway epoch, 帧递交端, presence level} with the独立性不变式
//     (level's ONLY writer is a layer-3 testimony update — never co-written with
//     a layer-2 rebind) and the边沿交付模型 (observers registered by incarnation
//     token, updates delivered as {epoch, edgeSeq, level}, same-epoch strictly
//     increasing dedup, new-epoch revoke-then-snapshot).
package subjectgate
