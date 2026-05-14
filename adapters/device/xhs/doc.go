// Package xhs is the first concrete v5 device adapter — it owns the
// `tool:xhs-adapter` actor on every channel that carries an xhs binding
// and translates kind=request envelopes (`xhs.publish`, `xhs.search`,
// `xhs.note.fetch`, `xhs.recent.fetch`, `xhs.cookie.sync`) into device
// commands carried by the via_server_transit binding (L1 §11.7) +
// device_transit frame set (T1.3 / L4 §2.6.4).
//
// File layout (each file holds one concern):
//
//   - proto.go      wire schema (Command, Callback, type constants,
//     per-type allow-list helpers).
//   - handlers.go   per-type request encoder + callback decoder built
//     on top of proto.go. The R4-FIX-A per-type
//     allow-list is preserved (T115 regression guard —
//     the schema lesson of the M1.3 baseline).
//   - module.go     Module struct implementing kernel/adapter.Module
//     with BindingViaServerTransit; uses
//     adapters/device/framework.DeviceProxy for the
//     correlate + send + arm-timer trio.
//   - install.go    InstallSpec helper consumed by cmd/daemon (T7) to
//     seed actor_registry (`tool:xhs-adapter`) +
//     type_registry (5 R/R + 1 event row,
//     handler_binding=via_server_transit).
//
// Boundary discipline (go-arch-lint T2):
//
//   - adapters/device/xhs imports only kernel/* + pkg/* + standard
//     library + the sibling adapters/device/framework.
//   - The envelope sender on every emitted response equals
//     adapters/device/xhs.DefaultAdapterActorID. device_id /
//     device_session_id stay inside the payload (L4 §2.6 — device is
//     NOT an actor).
//
// Authoritative spec references:
//
//   - .dalek/pm/m1.5-tickets.md §T5      (device transit 完整链路)
//   - .dalek/pm/m1.5-tickets.md §T1.3    (device_transit frame field set)
//   - .dalek/pm/v4-layer4-spec.md §2.6   (device-not-actor invariant)
//   - .dalek/pm/v4-layer4-spec.md §2.2   (xhs response schemas — per-type
//     allow-list source)
package xhs
