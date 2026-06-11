// Package xhs is the first concrete v5 device adapter — it owns the
// `tool:xhs` actor on every channel that carries an xhs binding
// and translates kind=request envelopes (`xhs.publish`, `xhs.search`,
// `xhs.note.fetch`, `xhs.recent.fetch`, `xhs.cookie.sync`) into device
// commands carried by the runtime_inbound_via_relay binding (L1 §11.7) +
// device_transit frame set (T1.3 / L4 §2.6.4).
//
// File layout (each file holds one concern):
//
//   - proto.go        wire schema (Command, Callback, type constants,
//     per-type allow-list helpers).
//   - device_type.go  per-type request encoder + callback decoder built
//     on top of proto.go.
//   - describe.go     TypeMeta / FieldDoc / ErrorDoc for actor describe
//     responses (discovery = actor self-answer).
//   - template.go     xhs-creator channel template (ChannelType +
//     CreatorTemplate + domain prompt).
//
// Boundary discipline (go-arch-lint T2):
//
//   - actors/xhs imports only protocol/actor + lib/behavior + standard
//     library. No server, no platform internals.
//   - Device connection identity stays in the daemon-side local-device
//     bridge (an additive件, reintroduced with the concrete device actors —
//     platform-redesign §8 拍板项 4); payloads carry only business
//     parameters (L4 §2.6 — device is NOT an actor).
//
// Authoritative spec references:
//
//   - launch-ticket notes §T5      (device transit 完整链路)
//   - launch-ticket notes §T1.3    (device_transit frame field set)
//   - .dalek/pm/domain-xhs-spec.md §2.6  (device-not-actor invariant)
//   - .dalek/pm/domain-xhs-spec.md §2.2  (xhs response schemas — per-type
//     allow-list source)
package xhs
