// Package framework declares the device-adapter shared utilities that
// every runtime_inbound_via_relay binding adapter (the launch "device" adapter
// family — first concrete one is adapters/device/xhs) builds on top of.
//
// Two concerns live here, each in its own file:
//
//   - session.go   DeviceSession state machine + local-replica SessionStore
//     interface (T1.10 lifecycle pending → ready → active →
//     offline → expired | revoked). server.devicebus owns the
//     authoritative row; daemon adapter holds the mirror copy
//     per L4 §2.6.2 (covers codex 必修 #5 — single ownership).
//
//   - proxy.go     DeviceProxy bundles kernel/devicetransit.DeviceTransit +
//     CorrelationTracker + ErrorPolicy so a Module.Handle
//     body shrinks to "compose wire payload → proxy.Send".
//     Implements the daemon-side half of the impl-layer2 §5.3
//     device_transit.{send, recv, ack, error} frame set
//     (send = §5.3.1 device → adapter inbound; recv = §5.3.2
//     adapter → device outbound).
//
// Device bearer tokens are intentionally absent from this package. The
// server/devicebus package issues random opaque tokens, stores only
// HMAC(raw_token) at rest, and sends the daemon mirror only a short
// token_fingerprint for log / audit. Adapter code must treat the plain token
// as a server/device-only value and must not mint or parse it locally.
//
// Boundary discipline (go-arch-lint T2):
//
//   - framework depends ONLY on kernel/* + pkg/* + standard library +
//     allowed vendors (uuid). No imports of runtime/, server/, adapters/
//     siblings.
//   - All concrete IO (sqlite SessionStore impl, real DeviceTransit, real
//     CorrelationTracker, real ErrorPolicy) is wired by cmd/daemon in T7
//     against the kernel interfaces; framework holds the seams only.
//
// Authoritative spec references:
//
//   - launch-ticket notes §T5      (device transit 完整链路)
//   - launch-ticket notes §T1.3    (device callback + frame field set)
//   - launch-ticket notes §T1.10   (device session lifecycle state machine)
//   - .dalek/pm/domain-xhs-spec.md §2.6  (device-not-actor invariant)
//   - .dalek/pm/proto-layer1.md §11.7    (binding tri-class — runtime_inbound_via_relay)
package framework
