// Package framework declares the device-adapter shared utilities that
// every via_server_transit binding adapter (the M1.5 "device" adapter
// family — first concrete one is adapters/device/xhs) builds on top of.
//
// Three concerns live here, each in its own file:
//
//   - session.go   DeviceSession state machine + local-replica SessionStore
//                  interface (T1.10 lifecycle pending → ready → active →
//                  offline → expired | revoked). server.devicebus owns the
//                  authoritative row; daemon adapter holds the mirror copy
//                  per L4 §2.6.2 (covers codex 必修 #5 — single ownership).
//
//   - token.go     HMAC token issue + parse. server.devicebus signs the
//                  token; daemon adapter never holds the plain text — only
//                  a short fingerprint for log / audit. Per T1.10 ("server
//                  在 user 登录后签发 …") + T1.3 control.bind_device_session
//                  payload.token_fingerprint.
//
//   - proxy.go     DeviceProxy bundles kernel/adapter.DeviceTransit +
//                  CorrelationTracker + ErrorPolicy so a Module.Handle
//                  body shrinks to "compose wire payload → proxy.Send".
//                  Implements the daemon-side half of the T1.3 §2.6.4
//                  device_transit.send / .recv / .ack / .error frame set.
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
//   - .dalek/pm/m1.5-tickets.md §T5      (device transit 完整链路)
//   - .dalek/pm/m1.5-tickets.md §T1.3    (device callback + frame field set)
//   - .dalek/pm/m1.5-tickets.md §T1.10   (device session lifecycle state machine)
//   - .dalek/pm/v4-layer4-spec.md §2.6   (device-not-actor invariant)
//   - .dalek/pm/v4-layer1-spec.md §11.7  (binding tri-class — via_server_transit)
package framework
