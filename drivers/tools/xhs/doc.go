// Package xhs is the first concrete device-backed adapter actor — the
// boundary translator between the channel (actor/message world) and a real
// Xiaohongshu browser extension that speaks no actor primitives.
//
// It is the reference adapter (adapter-actor-spec.md): two faces, complexity
// pressed into one stateful actor.
//
//   - Inward (channel face): a plain channel actor. Receive(env) switches on a
//     small closed type set (xhs.publish / xhs.search / xhs.note.fetch /
//     xhs.recent.fetch), answers actor.describe, and emits channel primitives
//     through lib/behavior. No other actor ever learns what a browser
//     extension is.
//
//   - Outward (device face): a PRIVATE WS endpoint owned by this package (the
//     extension connect-ins to it). The wire is the minimal request/response
//     primitive {correlation_id, cmd, params} down / {correlation_id, ok,
//     result|error} up — NOT a channel envelope, NOT any device_transit frame
//     family. correlation_id pairs a reply to its request.
//
// Statefulness (substrate cell hosts one long-lived instance): the adapter
// holds the current device conn + an in-flight table keyed by correlation_id.
// Because the substrate has no self-send, device replies are emitted to the
// channel directly from the read-loop goroutine via the writer; an internal
// mutex guards the conn + in-flight table (the one cross-goroutine state).
//
// Fault posture (let-it-crash): a single request that times out or hits an
// offline device fails as a business response (the actor digests it). The
// actor-owned local maintenance loop collects past-deadline requests. It does
// not depend on the daemon↔Server Schedule arm, so link loss cannot kill the
// incarnation. A dropped conn flips the adapter offline and waits for a fresh
// connection.
//
// v1 scope (additive hardening deferred until the pain is concrete):
//
//   - Device presence is tracked INTERNALLY (an offline device fast-fails
//     device_offline) but NOT projected as a channel event — there is no
//     consumer yet and the audience of a device-presence broadcast is undefined. Emit
//     it additively once a consumer + named audience exist.
//   - The endpoint serves a LOCAL loopback extension, so a dropped socket
//     surfaces as a read error and flips offline. Ping/pong keepalive + read
//     deadlines (for a half-dead WAN peer that never sends FIN) are additive.
//   - The downstream write carries a write deadline, so a stuck peer fails the
//     conn instead of freezing the adapter.
//
// File layout (one concern per file):
//
//   - actor.go    Actor struct + NewActor/Def + run Proc loop, local device
//     maintenance, and describe dispatch.
//   - device.go   WS listener + accept + read loop + in-flight table + sweep +
//     downstream send (write-deadline bounded).
//   - wire.go     the minimal device frame structs.
//   - types.go    inward type constants + per-type cmd mapping + per-type deadline.
//   - describe.go the WordSpec catalog for actor.describe.
package xhs
