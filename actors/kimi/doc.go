// Package kimi is a concrete device-backed adapter actor — the boundary
// translator between the channel (actor/message world) and the user's real
// browser, driven through the Kimi WebBridge Chrome extension that speaks no
// actor primitives. It is an actorbase Proc (lib/actorbase, actorbase-spec-v1):
// Def(cfg) mints a fresh Actor + run per incarnation, run(sys) is the process
// body (entry=birth, sys.Recv() loop, return=death).
//
// It follows the adapter-actor shape: two faces, complexity pressed into one
// stateful actor. Disambiguation: this package (actors/kimi)
// is the WebBridge that controls a browser extension — it is unrelated to
// agent/provider/kimi, the go-kimi looper engine.
//
//   - Inward (channel face): run()'s sys.Recv() loop, dispatched in
//     Actor.handle. It serves a SINGLE request type, kimi.command, answers
//     actor.describe, and writes terminals through sys.Reply/sys.Fail. The
//     device verb is the payload's `action` (one of 13 browser primitives:
//     navigate / find_tab / snapshot / click / fill / evaluate / screenshot /
//     network / upload / save_as_pdf / list_tabs / close_tab / close_session);
//     `args` is forwarded to the device verbatim. An action outside the closed
//     set fails invalid_action before anything reaches the device. No other
//     actor ever learns what a browser extension is.
//
//   - Outward (device face): a PRIVATE WS endpoint owned by this package (the
//     extension connect-ins to it). The wire is the minimal request/response
//     primitive {correlation_id, cmd, params} down / {correlation_id, ok,
//     result|error} up — NOT a channel envelope, NOT any device_transit frame
//     family. correlation_id pairs a reply to its request.
//
// Statefulness (one incarnation per Proc, per spec §1.6): the adapter holds
// the current device conn + an in-flight table keyed by correlation_id (as its
// own Msg, not a re-fetched envelope — sys.Reply/sys.Fail take the Msg
// in-hand). Because Sys is concurrency-safe and Msg is immutable (spec §1.2
// fan-out), the device's read-loop goroutine calls sys.Reply/sys.Fail directly
// to close a request; an internal mutex guards the conn + in-flight table (the
// one cross-goroutine state device.go itself owns). The reaper is not a
// goroutine: it sweeps on the worker off a sys.After self-wake (期10 S3).
//
// Fault posture (let-it-crash): a single request that times out or hits an
// offline device fails as a business response (the actor digests it). The
// reaper sweep (armed by sys.After self-wake, run on the worker) collects
// past-deadline requests (single 60s budget — browser actions are sub-second to
// a few seconds, no minutes-long operation). A dropped conn flips the adapter offline and waits for a fresh
// connection — it does NOT panic; only an untrustworthy internal state would
// (positive death, i.e. run() returning a non-nil error).
//
// Domain note: screenshot and save_as_pdf return a LOCAL file path — the device
// writes the bytes to disk and the wire carries only the path, not the payload.
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
//   - actor.go    Actor struct + NewActor + Def + run (Proc body) + handle
//     dispatch + describe dispatch.
//   - device.go   WS listener + accept + read loop + in-flight table + sweep +
//     downstream send (write-deadline bounded).
//   - wire.go     the minimal device frame structs.
//   - types.go    the single inward type + action allowlist + payload shape + deadline.
//   - describe.go the TypeMeta catalog for actor.describe.
package kimi
