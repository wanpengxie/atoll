// Package kimi is a concrete device-backed adapter actor — the boundary
// translator between the channel (actor/message world) and the user's real
// browser, driven through the Kimi WebBridge Chrome extension that speaks no
// actor primitives. It is an actorbase Proc (lib/actorbase, actorbase-spec-v1):
// Def(cfg) mints a fresh Actor + run per incarnation, run(sys) is the process
// body (entry=birth, sys.Recv() loop, return=death).
//
// It follows the adapter-actor shape: two faces, complexity pressed into one
// stateful actor. This package is the WebBridge that controls a browser
// extension; "kimi" here names that retained browser-tool actor.
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
//   - Outward (device face): the WS endpoint the kimi-webbridge Chrome extension
//     dials, supplied by drivers/tools/plugindevice — the transport is shared
//     with xhs, this package keeps only its own dialect (the action allowlist,
//     its budget, and plugindevice.WebbridgeProtocol) and the two endpoint words
//     kimi.listen.set / kimi.listen.get.
//
//     The wire is NOT ours: it is kimi-webbridge's own (npm: kimi-webbridge,
//     MIT), read off that package's server, because this adapter stands in for
//     exactly that server. /ws, a hello/hello_ack handshake the extension needs
//     before it considers itself ready, {type:"tool_call",requestId,payload:
//     {name,args}} down, {type:"tool_result",responseToRequestId,payload:
//     {data|error}} up, and a {"type":"ping"} every 15s answered with
//     {"type":"pong"}. Port 10086 is likewise the extension's choice, not ours.
//     See plugindevice/protocol.go. The older text below described a frame
//     family this adapter never actually spoke to a real extension: the minimal
//     request/response
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
// one cross-goroutine state the shared transport owns). A small actor-owned local
// maintenance goroutine performs bind retry and reaping without depending on
// daemon↔Server Schedule availability.
//
// Fault posture (let-it-crash): a single request that times out or hits an
// offline device fails as a business response (the actor digests it). The
// local reaper collects past-deadline requests (single 60s budget — browser
// actions are sub-second to a few seconds, no minutes-long operation). A
// dropped conn flips the adapter offline and waits for a fresh connection.
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
//   - The endpoint defaults to loopback, where a dropped socket surfaces as a
//     read error and flips offline. kimi.listen.set can move it to a routable
//     address (so a browser on the operator's own laptop can reach a server-side
//     adapter); keepalive is armed for exactly that case, because a half-dead
//     WAN peer that never sends FIN would otherwise hold the single connection
//     slot and keep the real extension out. The endpoint stays KEYLESS, so the
//     bound address is the whole trust boundary — a wildcard bind is refused.
//   - The downstream write carries a write deadline, so a stuck peer fails the
//     conn instead of freezing the adapter.
//
// File layout (one concern per file):
//
//   - actor.go    Actor struct + NewActor + Def + run (Proc body) + handle
//     dispatch + describe dispatch.
//   - types.go    the single inward type + action allowlist + payload shape + deadline.
//   - describe.go the WordSpec catalog for actor.describe.
package kimi
