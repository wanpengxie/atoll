// Package behavior is the actor behavior base — the three-author closure set
// that every actor in the system uses to send and answer messages.
//
// Three authors, one base (P13):
//
//   - author#1 (respond.go / serve.go): the SERVE face — building and writing
//     kind=response envelopes, including the happy-path (RespondJSON), the
//     failure short-cut (Fail), and the raw builder (BuildResponseFromRequest).
//
//   - author#2 (call.go): the CALL face — building kind=request envelopes
//     (RequestSpec/BuildRequest) with their declared ExpiresAt. The deadline's
//     enforcement lives elsewhere (期12 义务归位): a live caller's actorbase
//     callLedger is the fast-path observer, the substrate expiry reaper the
//     guaranteed one. (The former per-actor Caller closure manager was拆删 —
//     an unconsumed twin of that machinery.)
//
//   - author#3 (death.go): the DEATH author — MaterialiseReceiverUnavailable
//     drains open requests for a dead actor and closes each with a
//     receiver_unavailable terminal.
//
// Supporting files: respond.go also hosts BuildEvent/EmitEvent (kind=event
// construction); correlation.go holds the derivation rule for correlation ids.
//
// Position in the stack: behavior sits in lib/ (the stdlib layer). It imports
// only protocol types (message, actor), runtime seams (harness, storespec),
// and uuid (envelope id generation). It has no knowledge of specific actor
// kinds — kind-neutrality is the whole point.
package behavior
