// Package behavior is the actor behavior base — the three-author closure set
// that every actor in the system uses to send and answer messages.
//
// Three authors, one base (P13):
//
//   - author#1 (respond.go / serve.go): the SERVE face — building and writing
//     kind=response envelopes, including the happy-path (RespondJSON), the
//     failure short-cut (Fail), and the raw builder (BuildResponseFromRequest).
//
//   - author#2 (call.go): the CALL face — building kind=request envelopes and
//     the per-actor Caller closure that arms in-flight timers and commits
//     unanswered_timeout terminals back to the caller's own mailbox when a
//     deadline fires.
//
//   - author#3 (death.go): the DEATH author — MaterialiseReceiverUnavailable
//     drains open requests for a dead actor and closes each with a
//     receiver_unavailable terminal.
//
// Supporting files: respond.go also hosts BuildEvent/EmitEvent (kind=event
// construction); correlation.go holds the derivation rule for correlation ids.
//
// Position in the stack: behavior sits in lib/ (the stdlib layer). It imports
// only protocol types (message, channel, actor) and runtime seams (harness,
// storespec). It has no knowledge of specific actor kinds — kind-neutrality is
// the whole point.
package behavior
