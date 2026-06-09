// Package callkit is the CLIENT-EDGE call kit for the sync-wrap LLM tool
// loop: subscribe-before-send futures (RequestCorrelator), a bounded blocking
// Await (Client), the tool-call spec (RequestSpec/WaitMode), the ErrorCode
// closed set, and payload normalisation helpers.
//
// Boundary axiom (lib-reshape spec §2.7): anything that can block-await is by
// definition NOT an actor — block-await violates the cell serial contract and
// the mailbox-sole-ingress rule. So this kit never merges into lib/behavior;
// the actor-side call face is behavior.BuildRequest + behavior.Caller
// (closure author#2: build / arm / match, OTP send_request-style). callkit
// exists for the current synchronous agent loop and dissolves with the
// first-class async refactor (LLM result vocabulary then folds into the
// metatool binding layer).
package callkit
