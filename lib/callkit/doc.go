// Package callkit is the CLIENT-EDGE call mechanism for the sync-wrap LLM
// tool loop: subscribe-before-send futures (RequestCorrelator) and a bounded
// blocking collector (Client: Submit/Await/Abandon/Pending/Deliver). Pure
// mechanism — it imports only protocol/message and carries ZERO vocabulary:
// the LLM tool-result vocabulary (ResultValue, ErrorCode closed set, ack
// shapes, RequestSpec/WaitMode, payload normalisation) lives in lib/metatool,
// the binding layer above.
//
// Boundary axiom (lib-reshape spec §2.7): anything that can block-await is by
// definition NOT an actor — block-await violates the cell serial contract and
// the mailbox-sole-ingress rule. So this kit never merges into lib/behavior;
// the actor-side call face is behavior.BuildRequest + behavior.Caller
// (closure author#2: build / arm / match, OTP send_request-style). callkit
// exists for the current synchronous agent loop and dissolves entirely with
// the first-class async refactor.
package callkit
