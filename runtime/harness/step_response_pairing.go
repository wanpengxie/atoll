package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepResponsePairing implements L1 §10.2 step 8 — The One Law /
// terminal-uniqueness contract. Applies only to kind=response.
//
// Concretely:
//
//   - response.parent_id must point to an existing kind=request message
//     (response_parent_invalid otherwise).
//   - response.sender must be one of the parent request's audience actors,
//     and response.audience must target the parent request sender exactly.
//     Trusted system terminal-failure fallbacks are the only sender
//     exception; they still must target the parent request sender.
//   - is_terminal is computed per type_registry.terminal_convention:
//     core types collapse to single-response semantics; business types
//     read payload.status (or single-response, when set).
//   - same-parent_id duplicate is enforced at engine append (the
//     store's UNIQUE constraint maps to terminal_duplicate). This step
//     does NOT pre-scan store; the store's unique-index plus the
//     classifyAppendErr mapping in runtime/store handles concurrency
//     correctly per L2 §1.4.1 invariant.
//
// We DO perform a non-authoritative early check (FindByID parent) so
// the harness can return response_parent_invalid before any sqlite
// transaction starts — saves a roundtrip on obviously wrong responses.
type stepResponsePairing struct {
	deps Deps
}

func newStepResponsePairing(d Deps) khar.Step { return &stepResponsePairing{deps: d} }

func (s *stepResponsePairing) ID() khar.StepID { return khar.StepResponsePairing }

func (s *stepResponsePairing) Run(ctx context.Context, env *message.Envelope) (khar.Outcome, error) {
	if env.Kind != message.KindResponse {
		return khar.Outcome{}, nil
	}

	// Parent existence + kind check.
	parent, ok, err := s.deps.Log.FindByID(ctx, s.deps.ChannelID, env.ParentID)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok {
		return khar.Outcome{
			RejectReason: message.HarnessResponseParentInvalid,
			Detail:       "parent_id not found: " + string(env.ParentID),
		}, nil
	}
	if parent.Kind != message.KindRequest {
		return khar.Outcome{
			RejectReason: message.HarnessResponseParentInvalid,
			Detail:       "parent_id is not kind=request: " + string(env.ParentID),
		}, nil
	}
	if !audienceContains(parent.Audience, env.Sender.ID) && !isSystemTerminalFallback(env) {
		return khar.Outcome{
			RejectReason: message.HarnessResponseUnauthorizedSender,
			Detail:       "response sender is not in parent request audience: " + string(env.Sender.ID),
		}, nil
	}
	if !audienceExactlySender(env.Audience, parent.Sender.ID) {
		return khar.Outcome{
			RejectReason: message.HarnessResponseAudienceMismatch,
			Detail:       "response audience must equal parent request sender: " + string(parent.Sender.ID),
		}, nil
	}

	// Compute is_terminal.
	if _, isCore := message.CoreTypeTable[env.Type]; isCore {
		env.IsTerminal = true
		return khar.Outcome{}, nil
	}
	if s.deps.TypeRegistry == nil {
		// Defensive — without type_registry we cannot decide terminal
		// convention; default to payload_status which still surfaces
		// completed/failed as terminal.
		env.IsTerminal = payloadStatusTerminal(env.Payload)
		return khar.Outcome{}, nil
	}
	view, ok, err := s.deps.TypeRegistry.Lookup(ctx, env.Type)
	if err != nil {
		return khar.Outcome{}, err
	}
	if !ok || view.TerminalConvention == "" || view.TerminalConvention == "payload_status" {
		env.IsTerminal = payloadStatusTerminal(env.Payload)
	} else {
		env.IsTerminal = true
	}
	return khar.Outcome{}, nil
}

func audienceContains(audience message.Audience, want actor.ActorID) bool {
	for _, id := range audience {
		if id == want {
			return true
		}
	}
	return false
}

func audienceExactlySender(audience message.Audience, sender actor.ActorID) bool {
	return len(audience) == 1 && audience[0] == sender
}

func isSystemTerminalFallback(env *message.Envelope) bool {
	if env.Sender.ID != actor.SystemActorID {
		return false
	}
	if len(env.Payload) == 0 {
		return false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(env.Payload, &doc); err != nil {
		return false
	}
	rawStatus, ok := doc["status"]
	if !ok {
		return false
	}
	var status string
	if err := json.Unmarshal(rawStatus, &status); err != nil || status != "failed" {
		return false
	}
	rawReason, ok := doc["reason"]
	if !ok {
		return false
	}
	var reason string
	if err := json.Unmarshal(rawReason, &reason); err != nil {
		return false
	}
	for _, r := range message.AllTerminalFailureReasons {
		if reason == string(r) {
			return true
		}
	}
	return false
}

// payloadStatusTerminal returns true when payload.status is one of
// {"completed","failed"} per L1 §10.2.
func payloadStatusTerminal(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return false
	}
	raw, ok := doc["status"]
	if !ok {
		return false
	}
	var status string
	if err := json.Unmarshal(raw, &status); err != nil {
		return false
	}
	return status == "completed" || status == "failed"
}
