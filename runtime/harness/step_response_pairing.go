package harness

import (
	"context"
	"encoding/json"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepResponsePairing implements proto-layer1 §2.8 Step 8 — The One Law
// / terminal-uniqueness contract. Applies only to kind=response.
//
// Concretely:
//
//   - response.parent_id must point to an existing message; missing →
//     harness_response_parent_not_found.
//   - parent.kind must equal "request"; otherwise →
//     harness_response_parent_not_request.
//   - payload.status MUST be one of {"completed","failed"}; otherwise →
//     harness_response_status_invalid.
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
// the harness can return the appropriate reject before any sqlite
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
			RejectReason: message.HarnessResponseParentNotFound,
			Detail:       "parent_id not found: " + string(env.ParentID),
		}, nil
	}
	if parent.Kind != message.KindRequest {
		return khar.Outcome{
			RejectReason: message.HarnessResponseParentNotRequest,
			Detail:       "parent_id is not kind=request: " + string(env.ParentID),
		}, nil
	}

	// payload.status strict closed set — proto-layer1 §2.8 #4. Missing
	// status (or non-string / out-of-set) → harness_response_status_invalid.
	if !payloadStatusValid(env.Payload) {
		return khar.Outcome{
			RejectReason: message.HarnessResponseStatusInvalid,
			Detail:       "payload.status must be one of {completed, failed}",
		}, nil
	}

	systemFallback, reasonCheck := allowsSystemTerminalFallback(env)
	if reasonCheck.invalid {
		return khar.Outcome{
			RejectReason: message.HarnessResponseReasonInvalid,
			Detail:       reasonCheck.detail,
		}, nil
	}
	if !audienceContains(parent.Audience, env.Sender.ID) && !systemFallback {
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
	if reasonCheck = checkFailedResponseReason(env.Payload); reasonCheck.invalid {
		return khar.Outcome{
			RejectReason: message.HarnessResponseReasonInvalid,
			Detail:       reasonCheck.detail,
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

type failedResponseReasonCheck struct {
	failed    bool
	hasReason bool
	reason    string
	invalid   bool
	detail    string
}

func allowsSystemTerminalFallback(env *message.Envelope) (bool, failedResponseReasonCheck) {
	if env.Sender.ID != actor.SystemActorID {
		return false, failedResponseReasonCheck{}
	}
	check := checkFailedResponseReason(env.Payload)
	if check.invalid {
		return false, check
	}
	return check.failed && check.hasReason && terminalFailureReasonAllowed(check.reason), check
}

func checkFailedResponseReason(payload []byte) failedResponseReasonCheck {
	var check failedResponseReasonCheck
	if len(payload) == 0 {
		return check
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return check
	}
	rawStatus, ok := doc["status"]
	if !ok {
		return check
	}
	var status string
	if err := json.Unmarshal(rawStatus, &status); err != nil || status != "failed" {
		return check
	}
	check.failed = true
	rawReason, ok := doc["reason"]
	if !ok {
		return check
	}
	check.hasReason = true
	if err := json.Unmarshal(rawReason, &check.reason); err != nil {
		check.invalid = true
		check.detail = "payload.reason must be a string"
		return check
	}
	if !terminalFailureReasonAllowed(check.reason) {
		check.invalid = true
		check.detail = "payload.reason not in terminal_failure_reason closed set: " + check.reason
	}
	return check
}

func terminalFailureReasonAllowed(reason string) bool {
	for _, r := range message.AllTerminalFailureReasons {
		if reason == string(r) {
			return true
		}
	}
	return false
}

// payloadStatusValid returns true when payload.status is present and
// equals one of the proto-layer1 §2.8 closed set {"completed","failed"}.
// Empty payload or missing/non-string status → false (caller rejects
// with harness_response_status_invalid).
func payloadStatusValid(payload []byte) bool {
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
