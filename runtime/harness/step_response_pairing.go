package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/wanpengxie/ActOS/kernel/actor"
	khar "github.com/wanpengxie/ActOS/kernel/harness"
	"github.com/wanpengxie/ActOS/kernel/message"
)

// stepResponsePairing implements proto-layer1 §2.8 Step 8 — Final
// Response Uniqueness + Response Parent Validation. Applies only to
// kind=response.
//
// Concretely:
//
//   - response.parent_id must point to an existing message; missing →
//     harness_response_parent_not_found.
//
//   - parent.kind must equal "request"; otherwise →
//     harness_response_parent_not_request.
//
//   - payload.status MUST belong to the proto-layer0 §2.5 half-closed
//     set:
//
//     – Layer 1 final (strict closed): {"completed","failed"} → is_terminal=true.
//     – Layer 2 provisional core (strict closed):
//     {"received","queued","processing","deferred","unavailable"} → is_terminal=false.
//     – Layer 3 provisional business extension (regex
//     `^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`): namespace part must not
//     collide with any Layer 1 / Layer 2 status name AND must equal
//     the sender.id local-name (everything after the last `:`). Anti-
//     spoofing per proto-layer0 §2.5.3. Otherwise →
//     harness_response_status_invalid or
//     harness_response_status_namespace_mismatch.
//
//   - When status=failed (Layer 1 final), payload.reason MUST be in the
//     terminal_failure_reason closed set; otherwise →
//     harness_response_reason_invalid.
//
//   - response.sender must be one of the parent request's audience actors,
//     and response.audience must target the parent request sender exactly.
//     Trusted system terminal-failure fallbacks are the only sender
//     exception; they still must target the parent request sender.
//
//   - Zombie chain defence (proto-layer1 §2.8 #8): when the log already
//     contains a final response for the same parent, a new final →
//     harness_terminal_duplicate; a new provisional →
//     harness_provisional_after_final. The ux_terminal_response_per_request
//     UNIQUE INDEX in store.schema is the authoritative defence for the
//     former (catching concurrent racers that slip past this pre-check);
//     this step surfaces the closed-set reject for both in the
//     non-racing single-writer path.
//
//   - is_terminal is computed from the payload.status classification:
//     Layer 1 → true; Layer 2 / Layer 3 → false. proto-layer0 §2.5.1
//     replaces the prior type_registry.terminal_convention dispatch —
//     terminal_convention rows kept in the schema for backward storage
//     compat but no longer drive harness classification.
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

	// payload.status half-closed-set classification — proto-layer0 §2.5.
	statusCls := classifyResponseStatus(env.Payload, env.Sender.ID)
	if statusCls.reject != "" {
		return khar.Outcome{
			RejectReason: statusCls.reject,
			Detail:       statusCls.detail,
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

	// Zombie chain defence (proto-layer1 §2.8 #8). A pre-existing final
	// response for the same parent forbids any further row: another
	// final is a duplicate; a provisional after final is a zombie.
	hasFinal, err := s.deps.Log.HasFinalResponse(ctx, s.deps.ChannelID, env.ParentID)
	if err != nil {
		return khar.Outcome{}, err
	}
	if hasFinal {
		if statusCls.isFinal {
			return khar.Outcome{
				RejectReason: message.HarnessTerminalDuplicate,
				Detail:       "final response already exists for parent: " + string(env.ParentID),
			}, nil
		}
		return khar.Outcome{
			RejectReason: message.HarnessProvisionalAfterFinal,
			Detail:       "provisional response after final is forbidden for parent: " + string(env.ParentID),
		}, nil
	}

	// is_terminal derives purely from the Layer 1 final closed set.
	// type_registry.terminal_convention rows are no longer consulted —
	// the proto-layer0 §2.5.1 derivation is uniform across all types.
	env.IsTerminal = statusCls.isFinal
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

// layer2ProvisionalStatuses is the Layer 2 provisional core closed set
// per proto-layer0 §2.5.2. Expansion is a protocol-level revision.
var layer2ProvisionalStatuses = map[string]struct{}{
	"received":    {},
	"queued":      {},
	"processing":  {},
	"deferred":    {},
	"unavailable": {},
}

// layer3StatusRegex enforces the Layer 3 provisional business extension
// grammar per proto-layer0 §2.5.3:
//
//	^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$
//
// Exactly one `.`; both halves non-empty; first character of each half a
// lowercase letter (no leading digit / underscore); remaining characters
// drawn from [a-z0-9_].
var layer3StatusRegex = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)

// statusClassification is the structured outcome of payload.status
// half-closed-set classification.
type statusClassification struct {
	// isFinal is true when the status belongs to the Layer 1 final
	// closed set ({"completed","failed"}). Layer 2 / Layer 3 statuses
	// are provisional and isFinal=false.
	isFinal bool

	// reject, when non-empty, names the closed-set reject reason the
	// caller MUST return; detail is the human-readable explanation.
	reject message.HarnessRejectReason
	detail string
}

// classifyResponseStatus runs the proto-layer0 §2.5 half-closed-set
// classification on env.Payload's `status` field. senderID is used for
// the Layer 3 namespace ownership check.
func classifyResponseStatus(payload []byte, senderID actor.ActorID) statusClassification {
	status, ok := extractPayloadStatus(payload)
	if !ok {
		return statusClassification{
			reject: message.HarnessResponseStatusInvalid,
			detail: "payload.status missing or non-string",
		}
	}
	if message.IsFinalStatus(status) {
		return statusClassification{isFinal: true}
	}
	if _, ok := layer2ProvisionalStatuses[status]; ok {
		return statusClassification{isFinal: false}
	}
	if layer3StatusRegex.MatchString(status) {
		namespace := status[:strings.IndexByte(status, '.')]
		if _, ok := layer2ProvisionalStatuses[namespace]; ok {
			return statusClassification{
				reject: message.HarnessResponseStatusInvalid,
				detail: fmt.Sprintf("payload.status namespace %q collides with Layer 2 provisional name", namespace),
			}
		}
		if message.IsFinalStatus(namespace) {
			return statusClassification{
				reject: message.HarnessResponseStatusInvalid,
				detail: fmt.Sprintf("payload.status namespace %q collides with Layer 1 final name", namespace),
			}
		}
		expected := senderLocalName(senderID)
		if namespace != expected {
			return statusClassification{
				reject: message.HarnessResponseStatusNamespaceMismatch,
				detail: fmt.Sprintf("payload.status namespace %q must equal sender local-name %q", namespace, expected),
			}
		}
		return statusClassification{isFinal: false}
	}
	return statusClassification{
		reject: message.HarnessResponseStatusInvalid,
		detail: fmt.Sprintf("payload.status %q not in any of {Layer 1 final, Layer 2 provisional, Layer 3 <ns>.<name>}", status),
	}
}

// extractPayloadStatus pulls the `status` string from the response
// payload. Returns ok=false when the payload is empty, malformed JSON,
// missing `status`, or has a non-string `status`.
func extractPayloadStatus(payload []byte) (string, bool) {
	if len(payload) == 0 {
		return "", false
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(payload, &doc); err != nil {
		return "", false
	}
	raw, ok := doc["status"]
	if !ok {
		return "", false
	}
	var status string
	if err := json.Unmarshal(raw, &status); err != nil {
		return "", false
	}
	return status, true
}

// senderLocalName derives the local-name portion of a sender.id per
// proto-layer0 §2.5.3 namespace ownership rule: everything after the
// last `:`, falling back to the full id when no `:` is present.
//
//	"tool:xhs"       → "xhs"
//	"agent:planner"  → "planner"
//	"daemon"         → "daemon"
//	"a:b:c"          → "c"
func senderLocalName(id actor.ActorID) string {
	s := string(id)
	if i := strings.LastIndexByte(s, ':'); i >= 0 {
		return s[i+1:]
	}
	return s
}
