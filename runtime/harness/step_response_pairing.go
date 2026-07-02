package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/wanpengxie/ActOS/protocol/actor"
	"github.com/wanpengxie/ActOS/protocol/message"
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
//   - When status=failed (Layer 1 final), payload.reason MUST be PRESENT and
//     in the terminal_failure_reason closed set; missing or out-of-set →
//     harness_response_reason_invalid (§2.6: a reason-less failed terminal
//     would break three-author attribution).
//
//   - response.sender authorization has THREE authors (actor-runtime-
//     redesign.md §0.5 Δ2), each welded 1:1 to its ONE failure word (§2.6
//     total matrix — the word IS the authorization), and response.audience
//     must target the parent request sender exactly:
//     1. receiver voluntary — sender ∈ parent.audience: completed /
//     provisional / failed+receiver_internal_error ONLY;
//     2. caller self-close — sender == parent.sender writing its own
//     caller-scoped status=failed + reason=unanswered_timeout ONLY;
//     3. substrate death — on the obs down edge a watcher
//     materialises the dead receiver's receiver_unavailable, SYSTEM-authored
//     (sender == SystemActorID; see substrateDeath gate below). This is a
//     distinct author, NOT a forged dead-receiver sender. The old generic
//     "system actor terminal fallback" author is DELETED.
//
//   - Terminal uniqueness: once a final response exists for the parent the
//     request is closed. A second final → harness_terminal_duplicate; a
//     provisional → harness_provisional_after_final. A receiver's genuine
//     LATE final (the caller already self-closed on a caller-scoped timeout)
//     is just one more post-final final — the request has no open closure
//     left for it to fill, so it rejects like any duplicate (gRPC/HTTP
//     fail-the-late-send). Surfacing "the receiver answered late" (true
//     latency, etc.) is a domain observability concern, never substrate
//     truth. The
//     ux_terminal_response_per_request UNIQUE INDEX in store.schema is the
//     authoritative defence for concurrent racers.
//
//   - is_terminal is computed from the payload.status classification:
//     Layer 1 → true; Layer 2 / Layer 3 → false. proto-layer0 §2.5.1
//     defines is_terminal as a uniform payload.status derivation.
type stepResponsePairing struct {
	deps Deps
}

func newStepResponsePairing(d Deps) step { return &stepResponsePairing{deps: d} }

func (s *stepResponsePairing) ID() stepID { return StepResponsePairing }

func (s *stepResponsePairing) Run(ctx context.Context, env *message.Envelope) (outcome, error) {
	if env.Kind != message.KindResponse {
		return outcome{}, nil
	}

	// Parent existence + kind check.
	parent, ok, err := s.deps.Log.FindByID(ctx, env.ParentID)
	if err != nil {
		return outcome{}, err
	}
	if !ok {
		return outcome{
			RejectReason: HarnessResponseParentNotFound,
			Detail:       "parent_id not found: " + string(env.ParentID),
		}, nil
	}
	if parent.Envelope.Kind != message.KindRequest {
		return outcome{
			RejectReason: HarnessResponseParentNotRequest,
			Detail:       "parent_id is not kind=request: " + string(env.ParentID),
		}, nil
	}

	// payload.status half-closed-set classification — proto-layer0 §2.5.
	statusCls := classifyResponseStatus(env.Payload, env.Sender.ID)
	if statusCls.reject != "" {
		return outcome{
			RejectReason: statusCls.reject,
			Detail:       statusCls.detail,
		}, nil
	}

	reasonCheck := checkFailedResponseReason(env.Payload)
	if reasonCheck.invalid {
		return outcome{
			RejectReason: HarnessResponseReasonInvalid,
			Detail:       reasonCheck.detail,
		}, nil
	}
	// §2.6 normative, second half: status=failed MUST carry payload.reason.
	// A reason-less failed terminal in truth breaks three-author attribution
	// (no reason ⇒ no author derivable) and every reason-dispatching
	// consumer; previously only present-but-out-of-set was rejected, so
	// {"status":"failed"} slipped through as a legal terminal.
	if reasonCheck.failed && !reasonCheck.hasReason {
		return outcome{
			RejectReason: HarnessResponseReasonInvalid,
			Detail:       "status=failed requires payload.reason in the terminal_failure_reason closed set",
		}, nil
	}

	// ── Step 8 authorization model (§0.5 Δ2 + proto-layer0 §2.6 1:1 矩阵) ──
	//
	// closure has exactly THREE authors, by "who holds the fact" — and the
	// terminal_failure_reason closed set has exactly three words, welded
	// 1:1: each author may write ITS one failure word and nothing else (the
	// word IS the authorization; the matrix is total, owner-pinned 2026-07-02
	// 选项A):
	//
	//   1. receiver voluntary — sender ∈ parent.audience answers:
	//      completed, any provisional, or failed+receiver_internal_error —
	//      the callee's own exit reason, the ONE failure word a receiver
	//      holds. receiver_unavailable would assert substrate-observed death
	//      and unanswered_timeout would assert the caller's own giving-up:
	//      facts the receiver does not hold, so writing them here is forgery
	//      and rejects.
	//   2. caller self-close   — parent.sender writes its OWN caller-scoped
	//      status=failed + reason=unanswered_timeout ONLY. It is not in its
	//      own audience, so this is a distinct authorization.
	//      audience==[parent.sender] (itself) is naturally satisfied (#7
	//      unchanged).
	//   3. substrate death     — the substrate materialises a dead/gone
	//      receiver's terminal, SYSTEM-signed (a dead receiver cannot sign
	//      for itself), under the narrow gate status=failed +
	//      reason=receiver_unavailable ONLY. The old generic "system actor
	//      terminal fallback" (any reason, any parent — the global-guess
	//      author) stays DELETED: the substrate never guesses "slow", it
	//      only materialises death it positively observed.
	//
	// The arms are OR'd: a self-request's sender is simultaneously caller
	// and receiver and may write either of its two words. A sender that is
	// none of the three authors rejects as unauthorized; an authorized
	// author writing ANOTHER author's word rejects the same way.
	isReceiver := audienceContains(parent.Envelope.Audience, env.Sender.ID)
	receiverAuthored := isReceiver &&
		(!reasonCheck.failed || reasonCheck.reason == string(message.TerminalReceiverInternalError))

	callerSelfClose := env.Sender.ID == parent.Envelope.Sender.ID &&
		reasonCheck.failed &&
		reasonCheck.reason == string(message.TerminalUnansweredTimeout)

	substrateDeath := env.Sender.ID == actor.SystemActorID &&
		reasonCheck.failed &&
		reasonCheck.reason == string(message.TerminalReceiverUnavailable)

	if !receiverAuthored && !callerSelfClose && !substrateDeath {
		return outcome{
			RejectReason: HarnessResponseUnauthorizedSender,
			Detail:       "response sender is not an authorized closure author for this status/reason (receiver→receiver_internal_error, caller→unanswered_timeout, substrate→receiver_unavailable): " + string(env.Sender.ID),
		}, nil
	}
	if !audienceExactlySender(env.Audience, parent.Envelope.Sender.ID) {
		return outcome{
			RejectReason: HarnessResponseAudienceMismatch,
			Detail:       "response audience must equal parent request sender: " + string(parent.Envelope.Sender.ID),
		}, nil
	}

	// Terminal uniqueness. Once a final response exists for the parent the
	// request is closed: a second final → harness_terminal_duplicate, a
	// provisional → harness_provisional_after_final. A receiver's genuine
	// LATE final (the caller already self-closed on a caller-scoped timeout)
	// is just a duplicate here — the request has no open closure for it to
	// fill, so it rejects like any post-final final. Preserving the late
	// answer (true latency, audit) is a domain observability concern, not
	// substrate truth.
	//
	// This HasFinalResponse is a FAST-PATH pre-check, NOT the authoritative
	// guard: it runs in a different tx than the eventual INSERT, so a final
	// committing in the window between here and append would slip a provisional
	// past it. The authoritative defense for BOTH facets lives at append: the
	// final facet on the ux_terminal_response_per_request UNIQUE INDEX, the
	// provisional facet on an in-tx re-check inside appendTx (same serialized
	// connection → atomic). The pre-check just rejects the common non-racing
	// case cleanly without spending a tx.
	hasFinal, err := s.deps.Log.HasFinalResponse(ctx, env.ParentID)
	if err != nil {
		return outcome{}, err
	}
	if hasFinal {
		if statusCls.isFinal {
			return outcome{
				RejectReason: HarnessTerminalDuplicate,
				Detail:       "final response already exists for parent: " + string(env.ParentID),
			}, nil
		}
		return outcome{
			RejectReason: HarnessProvisionalAfterFinal,
			Detail:       "provisional response after final is forbidden for parent: " + string(env.ParentID),
		}, nil
	}

	// is_terminal derives purely from the Layer 1 final closed set —
	// the proto-layer0 §2.5.1 derivation is uniform across all types.
	return outcome{IsTerminal: statusCls.isFinal}, nil
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
	return message.IsValidTerminalFailureReason(message.TerminalFailureReason(reason))
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
	reject HarnessRejectReason
	detail string
}

// classifyResponseStatus runs the proto-layer0 §2.5 half-closed-set
// classification on env.Payload's `status` field. senderID is used for
// the Layer 3 namespace ownership check.
func classifyResponseStatus(payload []byte, senderID actor.ActorID) statusClassification {
	status, ok := extractPayloadStatus(payload)
	if !ok {
		return statusClassification{
			reject: HarnessResponseStatusInvalid,
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
				reject: HarnessResponseStatusInvalid,
				detail: fmt.Sprintf("payload.status namespace %q collides with Layer 2 provisional name", namespace),
			}
		}
		if message.IsFinalStatus(namespace) {
			return statusClassification{
				reject: HarnessResponseStatusInvalid,
				detail: fmt.Sprintf("payload.status namespace %q collides with Layer 1 final name", namespace),
			}
		}
		expected := senderLocalName(senderID)
		if namespace != expected {
			return statusClassification{
				reject: HarnessResponseStatusNamespaceMismatch,
				detail: fmt.Sprintf("payload.status namespace %q must equal sender local-name %q", namespace, expected),
			}
		}
		return statusClassification{isFinal: false}
	}
	return statusClassification{
		reject: HarnessResponseStatusInvalid,
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
