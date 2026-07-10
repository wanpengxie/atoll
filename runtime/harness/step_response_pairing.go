package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/wanpengxie/atoll/protocol/actor"
	"github.com/wanpengxie/atoll/protocol/message"
)

// stepResponsePairing enforces Final Response Uniqueness + Response
// Parent Validation. Applies only to kind=response.
//
// Concretely:
//
//   - response.parent_id must point to an existing message; missing →
//     harness_response_parent_not_found.
//
//   - parent.kind must equal "request"; otherwise →
//     harness_response_parent_not_request.
//
//   - payload.status MUST belong to the half-closed set:
//
//     – Layer 1 final (strict closed): {"completed","failed"} → is_terminal=true.
//     – Layer 2 provisional core (strict closed):
//     {"received","queued","processing","deferred","unavailable"} → is_terminal=false.
//     – Layer 3 provisional business extension (regex
//     `^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`): namespace part must not
//     collide with any Layer 1 / Layer 2 status name AND must equal
//     the sender.id local-name (everything after the last `:`), to
//     prevent namespace spoofing. Otherwise →
//     harness_response_status_invalid or
//     harness_response_status_namespace_mismatch.
//
//   - When status=failed (Layer 1 final), payload.reason MUST be PRESENT and
//     in the terminal_failure_reason closed set; missing or out-of-set →
//     harness_response_reason_invalid (a reason-less failed terminal would
//     break three-author attribution).
//
//   - response.sender authorization is FACT-WELDED (期12 义务归位): the three
//     failure words map 1:1 onto three durable FACTS (receiver's own error /
//     substrate-observed death / declared-deadline passed unanswered), and an
//     author may write a word iff it durably HOLDS that word's fact. Three
//     facts, four authorized (author, word) arms — deadline-expiry has two
//     legitimate observers; response.audience must target the parent request
//     sender exactly:
//     1. receiver voluntary — sender ∈ parent.audience: completed /
//     provisional / failed+receiver_internal_error ONLY;
//     2. caller self-close — sender == parent.sender writing its own
//     caller-scoped status=failed + reason=unanswered_timeout (its fast-path
//     timer, or its own Cancel);
//     3. substrate death — on the obs down edge a watcher
//     materialises the dead receiver's receiver_unavailable, SYSTEM-authored
//     (sender == SystemActorID; see substrateDeath gate below). This is a
//     distinct author, NOT a forged dead-receiver sender.
//     4. substrate expiry — the expiry reaper (level-scan over declared
//     ExpiresAt) materialises failed+unanswered_timeout when the caller's
//     fast-path timer is gone (crash/dereg/kept-down). Deadline closure is
//     the SUBSTRATE'S obligation — the caller's own timer is a latency
//     optimisation of the same enforcement, so this is the guarantee arm,
//     not a fallback guess. Provenance (which observer wrote it) rides
//     sender + payload.closed_by, never the vocabulary.
//     The dividing line that keeps the DELETED generic "system actor terminal
//     fallback" author dead: the substrate only enforces facts written in
//     truth (an observed death edge, a declared deadline) — it never guesses.
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
//     Layer 1 → true; Layer 2 / Layer 3 → false. This is a uniform
//     payload.status derivation.
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

	// payload.status half-closed-set classification.
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
	// status=failed MUST carry payload.reason.
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

	// ── authorization model (fact-welded, 期12 义务归位) ──
	//
	// The word is welded to the FACT; the author set per word = whoever
	// durably HOLDS that fact. Three facts, three words, four authorized
	// (author, word) arms:
	//
	//   1. receiver voluntary — sender ∈ parent.audience answers:
	//      completed, any provisional, or failed+receiver_internal_error —
	//      the callee's own exit reason, the ONE failure word a receiver
	//      holds. receiver_unavailable would assert substrate-observed death
	//      and unanswered_timeout would assert the deadline fact: facts the
	//      receiver does not hold, so writing them here is forgery and
	//      rejects.
	//   2. caller self-close   — parent.sender writes status=failed +
	//      reason=unanswered_timeout: its own fast-path timer fired, or its
	//      own Cancel. It is not in its own audience, so this is a distinct
	//      authorization. audience==[parent.sender] (itself) is naturally
	//      satisfied (#7 unchanged).
	//   3. substrate death     — the substrate materialises a dead/gone
	//      receiver's terminal, SYSTEM-signed (a dead receiver cannot sign
	//      for itself), under the narrow gate status=failed +
	//      reason=receiver_unavailable ONLY.
	//   4. substrate expiry    — the substrate's reaper materialises
	//      status=failed + reason=unanswered_timeout for a request whose
	//      DECLARED ExpiresAt passed unanswered (期12 义务归位): deadline
	//      closure is a substrate obligation — a declared deadline is truth
	//      the substrate always holds, and the caller (who may crash, dereg,
	//      or be deliberately kept down) cannot structurally guarantee it.
	//      The caller's own timer (arm 2) is the fast-path observer of the
	//      SAME fact; provenance rides sender + payload.closed_by.
	//
	// The old generic "system actor terminal fallback" (any reason, any
	// parent — the global-guess author) stays DELETED. The durable line that
	// keeps it dead: the substrate only enforces facts written in truth
	// (observed death edge / declared deadline) — it never guesses "slow",
	// never invents a deadline, never revives anyone to close an account.
	//
	// The arms are OR'd: a self-request's sender is simultaneously caller
	// and receiver and may write either of its two words. A sender that is
	// none of the authorized arms rejects as unauthorized; an authorized
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

	// substrateExpiry (期12 义务归位): the substrate's expiry reaper
	// materialises a declared-deadline-passed-unanswered terminal when the
	// caller's own fast-path timer did not (caller crashed / deregistered /
	// deliberately kept down). Same fact as callerSelfClose's word, second
	// authorized observer — provenance rides sender + payload.closed_by,
	// never a new vocabulary word.
	substrateExpiry := env.Sender.ID == actor.SystemActorID &&
		reasonCheck.failed &&
		reasonCheck.reason == string(message.TerminalUnansweredTimeout)

	if !receiverAuthored && !callerSelfClose && !substrateDeath && !substrateExpiry {
		return outcome{
			RejectReason: HarnessResponseUnauthorizedSender,
			Detail:       "response sender is not an authorized closure author for this status/reason (receiver→receiver_internal_error, caller→unanswered_timeout, substrate→receiver_unavailable|unanswered_timeout): " + string(env.Sender.ID),
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
	// this derivation is uniform across all types.
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
	if err := json.Unmarshal(rawStatus, &status); err != nil || status != message.StatusFailed {
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

// layer2ProvisionalStatuses is the Layer 2 provisional core closed set.
// Expansion is a protocol-level revision.
var layer2ProvisionalStatuses = map[string]struct{}{
	"received":    {},
	"queued":      {},
	"processing":  {},
	"deferred":    {},
	"unavailable": {},
}

// layer3StatusRegex enforces the Layer 3 provisional business extension
// grammar:
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

// classifyResponseStatus runs the half-closed-set classification on
// env.Payload's `status` field. senderID is used for
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

// senderLocalName derives the local-name portion of a sender.id per the
// namespace ownership rule: everything after the last `:`, falling back
// to the full id when no `:` is present.
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
