package harness

import "github.com/wanpengxie/atoll/runtime/storespec"

// HarnessRejectReason is the closed set of reasons the 9-step Message-Write
// Harness can synchronously reject a write with. This is the WRITE ENGINE's
// errno vocabulary (Erlang/Unix prior-art): it co-evolves with the harness
// steps, so it lives with the engine, not in the kernel ADT layer.
//
// An errno vocabulary is exactly the set of reasons a producer can stamp,
// so a word with zero producers is not in it: the channel-write fence is
// obsolete under a single harness writer; the visibility-scoped audience
// wildcard was removed; substrate went type-agnostic (no type→handler
// routing left to mismatch); and the sender door's registry retirement
// later dropped harness_sender_deregistered the same way — leaving 27.
//
// The two store-produced members (id-duplicate / terminal-duplicate) are
// lifted from storespec's AppendReject* consts — the contract leaf is their
// canonical home because the store driver stamps them and must not import
// this package. (Earlier the id-duplicate word had a live producer but no
// const anywhere — the "surfaced via classifyAppendErr's wire string"
// rationale — which left the timer FireSink's dup/poison split and every
// other consumer comparing against string coincidence. An errno word with a
// producer belongs in the closed set.)
//
// No HTTPStatus() method: mapping a reason to an HTTP status code (strerror)
// is a transport binding concern, not a substrate engine concern. This type
// only carries String() (the wire form).
type HarnessRejectReason string

const (
	// Pen identity injection (pre-chain, boundPen.Write) — a writer hand-filled
	// env.sender.id / env.channel_id, which are substrate-injected (welded by the
	// pen), not caller-settable. Fail-fast rather than silently overwriting the
	// misuse.
	HarnessIdentityNotCallerSettable HarnessRejectReason = "harness_identity_not_caller_settable"

	// Step 0 — Caller Principal Validation
	HarnessEngineACLDenied HarnessRejectReason = "harness_engine_acl_denied"

	// Step 1 — Envelope Shape Validate
	HarnessEnvelopeFieldMissing      HarnessRejectReason = "harness_envelope_field_missing"
	HarnessPayloadInvalid            HarnessRejectReason = "harness_payload_invalid"
	HarnessKindInvalid               HarnessRejectReason = "harness_kind_invalid"
	HarnessVisibilityInvalid         HarnessRejectReason = "harness_visibility_invalid"
	HarnessAudienceWildcardForbidden HarnessRejectReason = "harness_audience_wildcard_forbidden"
	HarnessResponseMissingParent     HarnessRejectReason = "harness_response_missing_parent"

	// (harness_envelope_unknown_field is gone from this vocabulary: the
	// unknown-top-level-field fail-closed reject now rides the Envelope
	// type — message.Envelope.UnmarshalJSON returns message.UnknownFieldError
	// at decode, before a pen is ever involved.)

	// Step 3 — Normalize (time-relation guard)
	HarnessTimeInvalid HarnessRejectReason = "harness_time_invalid"

	// Step 4 — Sender Validate. (harness_sender_deregistered was retired with
	// the sender door's registry lookup — the step trusts the pen weld, and a
	// reason with zero producers is not in the errno vocabulary, same rule as
	// the v2 drops above.)
	HarnessSenderMismatch     HarnessRejectReason = "harness_sender_mismatch"
	HarnessSenderKindMismatch HarnessRejectReason = "harness_sender_kind_mismatch"

	// Step 5 — Type / Kind Validate
	HarnessTypeUnknown                    HarnessRejectReason = "harness_type_unknown"
	HarnessReservedTypeUnauthorizedSender HarnessRejectReason = "harness_reserved_type_unauthorized_sender"

	// Step 7 — Kind + Audience Validate
	HarnessKindNotAllowedForType HarnessRejectReason = "harness_kind_not_allowed_for_type"
	// HarnessAudienceEmpty applies to request/response empty arrays and empty
	// actor-id elements. A kind=event empty array is valid.
	HarnessAudienceEmpty           HarnessRejectReason = "harness_audience_empty"
	HarnessRequestAudienceInvalid  HarnessRejectReason = "harness_request_audience_invalid"
	HarnessResponseAudienceInvalid HarnessRejectReason = "harness_response_audience_invalid"

	// Step 8 — Terminal Uniqueness + Response Parent Validation
	HarnessResponseParentNotFound          HarnessRejectReason = "harness_response_parent_not_found"
	HarnessResponseParentNotRequest        HarnessRejectReason = "harness_response_parent_not_request"
	HarnessResponseStatusInvalid           HarnessRejectReason = "harness_response_status_invalid"
	HarnessResponseStatusNamespaceMismatch HarnessRejectReason = "harness_response_status_namespace_mismatch"
	HarnessResponseReasonInvalid           HarnessRejectReason = "harness_response_reason_invalid"
	HarnessResponseUnauthorizedSender      HarnessRejectReason = "harness_response_unauthorized_sender"
	HarnessResponseAudienceMismatch        HarnessRejectReason = "harness_response_audience_mismatch"
	HarnessProvisionalAfterFinal           HarnessRejectReason = "harness_provisional_after_final"

	HarnessReceiverNotMember HarnessRejectReason = "harness_receiver_not_member"

	// Step 10 — Engine Append (store-produced, lifted from the storespec
	// contract leaf so driver and vocabulary share ONE symbol; see the type
	// doc above).
	//
	// HarnessIDDuplicateConflict — messages.id UNIQUE hit. For a
	// deterministic producer (the timer fire id) this is the crash-replay
	// idempotency signal a FireSink translates to ErrDuplicateFire; for a
	// random uuid it is a pure integrity error.
	HarnessIDDuplicateConflict HarnessRejectReason = storespec.AppendRejectIDDuplicate
	// HarnessTerminalDuplicate — second terminal response for one request
	// (ux_terminal_response_per_request UNIQUE); lib/behavior's three
	// authors treat it as the benign lost-race signal.
	HarnessTerminalDuplicate HarnessRejectReason = storespec.AppendRejectTerminalDuplicate
)

// (No exported AllHarnessRejectReasons enumeration: the const block above IS
// the closed set. A duplicate exported slice is a second, mutable
// representation of a protocol closed set — an importer could append/rewrite
// it — with no substrate consumer. Iterate the wire strings at the transport
// binding boundary that needs them, e.g. a reason→HTTP-status map.)

// String returns the wire form.
func (r HarnessRejectReason) String() string { return string(r) }
