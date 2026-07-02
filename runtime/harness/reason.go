package harness

// HarnessRejectReason is the closed set of reasons the 9-step Message-Write
// Harness can synchronously reject a write with. This is the WRITE ENGINE's
// errno vocabulary (Erlang/Unix prior-art, kernel-construction-spec §0.1):
// it co-evolves with the harness steps, so it lives with the engine, not in
// the kernel ADT layer.
//
// v2 drops several v1 reasons because an errno vocabulary is exactly the set
// of reasons a producer can stamp, so a word with zero producers is not in it:
// the channel-write fence is obsolete under a single harness writer
// (runtime-construction-spec §4.1); the v1 id-dedupe step was retired
// (message.id is now a random uuid; an id UNIQUE collision is a pure integrity
// error surfaced via classifyAppendErr's wire string); the visibility-scoped
// audience wildcard was removed; and substrate went type-agnostic (no
// type→handler routing left to mismatch); the sender door's registry
// retirement later dropped harness_sender_deregistered the same way —
// leaving 27.
//
// No HTTPStatus() method: mapping a reason to an HTTP status code (strerror)
// is a transport binding concern, not a substrate engine concern. This type
// only carries String() (the wire form).
type HarnessRejectReason string

const (
	// Pen identity injection (pre-chain, boundPen.Write) — a writer hand-filled
	// env.sender.id / env.channel_id, which are substrate-injected (welded by the
	// pen), not caller-settable. Fail-fast rather than silently overwriting the
	// misuse (feedback_agent_consumer_structural_boundary).
	HarnessIdentityNotCallerSettable HarnessRejectReason = "harness_identity_not_caller_settable"

	// Step 0 — Caller Principal Validation
	HarnessEngineACLDenied HarnessRejectReason = "harness_engine_acl_denied"

	// Step 1 — Envelope Shape Validate
	HarnessEnvelopeFieldMissing      HarnessRejectReason = "harness_envelope_field_missing"
	HarnessChannelMismatch           HarnessRejectReason = "harness_channel_mismatch"
	HarnessKindInvalid               HarnessRejectReason = "harness_kind_invalid"
	HarnessVisibilityInvalid         HarnessRejectReason = "harness_visibility_invalid"
	HarnessAudienceWildcardForbidden HarnessRejectReason = "harness_audience_wildcard_forbidden"
	HarnessResponseMissingParent     HarnessRejectReason = "harness_response_missing_parent"

	// (harness_envelope_unknown_field is gone from this vocabulary: the L0
	// §7.3 unknown-top-level-field fail-closed reject now rides the Envelope
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
	HarnessKindNotAllowedForType   HarnessRejectReason = "harness_kind_not_allowed_for_type"
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
	HarnessTerminalDuplicate               HarnessRejectReason = "harness_terminal_duplicate"
	HarnessProvisionalAfterFinal           HarnessRejectReason = "harness_provisional_after_final"
)

// (No exported AllHarnessRejectReasons enumeration: the const block above IS
// the closed set. A duplicate exported slice is a second, mutable
// representation of a protocol closed set — an importer could append/rewrite
// it — with no substrate consumer. Iterate the wire strings at the transport
// binding boundary that needs them, e.g. a reason→HTTP-status map.)

// String returns the wire form.
func (r HarnessRejectReason) String() string { return string(r) }
