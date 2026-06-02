package harness

// HarnessRejectReason is the closed set of reasons the 9-step Message-Write
// Harness can synchronously reject a write with. This is the WRITE ENGINE's
// errno vocabulary (Erlang/Unix prior-art, kernel-construction-spec §0.1):
// it co-evolves with the harness steps, so it lives with the engine, not in
// the kernel ADT layer.
//
// Relocated from the deleted kernel/message (which held 32 values including
// HarnessWorkerFencingStale). v2 drops HarnessWorkerFencingStale — the
// channel-write fence is obsolete under single-server-harness-writer
// (runtime-construction-spec §4.1) — leaving 31.
//
// No HTTPStatus() method: reason→HTTP-status (strerror) is a binding concern
// that lives in server/gateway, not on this engine type. This type only
// carries String() (the wire form).
type HarnessRejectReason string

const (
	// Step 0 — Caller Principal Validation
	HarnessEngineACLDenied HarnessRejectReason = "harness_engine_acl_denied"

	// Step 2 — Envelope Shape Validate
	HarnessEnvelopeFieldMissing HarnessRejectReason = "harness_envelope_field_missing"
	HarnessChannelMismatch      HarnessRejectReason = "harness_channel_mismatch"
	HarnessKindInvalid          HarnessRejectReason = "harness_kind_invalid"
	HarnessVisibilityInvalid    HarnessRejectReason = "harness_visibility_invalid"
	// HarnessVisibilityAudienceInvalid is intentionally unreachable: the
	// visibility-scoped audience wildcard it guarded was removed (proto-layer1
	// §738 "历史保留；当前 wildcard 已移除，正常不再产生"). Kept in the closed
	// set as a tombstone for wire/log back-compat; emitted by no step. Whether
	// to drop it (count 31→30) is a reason-set decision, deferred.
	HarnessVisibilityAudienceInvalid HarnessRejectReason = "harness_visibility_audience_invalid"
	HarnessEnvelopeUnknownField      HarnessRejectReason = "harness_envelope_unknown_field"
	HarnessAudienceWildcardForbidden HarnessRejectReason = "harness_audience_wildcard_forbidden"

	// Step 3 — Id Dedupe
	HarnessIDDuplicateConflict HarnessRejectReason = "harness_id_duplicate_conflict"

	// Step 4 — Normalize (time-relation guard)
	HarnessTimeInvalid HarnessRejectReason = "harness_time_invalid"

	// Step 5 — Type / Kind Validate
	HarnessTypeUnknown                    HarnessRejectReason = "harness_type_unknown"
	HarnessKindNotAllowedForType          HarnessRejectReason = "harness_kind_not_allowed_for_type"
	HarnessReservedTypeUnauthorizedSender HarnessRejectReason = "harness_reserved_type_unauthorized_sender"

	// Step 6 — Sender Validate
	HarnessSenderMismatch     HarnessRejectReason = "harness_sender_mismatch"
	HarnessSenderKindMismatch HarnessRejectReason = "harness_sender_kind_mismatch"
	HarnessSenderDeregistered HarnessRejectReason = "harness_sender_deregistered"

	// Step 7 — Audience Validate
	HarnessAudienceEmpty           HarnessRejectReason = "harness_audience_empty"
	HarnessAudienceMemberNotActive HarnessRejectReason = "harness_audience_member_not_active"
	HarnessRequestAudienceInvalid  HarnessRejectReason = "harness_request_audience_invalid"
	HarnessResponseAudienceInvalid HarnessRejectReason = "harness_response_audience_invalid"
	HarnessAudienceHandlerMismatch HarnessRejectReason = "harness_audience_handler_mismatch"

	// Step 8 — Terminal Uniqueness + Response Parent Validation
	HarnessResponseMissingParent           HarnessRejectReason = "harness_response_missing_parent"
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

// AllHarnessRejectReasons enumerates every value (31 — v2, no fencing).
var AllHarnessRejectReasons = []HarnessRejectReason{
	HarnessEngineACLDenied,
	HarnessEnvelopeFieldMissing,
	HarnessChannelMismatch,
	HarnessKindInvalid,
	HarnessVisibilityInvalid,
	HarnessVisibilityAudienceInvalid,
	HarnessEnvelopeUnknownField,
	HarnessAudienceWildcardForbidden,
	HarnessIDDuplicateConflict,
	HarnessTimeInvalid,
	HarnessTypeUnknown,
	HarnessKindNotAllowedForType,
	HarnessReservedTypeUnauthorizedSender,
	HarnessSenderMismatch,
	HarnessSenderKindMismatch,
	HarnessSenderDeregistered,
	HarnessAudienceEmpty,
	HarnessAudienceMemberNotActive,
	HarnessRequestAudienceInvalid,
	HarnessResponseAudienceInvalid,
	HarnessAudienceHandlerMismatch,
	HarnessResponseMissingParent,
	HarnessResponseParentNotFound,
	HarnessResponseParentNotRequest,
	HarnessResponseStatusInvalid,
	HarnessResponseStatusNamespaceMismatch,
	HarnessResponseReasonInvalid,
	HarnessResponseUnauthorizedSender,
	HarnessResponseAudienceMismatch,
	HarnessTerminalDuplicate,
	HarnessProvisionalAfterFinal,
}

// String returns the wire form.
func (r HarnessRejectReason) String() string { return string(r) }
