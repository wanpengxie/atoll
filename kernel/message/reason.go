package message

// Reason is the interface every closed reason set satisfies. It bundles
// the string form (for log / wire), the HTTP status mapping (per
// impl-layer3 §1.4.1; non-applicable cases return 0), and a stable class tag.
//
// Authoritative spec: L1 §10.3 partitions reason into 3 closed sets
// (harness reject / install / terminal failure); L2 §3.6.1 lays down the
// HTTP status table for binding-specific transports.
type Reason interface {
	String() string
	HTTPStatus() int
	Class() string
}

// ===========================================================================
// HarnessRejectReason — proto-layer1 §2.11.1
// ===========================================================================

// HarnessRejectReason is the closed set of reasons the Message-Write
// Harness can synchronously reject a message write with. Caller receives
// these via the binding-specific transport (HTTP for daemon-RPC adapters
// → L2 §3.6.1 status table; in-process Result.Err for in-worker bus
// adapters → L2 §3.6.2).
type HarnessRejectReason string

// HarnessRejectReason closed set (per proto-layer1 §2.11.1).
const (
	// Step 1 — Fence Check
	HarnessWorkerFencingStale HarnessRejectReason = "harness_worker_fencing_stale"

	// Step 2 — Envelope Shape Validate
	HarnessEnvelopeFieldMissing      HarnessRejectReason = "harness_envelope_field_missing"
	HarnessChannelMismatch           HarnessRejectReason = "harness_channel_mismatch"
	HarnessKindInvalid               HarnessRejectReason = "harness_kind_invalid"
	HarnessVisibilityInvalid         HarnessRejectReason = "harness_visibility_invalid"
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
	HarnessResponseMissingParent             HarnessRejectReason = "harness_response_missing_parent"
	HarnessResponseParentNotFound            HarnessRejectReason = "harness_response_parent_not_found"
	HarnessResponseParentNotRequest          HarnessRejectReason = "harness_response_parent_not_request"
	HarnessResponseStatusInvalid             HarnessRejectReason = "harness_response_status_invalid"
	HarnessResponseStatusNamespaceMismatch   HarnessRejectReason = "harness_response_status_namespace_mismatch"
	HarnessResponseReasonInvalid             HarnessRejectReason = "harness_response_reason_invalid"
	HarnessResponseUnauthorizedSender        HarnessRejectReason = "harness_response_unauthorized_sender"
	HarnessResponseAudienceMismatch          HarnessRejectReason = "harness_response_audience_mismatch"
	HarnessTerminalDuplicate                 HarnessRejectReason = "harness_terminal_duplicate"
	HarnessProvisionalAfterFinal             HarnessRejectReason = "harness_provisional_after_final"

	// Step 0 — Caller Principal Validation (pre-harness)
	HarnessEngineACLDenied HarnessRejectReason = "harness_engine_acl_denied"
)

// Step 8 in this version covers Terminal Uniqueness + Response Parent
// Validation (formerly Step 9). The Payload Schema step (formerly Step
// 8) was removed when the protocol moved to Level A
// (proto-layer0 §1.4.1): payload is opaque to the protocol layer, so
// the harness no longer validates payload schemas. The reasons
// `harness_schema_missing` and `harness_payload_schema_invalid` are no
// longer part of the closed set.

// AllHarnessRejectReasons enumerates every value of the HarnessRejectReason
// closed set, in their proto-layer1 §2.11.1 listed order.
var AllHarnessRejectReasons = []HarnessRejectReason{
	HarnessWorkerFencingStale,
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
	HarnessSenderDeregistered,
	HarnessSenderKindMismatch,
	HarnessSenderMismatch,
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
	HarnessEngineACLDenied,
}

// String returns the wire form of r.
func (r HarnessRejectReason) String() string { return string(r) }

// Class returns "harness_reject" — the proto-layer1 §2.11.1 class tag.
func (r HarnessRejectReason) Class() string { return "harness_reject" }

// HTTPStatus returns the HTTP status code the daemon-RPC binding MUST
// emit when rejecting with this reason. Per impl-layer3 §1.4.1 table.
//
// Returns 0 for unknown values (defensive: should be unreachable if the
// caller restricts itself to the named constants).
func (r HarnessRejectReason) HTTPStatus() int {
	switch r {
	case HarnessSenderMismatch,
		HarnessAudienceMemberNotActive,
		HarnessAudienceHandlerMismatch,
		HarnessResponseUnauthorizedSender,
		HarnessReservedTypeUnauthorizedSender:
		return 403
	case HarnessEngineACLDenied:
		return 500
	case HarnessSenderDeregistered, HarnessWorkerFencingStale:
		return 410
	case HarnessIDDuplicateConflict, HarnessTerminalDuplicate, HarnessProvisionalAfterFinal:
		return 409
	case HarnessEnvelopeFieldMissing,
		HarnessChannelMismatch,
		HarnessKindInvalid,
		HarnessVisibilityInvalid,
		HarnessVisibilityAudienceInvalid,
		HarnessEnvelopeUnknownField,
		HarnessAudienceWildcardForbidden,
		HarnessAudienceEmpty,
		HarnessSenderKindMismatch,
		HarnessResponseAudienceInvalid,
		HarnessResponseMissingParent,
		HarnessResponseParentNotFound,
		HarnessResponseParentNotRequest,
		HarnessResponseStatusInvalid,
		HarnessResponseStatusNamespaceMismatch,
		HarnessTypeUnknown,
		HarnessKindNotAllowedForType,
		HarnessRequestAudienceInvalid,
		HarnessResponseAudienceMismatch,
		HarnessResponseReasonInvalid,
		HarnessTimeInvalid:
		return 400
	}
	return 0
}

// ===========================================================================
// InstallReason — L1 §10.3.2
// ===========================================================================

// InstallReason is the closed set of reasons the type_registry /
// actor_registry / channel install / worker-spawn APIs can synchronously
// reject with. Surfaced through the same binding transport as harness
// reject (HTTP for daemon-RPC, Result.Err for in-worker bus).
type InstallReason string

// InstallReason closed set (per L1 §10.3.2 + L2 §3.6.1 install-time table).
const (
	InstallAdapterTimeoutMissing         InstallReason = "adapter_timeout_missing"
	InstallHandlerActorNotRegistered     InstallReason = "handler_actor_not_registered"
	InstallHandlerActorBindingMismatch   InstallReason = "handler_actor_binding_mismatch"
	InstallTypeRegistryInvalid           InstallReason = "type_registry_invalid"
	InstallTypeRegistryReservedNamespace InstallReason = "type_registry_reserved_namespace"
	InstallWorkerLockHeld                InstallReason = "worker_lock_held"
	InstallBootstrapInProgress           InstallReason = "bootstrap_in_progress"
)

// AllInstallReasons enumerates every value of the InstallReason closed set.
var AllInstallReasons = []InstallReason{
	InstallAdapterTimeoutMissing,
	InstallHandlerActorNotRegistered,
	InstallHandlerActorBindingMismatch,
	InstallTypeRegistryInvalid,
	InstallTypeRegistryReservedNamespace,
	InstallWorkerLockHeld,
	InstallBootstrapInProgress,
}

// String returns the wire form of r.
func (r InstallReason) String() string { return string(r) }

// Class returns "install" — the L1 §10.3 class tag.
func (r InstallReason) Class() string { return "install" }

// HTTPStatus returns the HTTP status the daemon-RPC binding MUST emit
// when an install API rejects with this reason. Per L2 §3.6.1
// install-time + spawn-API tables.
//
// Returns 0 for unknown values.
func (r InstallReason) HTTPStatus() int {
	switch r {
	case InstallWorkerLockHeld, InstallBootstrapInProgress:
		return 409
	case InstallAdapterTimeoutMissing,
		InstallHandlerActorNotRegistered,
		InstallHandlerActorBindingMismatch,
		InstallTypeRegistryInvalid,
		InstallTypeRegistryReservedNamespace:
		return 400
	}
	return 0
}

// ===========================================================================
// TerminalFailureReason — L1 §10.3.3
// ===========================================================================

// TerminalFailureReason is the closed set of reasons system / framework
// fallback code stamps into a kind=response payload's `reason` field.
// These are NOT rejects — callers observe them as normal terminal
// responses, not as RPC errors (L1 §10.3.3 + L1 §6.4 long-pending
// scheduler tie-in).
type TerminalFailureReason string

// TerminalFailureReason closed set (per L1 §10.3.3).
const (
	TerminalUnansweredTimeout     TerminalFailureReason = "unanswered_timeout"
	TerminalReceiverInternalError TerminalFailureReason = "receiver_internal_error"
	TerminalReceiverUnavailable   TerminalFailureReason = "receiver_unavailable"
)

// AllTerminalFailureReasons enumerates every value of the
// TerminalFailureReason closed set.
var AllTerminalFailureReasons = []TerminalFailureReason{
	TerminalUnansweredTimeout,
	TerminalReceiverInternalError,
	TerminalReceiverUnavailable,
}

// String returns the wire form of r.
func (r TerminalFailureReason) String() string { return string(r) }

// Class returns "terminal_failure" — the L1 §10.3 class tag.
func (r TerminalFailureReason) Class() string { return "terminal_failure" }

// HTTPStatus is 0 for every TerminalFailureReason. These reasons never
// travel as an HTTP reject — they sit inside the `payload.reason` field
// of an otherwise successful kind=response message (L1 §10.3.3). The
// method satisfies the Reason interface for symmetry.
func (r TerminalFailureReason) HTTPStatus() int { return 0 }
