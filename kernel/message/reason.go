package message

// Reason is the interface every closed reason set satisfies. It bundles
// the string form (for log / wire), the HTTP status mapping (per L2
// §3.6.1; non-applicable cases return 0), and a stable class tag.
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
// HarnessRejectReason — L1 §10.3.1
// ===========================================================================

// HarnessRejectReason is the closed set of reasons the Message-Write
// Harness can synchronously reject a message write with. Caller receives
// these via the binding-specific transport (HTTP for daemon-RPC adapters
// → L2 §3.6.1 status table; in-process Result.Err for in-worker bus
// adapters → L2 §3.6.2).
type HarnessRejectReason string

// HarnessRejectReason closed set (per L1 §10.3.1 + L2 §3.6.1).
const (
	HarnessAuthFailed                 HarnessRejectReason = "auth_failed"
	HarnessMissingRequiredField       HarnessRejectReason = "missing_required_field"
	HarnessKindInvalid                HarnessRejectReason = "kind_invalid"
	HarnessResponseMissingParentID    HarnessRejectReason = "response_missing_parent_id"
	HarnessSenderMismatch             HarnessRejectReason = "sender_mismatch"
	HarnessSenderKindMismatch         HarnessRejectReason = "sender_kind_mismatch"
	HarnessSenderDeregistered         HarnessRejectReason = "sender_deregistered"
	HarnessUnknownType                HarnessRejectReason = "unknown_type"
	HarnessKindNotAllowed             HarnessRejectReason = "kind_not_allowed"
	HarnessRequestAudienceInvalid     HarnessRejectReason = "request_audience_invalid"
	HarnessAudienceActorNotRegistered HarnessRejectReason = "audience_actor_not_registered"
	HarnessAudienceHandlerMismatch    HarnessRejectReason = "audience_handler_mismatch"
	HarnessPayloadSchemaViolation     HarnessRejectReason = "payload_schema_violation"
	HarnessDocRefsInvalid             HarnessRejectReason = "doc_refs_invalid"
	HarnessResponseParentInvalid      HarnessRejectReason = "response_parent_invalid"
	HarnessTerminalDuplicate          HarnessRejectReason = "terminal_duplicate"
	HarnessWorkerFencingStale         HarnessRejectReason = "worker_fencing_stale"
	HarnessEngineACLDenied            HarnessRejectReason = "engine_acl_denied"
	HarnessMessageIDConflict          HarnessRejectReason = "message_id_conflict"
)

// AllHarnessRejectReasons enumerates every value of the HarnessRejectReason
// closed set, in their L1 §10.3.1 listed order.
var AllHarnessRejectReasons = []HarnessRejectReason{
	HarnessAuthFailed,
	HarnessMissingRequiredField,
	HarnessKindInvalid,
	HarnessResponseMissingParentID,
	HarnessSenderMismatch,
	HarnessSenderKindMismatch,
	HarnessSenderDeregistered,
	HarnessUnknownType,
	HarnessKindNotAllowed,
	HarnessRequestAudienceInvalid,
	HarnessAudienceActorNotRegistered,
	HarnessAudienceHandlerMismatch,
	HarnessPayloadSchemaViolation,
	HarnessDocRefsInvalid,
	HarnessResponseParentInvalid,
	HarnessTerminalDuplicate,
	HarnessWorkerFencingStale,
	HarnessEngineACLDenied,
	HarnessMessageIDConflict,
}

// String returns the wire form of r.
func (r HarnessRejectReason) String() string { return string(r) }

// Class returns "harness_reject" — the L1 §10.3 class tag.
func (r HarnessRejectReason) Class() string { return "harness_reject" }

// HTTPStatus returns the HTTP status code the daemon-RPC binding MUST
// emit when rejecting with this reason. Per L2 §3.6.1 table.
//
// Returns 0 for unknown values (defensive: should be unreachable if the
// caller restricts itself to the named constants).
func (r HarnessRejectReason) HTTPStatus() int {
	switch r {
	case HarnessAuthFailed:
		return 401
	case HarnessSenderMismatch, HarnessSenderKindMismatch, HarnessEngineACLDenied:
		return 403
	case HarnessSenderDeregistered, HarnessWorkerFencingStale:
		return 410
	case HarnessMessageIDConflict, HarnessTerminalDuplicate:
		return 409
	case HarnessMissingRequiredField,
		HarnessKindInvalid,
		HarnessResponseMissingParentID,
		HarnessResponseParentInvalid,
		HarnessUnknownType,
		HarnessKindNotAllowed,
		HarnessRequestAudienceInvalid,
		HarnessAudienceActorNotRegistered,
		HarnessAudienceHandlerMismatch,
		HarnessPayloadSchemaViolation,
		HarnessDocRefsInvalid:
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
	InstallFallbackResponseSchemaInvalid InstallReason = "fallback_response_schema_invalid"
	InstallHandlerActorNotRegistered     InstallReason = "handler_actor_not_registered"
	InstallHandlerActorBindingMismatch   InstallReason = "handler_actor_binding_mismatch"
	InstallTypeRegistryInvalid           InstallReason = "type_registry_invalid"
	InstallWorkerLockHeld                InstallReason = "worker_lock_held"
	InstallBootstrapInProgress           InstallReason = "bootstrap_in_progress"
)

// AllInstallReasons enumerates every value of the InstallReason closed set.
var AllInstallReasons = []InstallReason{
	InstallAdapterTimeoutMissing,
	InstallFallbackResponseSchemaInvalid,
	InstallHandlerActorNotRegistered,
	InstallHandlerActorBindingMismatch,
	InstallTypeRegistryInvalid,
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
		InstallFallbackResponseSchemaInvalid,
		InstallHandlerActorNotRegistered,
		InstallHandlerActorBindingMismatch,
		InstallTypeRegistryInvalid:
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
	TerminalUnansweredTimeout      TerminalFailureReason = "unanswered_timeout"
	TerminalAdapterDefaultTimeout  TerminalFailureReason = "adapter_default_timeout"
	TerminalReceiverUnavailable    TerminalFailureReason = "receiver_unavailable"
	TerminalHumanUnansweredTimeout TerminalFailureReason = "human_unanswered_timeout"
	TerminalAdapterExecutionFailed TerminalFailureReason = "adapter_execution_failed"
	TerminalAdapterPanic           TerminalFailureReason = "adapter_panic"
)

// AllTerminalFailureReasons enumerates every value of the
// TerminalFailureReason closed set.
var AllTerminalFailureReasons = []TerminalFailureReason{
	TerminalUnansweredTimeout,
	TerminalAdapterDefaultTimeout,
	TerminalReceiverUnavailable,
	TerminalHumanUnansweredTimeout,
	TerminalAdapterExecutionFailed,
	TerminalAdapterPanic,
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
