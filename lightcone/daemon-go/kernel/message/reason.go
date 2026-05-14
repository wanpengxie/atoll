package message

// L1 §10.3 partitions reason into 3 closed sets:
//
//   - HarnessRejectReason — synchronous harness reject (HTTP / Err)
//   - InstallReason       — install-time / spawn-time API reject
//   - TerminalFailureReason — system-stamped terminal response reason
//
// Full migration of the enum sets and HTTP-status mapping (currently
// in pkg/v4types.Reason*) lands in T1; this file is the T2 skeleton
// holding the closed sets.

// HarnessRejectReason is the closed set of reasons the Message-Write
// Harness can synchronously reject with (L1 §10.3.1).
type HarnessRejectReason string

// HarnessRejectReason closed set.
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
	HarnessChannelMismatch            HarnessRejectReason = "channel_mismatch"
)

// InstallReason is the closed set of reasons type_registry /
// actor_registry / channel install / worker-spawn APIs can synchronously
// reject with (L1 §10.3.2).
type InstallReason string

// InstallReason closed set.
const (
	InstallAdapterTimeoutMissing         InstallReason = "adapter_timeout_missing"
	InstallFallbackResponseSchemaInvalid InstallReason = "fallback_response_schema_invalid"
	InstallHandlerActorNotRegistered     InstallReason = "handler_actor_not_registered"
	InstallHandlerActorBindingMismatch   InstallReason = "handler_actor_binding_mismatch"
	InstallTypeRegistryInvalid           InstallReason = "type_registry_invalid"
	InstallWorkerLockHeld                InstallReason = "worker_lock_held"
	InstallBootstrapInProgress           InstallReason = "bootstrap_in_progress"
)

// TerminalFailureReason is the closed set of reasons system / framework
// fallback code stamps into a kind=response payload's `reason` field
// (L1 §10.3.3). Observed as normal terminal responses, not RPC errors.
type TerminalFailureReason string

// TerminalFailureReason closed set.
const (
	TerminalUnansweredTimeout      TerminalFailureReason = "unanswered_timeout"
	TerminalAdapterDefaultTimeout  TerminalFailureReason = "adapter_default_timeout"
	TerminalReceiverUnavailable    TerminalFailureReason = "receiver_unavailable"
	TerminalHumanUnansweredTimeout TerminalFailureReason = "human_unanswered_timeout"
)
