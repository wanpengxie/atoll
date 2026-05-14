// Package harness defines the Message-Write Harness contract — the
// 9-step normalize+validate chain that every channel-truth write goes
// through (L1 §0.1 / §10.2). It does not implement the chain (that
// lives in runtime/harness — T3); it only declares the interfaces +
// re-exports the harness-reject closed reason set.
package harness

import "github.com/coagent-ai/coagent/kernel/message"

// RejectReason is re-exported from kernel/message so callers can refer
// to it as `harness.RejectReason` without crossing into kernel/message
// for the closed-set type.
type RejectReason = message.HarnessRejectReason

// Re-exports for the closed set values — keep symbol-for-symbol with
// kernel/message/reason.go for ergonomic lookup.
const (
	RejectAuthFailed                 = message.HarnessAuthFailed
	RejectMissingRequiredField       = message.HarnessMissingRequiredField
	RejectKindInvalid                = message.HarnessKindInvalid
	RejectResponseMissingParentID    = message.HarnessResponseMissingParentID
	RejectSenderMismatch             = message.HarnessSenderMismatch
	RejectSenderKindMismatch         = message.HarnessSenderKindMismatch
	RejectSenderDeregistered         = message.HarnessSenderDeregistered
	RejectUnknownType                = message.HarnessUnknownType
	RejectKindNotAllowed             = message.HarnessKindNotAllowed
	RejectRequestAudienceInvalid     = message.HarnessRequestAudienceInvalid
	RejectAudienceActorNotRegistered = message.HarnessAudienceActorNotRegistered
	RejectAudienceHandlerMismatch    = message.HarnessAudienceHandlerMismatch
	RejectPayloadSchemaViolation     = message.HarnessPayloadSchemaViolation
	RejectDocRefsInvalid             = message.HarnessDocRefsInvalid
	RejectResponseParentInvalid      = message.HarnessResponseParentInvalid
	RejectTerminalDuplicate          = message.HarnessTerminalDuplicate
	RejectWorkerFencingStale         = message.HarnessWorkerFencingStale
	RejectEngineACLDenied            = message.HarnessEngineACLDenied
	RejectMessageIDConflict          = message.HarnessMessageIDConflict
	RejectChannelMismatch            = message.HarnessChannelMismatch
)

// AllRejectReasons is re-exported from kernel/message.
var AllRejectReasons = message.AllHarnessRejectReasons
