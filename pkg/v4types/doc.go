// Package v4types is the M1.5 outward-facing re-export of the kernel/
// types most callers need (envelope schema, kind / sender_kind /
// visibility enums, the three closed reason sets).
//
// All declarations are Go type aliases / value re-exports — there is
// **zero runtime cost** to importing v4types vs kernel/message, and
// every method set / interface satisfaction is preserved.
//
// Why this package exists:
//
//   - Historical: lightcone/daemon-go/pkg/v4types was the original home
//     of these types. The M1.5 reorg moved the source of truth into
//     kernel/message (.dalek/pm/m1.5-tickets.md §T1).
//   - Current: external callers (cmd/, server/, runtime/, adapters/, ui)
//     import pkg/v4types so they don't need to thread through
//     kernel/message paths. T2 go-arch-lint allows pkg/ → kernel/.
//   - Future: when an envelope shape change happens (M1.x L0 revision),
//     pkg/v4types stays unchanged — only kernel/message moves. Callers
//     keep their import paths.
//
// Drift discipline: this file MUST stay value-equal to kernel/message's
// public surface. Add a re-export here when kernel/message exposes a
// new public symbol that downstream packages need; never define new
// types here directly.
package v4types

import "github.com/coagent-ai/coagent/kernel/message"

// ===========================================================================
// Envelope + Sender (L0 §2.1)
// ===========================================================================

// Envelope is re-exported from kernel/message. See kernel/message/envelope.go
// for the full schema doc — 17 content fields, 4 delivery metadata, 2
// store-derived columns.
type Envelope = message.Envelope

// Sender is the nested `sender` struct on Envelope. Re-exported alias.
type Sender = message.Sender

// ===========================================================================
// Kind / SenderKind / Visibility enums (L0 §2.3 / §2.4 / §3.1 I7)
// ===========================================================================

// Kind is the message ADT classifier (event / request / response).
type Kind = message.Kind

const (
	KindEvent    = message.KindEvent
	KindRequest  = message.KindRequest
	KindResponse = message.KindResponse
)

// AllKinds re-export.
var AllKinds = message.AllKinds

// SenderKind is the actor physical-position classifier.
type SenderKind = message.SenderKind

const (
	SenderHuman  = message.SenderHuman
	SenderAgent  = message.SenderAgent
	SenderSystem = message.SenderSystem
	SenderTool   = message.SenderTool
)

// AllSenderKinds re-export.
var AllSenderKinds = message.AllSenderKinds

// Visibility is the envelope `visibility` field.
type Visibility = message.Visibility

const (
	VisibilityPublic  = message.VisibilityPublic
	VisibilityPrivate = message.VisibilityPrivate
	VisibilitySystem  = message.VisibilitySystem
)

// AllVisibilities re-export.
var AllVisibilities = message.AllVisibilities

// HashInputFields re-export — the 14 keys feeding CanonicalHash per L2
// §1.4.10.2.
var HashInputFields = message.HashInputFields

// ===========================================================================
// Reason closed sets (L1 §10.3)
// ===========================================================================

// Reason — the interface every closed reason set satisfies.
type Reason = message.Reason

// HarnessRejectReason — the 20-value closed set.
type HarnessRejectReason = message.HarnessRejectReason

const (
	HarnessAuthFailed                 = message.HarnessAuthFailed
	HarnessMissingRequiredField       = message.HarnessMissingRequiredField
	HarnessKindInvalid                = message.HarnessKindInvalid
	HarnessResponseMissingParentID    = message.HarnessResponseMissingParentID
	HarnessSenderMismatch             = message.HarnessSenderMismatch
	HarnessSenderKindMismatch         = message.HarnessSenderKindMismatch
	HarnessSenderDeregistered         = message.HarnessSenderDeregistered
	HarnessUnknownType                = message.HarnessUnknownType
	HarnessKindNotAllowed             = message.HarnessKindNotAllowed
	HarnessRequestAudienceInvalid     = message.HarnessRequestAudienceInvalid
	HarnessAudienceActorNotRegistered = message.HarnessAudienceActorNotRegistered
	HarnessAudienceHandlerMismatch    = message.HarnessAudienceHandlerMismatch
	HarnessPayloadSchemaViolation     = message.HarnessPayloadSchemaViolation
	HarnessDocRefsInvalid             = message.HarnessDocRefsInvalid
	HarnessResponseParentInvalid      = message.HarnessResponseParentInvalid
	HarnessTerminalDuplicate          = message.HarnessTerminalDuplicate
	HarnessWorkerFencingStale         = message.HarnessWorkerFencingStale
	HarnessEngineACLDenied            = message.HarnessEngineACLDenied
	HarnessMessageIDConflict          = message.HarnessMessageIDConflict
	HarnessChannelMismatch            = message.HarnessChannelMismatch
)

// AllHarnessRejectReasons re-export.
var AllHarnessRejectReasons = message.AllHarnessRejectReasons

// InstallReason — the 7-value install-time closed set.
type InstallReason = message.InstallReason

const (
	InstallAdapterTimeoutMissing         = message.InstallAdapterTimeoutMissing
	InstallFallbackResponseSchemaInvalid = message.InstallFallbackResponseSchemaInvalid
	InstallHandlerActorNotRegistered     = message.InstallHandlerActorNotRegistered
	InstallHandlerActorBindingMismatch   = message.InstallHandlerActorBindingMismatch
	InstallTypeRegistryInvalid           = message.InstallTypeRegistryInvalid
	InstallWorkerLockHeld                = message.InstallWorkerLockHeld
	InstallBootstrapInProgress           = message.InstallBootstrapInProgress
)

// AllInstallReasons re-export.
var AllInstallReasons = message.AllInstallReasons

// TerminalFailureReason — the 4-value terminal-payload closed set.
type TerminalFailureReason = message.TerminalFailureReason

const (
	TerminalUnansweredTimeout      = message.TerminalUnansweredTimeout
	TerminalAdapterDefaultTimeout  = message.TerminalAdapterDefaultTimeout
	TerminalReceiverUnavailable    = message.TerminalReceiverUnavailable
	TerminalHumanUnansweredTimeout = message.TerminalHumanUnansweredTimeout
)

// AllTerminalFailureReasons re-export.
var AllTerminalFailureReasons = message.AllTerminalFailureReasons

// ===========================================================================
// Canonical hash (L2 §1.4.10.2)
// ===========================================================================

// CanonicalHash re-exports message.CanonicalHash — same algorithm, same
// hex-lowercase 64-char SHA-256 output.
func CanonicalHash(e Envelope) (string, error) {
	return message.CanonicalHash(e)
}

// CanonicalHashPayload re-exports message.CanonicalHashPayload.
func CanonicalHashPayload(payload []byte) (string, error) {
	return message.CanonicalHashPayload(payload)
}

// CanonicalizeJSON re-exports message.CanonicalizeJSON.
func CanonicalizeJSON(raw []byte) ([]byte, error) {
	return message.CanonicalizeJSON(raw)
}
