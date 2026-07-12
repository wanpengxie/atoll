package platform

import "github.com/wanpengxie/atoll/platform/internal/subjectgate"

// frame.go re-exports the subjectgate wire-frame contract at the platform
// boundary (裁决3 / 表⑤ "帧类型住 platform 导出面"): the drivers/gateway伞包 is
// NOT under platform/, so it cannot import platform/internal/subjectgate — the
// concrete lives internal, and these aliases are the ONLY surface a connector or
// the gateway speaks. Pure DTOs over protocol scalars + string + json.RawMessage;
// no schedule/accessdoor type ever crosses the wire.
type (
	Frame     = subjectgate.Frame
	FrameType = subjectgate.FrameType

	AttachPayload      = subjectgate.AttachPayload
	DetachPayload      = subjectgate.DetachPayload
	SubmitPayload      = subjectgate.SubmitPayload
	ResolvePayload     = subjectgate.ResolvePayload
	CancelPayload      = subjectgate.CancelPayload
	AfterPayload       = subjectgate.AfterPayload
	CancelTimerPayload = subjectgate.CancelTimerPayload
	ResourcePayload    = subjectgate.ResourcePayload
	ResourceQuery      = subjectgate.ResourceQuery
	ResourceOp         = subjectgate.ResourceOp
	PresencePayload    = subjectgate.PresencePayload

	FeedPayload        = subjectgate.FeedPayload
	AttachReceipt      = subjectgate.AttachReceipt
	SubmitReceipt      = subjectgate.SubmitReceipt
	ResolveReceipt     = subjectgate.ResolveReceipt
	CancelReceipt      = subjectgate.CancelReceipt
	AfterReceipt       = subjectgate.AfterReceipt
	CancelTimerReceipt = subjectgate.CancelTimerReceipt
	ResourceOutcome    = subjectgate.ResourceOutcome
	ResourceStat       = subjectgate.ResourceStat
	ResourcePage       = subjectgate.ResourcePage
	ErrorPayload       = subjectgate.ErrorPayload
	NotifyPayload      = subjectgate.NotifyPayload
)

// Frame envelope constants + limits.
const (
	FrameVersion  = subjectgate.FrameVersion
	MaxFrameBytes = subjectgate.MaxFrameBytes
)

// Frame type vocabulary (the closed set the connector/gateway dispatch on).
const (
	FrameAttach      = subjectgate.FrameAttach
	FrameDetach      = subjectgate.FrameDetach
	FrameSubmit      = subjectgate.FrameSubmit
	FrameResolve     = subjectgate.FrameResolve
	FrameCancel      = subjectgate.FrameCancel
	FrameAfter       = subjectgate.FrameAfter
	FrameCancelTimer = subjectgate.FrameCancelTimer
	FrameResource    = subjectgate.FrameResource
	FramePresence    = subjectgate.FramePresence
	FrameFeed        = subjectgate.FrameFeed
	FrameReceipt     = subjectgate.FrameReceipt
	FrameError       = subjectgate.FrameError
	FrameNotify      = subjectgate.FrameNotify
)

// Resource op vocabulary.
const (
	ResCreate       = subjectgate.ResCreate
	ResRead         = subjectgate.ResRead
	ResWrite        = subjectgate.ResWrite
	ResDelete       = subjectgate.ResDelete
	ResStat         = subjectgate.ResStat
	ResList         = subjectgate.ResList
	ResShareActor   = subjectgate.ResShareActor
	ResShareMembers = subjectgate.ResShareMembers
)

// Error-code closed set (表①, 裁决8 平面词律).
const (
	CodeBadPayload         = subjectgate.CodeBadPayload
	CodeNotMember          = subjectgate.CodeNotMember
	CodeNotInAudience      = subjectgate.CodeNotInAudience
	CodeUnauthorizedSender = subjectgate.CodeUnauthorizedSender
	CodeAlreadyClosed      = subjectgate.CodeAlreadyClosed
	CodeRequestNotFound    = subjectgate.CodeRequestNotFound
	CodeInvalidDecision    = subjectgate.CodeInvalidDecision
	CodeUnavailable        = subjectgate.CodeUnavailable
	CodeStaleBinding       = subjectgate.CodeStaleBinding
	CodeForbidden          = subjectgate.CodeForbidden
	CodeClosed             = subjectgate.CodeClosed
)

// ParseFrame / NewFrame re-exported so a connector decodes/encodes wire bytes
// without reaching internal.
var (
	ParseFrame = subjectgate.ParseFrame
	NewFrame   = subjectgate.NewFrame
)
