package driverproto

import "encoding/json"

type DriverEvent interface{ driverEvent() }

type WorkerDisposition uint8

const (
	KeepWorker WorkerDisposition = iota
	RetireWorker
)

type FailureClass string

const (
	FailureInvalidInput  FailureClass = "invalid_input"
	FailureOverloaded    FailureClass = "overloaded"
	FailureProvider      FailureClass = "provider"
	FailureTransport     FailureClass = "transport"
	FailureResumeInvalid FailureClass = "resume_invalid"
)

type TurnEndStatus uint8

const (
	TurnOK TurnEndStatus = iota
	TurnFailed
	TurnInterrupted
)

type WorkerEndCause uint8

const (
	WorkerCrash WorkerEndCause = iota
	WorkerTransportEnded
	WorkerProtocolFault
)

type ToolPhase uint8

const (
	ToolStarted ToolPhase = iota
	ToolEnded
)

type ToolStatus uint8

const (
	ToolStatusUnknown ToolStatus = iota
	ToolStatusCompleted
	ToolStatusFailed
)

type DiagnosticLevel uint8

const (
	DiagnosticDebug DiagnosticLevel = iota
	DiagnosticInfo
	DiagnosticWarn
	DiagnosticError
)

type ControlVerdict uint8

const (
	ControlAccepted ControlVerdict = iota
	ControlRejected
	ControlNotSteerable
	ControlTargetGone
)

type WorkerReady struct{}
type OpenRejected struct {
	Class       FailureClass
	Detail      string
	Disposition WorkerDisposition
}
type SubmissionRejected struct {
	Attempt     AttemptToken
	Class       FailureClass
	Detail      string
	Disposition WorkerDisposition
}
type ControlOutcome struct {
	Action      ActionToken
	Target      WorkerTurnTarget
	Verdict     ControlVerdict
	Detail      string
	Disposition WorkerDisposition
}

type TurnStarted struct{ Target WorkerTurnTarget }
type Activity struct{ Target WorkerTurnTarget }

// ProgressNote 是回合内一件"已完成的中间产物"的截断摘要。Kind 是 provider
// 无关的统一词表（下方常量），各 provider 把自家 wire 的条目映射填充进来；
// 阶段感（思考中/正在写……）由前端从 note 流自行推断，后端不做阶段机制。
// 判据是完成性：delta（逐 token 碎片）恒不在此列。它与 Tool 同为可丢弃的
// 过程观测，拥塞时丢，恒不因它拉闸。
type ProgressNote struct {
	Target WorkerTurnTarget
	Kind   string
	Text   string
}

const (
	NoteThinking = "thinking" // 思考/推理小结（codex reasoning、claude thinking）
	NotePlan     = "plan"     // 计划条目
	NoteText     = "text"     // 中间正文块
)

type Tool struct {
	Target WorkerTurnTarget
	CallID string
	Phase  ToolPhase
	Name   string
	Status ToolStatus
	Detail string
	Input  json.RawMessage
	Output json.RawMessage
}
type TurnUsage struct {
	ContextTokens int64
	ContextWindow int64
	Model         string
	Effort        string
}
type TurnEnded struct {
	Target      WorkerTurnTarget
	Status      TurnEndStatus
	FinalText   string
	ErrorDetail string
	Usage       TurnUsage
}
type SeedUpdated struct{ Value []byte }
type WorkerEnded struct {
	Cause  WorkerEndCause
	Detail string
}
type Diagnostic struct {
	Level  DiagnosticLevel
	Code   string
	Detail string
}

func (TurnStarted) driverEvent()        {}
func (Activity) driverEvent()           {}
func (ProgressNote) driverEvent()       {}
func (Tool) driverEvent()               {}
func (TurnEnded) driverEvent()          {}
func (SeedUpdated) driverEvent()        {}
func (WorkerEnded) driverEvent()        {}
func (Diagnostic) driverEvent()         {}
func (WorkerReady) driverEvent()        {}
func (OpenRejected) driverEvent()       {}
func (SubmissionRejected) driverEvent() {}
func (ControlOutcome) driverEvent()     {}
