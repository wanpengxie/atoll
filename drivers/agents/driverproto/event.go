package driverproto

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

// Activity 是回合内的活性证据。Stage 是可选的粗粒度阶段读数
// （StageThinking/StageWriting），空值 = 纯心跳。它恒不携带正文片段——
// 过程碎片不进任何投影，阶段台阶才进。
type Activity struct {
	Target WorkerTurnTarget
	Stage  string
}

const (
	StageThinking = "thinking"
	StageWriting  = "writing"
)
type Tool struct {
	Target WorkerTurnTarget
	CallID string
	Phase  ToolPhase
	Name   string
	Status ToolStatus
	Detail string
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
func (Tool) driverEvent()               {}
func (TurnEnded) driverEvent()          {}
func (SeedUpdated) driverEvent()        {}
func (WorkerEnded) driverEvent()        {}
func (Diagnostic) driverEvent()         {}
func (WorkerReady) driverEvent()        {}
func (OpenRejected) driverEvent()       {}
func (SubmissionRejected) driverEvent() {}
func (ControlOutcome) driverEvent()     {}
