package driverproto

type DriverEvent interface{ driverEvent() }

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

type TurnStarted struct{ Target WorkerTurnTarget }
type Activity struct{ Target WorkerTurnTarget }
type Tool struct {
	Target WorkerTurnTarget
	CallID string
	Phase  ToolPhase
	Name   string
	Status ToolStatus
	Detail string
}
type TurnEnded struct {
	Target      WorkerTurnTarget
	Status      TurnEndStatus
	FinalText   string
	ErrorDetail string
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

func (TurnStarted) driverEvent() {}
func (Activity) driverEvent()    {}
func (Tool) driverEvent()        {}
func (TurnEnded) driverEvent()   {}
func (SeedUpdated) driverEvent() {}
func (WorkerEnded) driverEvent() {}
func (Diagnostic) driverEvent()  {}
