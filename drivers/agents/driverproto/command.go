package driverproto

type OpenRequest struct {
	ResumeSeed []byte
}

type ControlKind uint8

const (
	ControlSteer ControlKind = iota
	ControlInterrupt
)

type ControlRequest struct {
	Action  ActionToken
	Kind    ControlKind
	Target  WorkerTurnTarget
	Message *DriverMessage
}
