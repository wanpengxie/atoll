package actor

// ReadinessState is the actor readiness closed-set vocabulary. The
// readiness PROJECTION (the per-actor Readiness struct, its normalize
// logic, and the update/transition shapes) is engine state and lives in
// runtime — kernel only owns the vocabulary closed set.
type ReadinessState string

const (
	ReadinessUnknown  ReadinessState = "unknown"
	ReadinessReady    ReadinessState = "ready"
	ReadinessNotReady ReadinessState = "not_ready"
)

// AllReadinessStates enumerates every valid value, in spec order.
var AllReadinessStates = []ReadinessState{ReadinessUnknown, ReadinessReady, ReadinessNotReady}

// String returns the wire form.
func (s ReadinessState) String() string { return string(s) }
