package metatool

import (
	"encoding/json"
	"time"
)

// DefaultTimeout is the caller-side default that bounds the fast-path
// Await WINDOW, NOT the persisted closure deadline.
const DefaultTimeout = 30 * time.Second

// WaitMode selects the caller-side wait policy for one channel request.
type WaitMode int

const (
	// WaitFastPath is the default: Submit + Await(window~15s).
	WaitFastPath WaitMode = iota
	// WaitUnbounded is call_actor(wait=true): Await to the type timeout.
	WaitUnbounded
	// WaitNone is call_actor(wait=false): window 0, immediate ack.
	WaitNone
)

// RequestSpec is the bag of fields a single call_actor invocation needs
// to emit + wait.
type RequestSpec struct {
	ToolName       string
	EnvelopeType   string
	HandlerActorID string
	Payload        json.RawMessage
	Timeout        time.Duration
	WaitMode       WaitMode
}
